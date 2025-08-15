package com.table2table.foodservice.service.eventhandler;

import com.table2table.foodservice.dto.FoodPostRequest;
import com.table2table.foodservice.dto.enums.FoodStatus;
import com.table2table.foodservice.service.FoodPostService;
import com.table2table.security.events.FoodRequestEvent;
import com.table2table.security.events.OrderStatusEvent;
import com.table2table.security.events.PaymentEvent;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class FoodEventHandler {

    @Autowired
    private FoodPostService foodService;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Enhanced food request handler with comprehensive error handling
     * Processes inventory checks and updates, publishes order status events
     */
    @KafkaListener(topics = "food-request-events", groupId = "food-service", containerFactory = "foodReqListenerContainerFactory")
    public void onFoodRequest(
            @Payload FoodRequestEvent evt,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            log.info("Processing FoodRequestEvent: requestId={}, foodPostId={}, requestedQty={}, availableQty={}, partition={}, offset={}",
                    evt.getFoodRequestId(), evt.getFoodPostId(), evt.getRequestedQuantity(),
                    evt.getAvailableQuantity(), partition, offset);

            // Validate event data
            if (!evt.isValid()) {
                log.error("Invalid FoodRequestEvent: {}", evt);
                sendToDlq("food-request-events-dlq", evt, "Invalid event data");
                acknowledgment.acknowledge();
                return;
            }

            // Check for sufficient quantity
            if (evt.getRequestedQuantity() > evt.getAvailableQuantity()) {
                log.info("Insufficient quantity for request: {} requested, {} available",
                        evt.getRequestedQuantity(), evt.getAvailableQuantity());
                rejectOrder(evt.getFoodRequestId(), evt.getAuthHeader(), "Insufficient quantity");
                acknowledgment.acknowledge();
                return;
            }

            // Process inventory update
            processInventoryUpdate(evt);

            // Determine order status based on remaining inventory
            String orderStatus = determineOrderStatus(evt);

            // Publish order status update
            publishOrderStatusUpdate(evt.getFoodRequestId(), orderStatus, null, evt.getAuthHeader());

            log.info("Successfully processed FoodRequestEvent: requestId={}, newStatus={}",
                    evt.getFoodRequestId(), orderStatus);

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing FoodRequestEvent: {}", evt, e);
            sendToDlq("food-request-events-dlq", evt, "Processing error: " + e.getMessage());
            acknowledgment.acknowledge();
        }
    }


    /**
     * Handles inventory restoration when orders are rejected by cooks
     */
    @KafkaListener(topics = "food-inventory-restore-events", groupId = "food-service-restore",
            containerFactory = "foodRestoreListenerContainerFactory")
    public void onInventoryRestore(
            @Payload FoodRequestEvent evt,
            Acknowledgment acknowledgment, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            Acknowledgment ack) {

        try {
            // Only process if this is a restoration event (status = REFUND_INITIATED)
            log.info("Restore listener invoked on topic={} group={}", topic, groupId);
            if ("REFUND_INITIATED".equals(evt.getStatus())) {
                log.info("Restoring inventory for rejected request: requestId={}, quantity={}",
                        evt.getFoodRequestId(), evt.getRequestedQuantity());

                // Restore the inventory
                FoodPostRequest restoreRequest = new FoodPostRequest();
                restoreRequest.setQuantity(evt.getAvailableQuantity() + evt.getRequestedQuantity());
                restoreRequest.setStatus(FoodStatus.AVAILABLE);

                foodService.updateQuantity(evt.getFoodPostId(), restoreRequest);

                log.info("Successfully restored inventory: requestId={}, restoredQty={}",
                        evt.getFoodRequestId(), evt.getRequestedQuantity());
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error restoring inventory for FoodRequestEvent: {}", evt, e);
            acknowledgment.acknowledge();
        }
    }

    /**
     * Dead Letter Queue handler for failed food request events
     */
    @KafkaListener(topics = "food-request-events-dlq", groupId = "food-service-dlq",containerFactory = "foodReqListenerContainerFactory")
    public void handleFoodRequestDlq(
            @Payload FoodRequestEvent evt,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.warn("Received failed FoodRequestEvent in DLQ: requestId={}, foodPostId={}, partition={}, offset={}",
                evt.getFoodRequestId(), evt.getFoodPostId(), partition, offset);

        // Alert operations team or implement retry logic here
        acknowledgment.acknowledge();
    }

    /**
     * Process inventory update based on the food request
     */
    private void processInventoryUpdate(FoodRequestEvent evt) {
        try {
            int remainingQuantity = evt.getAvailableQuantity() - evt.getRequestedQuantity();

            FoodPostRequest updateRequest = new FoodPostRequest();
            updateRequest.setQuantity(remainingQuantity);

            // Set status based on remaining quantity
            if (remainingQuantity == 0) {
                updateRequest.setStatus(FoodStatus.SOLD_OUT);
            } else {
                updateRequest.setStatus(FoodStatus.AVAILABLE);
            }

            foodService.updateQuantity(evt.getFoodPostId(), updateRequest);

            log.info("Updated inventory: foodPostId={}, newQuantity={}, status={}",
                    evt.getFoodPostId(), remainingQuantity, updateRequest.getStatus());

        } catch (Exception e) {
            log.error("Failed to update inventory: foodPostId={}", evt.getFoodPostId(), e);
            throw e;
        }
    }

    /**
     * Determine order status based on inventory state
     */
    private String determineOrderStatus(FoodRequestEvent evt) {
        int remainingQuantity = evt.getAvailableQuantity() - evt.getRequestedQuantity();
        return remainingQuantity == 0 ? "RESERVED_SOLDOUT" : "RESERVED";
    }

    /**
     * Publish order status update event
     */
    private void publishOrderStatusUpdate(Long requestId, String status, String reason, String authHeader) {
        try {
            OrderStatusEvent statusEvent = new OrderStatusEvent(
                    requestId,
                    status,
                    reason,
                    LocalDateTime.now(),
                    authHeader
            );

            kafkaTemplate.executeInTransaction(ops -> {
                ops.send("order-status-events", statusEvent);
                return true;
            });

            log.info("Published OrderStatusEvent: requestId={}, status={}", requestId, status);

        } catch (Exception e) {
            log.error("Failed to publish OrderStatusEvent: requestId={}, status={}", requestId, status, e);
            throw e;
        }
    }

    /**
     * Reject order due to insufficient inventory
     */
    private void rejectOrder(Long requestId, String authHeader, String reason) {
        publishOrderStatusUpdate(requestId, "REJECTED", reason, authHeader);
        log.info("Published rejection for request: {}, reason: {}", requestId, reason);
    }

    /**
     * Send failed events to Dead Letter Queue
     */
    private void sendToDlq(String dlqTopic, FoodRequestEvent event, String reason) {
        try {
            kafkaTemplate.send(dlqTopic, event);  // no executeInTransaction
            log.info("Sent FoodRequestEvent to DLQ: topic={}, requestId={}, reason={}",
                    dlqTopic, event.getFoodRequestId(), reason);
        } catch (Exception e) {
            log.error("Failed to send FoodRequestEvent to DLQ: topic={}, event={}", dlqTopic, event, e);
        }
    }

}