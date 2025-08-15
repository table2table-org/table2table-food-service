package com.table2table.foodservice.config;

import com.table2table.security.events.FoodRequestEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, FoodRequestEvent> foodReqConsumerFactory() {
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "food-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);              // disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new JsonDeserializer<>(FoodRequestEvent.class)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FoodRequestEvent> foodReqListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, FoodRequestEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(foodReqConsumerFactory());

        // 1) Manual ack mode so Acknowledgment is injected
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 2) Configure error handler with retry + DLQ
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
        // never retry on bad data
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        // 3) Concurrency matches your partition count
        factory.setConcurrency(3);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, FoodRequestEvent> foodRestoreConsumerFactory() {
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "food-service-restore");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);              // disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new JsonDeserializer<>(FoodRequestEvent.class)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FoodRequestEvent> foodRestoreListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, FoodRequestEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(foodRestoreConsumerFactory());

        // 1) Manual ack mode so Acknowledgment is injected
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // No retries, immediate commit on any exception
        DefaultErrorHandler noRetryErrorHandler = new DefaultErrorHandler(
                // BackOff with maxAttempts = 0 means no retry
                new FixedBackOff(0L, 0L)
        );
        factory.setCommonErrorHandler(noRetryErrorHandler);

        factory.setConcurrency(1);

        return factory;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // enable idempotence & transactions if desired
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(props);

        // if you use executeInTransaction(), set a transaction id prefix:
        factory.setTransactionIdPrefix("food-service-tx-");

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // If you need DLQ topics auto-created, you can also add:
    @Bean
    public NewTopic foodRequestEventsDlq() {
        return TopicBuilder.name("food-request-events-dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic foodRestoreEventsDlq() {
        return TopicBuilder.name("food-inventory-restore-events-dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
