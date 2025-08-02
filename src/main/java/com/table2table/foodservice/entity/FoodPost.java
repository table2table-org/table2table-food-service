package com.table2table.foodservice.entity;

import com.table2table.foodservice.dto.enums.FoodStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@Entity
@Table(name = "food_posts")
public class FoodPost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long foodId;
    private Long cookId;
    private String title;
    private String description;
    private Double price;
    private Integer quantity;
    private LocalDateTime expiresAt;
    private LocalDateTime createdAt;
    private LocalDateTime lastUpdatedAt;
    private Long communityId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private FoodStatus status; // ACTIVE, EXPIRED, CLAIMED

    @Column(name = "image_url")
    private String imageUrl;


}
