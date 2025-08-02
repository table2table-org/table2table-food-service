package com.table2table.foodservice.util;



import com.table2table.foodservice.dto.FoodPostResponse;
import com.table2table.foodservice.entity.FoodPost;

import java.util.List;
import java.util.stream.Collectors;

public class FoodManagementUtil {


    public static FoodPostResponse convertToFoodPostDto(FoodPost foodPost) {
        FoodPostResponse dto = new FoodPostResponse();
        dto.setId(foodPost.getFoodId());
        dto.setTitle(foodPost.getTitle());
        dto.setDescription(foodPost.getDescription());
        dto.setPrice(foodPost.getPrice());
        dto.setQuantity(foodPost.getQuantity());
        dto.setExpiresAt(foodPost.getExpiresAt());
        dto.setImageUrl(foodPost.getImageUrl());
        dto.setStatus(foodPost.getStatus());
        dto.setPostedAt(foodPost.getCreatedAt());
        dto.setCommunityId(foodPost.getCommunityId());
        dto.setCookId(foodPost.getCookId());
        return dto;
    }

}
