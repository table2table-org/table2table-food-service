package com.table2table.foodservice.repository;


import com.table2table.foodservice.entity.FoodPost;
import com.table2table.security.dto.UserResponseDto;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;

public interface FoodPostRepository extends JpaRepository<FoodPost, Long>, JpaSpecificationExecutor<FoodPost> {
}

