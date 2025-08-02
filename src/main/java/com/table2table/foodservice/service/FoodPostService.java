package com.table2table.foodservice.service;


import com.table2table.foodservice.dto.FoodPostFilterRequest;
import com.table2table.foodservice.dto.FoodPostRequest;
import com.table2table.foodservice.dto.FoodPostResponse;
import com.table2table.foodservice.entity.FoodPost;
import com.table2table.foodservice.repository.FoodPostRepository;
import com.table2table.foodservice.util.FoodManagementUtil;
import com.table2table.security.constants.Table2tableServiceURLs;
import com.table2table.security.dto.UserResponseDto;
import com.table2table.security.exceptions.ResourceNotFoundException;
import jakarta.persistence.criteria.Predicate;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class FoodPostService {

    private final FoodPostRepository foodPostRepository;
    private final CloudinaryService cloudinaryService;
    private final WebClient.Builder webClientBuilder;

    public void createFoodPost(FoodPostRequest foodRequset, UserResponseDto user) {
        FoodPost post = new FoodPost();
        post.setTitle(foodRequset.getTitle());
        post.setDescription(foodRequset.getDescription());
        post.setPrice(foodRequset.getPrice());
        post.setQuantity(foodRequset.getQuantity());
        post.setImageUrl(foodRequset.getImageUrl());
        post.setExpiresAt(foodRequset.getExpiresAt());
        post.setStatus(foodRequset.getStatus());
        post.setCookId(user.getId());
        post.setCommunityId(user.getCommunityId());
        post.setCreatedAt(LocalDateTime.now());
        post.setLastUpdatedAt(LocalDateTime.now());
        foodPostRepository.save(post);
    }

    public Page<FoodPostResponse> filterFoodPosts(FoodPostFilterRequest filter) {
        Pageable pageable = PageRequest.of(
                filter.getPage(),
                filter.getSize(),
                Sort.by(Sort.Direction.fromString(filter.getSortDirection()), filter.getSortBy())
        );

        Specification<FoodPost> spec = (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (filter.getStatus() != null) {
                predicates.add(cb.equal(root.get("status"), filter.getStatus()));
            }

            if (filter.getMinPrice() != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("price"), filter.getMinPrice()));
            }

            if (filter.getMaxPrice() != null) {
                predicates.add(cb.lessThanOrEqualTo(root.get("price"), filter.getMaxPrice()));
            }

            if (filter.getCommunityId() != null) {
                predicates.add(cb.equal(root.get("user").get("community").get("id"), filter.getCommunityId()));
            }

            if (filter.getKeyword() != null && !filter.getKeyword().isEmpty()) {
                Predicate titlePredicate = cb.like(cb.lower(root.get("title")), "%" + filter.getKeyword().toLowerCase() + "%");
                Predicate descPredicate = cb.like(cb.lower(root.get("description")), "%" + filter.getKeyword().toLowerCase() + "%");
                predicates.add(cb.or(titlePredicate, descPredicate));
            }

            if (filter.getExpiresBefore() != null) {
                predicates.add(cb.lessThan(root.get("expiresAt"), filter.getExpiresBefore()));
            }

            if (filter.getExpiresAfter() != null) {
                predicates.add(cb.greaterThan(root.get("expiresAt"), filter.getExpiresAfter()));
            }

            if (filter.getPostedByEmail() != null) {
                predicates.add(cb.equal(root.get("user").get("email"), filter.getPostedByEmail()));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };

        Page<FoodPost> posts = foodPostRepository.findAll(spec, pageable);
        return posts.map(FoodManagementUtil::convertToFoodPostDto);
    }

    public FoodPostResponse getFoodPostById(Long id) {
        FoodPost post = foodPostRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("FoodPost not found with id " + id));

        return FoodManagementUtil.convertToFoodPostDto(post);
    }

    public FoodPostResponse updateFoodPost(Long id, FoodPostRequest request, String userEmail, MultipartFile file) throws IOException {
        FoodPost foodPost = foodPostRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("FoodPost not found"));

        UserResponseDto user =  webClientBuilder.build()
                .get()
                .uri(Table2tableServiceURLs.GET_USER_BY_EMAIL + "{email}", userEmail)
                .retrieve()
                .bodyToMono(UserResponseDto.class)
                .block();

        // Allow if admin or owner
        if (!foodPost.getCookId().equals(user.getId()) && !user.getRole().equals("ADMIN")) {
            throw new AccessDeniedException("You do not have permission to update this post");
        }
        if (file != null && !file.isEmpty()) {
            String newImageUrl = cloudinaryService.uploadImage(file); // implement this
            foodPost.setImageUrl(newImageUrl);
        }

        foodPost.setTitle(request.getTitle());
        foodPost.setDescription(request.getDescription());
        foodPost.setQuantity(request.getQuantity());
        foodPost.setExpiresAt(request.getExpiresAt());
        foodPost.setStatus(request.getStatus());
        foodPost.setPrice(request.getPrice());
        foodPost.setLastUpdatedAt(LocalDateTime.now());

        foodPostRepository.save(foodPost);

        return FoodManagementUtil.convertToFoodPostDto(foodPost);
    }

    public void deleteFoodPost(Long id, String userEmail) {
        FoodPost post = foodPostRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("FoodPost not found"));


        UserResponseDto user =  webClientBuilder.build()
                .get()
                .uri(Table2tableServiceURLs.GET_USER_BY_EMAIL + "{email}", userEmail)
                .retrieve()
                .bodyToMono(UserResponseDto.class)
                .block();

        if (!post.getCookId().equals(user.getId()) && !user.getRole().equals("ADMIN")) {
            throw new AccessDeniedException("You do not have permission to delete this post");
        }

        foodPostRepository.delete(post);
    }

}
