package com.table2table.foodservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication(scanBasePackages = {
		"com.table2table.foodservice",         // local package
		"com.table2table.security"            // shared lib package
})
@EnableDiscoveryClient
public class FoodServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(FoodServiceApplication.class, args);
	}

}
