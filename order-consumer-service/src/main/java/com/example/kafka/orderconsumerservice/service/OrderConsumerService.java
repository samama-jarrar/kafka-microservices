package com.example.kafka.orderconsumerservice.service;

import com.example.kafka.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);

    @KafkaListener(topics = "${app.topic.name}", groupId = "order-group")
    public void consume(Order order) {
        log.info("Received Order: {}", order);

        // Business logic here
    }
}