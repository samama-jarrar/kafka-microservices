package com.example.kafka.orderproducerservice.controller;

import com.example.kafka.common.Order;
import com.example.kafka.orderproducerservice.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;

    @PostMapping
    public String sendOrder(@RequestBody Order order) {
        producerService.sendOrder(order);
        return "Order sent to Kafka";
    }
}
