package com.example.kafka.orderproducerservice.service;

import com.example.kafka.common.command.CreateOrderCommand;
import com.example.kafka.common.command.UpdateOrderCommand;
import com.example.kafka.common.event.OrderEvent;
import com.example.kafka.common.event.OrderEventType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Handles commands on the write side of CQRS.
 * Each method builds a domain event and publishes it to Kafka for the projection service.
 */
@Service
@RequiredArgsConstructor
public class OrderCommandService {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandService.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topic;

    public void createOrder(CreateOrderCommand command) {
        OrderEvent event = OrderEvent.builder()
                .eventType(OrderEventType.CREATED)
                .orderId(command.getOrderId())
                .product(command.getProduct())
                .quantity(command.getQuantity())
                .timestamp(Instant.now())
                .build();
        publish(event);
    }

    public void updateOrder(String orderId, UpdateOrderCommand command) {
        OrderEvent event = OrderEvent.builder()
                .eventType(OrderEventType.UPDATED)
                .orderId(orderId)
                .product(command.getProduct())
                .quantity(command.getQuantity())
                .timestamp(Instant.now())
                .build();
        publish(event);
    }

    public void deleteOrder(String orderId) {
        OrderEvent event = OrderEvent.builder()
                .eventType(OrderEventType.DELETED)
                .orderId(orderId)
                .timestamp(Instant.now())
                .build();
        publish(event);
    }

    private void publish(OrderEvent event) {
        log.info("Publishing {} event for orderId={}", event.getEventType(), event.getOrderId());
        kafkaTemplate.send(topic, event.getOrderId(), event);
    }
}
