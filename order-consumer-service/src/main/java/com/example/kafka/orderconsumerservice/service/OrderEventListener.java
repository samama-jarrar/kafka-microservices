package com.example.kafka.orderconsumerservice.service;

import com.example.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Kafka listener that receives domain events and delegates to the projection service.
 */
@Service
@RequiredArgsConstructor
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderProjectionService projectionService;

    @KafkaListener(topics = "${app.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderEvent(OrderEvent event) {
        log.info("Received {} event for orderId={}", event.getEventType(), event.getOrderId());
        projectionService.project(event);
    }
}
