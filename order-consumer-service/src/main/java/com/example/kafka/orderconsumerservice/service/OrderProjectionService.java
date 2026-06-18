package com.example.kafka.orderconsumerservice.service;

import com.example.kafka.common.document.OrderDocument;
import com.example.kafka.common.event.OrderEvent;
import com.example.kafka.common.event.OrderEventType;
import com.example.kafka.orderconsumerservice.repository.OrderReadRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Projection service: applies domain events to the MongoDB read model (CQRS write path for queries).
 */
@Service
@RequiredArgsConstructor
public class OrderProjectionService {

    private static final Logger log = LoggerFactory.getLogger(OrderProjectionService.class);

    private final OrderReadRepository repository;

    public void project(OrderEvent event) {
        switch (event.getEventType()) {
            case CREATED -> handleCreated(event);
            case UPDATED -> handleUpdated(event);
            case DELETED -> handleDeleted(event);
            default -> log.warn("Unknown event type: {}", event.getEventType());
        }
    }


    private void handleCreated(OrderEvent event) {
        if (repository.existsById(event.getOrderId())) {
            log.warn("Order {} already exists in read model, skipping CREATED", event.getOrderId());
            return;
        }

        OrderDocument document = OrderDocument.builder()
                .orderId(event.getOrderId())
                .product(event.getProduct())
                .quantity(event.getQuantity())
                .createdAt(event.getTimestamp())
                .updatedAt(event.getTimestamp())
                .build();

        repository.save(document);
        log.info("Projected CREATED event into MongoDB for orderId={}", event.getOrderId());
    }

    private void handleUpdated(OrderEvent event) {
        repository.findById(event.getOrderId()).ifPresentOrElse(document -> {
            document.setProduct(event.getProduct());
            document.setQuantity(event.getQuantity());
            document.setUpdatedAt(event.getTimestamp());
            repository.save(document);
            log.info("Projected UPDATED event into MongoDB for orderId={}", event.getOrderId());
        }, () -> log.warn("Order {} not found for UPDATED event", event.getOrderId()));
    }

    private void handleDeleted(OrderEvent event) {
        if (repository.existsById(event.getOrderId())) {
            repository.deleteById(event.getOrderId());
            log.info("Projected DELETED event into MongoDB for orderId={}", event.getOrderId());
        } else {
            log.warn("Order {} not found for DELETED event", event.getOrderId());
        }
    }
}
