package com.example.kafka.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Domain event published to Kafka after a command is accepted.
 * The consumer uses {@link #eventType} to decide how to update the MongoDB read model.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    private OrderEventType eventType;
    private String orderId;
    private String product;
    private int quantity;
    private Instant timestamp;
}
