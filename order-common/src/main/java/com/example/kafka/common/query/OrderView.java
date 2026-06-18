package com.example.kafka.common.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DTO returned by the query service. Exposes read-model data without MongoDB internals.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderView {
    private String orderId;
    private String product;
    private int quantity;
    private Instant createdAt;
    private Instant updatedAt;
}
