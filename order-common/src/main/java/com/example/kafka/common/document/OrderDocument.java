package com.example.kafka.common.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * MongoDB read-model document. Optimized for queries — separate from command/event shapes (CQRS).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "orders")
public class OrderDocument {

    @Id
    private String orderId;
    private String product;
    private int quantity;
    private Instant createdAt;
    private Instant updatedAt;
}
