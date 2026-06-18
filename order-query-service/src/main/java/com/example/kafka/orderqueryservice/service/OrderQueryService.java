package com.example.kafka.orderqueryservice.service;

import com.example.kafka.common.document.OrderDocument;
import com.example.kafka.common.query.OrderView;
import com.example.kafka.orderqueryservice.repository.OrderReadRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Query-side service. Reads from MongoDB only — never publishes commands or events.
 */
@Service
@RequiredArgsConstructor
public class OrderQueryService {

    private final OrderReadRepository repository;

    public List<OrderView> findAll() {
        return repository.findAll().stream()
                .map(this::toView)
                .toList();
    }

    public OrderView findById(String orderId) {
        return repository.findById(orderId)
                .map(this::toView)
                .orElse(null);
    }

    public List<OrderView> searchByProduct(String product) {
        return repository.findByProductContainingIgnoreCase(product).stream()
                .map(this::toView)
                .toList();
    }

    private OrderView toView(OrderDocument document) {
        return OrderView.builder()
                .orderId(document.getOrderId())
                .product(document.getProduct())
                .quantity(document.getQuantity())
                .createdAt(document.getCreatedAt())
                .updatedAt(document.getUpdatedAt())
                .build();
    }
}
