package com.example.kafka.orderqueryservice.controller;

import com.example.kafka.common.query.OrderView;
import com.example.kafka.orderqueryservice.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Query-side REST API (CQRS read model).
 * All endpoints read from MongoDB — no writes, no Kafka.
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderQueryController {

    private final OrderQueryService queryService;

    @GetMapping
    public List<OrderView> getAllOrders() {
        return queryService.findAll();
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderView> getOrderById(@PathVariable String orderId) {
        OrderView order = queryService.findById(orderId);
        if (order == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(order);
    }

    @GetMapping("/by-product")
    public List<OrderView> searchByProduct(@RequestParam String product) {
        return queryService.searchByProduct(product);
    }

    @GetMapping("/")
    public List<OrderView> getAll() {
        return queryService.findAll();
    }
}
