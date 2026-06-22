package com.example.kafka.orderanalyticsservice.controller;

import com.example.kafka.orderanalyticsservice.model.ProductSales;
import com.example.kafka.orderanalyticsservice.service.ProductSalesQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only HTTP API over the live Kafka Streams analytics.
 *
 * <p>These endpoints never touch a database; every response is served straight from the in-process
 * Kafka Streams state store, so the numbers update within ~1 second of an order being created.</p>
 */
@RestController
@RequestMapping("/analytics/products")
public class AnalyticsController {

    private final ProductSalesQueryService queryService;

    public AnalyticsController(ProductSalesQueryService queryService) {
        this.queryService = queryService;
    }

    /** All products with their running order count and total quantity. */
    @GetMapping
    public List<ProductSales> all() {
        return queryService.findAll();
    }

    /** Aggregate for a single product, or 404 if no orders have been seen for it yet. */
    @GetMapping("/{product}")
    public ResponseEntity<ProductSales> byProduct(@PathVariable String product) {
        return queryService.findByProduct(product)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** While the streams engine is still starting/restoring, return 503 instead of a 500. */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleNotReady(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
    }
}
