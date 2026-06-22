package com.example.kafka.orderanalyticsservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Running aggregate maintained by the Kafka Streams topology, one instance per product.
 *
 * <p>This is the <i>value</i> of a {@code KTable<String, ProductSales>} where the key is the
 * product name. Kafka Streams updates it incrementally as new order events arrive and stores it
 * in a local state store so we can serve it over HTTP via interactive queries.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductSales {

    private String product;

    /** Number of orders placed for this product (count of CREATED events). */
    private long orderCount;

    /** Sum of quantities across all orders for this product. */
    private long totalQuantity;
}
