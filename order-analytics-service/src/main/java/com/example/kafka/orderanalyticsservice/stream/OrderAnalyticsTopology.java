package com.example.kafka.orderanalyticsservice.stream;

import com.example.kafka.common.event.OrderEvent;
import com.example.kafka.common.event.OrderEventType;
import com.example.kafka.orderanalyticsservice.model.ProductSales;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Defines the Kafka Streams processing graph (the "topology").
 *
 * <p>A topology is a DAG of stream processors. Spring Boot injects a managed {@link StreamsBuilder}
 * (because of {@code @EnableKafkaStreams}); we describe the graph against it, and Spring starts the
 * underlying {@code KafkaStreams} engine for us.</p>
 *
 * <p>Two independent views are built from the same source stream:</p>
 * <ol>
 *   <li><b>Running totals per product</b> &rarr; a {@code KTable} materialized into the
 *       {@code product-sales-store} state store (served live over HTTP via interactive queries) and
 *       also emitted to {@code product-sales-topic}.</li>
 *   <li><b>Orders per product per 1-minute window</b> &rarr; a windowed count emitted to
 *       {@code orders-per-product-windowed-topic} (view it in Kafka UI).</li>
 * </ol>
 */
@Configuration
public class OrderAnalyticsTopology {

    private static final Logger log = LoggerFactory.getLogger(OrderAnalyticsTopology.class);

    @Value("${app.topic.orders}")
    private String ordersTopic;

    @Value("${app.topic.product-sales}")
    private String productSalesTopic;

    @Value("${app.topic.orders-windowed}")
    private String ordersWindowedTopic;

    @Value("${app.store.product-sales}")
    private String productSalesStore;

    /**
     * Building the topology inside a {@code @Bean} method (rather than just a constructor) makes the
     * dependency on {@link StreamsBuilder} explicit and keeps the graph wiring in one place.
     */
    @Bean
    public KStream<String, OrderEvent> orderAnalyticsStream(StreamsBuilder builder,
                                                            Serde<OrderEvent> orderEventSerde,
                                                            Serde<ProductSales> productSalesSerde) {

        // 1. SOURCE: read the order events. The key on orders-topic is the orderId (String).
        KStream<String, OrderEvent> orders =
                builder.stream(ordersTopic, Consumed.with(Serdes.String(), orderEventSerde));

        // 2. FILTER (stateless): only CREATED events count as "an order was placed".
        //    UPDATED/DELETED would require subtracting prior state, which we skip for clarity.
        KStream<String, OrderEvent> created =
                orders.filter((orderId, event) -> event != null
                        && event.getEventType() == OrderEventType.CREATED
                        && event.getProduct() != null);

        created.peek((orderId, event) ->
                log.info("Analytics saw CREATED order={} product={} qty={}",
                        orderId, event.getProduct(), event.getQuantity()));

        // 3. RE-KEY (stateless): switch the key from orderId to product so we can group by product.
        KStream<String, OrderEvent> byProduct =
                created.selectKey((orderId, event) -> event.getProduct());

        // ---- View 1: running totals per product (KTable + state store) ----

        // 4. GROUP + 5. AGGREGATE (stateful): fold every event for a product into one ProductSales.
        KTable<String, ProductSales> salesByProduct = byProduct
                .groupByKey(Grouped.with(Serdes.String(), orderEventSerde))
                .aggregate(
                        // initializer: the starting value for a brand-new product key
                        () -> ProductSales.builder().orderCount(0).totalQuantity(0).build(),
                        // aggregator: called for every event; returns the updated aggregate
                        (product, event, aggregate) -> {
                            aggregate.setProduct(product);
                            aggregate.setOrderCount(aggregate.getOrderCount() + 1);
                            aggregate.setTotalQuantity(aggregate.getTotalQuantity() + event.getQuantity());
                            return aggregate;
                        },
                        // materialized: name the backing state store so we can query it over HTTP
                        Materialized.<String, ProductSales, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>
                                        as(productSalesStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(productSalesSerde));

        // 6. SINK: also publish each updated aggregate so it is visible in Kafka UI / downstream apps.
        salesByProduct.toStream()
                .to(productSalesTopic, Produced.with(Serdes.String(), productSalesSerde));

        // ---- View 2: orders per product per 1-minute tumbling window ----

        byProduct
                .groupByKey(Grouped.with(Serdes.String(), orderEventSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count()
                .toStream()
                // windowed key = (product, time-window); flatten it into a readable String key
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().startTime(),
                        count == null ? "0" : count.toString()))
                .to(ordersWindowedTopic, Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
