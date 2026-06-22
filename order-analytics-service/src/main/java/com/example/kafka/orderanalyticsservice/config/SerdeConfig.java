package com.example.kafka.orderanalyticsservice.config;

import com.example.kafka.common.event.OrderEvent;
import com.example.kafka.orderanalyticsservice.model.ProductSales;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * SerDes (serializer + deserializer) used by the Kafka Streams topology.
 *
 * <p>A stream of bytes on a topic is meaningless until you tell Kafka how to turn those bytes into
 * Java objects and back. Every {@code KStream}/{@code KTable} operation that reads or writes a topic
 * (or a state store) needs a matching key SerDe and value SerDe.</p>
 *
 * <p>We use JSON SerDes so messages stay human-readable in Kafka UI, mirroring the producer/consumer
 * services. {@code ignoreTypeHeaders()} makes the deserializer trust the target class instead of the
 * {@code __TypeId__} header the producer adds, and {@code noTypeInfo()} stops us from writing those
 * headers onto our own output topics.</p>
 */
@Configuration
public class SerdeConfig {

    @Bean
    public ObjectMapper analyticsObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule()); // needed for OrderEvent.timestamp (Instant)
        return mapper;
    }

    @Bean
    public Serde<OrderEvent> orderEventSerde(ObjectMapper analyticsObjectMapper) {
        JsonSerde<OrderEvent> serde = new JsonSerde<>(OrderEvent.class, analyticsObjectMapper);
        serde.ignoreTypeHeaders();
        return serde;
    }

    @Bean
    public Serde<ProductSales> productSalesSerde(ObjectMapper analyticsObjectMapper) {
        JsonSerde<ProductSales> serde = new JsonSerde<>(ProductSales.class, analyticsObjectMapper);
        serde.noTypeInfo();
        serde.ignoreTypeHeaders();
        return serde;
    }
}
