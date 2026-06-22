package com.example.kafka.orderanalyticsservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Analytics service for the CQRS demo.
 *
 * <p>Unlike {@code order-consumer-service} (which uses a plain {@code @KafkaListener} to project
 * events into MongoDB), this service uses <b>Kafka Streams</b> to build live, in-memory
 * materialized views over the same {@code orders-topic} event stream.</p>
 *
 * <p>{@link EnableKafkaStreams} tells Spring Boot to auto-create a {@code StreamsBuilder} and start
 * the {@code KafkaStreams} engine using the {@code spring.kafka.streams.*} properties.</p>
 */
@SpringBootApplication
@EnableKafkaStreams
public class OrderAnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderAnalyticsServiceApplication.class, args);
    }
}
