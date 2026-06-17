package com.example.kafka.orderproducerservice.service;


import com.example.kafka.common.Order;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topic;

    public void sendOrder(Order order) {
        log.info("Sending order: {}", order);
        kafkaTemplate.send(topic, order.getOrderId(), order);
    }
}