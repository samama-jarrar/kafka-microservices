package com.example.kafka.orderconsumerservice.repository;

import com.example.kafka.common.document.OrderDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OrderReadRepository extends MongoRepository<OrderDocument, String> {

    List<OrderDocument> findByProductContainingIgnoreCase(String product);
}
