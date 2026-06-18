package com.example.kafka.orderproducerservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CommandAcceptedResponse {
    private String orderId;
    private String message;
}
