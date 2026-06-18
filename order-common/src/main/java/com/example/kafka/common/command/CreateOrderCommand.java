package com.example.kafka.common.command;

import lombok.Data;

@Data
public class CreateOrderCommand {
    private String orderId;
    private String product;
    private int quantity;
}
