package com.example.kafka.common.command;

import lombok.Data;

@Data
public class UpdateOrderCommand {
    private String product;
    private int quantity;
}
