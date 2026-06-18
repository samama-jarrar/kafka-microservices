package com.example.kafka.orderproducerservice.controller;

import com.example.kafka.common.command.CreateOrderCommand;
import com.example.kafka.common.command.UpdateOrderCommand;
import com.example.kafka.orderproducerservice.dto.CommandAcceptedResponse;
import com.example.kafka.orderproducerservice.service.OrderCommandService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Command-side REST API (CQRS write model).
 * Accepts create/update/delete commands and publishes events to Kafka — never reads from MongoDB.
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderCommandController {

    private final OrderCommandService commandService;

    @PostMapping
    public ResponseEntity<CommandAcceptedResponse> createOrder(@RequestBody CreateOrderCommand command) {
        commandService.createOrder(command);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new CommandAcceptedResponse(
                        command.getOrderId(),
                        "Create order command accepted. Read model will update shortly."
                ));
    }

    @PutMapping("/{orderId}")
    public ResponseEntity<CommandAcceptedResponse> updateOrder(
            @PathVariable String orderId,
            @RequestBody UpdateOrderCommand command) {
        commandService.updateOrder(orderId, command);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new CommandAcceptedResponse(
                        orderId,
                        "Update order command accepted. Read model will update shortly."
                ));
    }

    @DeleteMapping("/{orderId}")
    public ResponseEntity<CommandAcceptedResponse> deleteOrder(@PathVariable String orderId) {
        commandService.deleteOrder(orderId);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new CommandAcceptedResponse(
                        orderId,
                        "Delete order command accepted. Read model will update shortly."
                ));
    }
}
