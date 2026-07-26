package com.example.inventory.listener;

import com.example.events.OrderCreatedEvent;
import com.example.inventory.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderCreated(String payload, Acknowledgment ack) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        log.info("Processing order {}", event.orderId());

        // Any exception here is caught by DefaultErrorHandler: retried per the
        // backoff policy, then routed to order-events.DLT if still failing.
        inventoryService.reserveStock(event.productId(), event.quantity());

        ack.acknowledge(); // manual commit - only after successful processing
    }
}
