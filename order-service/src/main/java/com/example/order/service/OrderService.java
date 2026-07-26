package com.example.order.service;

import com.example.events.OrderCreatedEvent;
import com.example.order.entity.OrderEntity;
import com.example.order.entity.OrderStatus;
import com.example.order.entity.OutboxEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional // order insert + outbox insert commit together, or not at all
    public String createOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderEntity order = OrderEntity.builder()
                .id(orderId)
                .customerId(request.customerId())
                .productId(request.productId())
                .quantity(request.quantity())
                .totalAmount(request.totalAmount())
                .status(OrderStatus.CREATED)
                .createdAt(Instant.now())
                .build();
        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                request.customerId(),
                request.productId(),
                request.quantity(),
                request.totalAmount(),
                Instant.now()
        );

        try {
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateType("Order")
                    .aggregateId(orderId)
                    .eventType("OrderCreated")
                    .payload(objectMapper.writeValueAsString(event))
                    .createdAt(Instant.now())
                    .processed(false)
                    .build();
            outboxEventRepository.save(outboxEvent);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize OrderCreatedEvent", e);
        }

        return orderId;
        // If the process crashes right after this returns but before Kafka publish,
        // the event row is safely persisted in outbox_events - nothing is lost.
        // The OutboxPoller will pick it up on the next run.
    }
}
