package com.example.events;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String productId,
        int quantity,
        BigDecimal totalAmount,
        Instant createdAt
) {}
