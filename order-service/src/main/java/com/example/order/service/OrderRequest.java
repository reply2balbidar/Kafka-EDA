package com.example.order.service;

import java.math.BigDecimal;

public record OrderRequest(
        String customerId,
        String productId,
        int quantity,
        BigDecimal totalAmount
) {}
