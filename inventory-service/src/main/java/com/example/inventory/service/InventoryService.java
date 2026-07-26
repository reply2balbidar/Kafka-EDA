package com.example.inventory.service;

import com.example.inventory.exception.InsufficientStockException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class InventoryService {

    // In-memory stock for demo purposes; every productId starts with 50 units
    private final ConcurrentHashMap<String, AtomicInteger> stock = new ConcurrentHashMap<>();

    public void reserveStock(String productId, int quantity) {
        // Test hook: send productId = "FAIL_TEST" to always throw, so you can
        // watch the retry -> DLT flow without needing to exhaust real stock.
        if ("FAIL_TEST".equals(productId)) {
            throw new RuntimeException("Simulated processing failure for FAIL_TEST product");
        }

        AtomicInteger available = stock.computeIfAbsent(productId, id -> new AtomicInteger(50));

        int updated = available.updateAndGet(current -> current - quantity);

        if (updated < 0) {
            available.addAndGet(quantity); // roll back the decrement
            throw new InsufficientStockException(
                    "Not enough stock for product " + productId + ", requested " + quantity);
        }

        log.info("Reserved {} unit(s) of {}. Remaining stock: {}", quantity, productId, updated);
    }
}
