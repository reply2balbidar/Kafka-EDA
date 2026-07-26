package com.example.notification.service;

import org.springframework.stereotype.Service;

/**
 * Stub for a real Customer Service lookup. In a production system this would
 * call a Customer microservice (REST/gRPC) or read from a shared customer
 * table to resolve the email address for a given customerId.
 *
 * For this demo, it deterministically derives an email from the customerId
 * so the whole flow is runnable without any external dependency.
 */
@Service
public class CustomerLookupService {

    public String resolveEmail(String customerId) {
        return customerId.toLowerCase().replaceAll("[^a-z0-9]", "") + "@example.com";
    }

    public String resolveDisplayName(String customerId) {
        return customerId;
    }
}
