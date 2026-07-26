package com.example.notification.listener;

import com.example.events.OrderCreatedEvent;
import com.example.notification.service.EmailService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationEventListener {

    private final EmailService emailService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-events",
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderCreated(String payload, Acknowledgment ack) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        log.info("Sending order confirmation email for order {}", event.orderId());

        // Test hook: customerId = "FAIL_TEST" always throws inside EmailService's
        // resolved address, letting you exercise retry -> DLT without needing to
        // break a real SMTP connection. See CustomerLookupService/EmailService.
        emailService.sendOrderConfirmation(event);

        ack.acknowledge(); // manual commit - only after the email actually sent
    }
}
