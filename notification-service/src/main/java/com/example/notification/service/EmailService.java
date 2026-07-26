package com.example.notification.service;

import com.example.events.OrderCreatedEvent;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmailService {

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;
    private final CustomerLookupService customerLookupService;

    @Value("${notification.from-address}")
    private String fromAddress;

    @Value("${notification.from-name}")
    private String fromName;

    public void sendOrderConfirmation(OrderCreatedEvent event) {
        // Test hook: send customerId = "FAIL_TEST" to always throw, so you can
        // watch the retry -> DLT flow without needing to break a real SMTP
        // connection to test it.
        if ("FAIL_TEST".equals(event.customerId())) {
            throw new EmailSendException(
                    "Simulated email failure for FAIL_TEST customer", new RuntimeException());
        }

        String toEmail = customerLookupService.resolveEmail(event.customerId());
        String customerName = customerLookupService.resolveDisplayName(event.customerId());

        Context context = new Context();
        context.setVariable("customerName", customerName);
        context.setVariable("orderId", event.orderId());
        context.setVariable("productId", event.productId());
        context.setVariable("quantity", event.quantity());
        context.setVariable("totalAmount", event.totalAmount());

        String htmlBody = templateEngine.process("order-confirmation", context);

        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, "UTF-8");
            helper.setFrom(fromAddress, fromName);
            helper.setTo(toEmail);
            helper.setSubject("Your order " + event.orderId() + " is confirmed");
            helper.setText(htmlBody, true); // true = isHtml

            mailSender.send(message);
            log.info("Sent order confirmation email to {} for order {}", toEmail, event.orderId());

        } catch (Exception e) {
            // Rethrow as unchecked so the Kafka error handler can catch it,
            // retry, and eventually route to the DLT if sending keeps failing
            // (e.g. SMTP server down, invalid address, provider rate limit).
            throw new EmailSendException(
                    "Failed to send order confirmation for order " + event.orderId(), e);
        }
    }
}
