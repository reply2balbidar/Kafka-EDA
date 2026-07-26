package com.example.notification.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DltMonitor {

    @KafkaListener(
            topics = "order-events.notification-service.DLT",
            groupId = "dlt-monitor-notification",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDlt(ConsumerRecord<String, String> record) {
        String exceptionMessage = headerValue(record, "kafka_dlt-exception-message");
        String originalTopic = headerValue(record, "kafka_dlt-original-topic");
        String originalOffset = headerValue(record, "kafka_dlt-original-offset");

        log.error("DEAD LETTER (email send failed) from topic={} offset={}: key={}, value={}, reason={}",
                originalTopic, originalOffset, record.key(), record.value(), exceptionMessage);

        // In production: persist to a DB table for manual review/replay, or
        // trigger a Slack/PagerDuty alert - a failed order confirmation email
        // is usually not worth blocking the order, but IS worth someone knowing.
    }

    private String headerValue(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value()) : "unknown";
    }
}
