package com.example.order.outbox;

import com.example.order.entity.OutboxEvent;
import com.example.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${outbox.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:500}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository
                .findUnprocessedForUpdate(PageRequest.of(0, batchSize));

        if (pending.isEmpty()) {
            return;
        }

        log.info("Publishing {} pending outbox event(s)", pending.size());

        for (OutboxEvent event : pending) {
            try {
                String topic = resolveTopic(event.getEventType());

                // .get() blocks until the broker ack is received so we know for
                // certain the message landed before marking the row processed.
                kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload()).get();

                event.setProcessed(true);
                event.setProcessedAt(Instant.now());
                outboxEventRepository.save(event);

                log.info("Published outbox event {} (aggregateId={}) to topic {}",
                        event.getId(), event.getAggregateId(), topic);

            } catch (Exception e) {
                // Leave processed=false - it will be retried on the next poll cycle.
                log.error("Failed to publish outbox event {}, will retry next poll",
                        event.getId(), e);
            }
        }
    }

    private String resolveTopic(String eventType) {
        return switch (eventType) {
            case "OrderCreated" -> "order-events";
            default -> throw new IllegalStateException("Unknown event type: " + eventType);
        };
    }
}
