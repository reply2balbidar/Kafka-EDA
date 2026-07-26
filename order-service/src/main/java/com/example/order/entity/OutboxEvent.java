package com.example.order.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    private String id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;

    @Column(columnDefinition = "CLOB")
    private String payload;

    private Instant createdAt;
    private boolean processed;
    private Instant processedAt;
}
