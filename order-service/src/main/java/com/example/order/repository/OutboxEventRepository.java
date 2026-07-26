package com.example.order.repository;

import com.example.order.entity.OutboxEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    // Row-level lock so that if you ever scale OutboxPoller to multiple instances,
    // they don't grab and publish the same unprocessed rows twice.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM OutboxEvent o WHERE o.processed = false ORDER BY o.createdAt ASC")
    List<OutboxEvent> findUnprocessedForUpdate(Pageable pageable);
}
