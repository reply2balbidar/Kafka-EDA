package com.example.order.service;

import com.example.events.OrderCreatedEvent;
import com.example.order.entity.OrderEntity;
import com.example.order.entity.OrderStatus;
import com.example.order.entity.OutboxEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;
        import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderService - createOrder Unit Tests")
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private OrderService orderService;

    private OrderRequest orderRequest;

    @BeforeEach
    void setUp() {
        orderRequest = new OrderRequest(
                "CUST-001",
                "PROD-001",
                5,
                new BigDecimal("250.00")
        );
    }

    @Test
    @DisplayName("Should create order successfully and return order ID")
    void testCreateOrderSuccess() throws Exception {
        // Arrange
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{\"orderId\":\"test-id\"}");

        // Act
        String orderId = orderService.createOrder(orderRequest);

        // Assert
        assertThat(orderId).isNotNull().isNotBlank();
        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(outboxEventRepository, times(1)).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Should save OrderEntity with correct details")
    void testCreateOrderSavesOrderEntityCorrectly() throws Exception {
        // Arrange
        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{\"orderId\":\"test-id\"}");

        // Act
        orderService.createOrder(orderRequest);

        // Assert
        verify(orderRepository).save(orderCaptor.capture());
        OrderEntity savedOrder = orderCaptor.getValue();

        assertThat(savedOrder.getId()).isNotNull().isNotBlank();
        assertThat(savedOrder.getCustomerId()).isEqualTo("CUST-001");
        assertThat(savedOrder.getProductId()).isEqualTo("PROD-001");
        assertThat(savedOrder.getQuantity()).isEqualTo(5);
        assertThat(savedOrder.getTotalAmount()).isEqualTo(new BigDecimal("250.00"));
        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.CREATED);
        assertThat(savedOrder.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should save OutboxEvent with correct details")
    void testCreateOrderSavesOutboxEventCorrectly() throws Exception {
        // Arrange
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{\"payload\":\"serialized-event\"}");

        // Act
        orderService.createOrder(orderRequest);

        // Assert
        verify(outboxEventRepository).save(outboxCaptor.capture());
        OutboxEvent savedOutboxEvent = outboxCaptor.getValue();

        assertThat(savedOutboxEvent.getId()).isNotNull().isNotBlank();
        assertThat(savedOutboxEvent.getAggregateType()).isEqualTo("Order");
        assertThat(savedOutboxEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(savedOutboxEvent.getPayload()).isEqualTo("{\"payload\":\"serialized-event\"}");
        assertThat(savedOutboxEvent.isProcessed()).isFalse();
        assertThat(savedOutboxEvent.getCreatedAt()).isNotNull();
        assertThat(savedOutboxEvent.getAggregateId()).isNotNull();
    }

    @Test
    @DisplayName("Should throw RuntimeException when serialization fails")
    void testCreateOrderThrowsExceptionOnSerializationFailure() throws Exception {
        // Arrange
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenThrow(new RuntimeException("Serialization error"));

        // Act & Assert
        assertThatThrownBy(() -> orderService.createOrder(orderRequest))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize OrderCreatedEvent");

        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Should rollback both order and outbox on transaction failure")
    void testCreateOrderTransactionRollback() throws Exception {
        // Arrange
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{\"payload\":\"serialized\"}");
        when(outboxEventRepository.save(any(OutboxEvent.class)))
                .thenThrow(new RuntimeException("Database error"));

        // Act & Assert
        assertThatThrownBy(() -> orderService.createOrder(orderRequest))
                .isInstanceOf(RuntimeException.class);

        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(outboxEventRepository, times(1)).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Should assign unique UUID to order ID")
    void testCreateOrderAssignsUniqueId() throws Exception {
        // Arrange
        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{}");

        // Act
        String orderId1 = orderService.createOrder(orderRequest);
        String orderId2 = orderService.createOrder(orderRequest);

        // Assert
        assertThat(orderId1).isNotEqualTo(orderId2);
        verify(orderRepository, times(2)).save(orderCaptor.capture());
    }

    @Test
    @DisplayName("Should include order ID in outbox event")
    void testCreateOrderOutboxEventIncludesOrderId() throws Exception {
        // Arrange
        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{}");

        // Act
        orderService.createOrder(orderRequest);

        // Assert
        verify(orderRepository).save(orderCaptor.capture());
        verify(outboxEventRepository).save(outboxCaptor.capture());

        String orderId = orderCaptor.getValue().getId();
        String aggregateId = outboxCaptor.getValue().getAggregateId();

        assertThat(aggregateId).isEqualTo(orderId);
    }

    @Test
    @DisplayName("Should handle different order requests correctly")
    void testCreateOrderWithDifferentData() throws Exception {
        // Arrange
        OrderRequest request2 = new OrderRequest(
                "CUST-002",
                "PROD-002",
                10,
                new BigDecimal("500.00")
        );
        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{}");

        // Act
        orderService.createOrder(request2);

        // Assert
        verify(orderRepository).save(orderCaptor.capture());
        OrderEntity savedOrder = orderCaptor.getValue();

        assertThat(savedOrder.getCustomerId()).isEqualTo("CUST-002");
        assertThat(savedOrder.getProductId()).isEqualTo("PROD-002");
        assertThat(savedOrder.getQuantity()).isEqualTo(10);
        assertThat(savedOrder.getTotalAmount()).isEqualTo(new BigDecimal("500.00"));
    }

    @Test
    @DisplayName("Should call both repositories exactly once each on success")
    void testCreateOrderRepositoryInvocationCount() throws Exception {
        // Arrange
        when(objectMapper.writeValueAsString(any(OrderCreatedEvent.class)))
                .thenReturn("{}");

        // Act
        orderService.createOrder(orderRequest);

        // Assert
        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(outboxEventRepository, times(1)).save(any(OutboxEvent.class));
        verifyNoMoreInteractions(orderRepository, outboxEventRepository);
    }
}
