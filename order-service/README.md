# Event-Driven Order Processing Demo (Spring Boot + Kafka)

Two independent Spring Boot services communicating only through Kafka:

- **order-service** (port 8081) - REST API, writes orders to H2 using the
  **transactional outbox pattern**, and polls the outbox to publish `OrderCreated`
  events to Kafka.
- **inventory-service** (port 8082) - consumes `order-events`, reserves stock,
  and routes failed messages to a **Dead Letter Topic** (`order-events.DLT`)
  after 3 retries.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker + Docker Compose

## 1. Start Kafka

```bash
docker compose up -d
```

This starts Zookeeper, Kafka (localhost:9092), and Kafka UI at http://localhost:8080
(handy for watching topics/messages/consumer groups live).

## 2. Run order-service

```bash
cd order-service
mvn spring-boot:run
```

## 3. Run inventory-service (separate terminal)

```bash
cd inventory-service
mvn spring-boot:run
```

## 4. Test the happy path

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "cust-1",
        "productId": "prod-42",
        "quantity": 3,
        "totalAmount": 59.97
      }'
```

Watch the **order-service** logs: outbox row created -> polled -> published to Kafka.
Watch the **inventory-service** logs: event consumed -> stock reserved.

Each `productId` starts with 50 units of in-memory stock. Order more than 50 of
the same product in total and you'll see a real `InsufficientStockException`
retried 3x then routed to the DLT.

## 5. Test the DLT flow directly

Send `"productId": "FAIL_TEST"` - this always throws inside the consumer,
regardless of stock:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "cust-2",
        "productId": "FAIL_TEST",
        "quantity": 1,
        "totalAmount": 9.99
      }'
```

In the inventory-service logs you'll see:
1. 3 retry attempts, 2 seconds apart
2. A final `DEAD LETTER received from topic=order-events ...` log line from `DltMonitor`

You can also see the `order-events.DLT` topic and its message directly in
Kafka UI (http://localhost:8080).

## 6. Prove the outbox pattern survives a crash

1. Stop **order-service** with Ctrl+C right after `mvn spring-boot:run` finishes
   compiling but *before* sending any request - not useful on its own.
   Instead: send a request, then immediately kill the process before ~500ms
   passes (the poll interval) - the order row and outbox row are already
   committed to H2 (`order-service/data/orderdb.mv.db`).
2. Restart order-service. The `OutboxPoller` picks up the unprocessed row on
   its very first scheduled run and publishes it - no data was lost despite
   the crash between "order created" and "event published".

## Inspect the H2 database

Enabled at http://localhost:8081/h2-console
- JDBC URL: `jdbc:h2:file:./data/orderdb;AUTO_SERVER=TRUE`
- User: `sa`, Password: (blank)

Query `SELECT * FROM outbox_events` to see `processed = true/false` state.

## Notes

- Both services use H2 the same way a real Postgres/MySQL setup would with
  JPA - swap the datasource config for production.
- `OutboxPoller` uses `@Scheduled(fixedDelay = 500ms)`. For lower latency and
  no polling overhead in production, replace this with Debezium (CDC) reading
  the DB write-ahead log directly.
- The DLT-routed message headers (`kafka_dlt-exception-message`,
  `kafka_dlt-original-topic`, `kafka_dlt-original-offset`) are added
  automatically by `DeadLetterPublishingRecoverer`.
