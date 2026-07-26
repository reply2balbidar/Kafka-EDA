# Event-Driven Order Processing Demo (Spring Boot + Kafka)

Three independent Spring Boot services communicating only through Kafka:

- **order-service** (port 8081) - REST API, writes orders to H2 using the
  **transactional outbox pattern**, and polls the outbox to publish `OrderCreated`
  events to Kafka.
- **inventory-service** (port 8082) - consumes `order-events` (own consumer
  group `inventory-service`), reserves stock, and routes failed messages to
  `order-events.inventory-service.DLT` after 3 retries.
- **notification-service** (port 8083) - consumes `order-events` independently
  (own consumer group `notification-service`), sends an HTML order-confirmation
  email via SMTP, and routes failed sends to
  `order-events.notification-service.DLT` after 3 retries.

Each DLT is namespaced by consumer group. Both inventory-service and
notification-service read the *same* `order-events` topic (that's the
pub/sub fan-out), so a shared, un-namespaced DLT topic would mix up failures
from two unrelated services - namespacing keeps them separable for replay.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker + Docker Compose

## 1. Start Kafka

```bash
docker compose up -d
```

This starts Zookeeper, Kafka (localhost:9092), Kafka UI (http://localhost:8080),
and **MailHog** - a fake SMTP server with a web UI at http://localhost:8025
where you can see every "sent" email without needing real credentials.

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

## 3b. Run notification-service (separate terminal)

```bash
cd notification-service
mvn spring-boot:run
```

By default it points at MailHog (`localhost:1025`, no auth). To use a real
SMTP provider instead, set environment variables before running, e.g.:

```bash
export MAIL_HOST=smtp.gmail.com
export MAIL_PORT=587
export MAIL_USERNAME=you@gmail.com
export MAIL_PASSWORD=your-app-specific-password
export MAIL_SMTP_AUTH=true
export MAIL_SMTP_STARTTLS=true
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
Watch the **notification-service** logs: event consumed -> email sent - then
open http://localhost:8025 (MailHog) and you'll see the actual HTML order
confirmation email sitting in the inbox.

Each `productId` starts with 50 units of in-memory stock. Order more than 50 of
the same product in total and you'll see a real `InsufficientStockException`
retried 3x then routed to the DLT.

## 5. Test the DLT flow directly

**Inventory failure** - send `"productId": "FAIL_TEST"`, which always throws
inside `InventoryService`, regardless of stock:

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

In the inventory-service logs you'll see 3 retry attempts (2s apart), then a
`DEAD LETTER received from topic=order-events ...` line from its `DltMonitor`.
Notice notification-service is unaffected - it still sends the email fine,
since the two services are fully decoupled.

**Notification failure** - send `"customerId": "FAIL_TEST"`, which always
throws inside `EmailService`, regardless of SMTP availability:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "FAIL_TEST",
        "productId": "prod-1",
        "quantity": 1,
        "totalAmount": 9.99
      }'
```

In the notification-service logs you'll see the same retry pattern, then a
`DEAD LETTER (email send failed) ...` line. Inventory-service still reserves
stock for this order fine.

Both DLT topics (`order-events.inventory-service.DLT` and
`order-events.notification-service.DLT`) are visible directly in Kafka UI
(http://localhost:8080).

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
