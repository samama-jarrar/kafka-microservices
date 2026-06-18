# Kafka Microservices with MongoDB and CQRS

A hands-on training project that combines **Apache Kafka**, **MongoDB**, and the **CQRS pattern** (Command Query Responsibility Segregation) using Spring Boot 3.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CQRS + Event-Driven                            │
└─────────────────────────────────────────────────────────────────────────────┘

  WRITE SIDE (Commands)                    READ SIDE (Queries)
  ─────────────────────                    ───────────────────

  order-producer-service                   order-query-service
  (port 8081)                              (port 8083)
       │                                        │
       │  POST /orders      (Create)             │  GET /orders
       │  PUT /orders/{id} (Update)            │  GET /orders/{id}
       │  DELETE /orders/{id} (Delete)           │  GET /orders/by-product
       │                                        │
       ▼                                        ▼
  OrderCommandService                      OrderQueryService
  (builds OrderEvent)                      (reads OrderView)
       │                                        │
       ▼                                        ▼
  Kafka Topic                              MongoDB
  orders-topic                             (orders collection)
       │                                        ▲
       ▼                                        │
  order-consumer-service                        │
  (port 8082)                                   │
       │                                        │
  OrderEventListener                            │
       │                                        │
  OrderProjectionService ───────────────────────┘
  (applies events to read model)
```

### Key idea: separate write and read paths

| Concern | Service | Responsibility |
|---------|---------|----------------|
| **Commands** (CUD) | `order-producer-service` | Accept HTTP writes, publish events to Kafka |
| **Projection** | `order-consumer-service` | Listen to events, update MongoDB read model |
| **Queries** (R) | `order-query-service` | Serve HTTP reads from MongoDB only |

The producer **never** touches MongoDB. The query service **never** publishes to Kafka. That separation is CQRS.

---

## What is CQRS?

**CQRS** = Command Query Responsibility Segregation.

In a traditional CRUD app, one service handles both reads and writes against one database:

```
Client → REST API → Service → Database
         (GET + POST + PUT + DELETE all go through the same path)
```

With CQRS, you split that into two models:

1. **Command model (write)** — handles creates, updates, deletes. Optimized for validating and recording *intent*.
2. **Query model (read)** — handles reads. Optimized for fast lookups, filtering, and display.

They can use **different databases**, **different schemas**, and **scale independently**.

### Why use CQRS here?

- **Kafka** is a natural fit for the write path: commands become **events** that other services consume asynchronously.
- **MongoDB** stores a **read model** (projection) shaped for queries — documents with `createdAt`, `updatedAt`, etc.
- You can add more consumers later (email notifications, analytics) without changing the command or query APIs.

### Eventual consistency

After you `POST /orders`, the command returns **202 Accepted** immediately. The order appears in MongoDB only after the consumer processes the Kafka event — usually within milliseconds, but not instantly. Reads may lag writes by a short window. That is normal in event-driven CQRS.

---

## Module Breakdown

### `order-common` — Shared contracts

Shared types used across services. No Spring Boot app here — just a library JAR.

| Package | Class | Purpose |
|---------|-------|---------|
| `command` | `CreateOrderCommand` | HTTP body for creating an order |
| `command` | `UpdateOrderCommand` | HTTP body for updating an order |
| `event` | `OrderEvent` | Message published to Kafka after a command |
| `event` | `OrderEventType` | `CREATED`, `UPDATED`, `DELETED` |
| `document` | `OrderDocument` | MongoDB `@Document` — the read model |
| `query` | `OrderView` | DTO returned by the query API |

**Why separate Command, Event, Document, and View?**

- **Command** = what the user sends (input).
- **Event** = what happened (immutable fact for other services).
- **Document** = how data is stored for reads (MongoDB shape).
- **View** = what the API returns (output DTO).

They often look similar early on, but decoupling them lets each evolve independently — a core CQRS benefit.

---

### `order-producer-service` — Command side (CUD)

**Port:** 8081

```
OrderCommandController → OrderCommandService → KafkaTemplate → orders-topic
```

| Endpoint | HTTP | What happens |
|----------|------|--------------|
| Create | `POST /orders` | Builds `OrderEvent(CREATED)`, publishes to Kafka |
| Update | `PUT /orders/{orderId}` | Builds `OrderEvent(UPDATED)`, publishes to Kafka |
| Delete | `DELETE /orders/{orderId}` | Builds `OrderEvent(DELETED)`, publishes to Kafka |

All write endpoints return **202 Accepted** with a message that the read model will update shortly.

**Files to study:**

- `OrderCommandController.java` — REST command API
- `OrderCommandService.java` — maps commands → events → Kafka
- `KafkaProducerConfig.java` — serializes `OrderEvent` as JSON

---

### `order-consumer-service` — Projection / sync handler

**Port:** 8082 (no public REST API for orders)

```
OrderEventListener (@KafkaListener) → OrderProjectionService → OrderReadRepository → MongoDB
```

When an `OrderEvent` arrives:

| Event type | MongoDB action |
|------------|----------------|
| `CREATED` | `insert` new `OrderDocument` |
| `UPDATED` | `findById`, update fields, `save` |
| `DELETED` | `deleteById` |

This service is the **bridge** between Kafka (write stream) and MongoDB (read store). In larger systems this role is often called a **projection worker** or **sync service**.

**Files to study:**

- `OrderEventListener.java` — Kafka consumer entry point
- `OrderProjectionService.java` — event → CRUD on read model
- `OrderReadRepository.java` — Spring Data `MongoRepository`

---

### `order-query-service` — Query side (R)

**Port:** 8083

```
OrderQueryController → OrderQueryService → OrderReadRepository → MongoDB
```

| Endpoint | HTTP | What happens |
|----------|------|--------------|
| List all | `GET /orders` | Returns all orders as `OrderView` |
| Get one | `GET /orders/{orderId}` | Returns one order or 404 |
| Search | `GET /orders/by-product?product=laptop` | Case-insensitive product search |

This service **only reads** MongoDB. It never publishes to Kafka. You can scale it independently when read traffic grows.

**Files to study:**

- `OrderQueryController.java` — read-only REST API
- `OrderQueryService.java` — maps `OrderDocument` → `OrderView`

---

## MongoDB Integration

### Docker

MongoDB runs via Docker Compose:

```yaml
mongodb:        port 27017
mongo-express:  port 8084  (web UI for browsing collections)
```

Connection string (both consumer and query services):

```properties
spring.data.mongodb.uri=mongodb://localhost:27017/orderdb
```

### OrderDocument

```java
@Document(collection = "orders")
public class OrderDocument {
    @Id
    private String orderId;   // MongoDB document _id
    private String product;
    private int quantity;
    private Instant createdAt;
    private Instant updatedAt;
}
```

Spring Data MongoDB provides `MongoRepository<OrderDocument, String>` — you get `save`, `findById`, `findAll`, `deleteById` for free, plus custom query methods like `findByProductContainingIgnoreCase`.

Browse data at **http://localhost:8084** (mongo-express) after starting Docker.

---

## How to Run

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts Zookeeper, Kafka, Kafka UI (8080), MongoDB (27017), and Mongo Express (8084).

### 2. Build the project

From the repo root:

```bash
mvn clean install -DskipTests
```

### 3. Start the three Spring Boot services (separate terminals)

```bash
# Terminal 1 — Command service
cd order-producer-service && mvn spring-boot:run

# Terminal 2 — Projection consumer
cd order-consumer-service && mvn spring-boot:run

# Terminal 3 — Query service
cd order-query-service && mvn spring-boot:run
```

**Startup order:** Infrastructure first, then consumer (so it is ready when events arrive), then producer and query. In practice any order works; events buffer in Kafka until the consumer is up.

---

## Full CRUD Walkthrough

### CREATE

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-001","product":"Laptop","quantity":2}'
```

**Flow:**
1. `OrderCommandController` receives `CreateOrderCommand`
2. `OrderCommandService` builds `OrderEvent(CREATED)` and sends to Kafka
3. `OrderEventListener` consumes the event
4. `OrderProjectionService` saves `OrderDocument` to MongoDB

**Verify (after ~1 second):**

```bash
curl http://localhost:8083/orders/ORD-001
```

### READ (all)

```bash
curl http://localhost:8083/orders
```

### READ (by product)

```bash
curl "http://localhost:8083/orders/by-product?product=laptop"
```

### UPDATE

```bash
curl -X PUT http://localhost:8081/orders/ORD-001 \
  -H "Content-Type: application/json" \
  -d '{"product":"Gaming Laptop","quantity":1}'
```

Then query again — `updatedAt` should change.

### DELETE

```bash
curl -X DELETE http://localhost:8081/orders/ORD-001
```

Then `GET /orders/ORD-001` returns **404**.

---

## End-to-End Request Flow (Create Example)

```
1. Client
   POST /orders  {"orderId":"ORD-001","product":"Laptop","quantity":2}
        │
        ▼
2. order-producer-service (8081)
   OrderCommandController.createOrder()
   OrderCommandService.createOrder()
   → OrderEvent { eventType: CREATED, orderId: "ORD-001", ... }
   → kafkaTemplate.send("orders-topic", "ORD-001", event)
        │
        ▼
3. Kafka (orders-topic)
   Message stored, keyed by orderId for ordered processing per key
        │
        ▼
4. order-consumer-service (8082)
   OrderEventListener.onOrderEvent()
   OrderProjectionService.handleCreated()
   → repository.save(OrderDocument)
        │
        ▼
5. MongoDB (orderdb.orders)
   { "_id": "ORD-001", "product": "Laptop", "quantity": 2, ... }
        │
        ▼
6. Client (read)
   GET http://localhost:8083/orders/ORD-001
   → OrderQueryService.findById()
   → OrderView returned as JSON
```

---

## Kafka Concepts Used

| Concept | Where | Why |
|---------|-------|-----|
| **Topic** | `orders-topic` | Channel for order domain events |
| **Message key** | `orderId` | Same key → same partition → ordered events per order |
| **Consumer group** | `order-group` | Allows scaling consumers; each message processed once per group |
| **JSON serialization** | Producer/consumer config | Human-readable events, easy debugging in Kafka UI |

View messages in **Kafka UI**: http://localhost:8080

---

## Project Structure

```
kafka-microservices/
├── docker-compose.yml
├── pom.xml
├── order-common/                 # Shared commands, events, documents, views
├── order-producer-service/       # CQRS command side (POST, PUT, DELETE)
├── order-consumer-service/       # Kafka → MongoDB projection
└── order-query-service/          # CQRS query side (GET)
```

---

## Learning Checklist

Use this to confirm you understand each piece:

- [ ] **CQRS** — Can you explain why writes go to port 8081 and reads to 8083?
- [ ] **Commands vs events** — Why is `CreateOrderCommand` different from `OrderEvent`?
- [ ] **Projection** — What does `OrderProjectionService` do for each `OrderEventType`?
- [ ] **Eventual consistency** — Why might a GET return 404 right after POST?
- [ ] **MongoDB** — Where is `@Document` defined? Who writes vs who reads?
- [ ] **Kafka** — What is the message key and why use `orderId`?
- [ ] **Repository** — How does `findByProductContainingIgnoreCase` work without SQL?

---

## Possible Next Steps

- Add Bean Validation on commands (`@NotBlank`, `@Min`)
- Add idempotency for duplicate `CREATED` events (already partially handled)
- Add a dead-letter topic for failed projections
- Add OpenAPI/Swagger on command and query APIs
- Use Testcontainers for integration tests with real Kafka + MongoDB

---

## Ports Reference

| Service | Port |
|---------|------|
| Kafka UI | 8080 |
| order-producer-service (commands) | 8081 |
| order-consumer-service (internal) | 8082 |
| order-query-service (queries) | 8083 |
| Mongo Express | 8084 |
| Kafka broker | 9092 |
| MongoDB | 27017 |
