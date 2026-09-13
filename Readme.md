# Two-Way Real-Time Data Integration Platform

> **An event-driven integration platform for synchronizing an internal customer database with Stripe in both directions.**

This project demonstrates a reliable, asynchronous approach to third-party data synchronization using **FastAPI, Kafka, MySQL, and Stripe webhooks**.

The core design separates API request handling from downstream integration work, while using **event-driven processing, HMAC verification, and idempotency** to make synchronization safer and more resilient.

---

## Architecture
<img width="1500" height="760" alt="architecture(1)" src="https://github.com/user-attachments/assets/592c9c1c-71a7-46ac-a1e4-41e3391b38ed" />



### High-Level Flow

The platform supports synchronization in both directions:

```text
                 INTERNAL SYSTEM
                       │
                       │ Customer Change
                       ▼
                  ┌─────────┐
                  │ FastAPI │
                  └────┬────┘
                       │
                Store + Publish
                       │
                       ▼
                 ┌───────────┐
                 │   Kafka   │
                 └─────┬─────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Stripe Consumer │
              └────────┬────────┘
                       │
                       ▼
                    Stripe
                       │
                  Webhook Event
                       │
                       ▼
                 ┌───────────┐
                 │   Kafka   │
                 └─────┬─────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Webhook Consumer│
              └────────┬────────┘
                       │
                       ▼
                    MySQL
```

---

## The Problem

Integrating an internal customer database directly with an external payment platform creates tight coupling between the internal API and the external provider.

A synchronous design can look like:

```text
Client → API → MySQL → Stripe → Response
```

This means the API request becomes dependent on the availability and response time of Stripe.

This project instead introduces Kafka between the internal application and external integration:

```text
Client → FastAPI → MySQL
                    │
                    ▼
                  Kafka
                    │
                    ▼
             Stripe Consumer
                    │
                    ▼
                  Stripe
```

Stripe changes are handled in the reverse direction through webhooks:

```text
Stripe → Webhook → Kafka → Consumer → MySQL
```

This creates a **two-way, event-driven synchronization pipeline**.

---

## Key Engineering Decisions

### 1. Asynchronous Integration with Kafka

The API stores the internal state and publishes an event to Kafka rather than waiting for the Stripe operation to complete.

This provides a clean boundary between:

- Request processing
- Database persistence
- Event publishing
- Third-party API communication

The Stripe integration can therefore process events independently from the API request lifecycle.

---

### 2. Two-Way Synchronization

The platform handles changes originating from either side.

**Internal → Stripe**

```text
Internal Customer DB
        │
        ▼
     FastAPI
        │
        ▼
      Kafka
        │
        ▼
Stripe Consumer
        │
        ▼
     Stripe
```

**Stripe → Internal**

```text
Stripe
   │
   ▼
Webhook
   │
   ▼
Kafka
   │
   ▼
Webhook Consumer
   │
   ▼
MySQL
```

This makes the architecture extensible to additional integrations and event types.

---

### 3. Stripe Webhook Security

Incoming Stripe webhooks are verified using **HMAC-based signature verification** before the event is processed.

```text
Stripe Webhook
      │
      ▼
Signature Verification
      │
   ┌──┴──┐
   │     │
Valid   Invalid
 │        │
 ▼        ▼
Kafka    Reject
```

This prevents unverified webhook payloads from entering the processing pipeline.

---

### 4. Idempotent Event Processing

Distributed event processing can encounter duplicate deliveries.

The system uses **event IDs / processed-event tracking** to ensure that an already-processed event is not applied again.

Conceptually:

```text
Incoming Event
      │
      ▼
 Is Event ID
 Already Processed?
    │          │
   Yes         No
    │           │
 Ignore      Process
                │
                ▼
        Mark Event Processed
```

This is particularly important for webhook-driven systems where duplicate event delivery must be handled safely.

---

## Why Kafka?

Kafka acts as the event backbone between the API, integration consumers, and webhook processing.

### Benefits

- **Decoupling** — API requests are separated from downstream Stripe operations.
- **Asynchronous processing** — external API calls happen outside the request path.
- **Independent consumers** — different event types can be handled by dedicated consumers.
- **Extensibility** — new integrations can consume relevant events without redesigning the API.
- **Fault isolation** — temporary downstream issues do not require the API layer to directly manage the entire integration flow.

The project uses separate Kafka topics based on event type, with dedicated consumers for processing.

---

## Data Flow

### Internal Customer → Stripe

```text
1. Client sends customer data
          ↓
2. FastAPI validates request
          ↓
3. Data is persisted in MySQL
          ↓
4. Event is published to Kafka
          ↓
5. Stripe consumer receives event
          ↓
6. Consumer synchronizes data with Stripe
```

### Stripe → Internal Customer Database

```text
1. Stripe generates an event
          ↓
2. Stripe sends webhook
          ↓
3. Webhook signature is verified
          ↓
4. Event is published to Kafka
          ↓
5. Webhook consumer receives event
          ↓
6. Event ID is checked for idempotency
          ↓
7. MySQL is updated
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| API | **FastAPI** |
| Language | **Python** |
| Event Streaming | **Apache Kafka** |
| Database | **MySQL** |
| External Integration | **Stripe API** |
| Webhook Security | **HMAC Signature Verification** |
| Containerization | **Docker Compose** |
| Local Webhook Tunneling | **ngrok** |

---

## Project Structure

```text
two-way-integration/
│
├── api/
│   ├── routes/              # API endpoints
│   ├── services/            # Application/business logic
│   └── ...
│
├── consumers/
│   ├── stripe/              # Internal → Stripe processing
│   └── webhook/             # Stripe → Internal processing
│
├── kafka/
│   ├── producers/
│   └── consumers/
│
├── database/
│   └── ...
│
├── docker-compose.yml
├── .env.example
└── README.md
```

> Adapt the structure above to the exact directories in the repository if your implementation uses different names.

---

## API Design

The API layer is responsible for validating incoming requests, persisting internal state, and publishing integration events.

A typical flow is:

```http
POST /customers
```

```json
{
  "name": "John Doe",
  "email": "john@example.com"
}
```

The request is handled by FastAPI, stored in MySQL, and propagated asynchronously through Kafka.

---

## Local Development

### Prerequisites

- Python
- Docker
- Docker Compose
- Kafka
- MySQL
- Stripe account / API credentials
- ngrok for local webhook testing

### Setup

```bash
git clone <your-repository-url>
cd two-way-integration
```

Create the environment file:

```bash
cp .env.example .env
```

Configure the required database, Kafka, Stripe, and webhook settings.

Start the infrastructure:

```bash
docker compose up -d
```

Run the FastAPI application and consumers according to the repository's entry points.

---

## Reliability Considerations

The architecture explicitly addresses several failure modes common in integration systems.

### Duplicate Events

Handled using event IDs and processed-event tracking.

### Untrusted Webhooks

Handled using HMAC signature verification before processing.

### Slow External APIs

Stripe operations are moved to asynchronous consumers instead of blocking the API request.

### Extending the System

The event-driven design allows additional consumers and integrations to be added without tightly coupling them to the API layer.

---

## Design Highlights

### Event-Driven

Kafka provides a durable event-based boundary between system components.

### Asynchronous

External integration work is handled by consumers instead of blocking API requests.

### Idempotent

Processed-event tracking prevents duplicate events from causing repeated state changes.

### Secure

Stripe webhook signatures are verified before events enter the processing pipeline.

### Extensible

Separate topics and consumers make it possible to introduce additional event types and integrations.

### Containerized

The system can be run locally using Docker Compose, including the supporting infrastructure.

---

## What This Project Demonstrates

**Distributed Systems**
- Event-driven architecture
- Asynchronous processing
- Producer-consumer patterns
- Service decoupling
- Event-based integration

**Backend Engineering**
- FastAPI
- MySQL persistence
- API validation
- Kafka producers and consumers
- External API integration

**Reliability Engineering**
- Idempotent event processing
- Duplicate event handling
- Durable event-driven workflows
- Failure isolation

**Security**
- HMAC-based webhook verification
- Validation of third-party events

**Infrastructure**
- Docker Compose
- Kafka
- MySQL
- ngrok-based local webhook testing

---

## Future Extensions

Potential extensions for the architecture include:

- Additional third-party integrations
- More granular event contracts
- Retry and dead-letter handling
- Integration health monitoring
- Event replay tooling
- Distributed tracing
- Metrics and dashboards
- Kubernetes deployment

---

## Takeaway

The main goal of this project was not simply to connect an internal database to Stripe, but to design the integration as a **reliable event-driven system**.

By introducing Kafka between the API and integration workers, and by combining **webhook verification with idempotent event processing**, the architecture establishes clear boundaries between internal state, asynchronous processing, and external systems.
