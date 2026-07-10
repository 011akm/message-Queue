# Distributed Message Queue System

A production-style message queue built on **Redis Streams**, featuring consumer groups, acknowledgment, retry logic, dead letter queue, MongoDB archiving, and a real-time monitoring dashboard — deployed via Docker Compose with multiple worker containers.

---

## Architecture

```
                 POST /jobs
                     │
                     ▼
            Producer (Express API)
                     │  XADD (MAXLEN ~1000)
                     ▼
            Redis Stream (jobs:stream)
                     │
                     │  XREADGROUP
    ├──────────────────────────────────────┐
    ▼                  ▼                   ▼
Worker-1           Worker-2           Worker-3
    │                  │                   │
    │  on success: archive to MongoDB → XACK
    │  on failure: retry (exp. backoff) → DLQ after MAX_RETRIES
                     ▼
        jobs:dlq (Dead Letter Queue)
                     │
                     ▼
        MongoDB (jobarchives collection)
```

---

## Features

- **At-least-once delivery** via Redis Streams consumer groups + XACK
- **Retry logic** — failed jobs requeued with incremented retry counter (max 3 retries)
- **Dead Letter Queue** — permanently failed jobs moved to `jobs:dlq` for inspection
- **Crash recovery** — on worker restart, unACKed messages are reclaimed and reprocessed
- **Idle timeout recovery** — jobs from permanently dead workers claimed by healthy workers after 30s via XCLAIM
- **MongoDB archiving** — every completed/failed job persisted before MAXLEN trims the stream
- **Stream bounding** — `MAXLEN ~1000` keeps Redis memory usage predictable
- **Real-time dashboard** — queue depth, DLQ count, pending messages, per-worker stats (auto-refresh every 1s)
- **Horizontal scaling** — N workers via Docker Compose, each identified by `CONSUMER_NAME` env var

---

## Tech Stack

|     Layer     |                          Tool                            |
|---------------|----------------------------------------------------------|
| Runtime       | Node.js 20                                               |
| Queue engine  | Redis Streams (XADD, XREADGROUP, XACK, XCLAIM, XPENDING) |
| Producer      | Express.js                                               |
| Consumers     | Node.js worker processes                                 |
| Database      | MongoDB + Mongoose                                       |
| Orchestration | Docker Compose                                           |
| Load testing  | autocannon                                               |

---

## Project Structure

```
message-queue/
├── docker-compose.yml
├── Dockerfile.producer
├── Dockerfile.worker
├── package.json
├── autocannon-test.js       # stress test runner
├── producer/
│   ├── server.js            # Express API (POST /jobs, GET /metrics, GET /dashboard)
│   └── dashboard.html       # real-time monitoring UI
├── consumer/
│   └── worker.js            # consumer group worker with retry + DLQ + archiving
├── db/
│   └── mongo.js             # MongoDB connection helper
└── models/
    └── jobArchive.js        # Mongoose schema for archived jobs
```

---

## Getting Started

### Prerequisites
- Docker Desktop
- Node.js 20+

### Run

```bash
git clone https://github.com/011akm/message-queue.git
cd message-queue

npm install

docker-compose up --build
```

Services started:
- Redis on `localhost:6379`
- MongoDB on `localhost:27018`
- Producer API on `localhost:3000`
- worker-1, worker-2, worker-3

### Dashboard

```
http://localhost:3000/dashboard
```

---

## API Reference

### POST /jobs
Add a job to the queue.

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "send_email", "payload": {"to": "user@example.com"}}'
```

Response:
```json
{
  "status": "queued",
  "messageId": "1783593028450-0",
  "type": "send_email"
}
```

### GET /metrics
Returns current queue stats.

```json
{
  "timestamp": "2026-07-09T21:30:00.000Z",
  "queue": {
    "depth": 42,
    "pending": 3
  },
  "dlq": {
    "depth": 1
  },
  "workers": {
    "worker-1": 1,
    "worker-2": 2,
    "worker-3": 0
  }
}
```

### GET /dashboard
Real-time monitoring dashboard (HTML).

### GET /jobs/stream-info
Returns raw stream length.

---

## How It Works

### Producer
Pushes jobs to Redis Stream via `XADD` with `MAXLEN ~1000` to keep stream size bounded. Each job is a flat key-value entry: `type`, `payload`, `createdAt`, `retries`.

### Consumer Groups
All workers join the `workers` consumer group. `XREADGROUP` with `>` delivers each message to exactly one worker — Redis's single-threaded command processing guarantees no duplicate delivery without any application-level locking.

### Acknowledgment Flow
```
Worker picks up job → job enters Pending Entries List (PEL)
→ Worker processes job successfully
→ Archives to MongoDB
→ XACK removes job from PEL
```

If a worker crashes before ACKing, the job stays in PEL and is recovered on restart via `recoverPendingMessages()`.

### Retry Logic
```
Job fails → retry count incremented → requeued as new message
→ fails again → retry count 2
→ fails again → MAX_RETRIES (3) reached → moved to DLQ → archived as 'failed'
```

### Idle Timeout Recovery
Every 30 seconds, each worker runs `claimAbandonedJobs()` which scans the PEL for messages idle > 30s (indicating a permanently dead worker) and claims them via `XCLAIM` for reprocessing. This ensures no job is permanently lost even if a worker never recovers.

### MongoDB Archiving
Every job is saved to MongoDB **before** being ACKed, ensuring no job history is lost when `MAXLEN` trims the stream. The `messageId` field has a unique index — duplicate archive attempts (e.g. from two workers claiming the same job) are safely ignored.

---

## Stress Test

```bash
node autocannon-test.js
```

Results (10 connections, 5 seconds):

|        Metric     |    Value     |
|-------------------|--------------|
| Throughput        | ~516 req/sec |
| p50 latency       | 17ms         |
| p99 latency       | 37ms         |
| Error rate        | 0%           |
| Total jobs queued | 2,582        |

---

## Trade-off Analysis

### At-least-once vs Exactly-once
This system implements **at-least-once delivery** — a job may be processed more than once if a worker crashes after processing but before ACKing. True exactly-once requires distributed transactions (e.g. Redis + MongoDB two-phase commit), which adds significant complexity. For most use cases (sending emails, notifications, resizing images), at-least-once with idempotent consumers is the right trade-off.

### Redis Streams vs Kafka vs RabbitMQ

|                      |           Redis Streams                |              Kafka               |       RabbitMQ       |
|----------------------|----------------------------------------|----------------------------------|----------------------|
| **Throughput**       | High (single instance)                 | Very high (distributed)          | Moderate             |
| **Setup complexity** | Low (already using Redis)              | High (Zookeeper/KRaft)           | Medium               |
| **Message replay**   | Yes (within MAXLEN)                    | Yes (long-term)                  | No                   |
| **Routing**          | Simple                                 | Topic/partition                  | Complex (exchanges)  |
| **Best for**         | Small-medium scale, Redis-heavy stacks | Event streaming at massive scale | Complex routing, RPC |

**Why Redis Streams here:** The system already uses Redis for other purposes (rate limiting). Adding Kafka for this scale would be operational overhead without meaningful benefit — demonstrating judgment about picking the right tool for the job, not just the trendiest one.

### MAXLEN + Archiving vs Infinite Retention
Infinite stream retention would exhaust Redis memory over time. `MAXLEN ~1000` keeps the stream bounded while MongoDB archiving preserves complete job history for audit and replay. The `~` (approximate trimming) is intentional — it's faster than exact trimming as Redis can trim at internal node boundaries.

### FIFO Ordering vs Throughput
Redis Streams guarantees per-stream ordering (jobs processed in insertion order globally). With multiple consumers, each consumer processes in order but interleaving across consumers means global ordering isn't strictly maintained. This is an acceptable trade-off for throughput — use a single consumer only if strict global ordering is required.

### Blocking Poll vs Tight Loop
`XREADGROUP BLOCK 5000` holds the connection open for up to 5 seconds waiting for new messages rather than looping continuously. This eliminates wasted CPU on idle queues and reduces Redis command overhead significantly.

---

## MongoDB Schema

```javascript
{
  messageId:   String,   // Redis Stream message ID (unique)
  type:        String,   // job type (e.g. "send_email")
  payload:     Mixed,    // job data
  status:      String,   // "completed" | "failed"
  retries:     Number,   // how many times this job was retried
  worker:      String,   // which worker processed it
  reason:      String,   // failure reason (if status = "failed")
  processedAt: Date,
  createdAt:   String,   // original job creation timestamp
}
```

---

## Environment Variables

|    Variable     |               Default                    |        Description        |
|-----------------|------------------------------------------|---------------------------|
| `REDIS_HOST`    | `localhost`                              | Redis hostname            |
| `REDIS_PORT`    | `6379`                                   | Redis port                |
| `PRODUCER_PORT` | `3000`                                   | Producer API port         |
| `CONSUMER_NAME` | `consumer-1`                             | Unique worker identity    |
| `MONGO_URI`     | `mongodb://localhost:27017/messagequeue` | MongoDB connection string |