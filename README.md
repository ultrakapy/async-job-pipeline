# Async Job Processing Pipeline

A production-ready REST API backed by an asynchronous job queue. Accepts work submissions,
executes them with configurable retry and timeout semantics, and provides full job lifecycle
visibility to callers.

Built in Go using only the standard library — zero third-party dependencies.

---

## Architecture

```
HTTP client
     │
     ▼  POST /api/v1/jobs → 202 + job ID
API handler ──────────────────────► Job store (save: queued)
     │                                   ▲         ▲
     │ enqueue                            │         │
     ▼                                   │ update   │ update
Channel queue (buffered chan *Job)        │ running  │ succeeded
     │                                   │         │
     │ dequeue                           │         │
     ▼                                   │         │
Worker pool (N goroutines) ─────────────►│         │
     │                                             │
     │ execute with context.WithTimeout            │
     ▼                                             │
Handler exec ────────────────────────────────────►(success)
     │
     │ failure (attempts < max_retries)
     ▼
Exp. backoff (2ⁿ second delay)
     │
     │ re-enqueue
     ▼
Channel queue ◄──── (retry loop)

Handler exec ──► (attempts ≥ max_retries) ──► Dead-letter store
                                              Job store (dead_lettered)
```

### Job state machine

```
queued → running → succeeded
                ↘ failed → queued  (retry, up to max_retries times)
                         ↘ dead_lettered
cancelled  (set via DELETE /api/v1/jobs/{id} while queued or failed)
```

### At-least-once delivery

| Guarantee | Scope |
|---|---|
| **At-least-once within a process lifetime** | A failed job is retried up to `max_retries` times before being dead-lettered. Jobs are never silently dropped. |
| **Not guaranteed across restarts** | The queue is an in-memory buffered channel. Jobs that are in-flight or enqueued at process shutdown are lost unless graceful drain completes within the 30-second window. |
| **Duplicate execution is bounded** | A job can execute more than once only if the handler succeeds but the subsequent store update fails (rare). Handlers should treat the job ID as an idempotency key to guard against this. |

For true durable at-least-once across restarts, replace `ChannelQueue` with a Redis-backed queue using `LPUSH`/`BRPOP` with a visibility timeout, so a job stays in the queue until explicitly ACKed after a successful store commit.

---

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/jobs` | Submit a job. Returns `202` with `job_id`. |
| `GET` | `/api/v1/jobs` | List all jobs. |
| `GET` | `/api/v1/jobs/{id}` | Get job status, attempt count, last error, and result. |
| `DELETE` | `/api/v1/jobs/{id}` | Cancel a queued or failed job. |
| `GET` | `/api/v1/queue/depth` | Current number of jobs waiting in the queue. |
| `POST` | `/api/v1/queue/drain` | Cancel all queued jobs and empty the queue. |
| `GET` | `/api/v1/dead-letter` | List all dead-lettered jobs. |
| `GET` | `/health` | Health check. Returns `{"status":"ok"}`. |

### Submit a job

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "payload":      {"task": "process-invoice", "invoice_id": "INV-001"},
    "priority":     5,
    "max_retries":  3,
    "timeout_secs": 30
  }'
# {"job_id":"a3f2..."}
```

**Request fields**

| Field | Type | Default | Description |
|---|---|---|---|
| `payload` | `map[string]any` | — | Arbitrary JSON passed to the handler. |
| `priority` | `int` | `0` | `0` = low, `5` = normal, `10` = high. Informational in this implementation; see [Extensions](#extensions--next-steps). |
| `max_retries` | `int` | `3` | Maximum execution attempts before dead-lettering. |
| `timeout_secs` | `int` | `30` | Per-attempt timeout in seconds. |
| `run_after` | `time` (RFC3339) | now | Earliest time to begin execution (delayed jobs). |

### Poll job status

```bash
curl -s http://localhost:8080/api/v1/jobs/a3f2...
```

```json
{
  "id":          "a3f2...",
  "status":      "succeeded",
  "attempts":    1,
  "last_error":  "",
  "result":      {"echo": {"task": "process-invoice"}, "processed_at": "..."},
  "created_at":  "2026-06-23T10:00:00Z",
  "updated_at":  "2026-06-23T10:00:01Z"
}
```

### Inspect the dead-letter queue

```bash
curl -s http://localhost:8080/api/v1/dead-letter
```

Returns an array of jobs that exhausted all retries, including `last_error` and `attempts`.

---

## Setup

### Prerequisites

- Go 1.22+
- Docker (for containerised runs and deployment)
- `make`

### Run locally

```bash
git clone https://github.com/ultrakapy/async-job-pipeline
cd async-job-pipeline

# Run tests
make test

# Build and start
make run
# Server listening on :8080
```

### Run with Docker

```bash
docker build -t async-job-pipeline .
docker run -p 8080:8080 \
  -e WORKER_COUNT=8 \
  -e QUEUE_CAPACITY=2000 \
  async-job-pipeline
```

---

## Worker configuration

Workers are configured via environment variables.

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server port. |
| `WORKER_COUNT` | `4` | Number of concurrent worker goroutines. |
| `QUEUE_CAPACITY` | `1000` | Buffered channel capacity. When full, `Enqueue` blocks (backpressure). |

### Sizing guidance

- **CPU-bound handlers:** `WORKER_COUNT` ≈ number of logical cores.
- **I/O-bound handlers:** `WORKER_COUNT` can be 2–4× logical cores; workers spend most of their time waiting.
- **`QUEUE_CAPACITY`:** Set to at least `WORKER_COUNT × expected_burst_seconds × submission_rate`. A depth of 1000 with 4 workers and 30-second jobs comfortably absorbs ~8 minutes of burst.

### Plugging in a custom handler

Edit `cmd/server/main.go` and replace `echoHandler`:

```go
func myHandler(ctx context.Context, payload map[string]any) (map[string]any, error) {
    // ctx carries the per-attempt deadline from timeout_secs.
    // Return (result, nil) on success or (nil, err) to trigger retry/dead-letter.
    invoiceID := payload["invoice_id"].(string)
    result, err := processInvoice(ctx, invoiceID)
    if err != nil {
        return nil, err
    }
    return map[string]any{"invoice": result}, nil
}

// In main():
pool := worker.NewPool(cfg.WorkerCount, q, st, dl, myHandler)
```

The `handler` signature is `func(ctx context.Context, payload map[string]any) (map[string]any, error)`. The worker supplies a `context.WithTimeout` derived from `timeout_secs`; honour `ctx.Done()` in long-running operations.

---

## Handling high load

### Backpressure

The channel queue is a fixed-capacity buffer. When full, `Enqueue` blocks. In production, switch to a non-blocking enqueue and return `HTTP 503` immediately:

```go
select {
case q.ch <- job:
    // enqueued
default:
    http.Error(w, `{"error":"queue full"}`, http.StatusServiceUnavailable)
    return
}
```

This prevents the HTTP layer from accumulating unbounded goroutines under spike load.

### Scaling workers independently of the API tier

The `Queue` interface is the seam:

```go
type Queue interface {
    Enqueue(job *domain.Job)
    Dequeue() <-chan *domain.Job
    Depth() int64
    Drain() []*domain.Job
}
```

Swap `ChannelQueue` for a `RedisQueue` (`LPUSH`/`BRPOP`) and you can run:

- **N API replicas** — each accepts submissions and writes to Redis
- **M worker replicas** — each pulls from the same Redis queue

Both tiers scale independently on DigitalOcean App Platform by adjusting `instance_count`.

### Priority queues

The current queue is FIFO. To support `priority` semantics:

- **In-process:** replace the channel with a `container/heap` min-heap behind a mutex; workers block on a `sync.Cond` when empty.
- **Redis-backed:** use a sorted set keyed by `priority_bucket:enqueue_time`; `BZPOPMAX` dequeues the highest-priority job.

### Observability signals to watch under load

| Signal | Threshold action |
|---|---|
| `GET /api/v1/queue/depth` rising continuously | Add workers or API replicas |
| Dead-letter rate increasing | Handler is flaky; inspect `last_error`; consider circuit breaker |
| p95 job latency > SLA | Reduce `QUEUE_CAPACITY` to shed load earlier; scale workers |
| Worker goroutines blocked in `Enqueue` | Enable non-blocking enqueue + 503 response |

---

## Testing

```bash
# All tests with race detector
make test

# Verbose
go test -v -race -timeout 60s ./...
```

Test coverage:

| Package | What is covered |
|---|---|
| `internal/store` | Save, get, update, duplicate detection, copy semantics, list |
| `internal/queue` | Enqueue, dequeue, depth, drain |
| `internal/worker` | Success, retry-then-succeed, dead-letter after max retries, per-attempt timeout |
| `internal/api` | Submit + poll, queue depth + drain, cancel, health, 404 on missing job |

---

## CI/CD

GitHub Actions runs on every push and pull request:

1. `go vet ./...` — static analysis
2. `go test -v -race -timeout 60s ./...` — full test suite with race detector
3. `go build -o bin/server ./cmd/server` — verifies the binary compiles

See `.github/workflows/ci.yml`.

---

## Deployment

### DigitalOcean App Platform

```bash
# Authenticate
doctl auth login

# Deploy from app.yaml (connects to your GitHub repo)
doctl apps create --spec app.yaml

# Check status
doctl apps list
```

The `app.yaml` spec:

- Builds from the `Dockerfile` on push to `main`
- Exposes port `8080` with a `/health` HTTP health check
- `WORKER_COUNT` and `QUEUE_CAPACITY` are runtime environment variables — adjust via the DO console or `doctl apps update` without rebuilding

### Health check

The App Platform polls `GET /health` every 10 seconds (configured in `app.yaml`). The endpoint returns `{"status":"ok"}` and is always fast — it does not check downstream dependencies.

### Scaling workers independently

In `app.yaml`, add a second service component with `WORKER_COUNT` set high and no HTTP port:

```yaml
services:
  - name: api
    instance_count: 2
    instance_size_slug: basic-xxs
    # ... (http_port: 8080, health_check, etc.)

  - name: worker
    instance_count: 4
    instance_size_slug: basic-sm
    envs:
      - key: WORKER_COUNT
        value: "8"
      - key: QUEUE_CAPACITY
        value: "0"   # workers only — no queue needed if backed by Redis
```

> **Note:** Independent worker scaling requires replacing `ChannelQueue` with a Redis-backed queue. With the in-memory channel, API and workers must run in the same process.

---

## Extensions & next steps

| Feature | Notes |
|---|---|
| **Delayed jobs** | Implemented via `run_after` field. A goroutine sleeps until `time.Until(*run_after)` then enqueues. For large volumes, replace with a Redis sorted set polled by a scheduler goroutine. |
| **Recurring jobs** | Add a `cron_expr` field; a background goroutine uses a cron parser to enqueue on schedule. |
| **Priority queue** | Replace `ChannelQueue` with a heap or Redis sorted set (see [Handling high load](#handling-high-load)). |
| **Durable queue** | Replace `ChannelQueue` with `RedisQueue` (LPUSH/BRPOP + visibility timeout) for at-least-once across restarts. |
| **Observability** | Instrument with Prometheus: `job_queue_depth`, `job_duration_seconds` (p50/p95), `worker_utilization_ratio`, `dead_letter_total`. Expose via `/metrics`. |
| **Dead-letter replay** | Add `POST /api/v1/dead-letter/{id}/replay` to re-enqueue a dead-lettered job after manual inspection. |

---

## Project structure

```
.
├── cmd/
│   └── server/
│       └── main.go          # Entry point: wires store, queue, worker pool, HTTP server
├── internal/
│   ├── api/
│   │   ├── handler.go       # HTTP handlers
│   │   ├── handler_test.go  # Integration tests (httptest)
│   │   └── router.go        # Route registration (Go 1.22 ServeMux)
│   ├── config/
│   │   └── config.go        # Env-var configuration
│   ├── domain/
│   │   └── job.go           # Job struct, Status type, request/response types
│   ├── queue/
│   │   ├── queue.go         # Queue interface + ChannelQueue
│   │   └── queue_test.go
│   ├── store/
│   │   ├── memory.go        # MemoryStore (sync.RWMutex map)
│   │   ├── deadletter.go    # MemoryDeadLetterStore
│   │   └── memory_test.go
│   └── worker/
│       ├── worker.go        # Pool, retry logic, dead-letter path
│       └── worker_test.go
├── .github/
│   └── workflows/
│       └── ci.yml           # Go vet + test + build on push
├── app.yaml                 # DigitalOcean App Platform spec
├── Dockerfile               # Multi-stage build
├── Makefile
└── README.md
```

---

## License

MIT
