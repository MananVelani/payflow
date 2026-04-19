# ADR-001: In-Memory vs. Persistent Outbox

**Date:** 2026-03-31
**Status:** Accepted
**Deciders:** C3 Worker Service Team

---

## Context

The PayFlow C3 Worker Service must report payment results to the C2 Coordinator via `ReportResult`. This is a critical path: if C3 crashes or C2 is transiently unavailable between the bank charge succeeding and the result being delivered, the payment outcome is lost and C2 may retry, causing a double-charge.

### Phase 1 (Week 1) — Why in-memory was acceptable

In the first implementation sprint, C3 used a simple in-memory queue for buffering undelivered results. This was acceptable because:
- The service was a **single-replica proof of concept** running in a controlled test environment
- Crash recovery was out of scope — the goal was to validate the overall pipeline architecture
- The development velocity benefit of no external dependencies outweighed the durability risk

### The Problem — Why in-memory is not acceptable in production

An in-memory outbox loses all buffered entries on pod restart. In production, this means:

1. **C3 pod crashes** after charging the bank but before C2 acknowledges `ReportResult`
2. **Buffered result is lost** — the pod restarts with an empty queue
3. **C2 never learns** that the payment succeeded
4. **C2 resubmits the task** → C3 charges the bank **again** → double-charge

Additionally, in Phase 2 the service adopted distributed reservation (Redis) and epoch-based fencing specifically to prevent double-execution. An in-memory outbox created a gap that undermined those guarantees.

---

## Decision

Use **BadgerDB** as the default outbox persistence layer, with an **in-memory fallback** for environments where a filesystem is not available.

### Architecture

```
outbox.New(...)
  ├── outbox.Store interface
  │     ├── BadgerStore   ← default (OUTBOX_DB_PATH set)
  │     └── MemoryStore   ← fallback (OUTBOX_DB_PATH empty, logs warning)
  └── relay goroutine  ← ticks every OUTBOX_FLUSH_INTERVAL
```

**BadgerStore** (`internal/outbox/badger_store.go`):
- Persists entries as serialised protobuf in BadgerDB key-value store
- Survives pod restarts, OOM kills, and node evictions (assuming the PVC is retained)
- Used for task lease recovery at startup (`ListLeases`, `SetLease`, `DeleteLease`)

**MemoryStore** (`internal/outbox/memory_store.go`):
- Zero-dependency, zero-configuration
- Suitable for local development and integration tests
- Data is lost on every restart — a WARN is logged at startup if this is used

### Stale Entry Policy

Entries older than `MAX_TASK_DURATION` are silently dropped during flush with a `WARN` log and a `worker_task_deadline_exceeded_total{stage="outbox"}` metric increment. This prevents the outbox from becoming a long-lived dead-letter queue for tasks that C2 has already given up on.

---

## Alternatives Considered

### Alternative A: Redis as outbox store

**Pros:** Already present for distributed reservation; no extra dependency.
**Cons:**
- Redis is **optional** in this deployment (not all environments have it)
- Redis TTL-based expiry is harder to control than BadgerDB's append-and-ack pattern
- Redis Pub/Sub would add significant architectural complexity for the relay goroutine
- Single Redis pod is a SPOF risk for what is already a reliability mechanism

**Decision:** Rejected. BadgerDB is embedded — no network dependency, no SPOF.

### Alternative B: WAL file (write-ahead log, custom)

**Pros:** Minimal dependency surface.
**Cons:**
- Significant implementation effort with no library support
- Crash-consistency (fsync ordering) is subtle to get right
- BadgerDB already provides exactly this guarantee

**Decision:** Rejected in favour of using a proven library.

### Alternative C: Keep in-memory, add crash-recovery via C2 resubmission

**Pros:** No persistence changes needed.
**Cons:**
- Relies on C2 detecting missed results and resubmitting — this tight coupling contradicts the design goal of C3 being independently resilient
- Increases C2 complexity and C2–C3 coordination surface
- Still has a window where a bank charge is unacknowledged

**Decision:** Rejected. C3 should be self-healing for its own output.

---

## Consequences

### ✅ Positive

- **Crash durability:** Undelivered results survive pod restarts as long as the PVC is retained
- **Lease recovery:** Orphaned task leases are recovered at startup, preventing zombie tasks
- **Zero network dependency:** BadgerDB is embedded — no external process required
- **Graceful degradation:** MemoryStore fallback keeps local dev simple

### ⚠️ Negative / Trade-offs

- **Added dependency:** BadgerDB (`github.com/dgraph-io/badger/v4`) adds ~4 MB to the binary and ~12 MB to `go.sum`
- **PVC lifecycle management:** Production Kubernetes deployments must mount a `PersistentVolumeClaim` at `OUTBOX_DB_PATH`. Deleting the PVC permanently loses undelivered results — document this in runbooks (see `docs/RUNBOOK.md`)
- **DB file locking:** BadgerDB holds an exclusive file lock. Only one C3 instance may share a PVC — multi-replica deployments must use separate PVCs per pod (use `volumeClaimTemplates` in a StatefulSet or pod-identity-based PVC naming)
- **Compaction overhead:** BadgerDB periodically runs value-log GC (`RunValueLogGC`). For the outbox workload (small records, short lifetime) this is negligible but should be monitored in very high-throughput environments (> 10k tasks/s)

---

## Additional ADRs

The following are planned architecture decisions spanning CP-A through CP-K. Each warrants its own ADR file.

| ADR | Topic | Status |
|-----|-------|--------|
| ADR-001 | In-memory vs. persistent outbox (this document) | Accepted |
| ADR-002 | Distributed reservation: local-only vs. Redis-backed | Planned |
| ADR-003 | gRPC contract version handshake (x-contract-version header) | Planned |
| ADR-004 | Epoch-based fencing vs. distributed locking | Planned |
| ADR-005 | Exactly-once delivery: idempotency key scope and lifetime | Planned |
| ADR-006 | Circuit breaker placement: per-handler vs. global | Planned |
| ADR-007 | Durable semaphore leases for crash-resilient concurrency | Planned |
