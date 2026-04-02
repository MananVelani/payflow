# M3 deliverables: Week 4 (Load Testing & Profiling)

This branch contains Member 3's independent deliverables for Week 4. It focuses on validating the API Gateway performance requirements and enabling generic profiling for Chaos Day testing.

## Accomplishments

### 1. Concurrency Simulation (`payflow/loadtest/main.go`)
- Crafted a specialized multi-goroutine worker pool configured out-of-the-box to spawn **1,000 distinct concurrent payments**.
- Utilizes the `sdk` to construct structured JSON payloads safely.
- Added strict metric analytics capturing `req/sec` total throughput alongside latency profiles corresponding directly to the benchmark `p50`, `p95`, and `p99` (<500ms) rulesets designed by the project syllabus.

### 2. Gateway Application Profiling
- Exposed an asynchronous `net/http/pprof` instance routing across port `localhost:6060` inside `gateway/main.go`.
- When integrated into Chaos Testing day, metrics across CPUs/Heap allocation inside the Gateway node are explicitly decoupled from regular application flow via native endpoints.

### 3. Code Review Readiness
- Audited `gateway/main.go` and `sdk/client.go` to ensure `Mutex` wraps securely around node changes (e.g., `Leader Cache Update`) preserving strict memory barriers.
- Implemented robust and transparent Go doc-comments enforcing export formatting standards.
