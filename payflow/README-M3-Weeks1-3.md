# M3 deliverables: Weeks 1, 2, and 3

This branch contains Member 3's core implementations for API Gateway mappings, Go Client SDK, and Distributed Tracing integration to fulfill Weeks 1 through 3 syllabus requirements.

## Accomplishments (Week 1 & 2: API Gateway & SDK)

### 1. Protobuf Consolidation (`payflow/proto`)
- Upgraded `payment.proto` by exposing two new RPCs to support API required interactions: `GetPaymentStatus` and `SubmitBatch`.
- Expanded the `SubmitTaskResponse` (and nested it inside the `SubmitBatchResponse`) with a `leader_address` string format to support elegant failover processing.
- Re-generated all gRPC stubs through the `protoc` compiler correctly rendering the newly injected payload properties into the Go ecosystem.

### 2. C1 API Gateway (`payflow/gateway`)
The Gateway (C1) now fulfills all specified functionality and gracefully handles faults within the Coordinator domain:
- **`POST /v1/payments` & `POST /v1/batch`**: Full handling for array/single task injection into the coordinator queue with exponential retry envelopes.
- **`GET /v1/payments/{id}`**: A new endpoint checking the status of an ongoing queued event or confirming an idempotency hit. 
- **Leader Redirect (C05)**: The Gateway implements a caching structure checking logic for `NOT_LEADER`. When standard proxy requests hit an out-of-sync backend, the response intercept reads the new coordinator string, automatically switches dials, and resends the transaction transparently, protecting the client.

### 3. Go Client SDK (`payflow/sdk`)
A distinct standalone entity has been built covering seamless integrations logic for consumers wanting to inject payloads into the cluster:
- Defined struct methodologies for `SubmitPayment`, `SubmitBatch`, and `GetStatus` interacting directly against the Gateway API.
- Included an intrinsic **Exponential Backoff** component resolving transaction 5xx failures smoothly across `1s, 2s, 4s` configurable spans.

## Accomplishments (Week 3: Distributed Tracing & Telemetry)

### 1. Tracing In C1 API Gateway 
The Gateway (C1) now fulfills all tracing specifications:
- **`initTracer()` Pipeline**: Bootstrapped an `sdktrace.TracerProvider` pointed permanently at `http://jaeger:14268/api/traces` corresponding with standard decoupled Docker topology.
- **Implicit gRPC Propagation**: Hooked the `grpc.WithStatsHandler(otelgrpc.NewClientHandler())` middleware inside the dynamic `conn` routing setup. Any call moving across `client.SubmitTask` inherently encapsulates metadata containing the OpenTelemetry `trace_id`.
