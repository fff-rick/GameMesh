# Stage 3 Backend Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make Gateway route authenticated business envelopes to independent Backend services with explicit routing, timeout, error mapping, fast failure, and observability.

**Architecture:** Keep Gateway transport/session concerns separate from backend business handling. A router resolves MessageType -> backend type, User -> Room, and Room -> BackendInstance, then calls a transport-neutral BackendClient under context timeout. The gRPC service/message contract is frozen in `.proto`; because grpc-go cannot be downloaded in the execution environment, tests use the same BackendClient contract with an in-process Fake Backend and the missing grpc-go transport adapter is explicitly reported rather than substituted with another protocol.

**Tech Stack:** Go standard library, existing Protobuf wire helpers, `.proto` gRPC contract, in-process fake backend for deterministic integration tests.

**Spec:** `docs/03-阶段性任务.md` Stage 3 and `docs/02-方案设计.md` Service Router / Message Router sections.

## Global Constraints

- No game rules in Gateway.
- Do not add Stage 4 Seq/ACK/Retry/Dedup behavior.
- Authenticated business messages only.
- Backend calls have strict timeouts and no unbounded retry/queue.
- Routing maps User -> Room and Room -> BackendInstance.
- Unknown MessageType fails without guessing a backend.
- Backend transport errors map to stable Gateway infrastructure error codes.
- Real grpc-go transport is not compilable offline; do not replace it with net/rpc or HTTP and call it gRPC.

---

### Task 1: Backend contract and routing tables
**Files:** create `api/proto/backend.proto`, `internal/backend/backend.go`, `internal/routing/router.go`, tests beside packages.
- [x] Write failing tests for MessageType route, User->Room, Room->Instance, unknown/missing routes, and backend request/response contract.
- [x] Run tests and confirm RED.
- [x] Implement minimal contract and concurrency-safe static router.
- [x] Run tests and confirm GREEN.
- [x] Commit.

### Task 2: Backend call timeout and error mapping
**Files:** create/modify `internal/backend/backend.go`, `internal/backend/backend_test.go`.
- [x] Write failing tests for success, timeout, unavailable, and backend-declared error.
- [x] Confirm RED.
- [x] Implement Caller with context timeout and stable error categories; no retries.
- [x] Confirm GREEN.
- [x] Commit.

### Task 3: Gateway business routing integration
**Files:** modify `internal/gateway/server.go`; modify/create gateway tests.
- [x] Write failing integration tests for normal request, unknown MessageType, missing User->Room, missing Room->Instance, backend error, backend timeout, and backend unavailable.
- [x] Confirm RED.
- [x] Inject router/backend registry and route authenticated business messages without parsing game Payload.
- [x] Return a controlled backend result envelope and keep Session ACTIVE on backend failure.
- [x] Confirm GREEN.
- [x] Commit.

### Task 4: Concurrent RPC and route-table changes
**Files:** routing/backend/gateway tests.
- [x] Write stress tests for concurrent requests and route updates.
- [x] Confirm RED if new behavior is missing.
- [x] Add only synchronization required for correctness.
- [x] Confirm GREEN and race-clean.
- [x] Commit.

### Task 5: Metrics, docs, and Stage 3 acceptance
**Files:** modify `internal/metrics/metrics.go`, `README.md`; create `docs/stage-3/*`.
- [x] Add tests for backend request/result/latency metrics without high-cardinality labels.
- [x] Confirm RED then implement metrics.
- [x] Document gRPC contract, implementation, tests, and the offline grpc-go adapter limitation.
- [x] Run gofmt, full tests, race detector, vet, and repeated backend routing stress tests.
- [x] Compare every Stage 3 acceptance requirement and report PASS/PARTIAL accurately.
