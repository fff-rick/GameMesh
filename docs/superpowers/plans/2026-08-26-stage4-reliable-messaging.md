# Stage 4 Reliable Messaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add bounded, testable Seq/ACK/Retry/Dedup reliable-delivery semantics to the authenticated single-node Gateway without implementing reconnect/resume.

**Architecture:** A new `internal/reliability` package owns per-Session inbound sequence/dedup state and outbound bounded pending state. Gateway classifies message types as reliable or unreliable, validates reliable inbound messages before Backend dispatch, emits cumulative ACKs, tracks reliable outbound envelopes, and runs one server-level retry scanner.

**Tech Stack:** Go 1.23 standard library, existing protobuf wire codec, existing WebSocket transport, existing metrics exporter.

**Spec:** `docs/03-阶段性任务.md` Stage 4 and `docs/stage-0/03-Envelope协议契约.md`.

## Global Constraints

- Do not implement Resume Token, Grace Period, pending recovery across connections, Redis, or multi-node state.
- Pending and dedup storage must be strictly bounded.
- Retry must have both timeout and maximum retry count; no infinite retry.
- Reliable classification is explicit per MessageType; the default for business messages remains unreliable for backward compatibility.
- ACK is cumulative transport receipt and does not mean Backend business success.
- Gateway must never dispatch the same accepted reliable inbound MessageID twice within the live Session.

---

### Task 1: Reliability state machine

**Files:**
- Create: `internal/reliability/reliability.go`
- Create: `internal/reliability/reliability_test.go`

**Interfaces:**
- Produces: `DeliveryClass`, `Classifier`, `StaticClassifier`, `Manager.AcceptInbound`, `Manager.TrackOutbound`, `Manager.Ack`, `Manager.CollectDue`, `Manager.RemoveSession`, `Manager.PendingCount`.

- [x] Write failing tests for in-order acceptance, duplicate detection, out-of-order rejection, MessageID conflict, cumulative ACK, pending limit, retry exhaustion, and uint64 Seq exhaustion.
- [x] Run `go test ./internal/reliability -count=1` and confirm RED because the package/API does not exist.
- [x] Implement the smallest per-session state machine satisfying the tests using a mutex, bounded pending map, and bounded dedup queue.
- [x] Run `go test ./internal/reliability -count=1` and confirm GREEN.
- [x] Commit the reliability core.

### Task 2: Protocol and configuration contract

**Files:**
- Modify: `internal/protocol/envelope.go`
- Modify: `api/proto/envelope.proto`
- Modify: `internal/config/config.go`
- Modify: `internal/protocol/envelope_test.go`

**Interfaces:**
- Produces: `MessageTypeAck = 15`; configuration values for retry interval, max retries, pending limit, and dedup window.

- [x] Write failing tests proving ACK is a known control type and an Envelope preserves MessageID/Seq/Ack.
- [x] Run the protocol tests and confirm RED for the new ACK type.
- [x] Add the ACK enum and Stage 4 reliability configuration defaults.
- [x] Run protocol/config tests and confirm GREEN.
- [x] Commit protocol/config changes.

### Task 3: Gateway inbound dedup and ACK

**Files:**
- Modify: `internal/gateway/server.go`
- Modify: `internal/gateway/server_test.go`

**Interfaces:**
- Consumes: reliability classifier and manager.
- Produces: reliable inbound validation before `handleBusiness`; duplicate packets return cumulative ACK without Backend redispatch; out-of-order packets return controlled error.

- [x] Add an integration test where the same reliable MessageID/Seq is sent twice and Backend call count remains exactly one.
- [x] Add an out-of-order test proving Seq 2 before Seq 1 is rejected and Backend is not called.
- [x] Run the targeted gateway tests and confirm RED.
- [x] Add `WithReliabilityClassifier`, process `MessageTypeAck`, and gate reliable inbound business messages before routing.
- [x] Run targeted and full gateway tests and confirm GREEN.
- [x] Commit inbound reliability.

### Task 4: Outbound Pending Queue and Retry

**Files:**
- Modify: `internal/gateway/server.go`
- Modify: `internal/gateway/server_test.go`

**Interfaces:**
- Produces: reliable outbound envelopes receive generated MessageID and Seq, enter bounded pending storage before enqueue, retry on one server ticker, clear on cumulative ACK, and close the connection on retry exhaustion or pending overflow.

- [x] Add tests for dropped ACK causing retransmission with identical MessageID/Seq, delayed ACK stopping later retries, close-before-ACK clearing pending state, ACK concurrent with retransmission, and pending overflow.
- [x] Run the targeted tests and confirm RED.
- [x] Implement server retry loop and reliable outbound send path.
- [x] Run targeted tests repeatedly and confirm GREEN.
- [x] Commit outbound reliability.

### Task 5: Metrics and Stage 4 acceptance

**Files:**
- Modify: `internal/metrics/metrics.go`
- Modify: `internal/metrics/metrics_test.go`
- Create: `docs/stage-4/01-可靠消息语义.md`
- Create: `docs/stage-4/02-测试报告.md`
- Create: `docs/stage-4/00-阶段4验收报告.md`
- Modify: `README.md`

**Interfaces:**
- Produces: low-cardinality pending/retry/dedup/out-of-order metrics and Stage 4 acceptance evidence.

- [x] Add failing metrics tests for reliability counters/gauges.
- [x] Implement metrics and confirm GREEN.
- [x] Run `gofmt -l .`, `go test ./... -count=1`, `go test -race ./...`, and `go vet ./...`.
- [x] Stress the reliability and gateway reliability tests with `-count=20`.
- [x] Write Stage 4 semantics, test report, and acceptance report from actual command output.
