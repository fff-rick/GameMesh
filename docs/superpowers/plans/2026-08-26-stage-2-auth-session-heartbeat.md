# Stage 2 Authentication, Session, and Heartbeat Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Upgrade the Stage 1 connection-only Gateway into authenticated player sessions with deterministic duplicate-login behavior, heartbeat handling, idle cleanup, and online-state metrics.

**Architecture:** Keep `Connection` as transport lifecycle owner. Add a pluggable `auth.Authenticator`, a concurrency-safe `session.Manager`, and a single server heartbeat scanner; `Server` coordinates these units and gates business messages until authentication. New-login-wins is resolved atomically in the session manager, then the replaced connection is closed outside the manager lock.

**Tech Stack:** Go 1.23 standard library, RFC 6455 implementation already in repo, Protobuf wire-compatible manual encoding already in repo, `log/slog`, standard Prometheus exposition text.

**Spec:** `docs/03-阶段性任务.md` Stage 2 plus `docs/stage-0/05-Session状态机.md`.

## Global Constraints

- Stage 2 only: Authentication Hook, ConnID/UserID/SessionID, Session Manager, duplicate login, Heartbeat, Idle Timeout, online-state Metrics.
- Default duplicate login policy: New Login Wins.
- No resume/grace-period behavior before Stage 5; a lost Stage 2 connection terminates its Session.
- No Backend routing, reliable messaging, Redis, rate limiting, or graceful drain implementation in this stage.
- Existing Stage 1 bounded send queue and 64 KiB client Envelope limit remain unchanged.
- No third-party Go modules; repository must remain offline-buildable.

---

### Task 1: Control protocol and Authentication Hook

**Files:**
- Modify: `internal/protocol/envelope.go`
- Create: `internal/protocol/control.go`
- Test: `internal/protocol/control_test.go`
- Create: `internal/auth/auth.go`
- Test: `internal/auth/auth_test.go`

**Interfaces:**
- Produces: `MessageTypeAuthRequest`, `MessageTypeAuthResult`, `MessageTypeHeartbeatRequest`, `MessageTypeHeartbeatResponse`.
- Produces: `protocol.AuthRequest`, `protocol.AuthResult`, marshal/unmarshal helpers.
- Produces: `auth.Authenticator.Authenticate(context.Context,string) (string,error)` and `auth.DevAuthenticator`.

- [x] Write tests that require control MessageTypes to validate, AuthRequest/AuthResult protobuf-wire round trips, and dev token `user:<id>` validation.
- [x] Run targeted tests and verify RED due to missing control protocol/auth package.
- [x] Implement only the protocol/auth behavior required by tests.
- [x] Run targeted tests and full `go test ./...` to verify GREEN.

### Task 2: Session Manager and duplicate login semantics

**Files:**
- Create: `internal/session/session.go`
- Test: `internal/session/session_test.go`

**Interfaces:**
- Produces: `Manager.Register(userID,connID) (current Session, replaced *Session, error)`.
- Produces: `Manager.TerminateByConn(connID) *Session`, `ByConn`, `ByUser`, `ActiveCount`.
- Session contains immutable `ID`, `UserID`, `ConnID`, `CreatedAt` while active.

- [x] Write tests for register, terminate, and concurrent same-user registration proving exactly one active winner.
- [x] Run tests and verify RED.
- [x] Implement a mutex-protected manager with atomic New Login Wins resolution.
- [x] Run targeted tests and full suite to verify GREEN.

### Task 3: Server authentication, identity binding, and business-message gate

**Files:**
- Modify: `internal/gateway/connection.go`
- Modify: `internal/gateway/server.go`
- Modify: `internal/gateway/server_test.go`

**Interfaces:**
- `Connection.Authenticated()`, `UserID()`, `SessionID()` expose synchronized identity.
- `gateway.WithAuthenticator(auth.Authenticator)` supplies test/production auth hook.
- AuthRequest is the only authentication entry point; business messages (`>=1000`) and EchoRequest are rejected before auth.

- [x] Write integration tests for correct token, invalid/expired token, unauthenticated business/echo blocking, and same-user duplicate login where old connection is closed.
- [x] Run targeted tests and verify RED.
- [x] Wire auth and session manager into Server; bind identity only after successful registration; terminate session from connection close callback.
- [x] Run targeted tests and full suite to verify GREEN.

### Task 4: Heartbeat, Idle Timeout, and half-open cleanup

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/gateway/connection.go`
- Modify: `internal/gateway/server.go`
- Modify/Test: `internal/gateway/server_test.go`

**Interfaces:**
- Config adds `HeartbeatCheckInterval` and `IdleTimeout` with production-safe defaults.
- Every successfully parsed Envelope touches connection activity.
- Authenticated HeartbeatRequest yields HeartbeatResponse.
- A single Server heartbeat goroutine closes OPEN connections idle beyond the configured timeout.

- [x] Write tests for normal heartbeats keeping a connection alive, stopped heartbeat closing it, half-open transport cleanup, and concurrent Close vs heartbeat scan.
- [x] Run targeted tests and verify RED.
- [x] Implement last-seen tracking and one server scanner with clean shutdown.
- [x] Run targeted tests, full suite, and `go test -race ./...` to verify GREEN.

### Task 5: Metrics, CLI behavior, Stage 2 docs and final verification

**Files:**
- Modify: `internal/metrics/metrics.go`
- Test: `internal/metrics/metrics_test.go`
- Modify: `cmd/gateway/main.go`
- Modify: `README.md`
- Create: `docs/stage-2/00-阶段2验收报告.md`
- Create: `docs/stage-2/01-实现说明.md`
- Create: `docs/stage-2/02-测试报告.md`

**Interfaces:**
- Add active-session gauge, auth result counters, heartbeat timeout counter.
- CLI default authenticator accepts development tokens `user:<id>` only and documents that it is a hook/demo, not production identity verification.

- [x] Write metrics test asserting active-session/auth/heartbeat exposition.
- [x] Run metrics test and verify RED.
- [x] Implement metrics and wire all state transitions without high-cardinality ID labels.
- [x] Update README and Stage 2 implementation/test/acceptance docs.
- [x] Run `gofmt`, `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- [x] Run a goroutine lifecycle regression test and record Stage 2 acceptance status.
