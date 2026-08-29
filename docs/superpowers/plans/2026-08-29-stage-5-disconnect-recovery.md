# Stage 5 Disconnect Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a single Gateway instance restore a recently disconnected authenticated session, including reliable outbound state, using a one-time Resume Token.

**Architecture:** Extend the in-memory Session Manager with a disconnected-grace state and an atomic token-based resume transition. Gateway keeps reliability state while the Session is in grace, binds the replacement connection atomically on resume, replays pending messages in sequence order, and centrally expires abandoned sessions.

**Tech Stack:** Go 1.25, standard-library synchronization and crypto/rand, existing WebSocket transport, protobuf-wire control payloads, Go testing.

**Spec:** `docs/superpowers/specs/2026-08-29-stage-5-disconnect-recovery-design.md`

## Global Constraints

- Scope is single-Gateway in-memory recovery only; do not add Redis or a new dependency.
- Resume Tokens are opaque CSPRNG secrets, are not logged or used as Metrics labels, and rotate after every successful resume.
- A Session has at most one effective connection; old connection closure cannot remove a resumed Session.
- Grace sessions retain Reliability Manager state; expiry removes both Session and reliability state.
- Recovery does not call Backend; business traffic after recovery keeps existing Router and Backend error semantics.
- Every production behavior starts with a focused failing test and is verified with `go test` before implementation.

---

### Task 1: Session grace and one-time resume state machine

**Files:**
- Modify: `internal/session/session.go`
- Modify: `internal/session/session_test.go`

**Interfaces:**
- Produces: `NewManager(gracePeriod ...time.Duration) *Manager`, `Disconnect(connID string, now time.Time) *Session`, `Resume(token, connID string, now time.Time) (Session, error)`, `Expire(now time.Time) []Session`.
- Produces: `Session{ResumeToken string, GraceDeadline time.Time}` and errors `ErrInvalidResumeToken`, `ErrSessionNotRecoverable`.

- [ ] **Step 1: Write failing session lifecycle tests**

```go
func TestDisconnectThenResumeKeepsSessionAndRotatesToken(t *testing.T) {
    now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
    m := NewManager(time.Minute)
    original, _, err := m.Register("alice", "old")
    if err != nil { t.Fatal(err) }
    if ended := m.Disconnect("old", now); ended == nil { t.Fatal("disconnect") }
    resumed, err := m.Resume(original.ResumeToken, "new", now.Add(time.Second))
    if err != nil { t.Fatal(err) }
    if resumed.ID != original.ID || resumed.ConnID != "new" || resumed.ResumeToken == original.ResumeToken { t.Fatalf("resumed=%#v", resumed) }
    if _, err := m.Resume(original.ResumeToken, "replay", now.Add(time.Second)); !errors.Is(err, ErrInvalidResumeToken) { t.Fatalf("err=%v", err) }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/session -run TestDisconnectThenResumeKeepsSessionAndRotatesToken -count=1`

Expected: FAIL because the new constructor and recovery methods do not exist.

- [ ] **Step 3: Implement the smallest session state transition**

```go
func (m *Manager) Resume(token, connID string, now time.Time) (Session, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    s, ok := m.byResumeToken[token]
    if !ok || s.GraceDeadline.IsZero() || !now.Before(s.GraceDeadline) { return Session{}, ErrInvalidResumeToken }
    delete(m.byConn, s.ConnID)
    delete(m.byResumeToken, s.ResumeToken)
    s.ConnID, s.GraceDeadline, s.ResumeToken = connID, time.Time{}, newToken()
    m.byConn[connID], m.byUser[s.UserID], m.byResumeToken[s.ResumeToken] = s, s, s
    return s, nil
}
```

- [ ] **Step 4: Run the session package tests**

Run: `go test ./internal/session -count=1`

Expected: PASS.

- [ ] **Step 5: Add expiry and stale-close tests, then implement them**

```go
func TestExpireRemovesOnlyGraceSessionsPastDeadline(t *testing.T) {
    now := time.Now(); m := NewManager(time.Minute)
    grace, _, _ := m.Register("grace", "old"); active, _, _ := m.Register("active", "current")
    m.Disconnect(grace.ConnID, now)
    expired := m.Expire(now.Add(time.Minute))
    if len(expired) != 1 || expired[0].ID != grace.ID { t.Fatalf("expired=%#v", expired) }
    if got, ok := m.ByUser(active.UserID); !ok || got.ID != active.ID { t.Fatal("active removed") }
}
func TestOldConnectionDisconnectDoesNotRemoveResumedSession(t *testing.T) {
    now := time.Now(); m := NewManager(time.Minute); s, _, _ := m.Register("alice", "old")
    m.Disconnect("old", now); resumed, _ := m.Resume(s.ResumeToken, "new", now)
    if ended := m.Disconnect("old", now); ended != nil { t.Fatalf("ended=%#v", ended) }
    if got, ok := m.ByUser("alice"); !ok || got.ConnID != "new" || got.ID != resumed.ID { t.Fatalf("got=%#v", got) }
}
```

Implement `Expire(now)` to delete expired by-user, by-conn and by-token entries and return their sessions.

- [ ] **Step 6: Verify and commit Task 1**

Run: `go test ./internal/session -count=1`

```bash
git add internal/session/session.go internal/session/session_test.go
git commit -m "feat: add recoverable session lifecycle"
```

### Task 2: Resume control protocol

**Files:**
- Modify: `internal/protocol/envelope.go`
- Modify: `internal/protocol/control.go`
- Modify: `internal/protocol/control_test.go`
- Modify: `internal/protocol/envelope_test.go`

**Interfaces:**
- Produces: `MessageTypeResumeRequest`, `MessageTypeResumeResult`.
- Produces: `ResumeRequest{ResumeToken string}`, `ResumeResult{OK bool, SessionID string, ResumeToken string, ErrorCode string}` and marshal/unmarshal functions.
- Modifies: `AuthResult` to include `ResumeToken string`.

- [ ] **Step 1: Write round-trip and known-control-type failing tests**

```go
func TestResumeControlPayloadRoundTrip(t *testing.T) {
    got, err := UnmarshalResumeResult(MarshalResumeResult(ResumeResult{OK: true, SessionID: "s", ResumeToken: "fresh"}))
    if err != nil || got.ResumeToken != "fresh" || !got.OK { t.Fatalf("got=%#v err=%v", got, err) }
}
func TestResumeMessageTypesAreValidControls(t *testing.T) {
    for _, mt := range []uint32{MessageTypeResumeRequest, MessageTypeResumeResult} {
        if err := (Envelope{Version: CurrentVersion, MessageType: mt}).Validate(); err != nil { t.Fatalf("type=%d err=%v", mt, err) }
    }
}
```

- [ ] **Step 2: Run protocol tests to verify failure**

Run: `go test ./internal/protocol -run 'TestResume(ControlPayloadRoundTrip|MessageTypesAreValidControls)' -count=1`

Expected: FAIL because the symbols are absent.

- [ ] **Step 3: Implement wire encoding and control registration**

Use new protobuf-wire field numbers after the existing AuthResult fields. Add resume types to `isKnownControlMessageType`; use existing unknown-field skipping behavior.

- [ ] **Step 4: Verify protocol package and commit Task 2**

Run: `go test ./internal/protocol -count=1`

```bash
git add internal/protocol/envelope.go internal/protocol/control.go internal/protocol/control_test.go internal/protocol/envelope_test.go
git commit -m "feat: add resume control protocol"
```

### Task 3: Gateway recovery lifecycle and expiry scanner

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/gateway/server.go`
- Modify: `internal/gateway/server_test.go`
- Modify: `internal/metrics/metrics.go`
- Modify: `internal/metrics/metrics_test.go`

**Interfaces:**
- Produces config field `SessionGracePeriod time.Duration`.
- Produces Gateway handlers `handleResume`, `expireSessions`, and a single session-expiry loop.
- Modifies `removeConn` to disconnect an authenticated session into grace unless Server is shutting down or the Session was explicitly replaced.

- [ ] **Step 1: Write failing Gateway recovery tests**

```go
func TestResumeWithinGraceKeepsSessionIdentity(t *testing.T) { /* authenticate, close, send ResumeRequest, assert identical SessionID and rotated token */ }
func TestExpiredResumeTokenIsRejected(t *testing.T) { /* configure 10ms grace, authenticate, close, wait for scanner, assert ResumeResult{OK:false, ErrorCode:"resume_token_invalid"} */ }
func TestOldConnectionCloseCannotRemoveResumedSession(t *testing.T) { /* resume while old connection closes; assert ActiveSessionCount()==1 and new connection remains usable */ }
```

- [ ] **Step 2: Run the focused tests to verify failure**

Run: `go test ./internal/gateway -run 'Test(ResumeWithinGraceKeepsSessionIdentity|ExpiredResumeTokenIsRejected|OldConnectionCloseCannotRemoveResumedSession)' -count=1`

Expected: FAIL because ResumeRequest and SessionGracePeriod are not wired into Gateway.

- [ ] **Step 3: Implement minimal resume wiring**

Add `SessionGracePeriod` default, construct the Session Manager with it, issue token in AuthResult, dispatch ResumeRequest before normal auth rejection, bind the resumed connection identity, and return ResumeResult. Add a Server-owned scanner that calls `sessions.Expire(now)` and `reliability.RemoveSession` for each expired Session.

- [ ] **Step 4: Add metrics tests, then implement metrics**

```go
func TestRecoveryMetricsDoNotExposeSessionSecrets(t *testing.T) { /* record a resume result, render /metrics, assert expected result label and absence of token/session value */ }
```

Expose recovery result counters, grace-session gauge, and grace-expired counter without identifiers.

- [ ] **Step 5: Verify Gateway and Metrics tests, then commit Task 3**

Run: `go test ./internal/gateway ./internal/metrics -count=1`

```bash
git add internal/config/config.go internal/gateway/server.go internal/gateway/server_test.go internal/metrics/metrics.go internal/metrics/metrics_test.go
git commit -m "feat: restore sessions during grace period"
```

### Task 4: Preserve and replay reliable outbound messages on recovery

**Files:**
- Modify: `internal/reliability/reliability.go`
- Modify: `internal/reliability/reliability_test.go`
- Modify: `internal/gateway/server.go`
- Modify: `internal/gateway/server_test.go`

**Interfaces:**
- Produces: `Pending(sessionID string) []protocol.Envelope`, sorted ascending by `Seq` and cloned.
- Modifies retry scan to leave pending state untouched while no active connection exists.

- [ ] **Step 1: Write failing reliability tests**

```go
func TestPendingReturnsClonedEnvelopesInSequenceOrder(t *testing.T) { /* track two envelopes, assert Seq 1 then 2, mutate first Payload, then assert a fresh snapshot retains its original bytes */ }
func TestCollectDueDoesNotConsumeRetriesWhileSessionIsDisconnected(t *testing.T) { /* Gateway reconnect test asserts no retry is emitted before resume and the first resume replay keeps the original envelope */ }
```

- [ ] **Step 2: Run reliability tests to verify failure**

Run: `go test ./internal/reliability -run 'Test(PendingReturnsClonedEnvelopesInSequenceOrder|CollectDueDoesNotConsumeRetriesWhileSessionIsDisconnected)' -count=1`

Expected: FAIL because ordered pending retrieval and disconnected pause behavior are absent.

- [ ] **Step 3: Implement minimal ordered replay support**

Add a cloned, Seq-sorted pending snapshot. Keep reliability transport-agnostic: Gateway skips `CollectDue` processing for Sessions without an active Connection, so `CollectDue` is never called in a way that consumes grace-session retries.

- [ ] **Step 4: Write and run Gateway replay test, then implement it**

```go
func TestResumeReplaysReliablePendingInOriginalSequence(t *testing.T) { /* mark backend response reliable, withhold ACKs, reconnect, read two envelopes, assert their Seq values are 1 and 2 and their MessageIDs are unchanged */ }
```

After successful binding, enqueue each `reliability.Pending(sessionID)` envelope in order. Do not call `TrackOutbound` during replay.

- [ ] **Step 5: Verify and commit Task 4**

Run: `go test ./internal/reliability ./internal/gateway -count=1`

```bash
git add internal/reliability/reliability.go internal/reliability/reliability_test.go internal/gateway/server.go internal/gateway/server_test.go
git commit -m "feat: replay reliable pending messages on resume"
```

### Task 5: End-to-end fault coverage and acceptance documentation

**Files:**
- Modify: `internal/gateway/server_test.go`
- Create: `docs/stage-5/00-阶段5验收报告.md`
- Create: `docs/stage-5/01-断线恢复语义.md`
- Create: `docs/stage-5/02-测试报告.md`

**Interfaces:**
- Consumes all Stage 5 Session, protocol, Gateway and reliability interfaces.
- Produces reproducible Stage 5 semantics and validation records.

- [ ] **Step 1: Write failing integration tests for remaining acceptance cases**

```go
func TestResumeSucceedsWhenBackendIsUnavailable(t *testing.T) { /* stop configured backend before recovery, assert ResumeResult OK, then assert next business request returns backend_unavailable */ }
func TestConcurrentResumeTokenUseHasOneWinner(t *testing.T) { /* send the same token from two clients, assert exactly one ResumeResult OK and one resume_token_invalid */ }
func TestGraceExpiryReleasesReliablePending(t *testing.T) { /* create outbound pending, disconnect, let grace expire, assert ReliablePendingCount returns zero */ }
```

- [ ] **Step 2: Run focused integration tests to verify failure**

Run: `go test ./internal/gateway -run 'Test(ResumeSucceedsWhenBackendIsUnavailable|ConcurrentResumeTokenUseHasOneWinner|GraceExpiryReleasesReliablePending)' -count=1`

Expected: FAIL until the end-to-end lifecycle is complete.

- [ ] **Step 3: Make only the corrections required by failing tests**

Keep Backend out of the resume path; ensure the scanner removal calls Reliability Manager removal; preserve one-winner token semantics under concurrent requests.

- [ ] **Step 4: Run full verification**

Run:

```bash
go test ./...
go test -race ./...
```

Expected: PASS without race reports.

- [ ] **Step 5: Write acceptance artifacts and commit Task 5**

Document protocol fields, lifecycle transitions, replay order, expiry behavior, test commands and their observed results.

```bash
git add internal/gateway/server_test.go docs/stage-5
git commit -m "docs: add stage 5 acceptance report"
```
