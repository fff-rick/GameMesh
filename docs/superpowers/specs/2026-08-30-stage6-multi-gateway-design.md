# Stage 6: Multi-Gateway Design

## Scope and decision

Stage 6 adds the minimum distributed coordination needed for multiple Gateway
processes. A successful authentication creates a Redis-backed, expiring user
ownership record. The record identifies the current Gateway and its local
connection; its contents are routing metadata, not a durable session.

The existing process-local `session.Manager` and `reliability.Manager` remain
authoritative for SessionID, Resume Token, sequence state, and pending reliable
messages. Consequently, resume remains valid only on the Gateway process that
created the Session. This deliberately excludes cross-node recovery and
Gateway-restart recovery from Stage 6.

## Architecture

Introduce a small `internal/presence` package behind a `Registry` interface:

```text
Gateway Server -- Registry -- Redis
      |              |
      +-- local connection map
```

`Registry.Claim` atomically replaces `user:<userID>` with a new opaque lease
token, GatewayID, and ConnID, then returns the prior owner. `Registry.Renew`
only extends the TTL when the stored lease token matches. `Registry.Release`
only deletes the key when its lease token matches. Redis Lua scripts perform
the compare-and-write operations, so a delayed old node cannot delete or renew
a newer owner. The key has a TTL and needs no scan-based cleanup after a
Gateway crash.

The Server assigns each local authenticated connection a lease token. It renews
active local leases periodically. On a successful claim that replaces an owner
on another Gateway, it publishes a best-effort eviction message on a dedicated
Redis Pub/Sub channel. Every Server subscribes to that channel and closes only
the matching local connection. The new claim is authoritative even if the
notification is delayed or lost; the old record's lease cannot be renewed and
eventually expires. The local same-Gateway duplicate-login rule continues to
close the replaced connection directly.

## Failure semantics

- Redis unavailable before authentication claim: authentication returns the
  retryable `presence_unavailable` result and does not create a Session. This
  prevents inconsistent cross-node duplicate-login outcomes.
- Redis unavailable after claim: already authenticated connections remain open;
  lease renewals retry in the background. The Gateway does not disconnect them
  merely because Redis is down.
- Lease expiry or Gateway process failure: Redis removes the user ownership
  record. A later login can claim it without stale long-lived routing.
- Close/session expiry: release is conditional on the lease token, so a stale
  close cannot remove a replacement owner's route.
- Subscription failure: the listener reconnects; Pub/Sub is an optimization for
  prompt eviction, while lease fencing is the correctness mechanism.

## Configuration and observability

Add Redis address, key prefix, lease TTL, renewal interval, and operation
timeout to `config.Config`. Redis is opt-in: an empty address retains the
single-node Stage 5 behavior for local development and existing tests.

Expose counters for claims, renewals, releases, evictions, and registry errors,
plus a gauge for locally owned distributed leases. GatewayID remains a metric
label and log field; user IDs, connection IDs, and lease tokens never become
metric labels.

## Verification

Focused tests use an in-memory Registry implementation to deterministically
exercise claim fencing, TTL expiry, failed renewal, and conditional release.
Gateway integration tests run two Servers against `miniredis`: different-user
distribution, same-user cross-node eviction, expired owner replacement, and a
temporary registry outage that leaves established connections alive. The full
suite, race detector, vet, and build are the acceptance checks.
