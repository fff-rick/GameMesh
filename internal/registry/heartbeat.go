package registry

import (
	"context"
	"time"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

// SnapshotSource is implemented by the M0 GameServer simulator and can later
// be implemented by a real GameServer sidecar/SDK.
type SnapshotSource interface {
	Snapshot() model.GameServerSnapshot
}

// RunHeartbeat registers a GameServer and continuously refreshes its lease.
// Cancellation intentionally does not deregister so tests/demos can model a
// process crash. Graceful shutdown should call Deregister explicitly.
func RunHeartbeat(ctx context.Context, r *InMemory, source SnapshotSource, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	if err := r.Register(ctx, source.Snapshot()); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.Heartbeat(ctx, source.Snapshot()); err != nil {
				return err
			}
		}
	}
}
