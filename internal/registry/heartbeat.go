package registry

import (
	"context"
	"time"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

// SnapshotSource is implemented by the M0 simulator and can later be
// implemented by a GameServer sidecar or SDK.
type SnapshotSource interface {
	Snapshot() model.GameServerSnapshot
}

// RunHeartbeat registers a GameServer and continuously refreshes its dynamic
// lease data. Cancellation models a crash; graceful shutdown must Deregister.
func RunHeartbeat(ctx context.Context, r *Registry, source SnapshotSource, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	if err := r.Register(source.Snapshot()); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			snapshot := source.Snapshot()
			if err := r.Heartbeat(Heartbeat{
				ServerID: snapshot.ID, State: snapshot.State,
				CurrentPlayers: snapshot.CurrentPlayers, Metrics: snapshot.Metrics,
			}); err != nil {
				return err
			}
		}
	}
}
