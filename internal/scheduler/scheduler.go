package scheduler

import (
	"context"
	"errors"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

var ErrNoCandidate = errors.New("no eligible game server")

// Scheduler selects a GameServer for new traffic.
// Implementations must not mutate candidate snapshots.
type Scheduler interface {
	Name() string
	Schedule(context.Context, model.AllocationRequest, Candidates) (model.AllocationResult, error)
}
