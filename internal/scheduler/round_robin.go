package scheduler

import (
	"context"
	"sync/atomic"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

// RoundRobin is the M0 baseline scheduler. It intentionally uses no load score.
type RoundRobin struct {
	next atomic.Uint64
}

func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

func (r *RoundRobin) Name() string { return "round_robin" }

func (r *RoundRobin) Schedule(_ context.Context, req model.AllocationRequest, candidates []model.GameServerSnapshot) (model.AllocationResult, error) {
	eligible := make([]model.GameServerSnapshot, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.CanAccept() {
			continue
		}
		if req.Region != "" && candidate.Region != req.Region {
			continue
		}
		if req.Version != "" && candidate.Version != req.Version {
			continue
		}
		eligible = append(eligible, candidate)
	}
	if len(eligible) == 0 {
		return model.AllocationResult{}, ErrNoCandidate
	}

	idx := r.next.Add(1) - 1
	selected := eligible[idx%uint64(len(eligible))]
	return model.AllocationResult{GameServerID: selected.ID, Strategy: r.Name()}, nil
}
