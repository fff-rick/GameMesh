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

func (r *RoundRobin) Schedule(_ context.Context, req model.AllocationRequest, candidates Candidates) (model.AllocationResult, error) {
	eligibleCount := 0
	for i := 0; i < candidates.Len(); i++ {
		candidate := candidates.At(i)
		if isEligible(req, candidate) {
			eligibleCount++
		}
	}
	if eligibleCount == 0 {
		return model.AllocationResult{}, ErrNoCandidate
	}

	idx := r.next.Add(1) - 1
	target := int(idx % uint64(eligibleCount))
	for i := 0; i < candidates.Len(); i++ {
		candidate := candidates.At(i)
		if !isEligible(req, candidate) {
			continue
		}
		if target == 0 {
			return model.AllocationResult{GameServerID: candidate.ID, Strategy: r.Name()}, nil
		}
		target--
	}

	// Candidates is a read-only point-in-time view. This is unreachable for a
	// conforming implementation, but protects the interface boundary.
	return model.AllocationResult{}, ErrNoCandidate
}

func isEligible(req model.AllocationRequest, candidate model.GameServerSnapshot) bool {
	if !candidate.CanAccept() {
		return false
	}
	if req.Region != "" && candidate.Region != req.Region {
		return false
	}
	return req.Version == "" || candidate.Version == req.Version
}
