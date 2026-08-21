package registry

import "github.com/gamemesh-labs/gamemesh/pkg/model"

// CandidateSource exposes the latest published registry view to schedulers.
// The slice is read-only by contract and loaded without rebuilding the pool.
type CandidateSource struct {
	Registry *InMemory
}

func (s CandidateSource) Candidates() []model.GameServerSnapshot {
	if s.Registry == nil {
		return nil
	}
	return s.Registry.Snapshot().Servers
}
