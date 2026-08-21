package registry

import "github.com/gamemesh-labs/gamemesh/internal/scheduler"

// CandidateSource exposes the latest immutable registry snapshot to schedulers.
type CandidateSource struct {
	Registry *Registry
}

func (s CandidateSource) Candidates() scheduler.Candidates {
	if s.Registry == nil {
		return scheduler.NewStaticCandidates(nil)
	}
	return s.Registry.Snapshot()
}
