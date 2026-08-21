package benchmark

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/registry"
	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/internal/simulator"
)

// CandidateSource provides scheduler candidates and observes successful
// simulator allocations. Keeping this boundary in the benchmark lets M1.5
// compare the legacy simulator scan with the Registry-published snapshot.
type CandidateSource interface {
	Name() string
	Candidates() scheduler.Candidates
	AfterAllocation(serverID string) error
	Flush() error
}

// ClusterCandidateSource preserves the M0 behaviour: every scheduling request
// rebuilds a candidate snapshot from the simulator.
type ClusterCandidateSource struct {
	cluster *simulator.Cluster
}

func NewClusterCandidateSource(cluster *simulator.Cluster) *ClusterCandidateSource {
	return &ClusterCandidateSource{cluster: cluster}
}

func (s *ClusterCandidateSource) Name() string { return "cluster_snapshot" }

func (s *ClusterCandidateSource) Candidates() scheduler.Candidates {
	return scheduler.BorrowCandidates(s.cluster.Snapshots())
}

func (s *ClusterCandidateSource) AfterAllocation(string) error { return nil }
func (s *ClusterCandidateSource) Flush() error                 { return nil }

// RegistryCandidateSource mirrors simulator state into a Registry. Load-only
// heartbeats are batched; state transitions are published immediately by the
// Registry itself. PublishEvery is a benchmark control, expressed in successful
// allocations, that makes the permitted snapshot staleness explicit.
type RegistryCandidateSource struct {
	cluster      *simulator.Cluster
	registry     *registry.Registry
	publishEvery uint64
	updates      atomic.Uint64
}

func NewRegistryCandidateSource(cluster *simulator.Cluster, r *registry.Registry, publishEvery int) (*RegistryCandidateSource, error) {
	if cluster == nil || r == nil {
		return nil, fmt.Errorf("cluster and registry are required")
	}
	if publishEvery <= 0 {
		publishEvery = 100
	}
	for _, snapshot := range cluster.Snapshots() {
		if err := r.Register(snapshot); err != nil {
			return nil, fmt.Errorf("register %q: %w", snapshot.ID, err)
		}
	}
	return &RegistryCandidateSource{cluster: cluster, registry: r, publishEvery: uint64(publishEvery)}, nil
}

func (s *RegistryCandidateSource) Name() string { return "registry_snapshot" }

func (s *RegistryCandidateSource) Candidates() scheduler.Candidates {
	return s.registry.Snapshot()
}

func (s *RegistryCandidateSource) AfterAllocation(serverID string) error {
	snapshot, err := s.cluster.Snapshot(serverID)
	if err != nil {
		return err
	}
	if err := s.registry.Heartbeat(registry.Heartbeat{
		ServerID: snapshot.ID, State: snapshot.State, CurrentPlayers: snapshot.CurrentPlayers, Metrics: snapshot.Metrics,
	}); err != nil {
		return err
	}
	if s.updates.Add(1)%s.publishEvery == 0 {
		s.registry.PublishPending(time.Now())
	}
	return nil
}

func (s *RegistryCandidateSource) Flush() error {
	s.registry.PublishPending(time.Now())
	return nil
}
