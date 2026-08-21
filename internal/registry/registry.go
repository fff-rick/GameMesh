// Package registry owns the control-plane view of GameServer membership and
// health. Writers serialize mutations; readers load an immutable snapshot
// without acquiring the writer lock.
package registry

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

var (
	ErrInvalidServer    = errors.New("invalid game server registration")
	ErrServerNotFound   = errors.New("game server is not registered")
	ErrInvalidHeartbeat = errors.New("invalid game server heartbeat")
)

// Config controls failure detection. Now is injectable to make TTL behavior
// deterministic in tests; production callers normally leave it unset.
type Config struct {
	HeartbeatTTL  time.Duration
	SweepInterval time.Duration
	Now           func() time.Time
}

// Heartbeat contains mutable state reported by an already registered server.
// Identity, capacity, region, and version are registration-time attributes.
type Heartbeat struct {
	ServerID       string
	State          model.ServerState
	CurrentPlayers int
	Metrics        model.ServerMetrics
}

type entry struct {
	server        model.GameServerSnapshot
	lastHeartbeat time.Time
}

// Snapshot is a point-in-time, read-only candidate view. Its elements are
// returned by value, preventing consumers from modifying published state.
type Snapshot struct {
	generation uint64
	createdAt  time.Time
	servers    []model.GameServerSnapshot
}

func (s *Snapshot) Generation() uint64                    { return s.generation }
func (s *Snapshot) CreatedAt() time.Time                  { return s.createdAt }
func (s *Snapshot) Len() int                              { return len(s.servers) }
func (s *Snapshot) At(index int) model.GameServerSnapshot { return s.servers[index] }

// Registry is the local authority for GameServer membership. Snapshot reads
// are lock-free; the cost of constructing a fresh snapshot stays on the
// control-plane write path.
type Registry struct {
	mu      sync.Mutex
	entries map[string]entry
	config  Config
	nextGen uint64

	snapshot atomic.Pointer[Snapshot]
}

func New(config Config) *Registry {
	if config.HeartbeatTTL <= 0 {
		config.HeartbeatTTL = 3 * time.Second
	}
	if config.SweepInterval <= 0 {
		config.SweepInterval = config.HeartbeatTTL / 3
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	r := &Registry{entries: make(map[string]entry), config: config}
	r.snapshot.Store(&Snapshot{createdAt: config.Now()})
	return r
}

// Register creates or refreshes a member. Register is intentionally the only
// operation allowed to change a server's identity and scheduling constraints.
func (r *Registry) Register(server model.GameServerSnapshot) error {
	if server.ID == "" || server.Capacity <= 0 || server.CurrentPlayers < 0 || server.CurrentPlayers > server.Capacity {
		return ErrInvalidServer
	}
	if server.State == "" {
		server.State = model.ServerStarting
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[server.ID] = entry{server: server, lastHeartbeat: r.config.Now()}
	r.publishLocked()
	return nil
}

// Heartbeat refreshes liveness and mutable load data. A heartbeat from an
// unknown server is rejected rather than implicitly creating an unverified
// member.
func (r *Registry) Heartbeat(heartbeat Heartbeat) error {
	if heartbeat.ServerID == "" || heartbeat.CurrentPlayers < 0 {
		return ErrInvalidHeartbeat
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.entries[heartbeat.ServerID]
	if !ok {
		return ErrServerNotFound
	}
	if heartbeat.CurrentPlayers > current.server.Capacity {
		return ErrInvalidHeartbeat
	}
	current.server.CurrentPlayers = heartbeat.CurrentPlayers
	current.server.Metrics = heartbeat.Metrics
	if heartbeat.State != "" {
		current.server.State = heartbeat.State
	}
	current.lastHeartbeat = r.config.Now()
	r.entries[heartbeat.ServerID] = current
	r.publishLocked()
	return nil
}

// Deregister removes a server that has been deliberately terminated. Failure
// detection uses UNHEALTHY instead, retaining visibility for observability.
func (r *Registry) Deregister(serverID string) error {
	if serverID == "" {
		return ErrServerNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[serverID]; !ok {
		return ErrServerNotFound
	}
	delete(r.entries, serverID)
	r.publishLocked()
	return nil
}

// Snapshot returns the latest immutable control-plane view in O(1).
func (r *Registry) Snapshot() *Snapshot { return r.snapshot.Load() }

// Sweep marks members whose heartbeat TTL has elapsed as UNHEALTHY. It returns
// the number of state transitions; repeated sweeps do not republish unchanged
// state.
func (r *Registry) Sweep() int {
	now := r.config.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	expired := 0
	for id, current := range r.entries {
		if current.server.State == model.ServerUnhealthy || current.server.State == model.ServerTerminated {
			continue
		}
		if !current.lastHeartbeat.Add(r.config.HeartbeatTTL).After(now) {
			current.server.State = model.ServerUnhealthy
			r.entries[id] = current
			expired++
		}
	}
	if expired > 0 {
		r.publishLocked()
	}
	return expired
}

// Start runs failure detection until ctx is cancelled. Callers that have their
// own control-plane loop may call Sweep directly instead.
func (r *Registry) Start(ctx context.Context) {
	ticker := time.NewTicker(r.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Sweep()
		}
	}
}

func (r *Registry) publishLocked() {
	servers := make([]model.GameServerSnapshot, 0, len(r.entries))
	for _, current := range r.entries {
		servers = append(servers, current.server)
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	r.nextGen++
	r.snapshot.Store(&Snapshot{generation: r.nextGen, createdAt: r.config.Now(), servers: servers})
}
