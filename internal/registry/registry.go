// Package registry owns the control-plane view of GameServer membership and
// health. Writers serialize mutations; readers load immutable snapshots
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

// Config controls lease expiry and publication. Now is injectable so failure
// detection is deterministic in tests.
type Config struct {
	HeartbeatTTL    time.Duration
	SweepInterval   time.Duration
	PublishInterval time.Duration
	Now             func() time.Time
}

func (c Config) normalized() Config {
	if c.HeartbeatTTL <= 0 {
		c.HeartbeatTTL = 3 * time.Second
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = c.HeartbeatTTL / 3
		if c.SweepInterval <= 0 {
			c.SweepInterval = time.Millisecond
		}
	}
	if c.PublishInterval <= 0 {
		c.PublishInterval = 100 * time.Millisecond
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Heartbeat contains only the mutable state reported by an already registered
// server. Identity, address, region, version and capacity are registration-time
// constraints and cannot be changed by a heartbeat.
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

// Snapshot is a point-in-time, read-only candidate view. Elements are returned
// by value, preventing consumers from modifying published state.
type Snapshot struct {
	generation uint64
	createdAt  time.Time
	servers    []model.GameServerSnapshot
}

func (s *Snapshot) Generation() uint64                    { return s.generation }
func (s *Snapshot) CreatedAt() time.Time                  { return s.createdAt }
func (s *Snapshot) Len() int                              { return len(s.servers) }
func (s *Snapshot) At(index int) model.GameServerSnapshot { return s.servers[index] }

// Stats is a cheap summary of the currently published view.
type Stats struct {
	Generation uint64
	Registered int
	Ready      int
	Allocated  int
	Unhealthy  int
	Draining   int
	Other      int
}

// Registry is the single-process M1 authority for GameServer membership.
// Non-critical heartbeat updates are batched; all routing-critical mutations
// publish immediately.
type Registry struct {
	mu      sync.Mutex
	entries map[string]entry
	config  Config
	nextGen uint64
	dirty   bool

	snapshot atomic.Pointer[Snapshot]
}

func New(config Config) *Registry {
	config = config.normalized()
	r := &Registry{entries: make(map[string]entry), config: config}
	r.snapshot.Store(&Snapshot{createdAt: config.Now()})
	return r
}

// Register creates or refreshes a member and its static scheduling attributes.
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
	r.publishLocked(r.config.Now())
	return nil
}

// Heartbeat refreshes liveness and dynamic load. State transitions publish
// immediately; player and metric-only changes are published periodically.
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

	previousState := current.server.State
	current.server.CurrentPlayers = heartbeat.CurrentPlayers
	current.server.Metrics = heartbeat.Metrics
	if heartbeat.State != "" {
		current.server.State = heartbeat.State
	}
	current.lastHeartbeat = r.config.Now()
	r.entries[heartbeat.ServerID] = current
	r.dirty = true
	if current.server.State != previousState {
		r.publishLocked(r.config.Now())
	}
	return nil
}

// Deregister removes a deliberately terminated server. Crashes are retained as
// UNHEALTHY by failure detection for observability.
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
	r.publishLocked(r.config.Now())
	return nil
}

// Snapshot returns the latest immutable control-plane view in O(1).
func (r *Registry) Snapshot() *Snapshot { return r.snapshot.Load() }

func (r *Registry) Stats() Stats {
	snapshot := r.Snapshot()
	stats := Stats{Generation: snapshot.Generation(), Registered: snapshot.Len()}
	for i := 0; i < snapshot.Len(); i++ {
		switch snapshot.At(i).State {
		case model.ServerReady:
			stats.Ready++
		case model.ServerAllocated:
			stats.Allocated++
		case model.ServerUnhealthy:
			stats.Unhealthy++
		case model.ServerDraining:
			stats.Draining++
		default:
			stats.Other++
		}
	}
	return stats
}

// Sweep marks members whose heartbeat lease has elapsed as UNHEALTHY.
func (r *Registry) Sweep() int { return r.expireStale(r.config.Now()) }

// ExpireStale evaluates leases against now, making failure-detection tests
// deterministic without sleeping.
func (r *Registry) ExpireStale(now time.Time) int { return r.expireStale(now) }

func (r *Registry) expireStale(now time.Time) int {
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
		r.publishLocked(now)
	}
	return expired
}

// PublishPending publishes accumulated non-critical heartbeat updates.
func (r *Registry) PublishPending(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dirty {
		return false
	}
	r.publishLocked(now)
	return true
}

// Run starts the sweeper and batched publisher until ctx is cancelled.
func (r *Registry) Run(ctx context.Context) error {
	sweepTicker := time.NewTicker(r.config.SweepInterval)
	publishTicker := time.NewTicker(r.config.PublishInterval)
	defer sweepTicker.Stop()
	defer publishTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sweepTicker.C:
			r.Sweep()
		case now := <-publishTicker.C:
			r.PublishPending(now)
		}
	}
}

// Start retains the original fire-and-forget API for callers that do not need
// the cancellation error.
func (r *Registry) Start(ctx context.Context) { _ = r.Run(ctx) }

func (r *Registry) publishLocked(now time.Time) {
	servers := make([]model.GameServerSnapshot, 0, len(r.entries))
	for _, current := range r.entries {
		servers = append(servers, current.server)
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	r.nextGen++
	r.dirty = false
	r.snapshot.Store(&Snapshot{generation: r.nextGen, createdAt: now, servers: servers})
}
