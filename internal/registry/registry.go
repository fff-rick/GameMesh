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
	ErrInvalidServer = errors.New("invalid game server snapshot")
	ErrNotRegistered = errors.New("game server is not registered")
)

// Config controls lease expiration. A GameServer that misses heartbeats for TTL
// is retained in the registry but published as UNHEALTHY.
type Config struct {
	TTL             time.Duration
	SweepInterval   time.Duration
	PublishInterval time.Duration
}

func (c Config) normalized() Config {
	if c.TTL <= 0 {
		c.TTL = 10 * time.Second
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = c.TTL / 3
		if c.SweepInterval <= 0 {
			c.SweepInterval = time.Second
		}
	}
	if c.PublishInterval <= 0 {
		c.PublishInterval = 100 * time.Millisecond
	}
	return c
}

// Entry stores control-plane metadata for a registered GameServer.
type Entry struct {
	Server        model.GameServerSnapshot `json:"server"`
	RegisteredAt  time.Time                `json:"registered_at"`
	LastHeartbeat time.Time                `json:"last_heartbeat"`
	ExpiresAt     time.Time                `json:"expires_at"`
	Generation    uint64                   `json:"generation"`
}

// View is a published immutable-by-contract snapshot. Readers must not mutate
// Servers. Publishing pays the O(N) copy cost so request-path reads stay O(1).
type View struct {
	Revision    uint64                     `json:"revision"`
	PublishedAt time.Time                  `json:"published_at"`
	Servers     []model.GameServerSnapshot `json:"servers"`
}

// Stats is a cheap summary generated from the currently published view.
type Stats struct {
	Revision   uint64 `json:"revision"`
	Registered int    `json:"registered"`
	Ready      int    `json:"ready"`
	Allocated  int    `json:"allocated"`
	Unhealthy  int    `json:"unhealthy"`
	Draining   int    `json:"draining"`
	Other      int    `json:"other"`
}

// InMemory is the M1 control-plane registry. Mutations are serialized under a
// lock; readers load the latest immutable view through atomic.Pointer.
type InMemory struct {
	mu       sync.Mutex
	cfg      Config
	entries  map[string]Entry
	order    []string
	revision uint64
	dirty    bool
	view     atomic.Pointer[View]
}

func NewInMemory(cfg Config) *InMemory {
	r := &InMemory{
		cfg:     cfg.normalized(),
		entries: make(map[string]Entry),
	}
	r.view.Store(&View{Servers: []model.GameServerSnapshot{}})
	return r
}

func validateSnapshot(s model.GameServerSnapshot) error {
	if s.ID == "" || s.Capacity <= 0 {
		return ErrInvalidServer
	}
	return nil
}

// Register adds or refreshes a GameServer registration. Re-registering the same
// ID is idempotent from the caller's perspective and preserves RegisteredAt.
func (r *InMemory) Register(_ context.Context, server model.GameServerSnapshot) error {
	if err := validateSnapshot(server); err != nil {
		return err
	}

	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, exists := r.entries[server.ID]
	if !exists {
		entry.RegisteredAt = now
		r.order = append(r.order, server.ID)
		sort.Strings(r.order)
	}
	entry.Server = server
	entry.LastHeartbeat = now
	entry.ExpiresAt = now.Add(r.cfg.TTL)
	entry.Generation++
	r.entries[server.ID] = entry
	r.publishLocked(now)
	return nil
}

// Heartbeat refreshes lease metadata and the latest GameServer snapshot.
// A heartbeat can recover a server previously marked UNHEALTHY by TTL expiry.
func (r *InMemory) Heartbeat(_ context.Context, server model.GameServerSnapshot) error {
	if err := validateSnapshot(server); err != nil {
		return err
	}

	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[server.ID]
	if !ok {
		return ErrNotRegistered
	}
	oldState := entry.Server.State
	entry.Server = server
	entry.LastHeartbeat = now
	entry.ExpiresAt = now.Add(r.cfg.TTL)
	entry.Generation++
	r.entries[server.ID] = entry
	r.dirty = true

	// State transitions affect routing correctness and are published immediately.
	// Metric/player-count refreshes are batched by PublishInterval.
	if oldState != server.State {
		r.publishLocked(now)
	}
	return nil
}

// Deregister removes a server immediately. Graceful shutdown should use this;
// crashes should simply stop heartbeats and be detected through TTL expiry.
func (r *InMemory) Deregister(_ context.Context, id string) error {
	if id == "" {
		return ErrNotRegistered
	}

	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[id]; !ok {
		return ErrNotRegistered
	}
	delete(r.entries, id)
	for i, registeredID := range r.order {
		if registeredID == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	r.publishLocked(now)
	return nil
}

func (r *InMemory) Get(id string) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id]
	return entry, ok
}

// Snapshot is an O(1) atomic read. The returned view is immutable-by-contract.
func (r *InMemory) Snapshot() *View {
	return r.view.Load()
}

func (r *InMemory) Stats() Stats {
	view := r.Snapshot()
	stats := Stats{Revision: view.Revision, Registered: len(view.Servers)}
	for _, server := range view.Servers {
		switch server.State {
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

// ExpireStale evaluates leases against the supplied time. The explicit time
// makes failure-detection tests deterministic and avoids sleeps.
func (r *InMemory) ExpireStale(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	changed := 0
	for id, entry := range r.entries {
		if entry.Server.State == model.ServerTerminated {
			continue
		}
		if now.Before(entry.ExpiresAt) {
			continue
		}
		if entry.Server.State == model.ServerUnhealthy {
			continue
		}
		entry.Server.State = model.ServerUnhealthy
		entry.Generation++
		r.entries[id] = entry
		changed++
	}
	if changed > 0 {
		r.publishLocked(now)
	}
	return changed
}

// PublishPending publishes accumulated non-critical heartbeat updates once.
// It returns true when a new view was produced.
func (r *InMemory) PublishPending(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dirty {
		return false
	}
	r.publishLocked(now)
	return true
}

// Run starts the periodic lease sweeper and blocks until ctx is cancelled.
func (r *InMemory) Run(ctx context.Context) error {
	sweepTicker := time.NewTicker(r.cfg.SweepInterval)
	publishTicker := time.NewTicker(r.cfg.PublishInterval)
	defer sweepTicker.Stop()
	defer publishTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-sweepTicker.C:
			r.ExpireStale(now)
		case now := <-publishTicker.C:
			r.PublishPending(now)
		}
	}
}

func (r *InMemory) publishLocked(now time.Time) {
	servers := make([]model.GameServerSnapshot, 0, len(r.order))
	for _, id := range r.order {
		servers = append(servers, r.entries[id].Server)
	}

	r.revision++
	r.dirty = false
	r.view.Store(&View{
		Revision:    r.revision,
		PublishedAt: now,
		Servers:     servers,
	})
}
