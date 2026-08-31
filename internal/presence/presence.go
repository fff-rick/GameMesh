// Package presence owns the small piece of distributed state required by a
// multi-Gateway deployment. It intentionally does not store Sessions.
package presence

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("presence registry unavailable")

// Owner is an expiring, fenced user-to-Gateway routing record. LeaseToken must
// be random and is never suitable for exposure to a client.
type Owner struct {
	UserID     string `json:"user_id"`
	GatewayID  string `json:"gateway_id"`
	ConnID     string `json:"conn_id"`
	LeaseToken string `json:"lease_token"`
}

func (o Owner) Valid() bool {
	return o.UserID != "" && o.GatewayID != "" && o.ConnID != "" && o.LeaseToken != ""
}

// Registry operations that change a lease are conditional on LeaseToken. This
// fences delayed close/renew calls from an old Gateway instance.
type Registry interface {
	Claim(context.Context, Owner) (*Owner, error)
	Renew(context.Context, Owner) (bool, error)
	Release(context.Context, Owner) (bool, error)
	PublishEviction(context.Context, Owner) error
	// Subscribe blocks until ctx is cancelled or the subscription fails. The
	// caller is responsible for reconnecting after an error.
	Subscribe(context.Context, func(Owner)) error
}

type memoryEntry struct {
	owner   Owner
	expires time.Time
}

// MemoryRegistry is a deterministic test double. It models the same fencing
// and TTL contract as Redis and can be shared by multiple test Servers.
type MemoryRegistry struct {
	mu        sync.Mutex
	ttl       time.Duration
	available bool
	owners    map[string]memoryEntry
	watchers  map[uint64]func(Owner)
	nextWatch uint64
}

func NewMemoryRegistry(ttl time.Duration) *MemoryRegistry {
	if ttl <= 0 {
		ttl = time.Second
	}
	return &MemoryRegistry{ttl: ttl, available: true, owners: map[string]memoryEntry{}, watchers: map[uint64]func(Owner){}}
}

func (m *MemoryRegistry) SetAvailable(available bool) {
	m.mu.Lock()
	m.available = available
	m.mu.Unlock()
}
func (m *MemoryRegistry) Claim(_ context.Context, owner Owner) (*Owner, error) {
	if !owner.Valid() {
		return nil, errors.New("invalid presence owner")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.available {
		return nil, ErrUnavailable
	}
	var previous *Owner
	if old, ok := m.owners[owner.UserID]; ok && time.Now().Before(old.expires) {
		copy := old.owner
		previous = &copy
	}
	m.owners[owner.UserID] = memoryEntry{owner: owner, expires: time.Now().Add(m.ttl)}
	return previous, nil
}
func (m *MemoryRegistry) Renew(_ context.Context, owner Owner) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.available {
		return false, ErrUnavailable
	}
	e, ok := m.owners[owner.UserID]
	if !ok || !time.Now().Before(e.expires) || e.owner.LeaseToken != owner.LeaseToken {
		return false, nil
	}
	e.expires = time.Now().Add(m.ttl)
	m.owners[owner.UserID] = e
	return true, nil
}
func (m *MemoryRegistry) Release(_ context.Context, owner Owner) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.available {
		return false, ErrUnavailable
	}
	e, ok := m.owners[owner.UserID]
	if !ok || e.owner.LeaseToken != owner.LeaseToken {
		return false, nil
	}
	delete(m.owners, owner.UserID)
	return true, nil
}
func (m *MemoryRegistry) PublishEviction(_ context.Context, target Owner) error {
	m.mu.Lock()
	if !m.available {
		m.mu.Unlock()
		return ErrUnavailable
	}
	callbacks := make([]func(Owner), 0, len(m.watchers))
	for _, cb := range m.watchers {
		callbacks = append(callbacks, cb)
	}
	m.mu.Unlock()
	for _, cb := range callbacks {
		cb(target)
	}
	return nil
}
func (m *MemoryRegistry) Subscribe(ctx context.Context, handler func(Owner)) error {
	m.mu.Lock()
	if !m.available {
		m.mu.Unlock()
		return ErrUnavailable
	}
	id := m.nextWatch
	m.nextWatch++
	m.watchers[id] = handler
	m.mu.Unlock()
	<-ctx.Done()
	m.mu.Lock()
	delete(m.watchers, id)
	m.mu.Unlock()
	return nil
}
