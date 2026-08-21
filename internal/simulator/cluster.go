package simulator

import (
	"fmt"
	"sort"
	"sync"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

// Cluster owns the in-memory GameServer pool.
type Cluster struct {
	mu      sync.RWMutex
	servers map[string]*GameServer
	order   []string
}

func NewCluster(configs []ServerConfig) *Cluster {
	c := &Cluster{servers: make(map[string]*GameServer, len(configs))}
	for _, cfg := range configs {
		s := NewGameServer(cfg)
		c.servers[cfg.ID] = s
		c.order = append(c.order, cfg.ID)
	}
	sort.Strings(c.order)
	return c
}

// NewHeterogeneousCluster creates a repeatable pool where capacity and idle
// cost differ slightly. This exposes RoundRobin's limitations in later reports.
func NewHeterogeneousCluster(count, baseCapacity int) *Cluster {
	if count <= 0 {
		count = 10
	}
	if baseCapacity <= 0 {
		baseCapacity = 1000
	}
	regions := []string{"ap-southeast", "ap-northeast", "us-west"}
	configs := make([]ServerConfig, 0, count)
	for i := 0; i < count; i++ {
		capacityMultiplier := []float64{0.80, 1.00, 1.20, 1.40}[i%4]
		configs = append(configs, ServerConfig{
			ID:               fmt.Sprintf("gs-%04d", i+1),
			Address:          fmt.Sprintf("127.0.0.1:%d", 7000+i),
			Region:           regions[i%len(regions)],
			Zone:             fmt.Sprintf("zone-%c", 'a'+rune(i%3)),
			Version:          "v1",
			Capacity:         int(float64(baseCapacity) * capacityMultiplier),
			BaseCPUPercent:   6 + float64((i*7)%13),
			BaseMemory:       12 + float64((i*5)%11),
			BaseTickMillis:   8 + float64(i%5),
			NetworkPerPlayer: 0.002,
		})
	}
	return NewCluster(configs)
}

func (c *Cluster) Snapshots() []model.GameServerSnapshot {
	c.mu.RLock()
	order := append([]string(nil), c.order...)
	servers := make([]*GameServer, 0, len(order))
	for _, id := range order {
		servers = append(servers, c.servers[id])
	}
	c.mu.RUnlock()

	out := make([]model.GameServerSnapshot, 0, len(servers))
	for _, s := range servers {
		out = append(out, s.Snapshot())
	}
	return out
}

// Snapshot returns the current state of one GameServer. It is used by control
// plane adapters to publish an allocation-induced heartbeat without rebuilding
// the entire simulated cluster snapshot.
func (c *Cluster) Snapshot(serverID string) (model.GameServerSnapshot, error) {
	c.mu.RLock()
	s, ok := c.servers[serverID]
	c.mu.RUnlock()
	if !ok {
		return model.GameServerSnapshot{}, fmt.Errorf("server %q not found", serverID)
	}
	return s.Snapshot(), nil
}

func (c *Cluster) Allocate(serverID string, players int) error {
	c.mu.RLock()
	s, ok := c.servers[serverID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("server %q not found", serverID)
	}
	return s.Allocate(players)
}

func (c *Cluster) Release(serverID string, players int) error {
	c.mu.RLock()
	s, ok := c.servers[serverID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("server %q not found", serverID)
	}
	s.Release(players)
	return nil
}

func (c *Cluster) SetState(serverID string, state model.ServerState) error {
	c.mu.RLock()
	s, ok := c.servers[serverID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("server %q not found", serverID)
	}
	s.SetState(state)
	return nil
}

func (c *Cluster) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.servers)
}
