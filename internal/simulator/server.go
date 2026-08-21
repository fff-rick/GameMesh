package simulator

import (
	"errors"
	"math"
	"sync"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

var (
	ErrServerUnavailable = errors.New("game server is not accepting new players")
	ErrServerFull        = errors.New("game server is full")
)

// ServerConfig makes heterogeneous GameServer pools easy to reproduce.
type ServerConfig struct {
	ID               string
	Region           string
	Version          string
	Capacity         int
	BaseCPUPercent   float64
	BaseMemory       float64
	BaseTickMillis   float64
	NetworkPerPlayer float64
}

// GameServer is a deterministic in-memory simulator, not a real game process.
type GameServer struct {
	mu sync.RWMutex

	cfg     ServerConfig
	state   model.ServerState
	players int
	metrics model.ServerMetrics
}

func NewGameServer(cfg ServerConfig) *GameServer {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1000
	}
	if cfg.Version == "" {
		cfg.Version = "v1"
	}
	if cfg.Region == "" {
		cfg.Region = "default"
	}
	if cfg.BaseTickMillis <= 0 {
		cfg.BaseTickMillis = 10
	}
	if cfg.NetworkPerPlayer <= 0 {
		cfg.NetworkPerPlayer = 0.002
	}

	s := &GameServer{cfg: cfg, state: model.ServerReady}
	s.recalculateLocked()
	return s
}

func (s *GameServer) ID() string { return s.cfg.ID }

func (s *GameServer) Snapshot() model.GameServerSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.GameServerSnapshot{
		ID:             s.cfg.ID,
		Region:         s.cfg.Region,
		Version:        s.cfg.Version,
		State:          s.state,
		Capacity:       s.cfg.Capacity,
		CurrentPlayers: s.players,
		Metrics:        s.metrics,
	}
}

func (s *GameServer) Allocate(players int) error {
	if players <= 0 {
		players = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != model.ServerReady && s.state != model.ServerAllocated {
		return ErrServerUnavailable
	}
	if s.players+players > s.cfg.Capacity {
		return ErrServerFull
	}
	s.players += players
	if s.players > 0 {
		s.state = model.ServerAllocated
	}
	s.recalculateLocked()
	return nil
}

func (s *GameServer) Release(players int) {
	if players <= 0 {
		players = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.players -= players
	if s.players < 0 {
		s.players = 0
	}
	if s.players == 0 && s.state == model.ServerAllocated {
		s.state = model.ServerReady
	}
	s.recalculateLocked()
}

func (s *GameServer) SetState(state model.ServerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

func (s *GameServer) InjectQueue(length int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if length < 0 {
		length = 0
	}
	s.metrics.QueueLength = length
}

func (s *GameServer) recalculateLocked() {
	util := float64(s.players) / float64(s.cfg.Capacity)
	// The non-linear tail intentionally models the way tick latency often grows
	// faster as a server approaches saturation.
	tail := 0.0
	if util > 0.75 {
		tail = math.Pow((util-0.75)/0.25, 2) * 35
	}

	s.metrics.CPUPercent = clamp(s.cfg.BaseCPUPercent+util*72, 0, 100)
	s.metrics.MemoryPercent = clamp(s.cfg.BaseMemory+util*55, 0, 100)
	s.metrics.NetworkMbps = float64(s.players) * s.cfg.NetworkPerPlayer
	s.metrics.TickLatencyMillis = s.cfg.BaseTickMillis + util*4 + tail
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
