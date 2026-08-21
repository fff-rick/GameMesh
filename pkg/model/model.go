package model

import "time"

// ServerState describes the lifecycle state of a simulated game server.
type ServerState string

const (
	ServerUnknown    ServerState = "UNKNOWN"
	ServerStarting   ServerState = "STARTING"
	ServerReady      ServerState = "READY"
	ServerAllocated  ServerState = "ALLOCATED"
	ServerDraining   ServerState = "DRAINING"
	ServerUnhealthy  ServerState = "UNHEALTHY"
	ServerTerminated ServerState = "TERMINATED"
)

// Player is the minimum player identity used by M0 simulations.
type Player struct {
	ID     string `json:"id"`
	Region string `json:"region"`
}

// Party models a group that should be kept in the same logical context.
type Party struct {
	ID        string   `json:"id"`
	PlayerIDs []string `json:"player_ids"`
	Region    string   `json:"region"`
}

// Match is a stable binding between players and a game server.
type Match struct {
	ID           string    `json:"id"`
	PartyIDs     []string  `json:"party_ids"`
	GameServerID string    `json:"game_server_id"`
	CreatedAt    time.Time `json:"created_at"`
}

// ServerMetrics are intentionally generic and normalized for future policies.
type ServerMetrics struct {
	CPUPercent        float64 `json:"cpu_percent"`
	MemoryPercent     float64 `json:"memory_percent"`
	NetworkMbps       float64 `json:"network_mbps"`
	TickLatencyMillis float64 `json:"tick_latency_ms"`
	QueueLength       int     `json:"queue_length"`
}

// GameServerSnapshot is an immutable view consumed by schedulers.
type GameServerSnapshot struct {
	ID             string        `json:"id"`
	Address        string        `json:"address,omitempty"`
	Region         string        `json:"region"`
	Zone           string        `json:"zone,omitempty"`
	Version        string        `json:"version"`
	State          ServerState   `json:"state"`
	Capacity       int           `json:"capacity"`
	CurrentPlayers int           `json:"current_players"`
	Metrics        ServerMetrics `json:"metrics"`
}

func (s GameServerSnapshot) Utilization() float64 {
	if s.Capacity <= 0 {
		return 1
	}
	return float64(s.CurrentPlayers) / float64(s.Capacity)
}

func (s GameServerSnapshot) CanAccept() bool {
	if s.State != ServerReady && s.State != ServerAllocated {
		return false
	}
	return s.CurrentPlayers < s.Capacity
}

// AllocationRequest is the scheduler input. Fields added now become semantic
// routing hooks in later milestones without changing the scheduler contract.
type AllocationRequest struct {
	PlayerID string `json:"player_id"`
	PartyID  string `json:"party_id,omitempty"`
	MatchID  string `json:"match_id,omitempty"`
	Region   string `json:"region,omitempty"`
	Version  string `json:"version,omitempty"`
}

// AllocationResult records the selected server and scheduling metadata.
type AllocationResult struct {
	GameServerID string        `json:"game_server_id"`
	Strategy     string        `json:"strategy"`
	Latency      time.Duration `json:"-"`
}
