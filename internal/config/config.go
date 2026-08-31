package config

import "time"

const (
	DefaultMaxEnvelopeBytes   = 64 * 1024
	DefaultSendQueueSize      = 256
	DefaultSessionGracePeriod = time.Minute
	DefaultPresenceLeaseTTL   = 30 * time.Second
	DefaultPresenceRenewEvery = 10 * time.Second
	DefaultPresenceTimeout    = 500 * time.Millisecond
	DefaultConnectionRate     = 1000
	DefaultConnectionBurst    = 200
	DefaultGlobalRate         = 20000
	DefaultGlobalBurst        = 4000
	DefaultBackendMaxInFlight = 1024
	DefaultDrainTimeout       = 10 * time.Second
)

type Config struct {
	ListenAddr               string
	MaxEnvelopeBytes         int64
	SendQueueSize            int
	WriteTimeout             time.Duration
	HeartbeatCheckInterval   time.Duration
	IdleTimeout              time.Duration
	SessionGracePeriod       time.Duration
	BackendRPCTimeout        time.Duration
	ReliableRetryInterval    time.Duration
	ReliableMaxRetries       int
	ReliablePendingLimit     int
	ReliableDedupWindow      int
	PresenceRedisAddr        string
	PresenceKeyPrefix        string
	PresenceLeaseTTL         time.Duration
	PresenceRenewInterval    time.Duration
	PresenceOperationTimeout time.Duration
	ConnectionRate           int
	ConnectionRateBurst      int
	GlobalRate               int
	GlobalRateBurst          int
	BackendMaxInFlight       int
	DrainTimeout             time.Duration
}

func Default() Config {
	return Config{
		ListenAddr:               ":8080",
		MaxEnvelopeBytes:         DefaultMaxEnvelopeBytes,
		SendQueueSize:            DefaultSendQueueSize,
		WriteTimeout:             5 * time.Second,
		HeartbeatCheckInterval:   15 * time.Second,
		IdleTimeout:              45 * time.Second,
		SessionGracePeriod:       DefaultSessionGracePeriod,
		BackendRPCTimeout:        500 * time.Millisecond,
		ReliableRetryInterval:    500 * time.Millisecond,
		ReliableMaxRetries:       3,
		ReliablePendingLimit:     128,
		ReliableDedupWindow:      256,
		PresenceKeyPrefix:        "game-gateway:presence",
		PresenceLeaseTTL:         DefaultPresenceLeaseTTL,
		PresenceRenewInterval:    DefaultPresenceRenewEvery,
		PresenceOperationTimeout: DefaultPresenceTimeout,
		ConnectionRate:           DefaultConnectionRate,
		ConnectionRateBurst:      DefaultConnectionBurst,
		GlobalRate:               DefaultGlobalRate,
		GlobalRateBurst:          DefaultGlobalBurst,
		BackendMaxInFlight:       DefaultBackendMaxInFlight,
		DrainTimeout:             DefaultDrainTimeout,
	}
}
