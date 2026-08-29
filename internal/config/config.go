package config

import "time"

const (
	DefaultMaxEnvelopeBytes   = 64 * 1024
	DefaultSendQueueSize      = 256
	DefaultSessionGracePeriod = time.Minute
)

type Config struct {
	ListenAddr             string
	MaxEnvelopeBytes       int64
	SendQueueSize          int
	WriteTimeout           time.Duration
	HeartbeatCheckInterval time.Duration
	IdleTimeout            time.Duration
	SessionGracePeriod     time.Duration
	BackendRPCTimeout      time.Duration
	ReliableRetryInterval  time.Duration
	ReliableMaxRetries     int
	ReliablePendingLimit   int
	ReliableDedupWindow    int
}

func Default() Config {
	return Config{
		ListenAddr:             ":8080",
		MaxEnvelopeBytes:       DefaultMaxEnvelopeBytes,
		SendQueueSize:          DefaultSendQueueSize,
		WriteTimeout:           5 * time.Second,
		HeartbeatCheckInterval: 15 * time.Second,
		IdleTimeout:            45 * time.Second,
		SessionGracePeriod:     DefaultSessionGracePeriod,
		BackendRPCTimeout:      500 * time.Millisecond,
		ReliableRetryInterval:  500 * time.Millisecond,
		ReliableMaxRetries:     3,
		ReliablePendingLimit:   128,
		ReliableDedupWindow:    256,
	}
}
