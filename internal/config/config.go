package config

import "time"

const (
	DefaultMaxEnvelopeBytes = 64 * 1024
	DefaultSendQueueSize    = 256
)

type Config struct {
	ListenAddr             string
	MaxEnvelopeBytes       int64
	SendQueueSize          int
	WriteTimeout           time.Duration
	HeartbeatCheckInterval time.Duration
	IdleTimeout            time.Duration
	BackendRPCTimeout      time.Duration
}

func Default() Config {
	return Config{
		ListenAddr:             ":8080",
		MaxEnvelopeBytes:       DefaultMaxEnvelopeBytes,
		SendQueueSize:          DefaultSendQueueSize,
		WriteTimeout:           5 * time.Second,
		HeartbeatCheckInterval: 15 * time.Second,
		IdleTimeout:            45 * time.Second,
		BackendRPCTimeout:      500 * time.Millisecond,
	}
}
