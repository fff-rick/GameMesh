package main

import (
	"context"
	"flag"
	"game-gateway/internal/config"
	"game-gateway/internal/gateway"
	"game-gateway/internal/presence"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "listen address")
	flag.StringVar(&cfg.PresenceRedisAddr, "presence-redis", cfg.PresenceRedisAddr, "Redis address; empty disables distributed presence")
	flag.StringVar(&cfg.PresenceKeyPrefix, "presence-prefix", cfg.PresenceKeyPrefix, "Redis key/channel prefix for distributed presence")
	flag.DurationVar(&cfg.PresenceLeaseTTL, "presence-lease-ttl", cfg.PresenceLeaseTTL, "distributed presence lease TTL")
	flag.DurationVar(&cfg.PresenceRenewInterval, "presence-renew-interval", cfg.PresenceRenewInterval, "distributed presence renewal interval")
	flag.DurationVar(&cfg.PresenceOperationTimeout, "presence-timeout", cfg.PresenceOperationTimeout, "distributed presence operation timeout")
	flag.IntVar(&cfg.ConnectionRate, "connection-rate", cfg.ConnectionRate, "maximum inbound envelopes per second per connection")
	flag.IntVar(&cfg.ConnectionRateBurst, "connection-rate-burst", cfg.ConnectionRateBurst, "per-connection inbound burst")
	flag.IntVar(&cfg.GlobalRate, "global-rate", cfg.GlobalRate, "maximum inbound envelopes per second per Gateway")
	flag.IntVar(&cfg.GlobalRateBurst, "global-rate-burst", cfg.GlobalRateBurst, "Gateway-wide inbound burst")
	flag.IntVar(&cfg.BackendMaxInFlight, "backend-max-in-flight", cfg.BackendMaxInFlight, "maximum concurrent backend RPCs")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "maximum graceful-drain duration")
	gatewayID := flag.String("gateway-id", "gateway-1", "gateway instance id")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var opts []gateway.Option
	var redisClient *redis.Client
	if cfg.PresenceRedisAddr != "" {
		redisClient = redis.NewClient(&redis.Options{Addr: cfg.PresenceRedisAddr})
		defer redisClient.Close()
		opts = append(opts, gateway.WithPresenceRegistry(presence.NewRedisRegistry(redisClient, cfg.PresenceKeyPrefix, cfg.PresenceLeaseTTL)))
	}
	gw := gateway.New(cfg, *gatewayID, logger, opts...)
	hs := &http.Server{Addr: cfg.ListenAddr, Handler: gw.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("gateway listening", "addr", cfg.ListenAddr)
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	gw.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil {
		logger.Warn("http shutdown did not finish cleanly", "error", err)
	}
	if err := gw.Drain(ctx); err != nil {
		logger.Warn("gateway drain timed out; remaining connections closed", "error", err)
	}
}
