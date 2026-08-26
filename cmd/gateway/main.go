package main

import (
	"context"
	"flag"
	"game-gateway/internal/config"
	"game-gateway/internal/gateway"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "listen address")
	gatewayID := flag.String("gateway-id", "gateway-1", "gateway instance id")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	gw := gateway.New(cfg, *gatewayID, logger)
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
	gw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(ctx)
}
