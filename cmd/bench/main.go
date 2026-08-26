package main

import (
	"flag"
	"fmt"
	"game-gateway/internal/config"
	"game-gateway/internal/gateway"
	"game-gateway/internal/protocol"
	"game-gateway/internal/ws"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	clients := flag.Int("clients", 50, "concurrent clients")
	messages := flag.Int("messages", 100, "messages per client")
	payloadSize := flag.Int("payload", 256, "payload bytes")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gw := gateway.New(config.Default(), "bench", logger)
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	payload := make([]byte, *payloadSize)
	lat := make([]int64, 0, *clients**messages)
	var mu sync.Mutex
	var failures atomic.Uint64
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := ws.Dial(url, 2*time.Second)
			if err != nil {
				failures.Add(1)
				return
			}
			defer c.Close()
			local := make([]int64, 0, *messages)
			for j := 0; j < *messages; j++ {
				env := protocol.Envelope{Version: 1, MessageType: 1, RequestID: fmt.Sprintf("%d-%d", id, j), Payload: payload}
				t0 := time.Now()
				if err := c.WriteBinary(protocol.Marshal(env)); err != nil {
					failures.Add(1)
					return
				}
				if _, err := c.ReadBinary(64 * 1024); err != nil {
					failures.Add(1)
					return
				}
				local = append(local, time.Since(t0).Nanoseconds())
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		idx := int(float64(len(lat)-1) * p)
		return time.Duration(lat[idx])
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	success := len(lat)
	fmt.Printf("date=%s\n", time.Now().Format(time.RFC3339))
	fmt.Printf("go_version=%s\n", runtime.Version())
	fmt.Printf("goos=%s goarch=%s cpus=%d\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("clients=%d messages_per_client=%d payload_bytes=%d\n", *clients, *messages, *payloadSize)
	fmt.Printf("success_messages=%d failures=%d elapsed=%s throughput_msg_s=%.2f\n", success, failures.Load(), elapsed, float64(success)/elapsed.Seconds())
	fmt.Printf("p50=%s p95=%s p99=%s\n", pct(.50), pct(.95), pct(.99))
	fmt.Printf("alloc_bytes=%d sys_bytes=%d goroutines=%d\n", ms.Alloc, ms.Sys, runtime.NumGoroutine())
	_ = os.Stdout.Sync()
}
