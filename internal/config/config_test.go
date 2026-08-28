package config

import "testing"

func TestReliableMessagingDefaultsAreBounded(t *testing.T) {
	cfg := Default()
	if cfg.ReliablePendingLimit <= 0 {
		t.Fatalf("pending_limit=%d", cfg.ReliablePendingLimit)
	}
	if cfg.ReliableDedupWindow <= 0 {
		t.Fatalf("dedup_window=%d", cfg.ReliableDedupWindow)
	}
	if cfg.ReliableRetryInterval <= 0 {
		t.Fatalf("retry_interval=%v", cfg.ReliableRetryInterval)
	}
	if cfg.ReliableMaxRetries <= 0 {
		t.Fatalf("max_retries=%d", cfg.ReliableMaxRetries)
	}
}
