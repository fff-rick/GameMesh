package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	gatewayID               string
	connections             atomic.Int64
	connectionsTotal        atomic.Uint64
	messagesReceived        atomic.Uint64
	messagesSent            atomic.Uint64
	bytesReceived           atomic.Uint64
	bytesSent               atomic.Uint64
	queueDepthMax           atomic.Int64
	sessionCounts           atomic.Pointer[sessionCounts]
	graceExpired            atomic.Uint64
	heartbeatTimeouts       atomic.Uint64
	reliablePending         atomic.Int64
	reliableRetries         atomic.Uint64
	reliableDedup           atomic.Uint64
	reliableOutOfOrder      atomic.Uint64
	reliableRetryExhausted  atomic.Uint64
	reliablePendingOverflow atomic.Uint64
	mu                      sync.Mutex
	disconnects             map[string]uint64
	authResults             map[string]uint64
	recoveryResults         map[string]uint64
	backendRPCResults       map[string]uint64
	backendRPCLatency       map[string]latencyAgg
	presenceResults         map[string]uint64
	presenceLeases          atomic.Int64
	messagesDropped         atomic.Uint64
	backendRejected         atomic.Uint64
	rateLimits              map[string]uint64
	shutdowns               map[string]uint64
}

type sessionCounts struct {
	active int64
	grace  int64
}

func New(gatewayID string) *Metrics {
	m := &Metrics{gatewayID: gatewayID, disconnects: map[string]uint64{}, authResults: map[string]uint64{}, recoveryResults: map[string]uint64{}, backendRPCResults: map[string]uint64{}, backendRPCLatency: map[string]latencyAgg{}, presenceResults: map[string]uint64{}, rateLimits: map[string]uint64{}, shutdowns: map[string]uint64{}}
	m.sessionCounts.Store(&sessionCounts{})
	return m
}
func (m *Metrics) ConnectionOpened() { m.connections.Add(1); m.connectionsTotal.Add(1) }
func (m *Metrics) ConnectionClosed(reason string) {
	m.connections.Add(-1)
	m.mu.Lock()
	m.disconnects[reason]++
	m.mu.Unlock()
}
func (m *Metrics) Received(n int) { m.messagesReceived.Add(1); m.bytesReceived.Add(uint64(n)) }
func (m *Metrics) Sent(n int)     { m.messagesSent.Add(1); m.bytesSent.Add(uint64(n)) }
func (m *Metrics) SetActiveSessions(n int) {
	for {
		old := m.sessionCounts.Load()
		next := &sessionCounts{active: int64(n), grace: old.grace}
		if m.sessionCounts.CompareAndSwap(old, next) {
			return
		}
	}
}
func (m *Metrics) SetGraceSessions(n int) {
	for {
		old := m.sessionCounts.Load()
		next := &sessionCounts{active: old.active, grace: int64(n)}
		if m.sessionCounts.CompareAndSwap(old, next) {
			return
		}
	}
}
func (m *Metrics) SetSessionCounts(active, grace int) {
	m.sessionCounts.Store(&sessionCounts{active: int64(active), grace: int64(grace)})
}
func (m *Metrics) GraceExpired()            { m.graceExpired.Add(1) }
func (m *Metrics) HeartbeatTimeout()        { m.heartbeatTimeouts.Add(1) }
func (m *Metrics) SetReliablePending(n int) { m.reliablePending.Store(int64(n)) }
func (m *Metrics) ReliableRetry()           { m.reliableRetries.Add(1) }
func (m *Metrics) ReliableDedup()           { m.reliableDedup.Add(1) }
func (m *Metrics) ReliableOutOfOrder()      { m.reliableOutOfOrder.Add(1) }
func (m *Metrics) ReliableRetryExhausted()  { m.reliableRetryExhausted.Add(1) }
func (m *Metrics) ReliablePendingOverflow() { m.reliablePendingOverflow.Add(1) }
func (m *Metrics) AuthResult(result string) {
	m.mu.Lock()
	m.authResults[result]++
	m.mu.Unlock()
}

func (m *Metrics) RecoveryResult(result string) {
	m.mu.Lock()
	m.recoveryResults[result]++
	m.mu.Unlock()
}
func (m *Metrics) Presence(result string)   { m.mu.Lock(); m.presenceResults[result]++; m.mu.Unlock() }
func (m *Metrics) SetPresenceLeases(n int)  { m.presenceLeases.Store(int64(n)) }
func (m *Metrics) MessageDropped()          { m.messagesDropped.Add(1) }
func (m *Metrics) BackendRejected()         { m.backendRejected.Add(1) }
func (m *Metrics) RateLimited(scope string) { m.mu.Lock(); m.rateLimits[scope]++; m.mu.Unlock() }
func (m *Metrics) Shutdown(result string)   { m.mu.Lock(); m.shutdowns[result]++; m.mu.Unlock() }

type latencyAgg struct {
	Count      uint64
	SumSeconds float64
}

func metricKey(parts ...string) string { return strings.Join(parts, "\x00") }

func (m *Metrics) BackendRPC(backendType, method, result string, d time.Duration) {
	m.mu.Lock()
	m.backendRPCResults[metricKey(backendType, method, result)]++
	k := metricKey(backendType, method)
	a := m.backendRPCLatency[k]
	a.Count++
	a.SumSeconds += d.Seconds()
	m.backendRPCLatency[k] = a
	m.mu.Unlock()
}

func (m *Metrics) ObserveQueueDepth(n int) {
	for {
		old := m.queueDepthMax.Load()
		if int64(n) <= old {
			return
		}
		if m.queueDepthMax.CompareAndSwap(old, int64(n)) {
			return
		}
	}
}
func (m *Metrics) WritePrometheus(w io.Writer) {
	gid := escape(m.gatewayID)
	sessions := m.sessionCounts.Load()
	fmt.Fprintf(w, "# TYPE game_gateway_connections gauge\ngame_gateway_connections{gateway_id=\"%s\",state=\"open\"} %d\n", gid, m.connections.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_connections_total counter\ngame_gateway_connections_total{gateway_id=\"%s\",result=\"accepted\"} %d\n", gid, m.connectionsTotal.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_messages_received_total counter\ngame_gateway_messages_received_total{gateway_id=\"%s\",class=\"envelope\"} %d\n", gid, m.messagesReceived.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_messages_sent_total counter\ngame_gateway_messages_sent_total{gateway_id=\"%s\",class=\"envelope\"} %d\n", gid, m.messagesSent.Load())
	fmt.Fprintf(w, "game_gateway_message_bytes_received_total{gateway_id=\"%s\"} %d\n", gid, m.bytesReceived.Load())
	fmt.Fprintf(w, "game_gateway_message_bytes_sent_total{gateway_id=\"%s\"} %d\n", gid, m.bytesSent.Load())
	fmt.Fprintf(w, "game_gateway_send_queue_depth_max{gateway_id=\"%s\"} %d\n", gid, m.queueDepthMax.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_sessions gauge\ngame_gateway_sessions{gateway_id=\"%s\",state=\"active\"} %d\n", gid, sessions.active)
	fmt.Fprintf(w, "game_gateway_sessions{gateway_id=\"%s\",state=\"grace\"} %d\n", gid, sessions.grace)
	fmt.Fprintf(w, "# TYPE game_gateway_session_grace_expired_total counter\ngame_gateway_session_grace_expired_total{gateway_id=\"%s\"} %d\n", gid, m.graceExpired.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_heartbeat_timeouts_total counter\ngame_gateway_heartbeat_timeouts_total{gateway_id=\"%s\"} %d\n", gid, m.heartbeatTimeouts.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_pending gauge\ngame_gateway_reliable_pending{gateway_id=\"%s\"} %d\n", gid, m.reliablePending.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_retries_total counter\ngame_gateway_reliable_retries_total{gateway_id=\"%s\"} %d\n", gid, m.reliableRetries.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_dedup_total counter\ngame_gateway_reliable_dedup_total{gateway_id=\"%s\"} %d\n", gid, m.reliableDedup.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_out_of_order_total counter\ngame_gateway_reliable_out_of_order_total{gateway_id=\"%s\"} %d\n", gid, m.reliableOutOfOrder.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_retry_exhausted_total counter\ngame_gateway_reliable_retry_exhausted_total{gateway_id=\"%s\"} %d\n", gid, m.reliableRetryExhausted.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_reliable_pending_overflow_total counter\ngame_gateway_reliable_pending_overflow_total{gateway_id=\"%s\"} %d\n", gid, m.reliablePendingOverflow.Load())
	fmt.Fprintf(w, "game_gateway_presence_leases{gateway_id=\"%s\"} %d\n", gid, m.presenceLeases.Load())
	fmt.Fprintf(w, "game_gateway_messages_dropped_total{gateway_id=\"%s\",policy=\"best_effort_queue_full\"} %d\n", gid, m.messagesDropped.Load())
	fmt.Fprintf(w, "game_gateway_backend_rejected_total{gateway_id=\"%s\",reason=\"in_flight_limit\"} %d\n", gid, m.backendRejected.Load())
	m.mu.Lock()
	keys := make([]string, 0, len(m.disconnects))
	for k := range m.disconnects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "game_gateway_disconnects_total{gateway_id=\"%s\",reason=\"%s\"} %d\n", gid, escape(k), m.disconnects[k])
	}

	authKeys := make([]string, 0, len(m.authResults))
	for k := range m.authResults {
		authKeys = append(authKeys, k)
	}
	sort.Strings(authKeys)
	for _, k := range authKeys {
		fmt.Fprintf(w, "game_gateway_auth_total{gateway_id=\"%s\",result=\"%s\"} %d\n", gid, escape(k), m.authResults[k])
	}
	recoveryKeys := make([]string, 0, len(m.recoveryResults))
	for k := range m.recoveryResults {
		recoveryKeys = append(recoveryKeys, k)
	}
	sort.Strings(recoveryKeys)
	for _, k := range recoveryKeys {
		fmt.Fprintf(w, "game_gateway_recovery_total{gateway_id=\"%s\",result=\"%s\"} %d\n", gid, escape(k), m.recoveryResults[k])
	}
	presenceKeys := make([]string, 0, len(m.presenceResults))
	for k := range m.presenceResults {
		presenceKeys = append(presenceKeys, k)
	}
	sort.Strings(presenceKeys)
	for _, k := range presenceKeys {
		fmt.Fprintf(w, "game_gateway_presence_operations_total{gateway_id=\"%s\",result=\"%s\"} %d\n", gid, escape(k), m.presenceResults[k])
	}
	rateKeys := make([]string, 0, len(m.rateLimits))
	for k := range m.rateLimits {
		rateKeys = append(rateKeys, k)
	}
	sort.Strings(rateKeys)
	for _, k := range rateKeys {
		fmt.Fprintf(w, "game_gateway_rate_limited_total{gateway_id=\"%s\",scope=\"%s\"} %d\n", gid, escape(k), m.rateLimits[k])
	}
	shutdownKeys := make([]string, 0, len(m.shutdowns))
	for k := range m.shutdowns {
		shutdownKeys = append(shutdownKeys, k)
	}
	sort.Strings(shutdownKeys)
	for _, k := range shutdownKeys {
		fmt.Fprintf(w, "game_gateway_shutdown_total{gateway_id=\"%s\",result=\"%s\"} %d\n", gid, escape(k), m.shutdowns[k])
	}
	rpcKeys := make([]string, 0, len(m.backendRPCResults))
	for k := range m.backendRPCResults {
		rpcKeys = append(rpcKeys, k)
	}
	sort.Strings(rpcKeys)
	for _, k := range rpcKeys {
		parts := strings.Split(k, "\x00")
		fmt.Fprintf(w, "game_gateway_backend_rpc_total{backend_type=\"%s\",method=\"%s\",result=\"%s\"} %d\n", escape(parts[0]), escape(parts[1]), escape(parts[2]), m.backendRPCResults[k])
	}
	latKeys := make([]string, 0, len(m.backendRPCLatency))
	for k := range m.backendRPCLatency {
		latKeys = append(latKeys, k)
	}
	sort.Strings(latKeys)
	for _, k := range latKeys {
		parts := strings.Split(k, "\x00")
		a := m.backendRPCLatency[k]
		fmt.Fprintf(w, "game_gateway_backend_rpc_seconds_count{backend_type=\"%s\",method=\"%s\"} %d\n", escape(parts[0]), escape(parts[1]), a.Count)
		fmt.Fprintf(w, "game_gateway_backend_rpc_seconds_sum{backend_type=\"%s\",method=\"%s\"} %.9f\n", escape(parts[0]), escape(parts[1]), a.SumSeconds)
	}
	m.mu.Unlock()
}
func escape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(s)
}
