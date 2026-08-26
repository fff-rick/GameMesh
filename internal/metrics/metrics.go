package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	gatewayID        string
	connections      atomic.Int64
	connectionsTotal atomic.Uint64
	messagesReceived atomic.Uint64
	messagesSent     atomic.Uint64
	bytesReceived    atomic.Uint64
	bytesSent        atomic.Uint64
	queueDepthMax    atomic.Int64
	mu               sync.Mutex
	disconnects      map[string]uint64
}

func New(gatewayID string) *Metrics {
	return &Metrics{gatewayID: gatewayID, disconnects: map[string]uint64{}}
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
	fmt.Fprintf(w, "# TYPE game_gateway_connections gauge\ngame_gateway_connections{gateway_id=\"%s\",state=\"open\"} %d\n", gid, m.connections.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_connections_total counter\ngame_gateway_connections_total{gateway_id=\"%s\",result=\"accepted\"} %d\n", gid, m.connectionsTotal.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_messages_received_total counter\ngame_gateway_messages_received_total{gateway_id=\"%s\",class=\"envelope\"} %d\n", gid, m.messagesReceived.Load())
	fmt.Fprintf(w, "# TYPE game_gateway_messages_sent_total counter\ngame_gateway_messages_sent_total{gateway_id=\"%s\",class=\"envelope\"} %d\n", gid, m.messagesSent.Load())
	fmt.Fprintf(w, "game_gateway_message_bytes_received_total{gateway_id=\"%s\"} %d\n", gid, m.bytesReceived.Load())
	fmt.Fprintf(w, "game_gateway_message_bytes_sent_total{gateway_id=\"%s\"} %d\n", gid, m.bytesSent.Load())
	fmt.Fprintf(w, "game_gateway_send_queue_depth_max{gateway_id=\"%s\"} %d\n", gid, m.queueDepthMax.Load())
	m.mu.Lock()
	keys := make([]string, 0, len(m.disconnects))
	for k := range m.disconnects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "game_gateway_disconnects_total{gateway_id=\"%s\",reason=\"%s\"} %d\n", gid, escape(k), m.disconnects[k])
	}
	m.mu.Unlock()
}
func escape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(s)
}
