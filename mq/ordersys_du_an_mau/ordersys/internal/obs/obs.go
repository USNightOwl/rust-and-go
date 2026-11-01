// Package obs: logging, trace id và metrics Prometheus tối giản (chỉ thư viện chuẩn).
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// NewLogger tạo logger dạng key=value, gắn sẵn tên service.
func NewLogger(service string) *slog.Logger {
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.String("t", a.Value.Time().Format("15:04:05.000"))
			}
			return a
		},
	})
	return slog.New(h).With("svc", service)
}

// ---- trace id: sinh ở HTTP, đi qua outbox, header AMQP x-trace-id, sang worker.

// HeaderTraceID là tên header dùng cho cả HTTP và AMQP.
const HeaderTraceID = "x-trace-id"

type traceKey struct{}

func NewTraceID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey{}, id)
}

func TraceID(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

// ---- metrics: chỉ counter, xuất theo định dạng văn bản của Prometheus.

type Metrics struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64 // khóa: name{k="v",...}
}

func NewMetrics() *Metrics { return &Metrics{counters: map[string]*atomic.Int64{}} }

// Inc tăng counter; labels là các cặp khóa, giá trị.
func (m *Metrics) Inc(name string, labels ...string) {
	var b strings.Builder
	b.WriteString(name)
	if len(labels) > 0 {
		b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s=%q", labels[i], labels[i+1])
		}
		b.WriteByte('}')
	}
	key := b.String()
	m.mu.Lock()
	c, ok := m.counters[key]
	if !ok {
		c = new(atomic.Int64)
		m.counters[key] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.counters))
	for k := range m.counters {
		keys = append(keys, k)
	}
	m.mu.Unlock()
	slices.Sort(keys)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	typed := map[string]bool{}
	for _, k := range keys {
		name, _, _ := strings.Cut(k, "{")
		if !typed[name] {
			fmt.Fprintf(w, "# TYPE %s counter\n", name)
			typed[name] = true
		}
		m.mu.Lock()
		v := m.counters[k].Load()
		m.mu.Unlock()
		fmt.Fprintf(w, "%s %d\n", k, v)
	}
}
