// Package notification: worker đọc payment.result và gửi thông báo cho khách hàng.
package notification

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
)

// Notifier gửi thông báo (email, SMS, push...)
type Notifier interface {
	Notify(ctx context.Context, userID, message string) error
}

// LogNotifier ghi thông báo ra log (dev)
type LogNotifier struct{ Log *slog.Logger }

func (n LogNotifier) Notify(_ context.Context, userID, msg string) error {
	n.Log.Info("gửi thông báo", "user_id", userID, "message", msg)
	return nil
}

// MemoryNotifier lưu thông báo trong bộ nhớ (test)
type MemoryNotifier struct {
	mu   sync.Mutex
	Sent []string
}

func (m *MemoryNotifier) Notify(_ context.Context, userID, msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Sent = append(m.Sent, userID+": "+msg)
	return nil
}

// Worker xử lý payment.result
type Worker struct{ n Notifier }

func NewWorker(n Notifier) *Worker { return &Worker{n: n} }

func (w *Worker) Handle(ctx context.Context, r *kgo.Record) error {
	var p events.PaymentResult
	if _, err := events.Decode(r.Value, events.TypePaymentResult, &p); err != nil {
		return kafkax.Permanent(err)
	}
	msg := fmt.Sprintf("Đơn %s đã thanh toán thành công", p.OrderID)
	if p.Status != "PAID" {
		msg = fmt.Sprintf("Đơn %s thanh toán thất bại: %s", p.OrderID, p.Reason)
	}
	return w.n.Notify(ctx, p.UserID, msg)
}
