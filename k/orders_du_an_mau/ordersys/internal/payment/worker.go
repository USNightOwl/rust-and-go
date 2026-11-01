// Package payment: worker đọc order.placed, thu tiền qua cổng thanh toán, phát payment.result.
package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"uuid"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
)

// Gateway là cổng thanh toán bên ngoài
type Gateway interface {
	// Charge thu tiền; idempotencyKey giúp cổng thanh toán bỏ qua yêu cầu trùng
	Charge(ctx context.Context, idempotencyKey, userID string, amount int64) error
}

// ErrDeclined: thẻ bị từ chối — lỗi nghiệp vụ, không thử lại
var ErrDeclined = errors.New("giao dịch bị từ chối")

// Deduper ghi nhớ event đã xử lý. Production: bảng processed_events(event_id PK)
// trong cùng transaction với thay đổi nghiệp vụ.
type Deduper interface {
	Seen(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}

// MemoryDeduper cài đặt Deduper trong bộ nhớ (dev/test)
type MemoryDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func NewMemoryDeduper() *MemoryDeduper { return &MemoryDeduper{seen: map[string]bool{}} }

func (d *MemoryDeduper) Seen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[id], nil
}

func (d *MemoryDeduper) Mark(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[id] = true
	return nil
}

// Worker xử lý event order.placed
type Worker struct {
	gw    Gateway
	dedup Deduper
	pub   *kgo.Client
	now   func() time.Time
}

func NewWorker(gw Gateway, dedup Deduper, pub *kgo.Client) *Worker {
	return &Worker{gw: gw, dedup: dedup, pub: pub, now: time.Now}
}

// Handle là kafkax.Handler cho topic orders
func (w *Worker) Handle(ctx context.Context, r *kgo.Record) error {
	var o events.OrderPlaced
	ev, err := events.Decode(r.Value, events.TypeOrderPlaced, &o)
	if err != nil {
		return kafkax.Permanent(err) // poison pill -> DLQ
	}
	if seen, err := w.dedup.Seen(ctx, ev.ID); err != nil {
		return err // lỗi tạm thời của kho dedup -> thử lại
	} else if seen {
		return nil // event trùng: đã xử lý, bỏ qua
	}

	result := events.PaymentResult{OrderID: o.OrderID, UserID: o.UserID, Status: "PAID"}
	if err := w.gw.Charge(ctx, ev.ID, o.UserID, o.Amount); err != nil {
		if !errors.Is(err, ErrDeclined) {
			return err // lỗi tạm thời của cổng thanh toán -> thử lại
		}
		result.Status, result.Reason = "FAILED", err.Error()
	}

	out, err := events.New(uuid.NewV7().String(), events.TypePaymentResult, w.now(), result)
	if err != nil {
		return kafkax.Permanent(err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return kafkax.Permanent(err)
	}
	rec := &kgo.Record{Topic: events.TopicPayments, Key: []byte(o.OrderID), Value: b}
	if err := w.pub.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("phát payment.result: %w", err)
	}
	// Đánh dấu SAU khi phát thành công: crash ở giữa -> xử lý lại, cổng thanh toán
	// nhận cùng idempotency key nên không thu tiền hai lần.
	return w.dedup.Mark(ctx, ev.ID)
}
