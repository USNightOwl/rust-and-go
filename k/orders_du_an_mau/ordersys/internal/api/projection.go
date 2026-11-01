package api

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
)

// OrderView là trạng thái đơn hàng mà API trả cho client
type OrderView struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Amount  int64  `json:"amount"`
	Status  string `json:"status"` // PENDING, PAID, FAILED
	Reason  string `json:"reason,omitempty"`
}

// Projection là "read model" trong bộ nhớ, dựng lại từ topic orders và payments.
// Mỗi instance API tự đọc toàn bộ hai topic (không dùng consumer group) nên luôn có đủ dữ liệu.
// Hệ thống lớn: thay bằng bảng trong database do một consumer riêng cập nhật.
type Projection struct {
	mu     sync.RWMutex
	orders map[string]*OrderView
}

func NewProjection() *Projection { return &Projection{orders: map[string]*OrderView{}} }

func (p *Projection) Get(id string) (OrderView, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.orders[id]
	if !ok {
		return OrderView{}, false
	}
	return *v, true
}

// Apply cập nhật read model theo một record; bỏ qua record không giải mã được
func (p *Projection) Apply(r *kgo.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch r.Topic {
	case events.TopicOrders:
		var o events.OrderPlaced
		if _, err := events.Decode(r.Value, events.TypeOrderPlaced, &o); err == nil {
			if _, exists := p.orders[o.OrderID]; !exists {
				p.orders[o.OrderID] = &OrderView{OrderID: o.OrderID, UserID: o.UserID, Amount: o.Amount, Status: "PENDING"}
			}
		}
	case events.TopicPayments:
		var pr events.PaymentResult
		if _, err := events.Decode(r.Value, events.TypePaymentResult, &pr); err == nil {
			v, ok := p.orders[pr.OrderID]
			if !ok { // payment đến trước order (khác partition): tạo bản ghi tạm
				v = &OrderView{OrderID: pr.OrderID, UserID: pr.UserID}
				p.orders[pr.OrderID] = v
			}
			v.Status, v.Reason = pr.Status, pr.Reason
		}
	}
}

// Run đọc liên tục hai topic từ đầu và áp dụng vào read model
func (p *Projection) Run(ctx context.Context, cl *kgo.Client) {
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachRecord(p.Apply)
	}
}
