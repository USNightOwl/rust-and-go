package payment

import (
	"context"
	"fmt"
	"sync"
)

// FakeGateway giả lập cổng thanh toán: từ chối giao dịch vượt hạn mức,
// ghi nhớ idempotency key để không thu tiền hai lần.
type FakeGateway struct {
	Limit int64

	mu      sync.Mutex
	charged map[string]int64 // idempotencyKey -> số tiền
}

func NewFakeGateway(limit int64) *FakeGateway {
	return &FakeGateway{Limit: limit, charged: map[string]int64{}}
}

func (g *FakeGateway) Charge(_ context.Context, key, _ string, amount int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.charged[key]; ok {
		return nil // yêu cầu trùng: trả thành công nhưng không thu thêm
	}
	if amount > g.Limit {
		return fmt.Errorf("%w: vượt hạn mức %d", ErrDeclined, g.Limit)
	}
	g.charged[key] = amount
	return nil
}

// Total trả tổng số tiền đã thu
func (g *FakeGateway) Total() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var t int64
	for _, v := range g.charged {
		t += v
	}
	return t
}
