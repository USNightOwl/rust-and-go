// Package events định nghĩa tên topic và cấu trúc event dùng chung giữa các dịch vụ.
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Tên topic: một nơi duy nhất để producer và consumer không lệch nhau
const (
	TopicOrders      = "orders"
	TopicPayments    = "payments"
	TopicOrdersDLQ   = "orders.dlq"
	TopicPaymentsDLQ = "payments.dlq"
)

// Loại event
const (
	TypeOrderPlaced   = "order.placed"
	TypePaymentResult = "payment.result"
)

// Event là envelope chung; Data chứa payload theo Type
type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type OrderPlaced struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Amount  int64  `json:"amount"` // đơn vị: đồng
}

type PaymentResult struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Status  string `json:"status"` // PAID hoặc FAILED
	Reason  string `json:"reason,omitempty"`
}

// New tạo envelope cho payload v
func New(id, typ string, at time.Time, v any) (Event, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Event{}, err
	}
	return Event{ID: id, Type: typ, Version: 1, OccurredAt: at.UTC(), Data: b}, nil
}

// Decode giải mã envelope và kiểm tra loại event mong muốn
func Decode(b []byte, wantType string, out any) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("envelope sai định dạng: %w", err)
	}
	if e.Type != wantType {
		return e, fmt.Errorf("loại event %q, cần %q", e.Type, wantType)
	}
	if err := json.Unmarshal(e.Data, out); err != nil {
		return e, fmt.Errorf("payload sai định dạng: %w", err)
	}
	return e, nil
}
