// Package events định nghĩa hợp đồng sự kiện giữa các service.
package events

import "time"

// Routing key trên exchange orders.events (cũng là giá trị thuộc tính AMQP "type").
const (
	OrderCreated  = "order.created"
	StockReserved = "stock.reserved"
	StockRejected = "stock.rejected"
)

type Item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type OrderCreatedEvent struct {
	OrderID       string    `json:"order_id"`
	CustomerEmail string    `json:"customer_email"`
	Items         []Item    `json:"items"`
	TotalCents    int64     `json:"total_cents"`
	CreatedAt     time.Time `json:"created_at"`
}

type StockEvent struct {
	OrderID       string `json:"order_id"`
	CustomerEmail string `json:"customer_email"`
	Reason        string `json:"reason,omitempty"`
}
