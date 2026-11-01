// Package inventory: giữ hàng cho đơn mới, phát stock.reserved/stock.rejected.
package inventory

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/lib/pq"

	"github.com/example/ordersys/internal/db"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/outbox"
	"github.com/example/ordersys/internal/worker"
)

const (
	Consumer       = "inventory-worker"
	OutboxTable    = "inventory.outbox"
	ProcessedTable = "inventory.processed_messages"
)

var DDL = `CREATE SCHEMA IF NOT EXISTS inventory;
CREATE TABLE IF NOT EXISTS inventory.stock (
	sku       text PRIMARY KEY,
	available int NOT NULL CHECK (available >= 0)
);
CREATE TABLE IF NOT EXISTS inventory.reservations (
	order_id text NOT NULL,
	sku      text NOT NULL,
	qty      int  NOT NULL,
	PRIMARY KEY (order_id, sku)
);
INSERT INTO inventory.stock VALUES ('BOOK-1', 100), ('PEN-2', 500), ('MUG-3', 3)
	ON CONFLICT DO NOTHING;
` + db.OutboxDDL("inventory") + db.ProcessedDDL("inventory")

type Handler struct {
	DB  *sql.DB
	Log *slog.Logger
}

// Handle xử lý order.created. Mọi thay đổi (khử trùng, trừ kho, outbox) nằm
// trong MỘT transaction: hoặc tất cả, hoặc không gì cả.
func (h *Handler) Handle(ctx context.Context, m worker.Message) error {
	if m.Type != events.OrderCreated {
		return worker.Permanent(fmt.Errorf("unexpected type %q", m.Type))
	}
	var ev events.OrderCreatedEvent
	if err := json.Unmarshal(m.Body, &ev); err != nil {
		return worker.Permanent(fmt.Errorf("decode: %w", err))
	}
	if ev.OrderID == "" || len(ev.Items) == 0 {
		return worker.Permanent(fmt.Errorf("invalid event: %s", m.Body))
	}

	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		return err // lỗi tạm thời (DB mất kết nối...) => retry
	}
	defer tx.Rollback() //nolint:errcheck

	fresh, err := worker.MarkProcessed(ctx, tx, ProcessedTable, Consumer, m.ID)
	if err != nil {
		return err
	}
	if !fresh {
		h.Log.Info("duplicate message, skipped", "msg_id", m.ID,
			"order_id", ev.OrderID, "trace_id", m.TraceID)
		return nil // ack: đã xử lý trước đó
	}

	reason, err := reserve(ctx, tx, ev)
	if err != nil {
		return err
	}
	out := events.StockEvent{OrderID: ev.OrderID, CustomerEmail: ev.CustomerEmail,
		Reason: reason}
	key := events.StockReserved
	if reason != "" {
		key = events.StockRejected
	}
	payload, _ := json.Marshal(out)
	if err := outbox.Insert(ctx, tx, OutboxTable, outbox.Event{
		MessageID:  key + ":" + ev.OrderID,
		RoutingKey: key,
		TraceID:    m.TraceID,
		Payload:    payload,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	h.Log.Info("order processed", "order_id", ev.OrderID, "result", key,
		"reason", reason, "trace_id", m.TraceID)
	return nil
}

// reserve khóa các dòng kho liên quan, kiểm tra đủ hàng rồi trừ. Trả về lý do
// từ chối ("" nếu giữ hàng thành công).
func reserve(ctx context.Context, tx *sql.Tx, ev events.OrderCreatedEvent) (string,
	error) {
	skus := make([]string, len(ev.Items))
	for i, it := range ev.Items {
		skus[i] = it.SKU
	}
	rows, err := tx.QueryContext(ctx, `SELECT sku, available FROM inventory.stock
		WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`, pq.Array(skus))
	if err != nil {
		return "", err
	}
	avail := map[string]int{}
	for rows.Next() {
		var sku string
		var n int
		if err := rows.Scan(&sku, &n); err != nil {
			rows.Close()
			return "", err
		}
		avail[sku] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	for _, it := range ev.Items {
		if n, ok := avail[it.SKU]; !ok || n < it.Qty {
			return fmt.Sprintf("insufficient stock for %s (want %d, have %d)",
				it.SKU, it.Qty, n), nil
		}
	}
	for _, it := range ev.Items {
		_, err := tx.ExecContext(ctx, `UPDATE inventory.stock
			SET available = available - $2 WHERE sku = $1`, it.SKU, it.Qty)
		if err != nil {
			return "", err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO inventory.reservations
			VALUES ($1, $2, $3)`, ev.OrderID, it.SKU, it.Qty); err != nil {
			return "", err
		}
	}
	return "", nil
}
