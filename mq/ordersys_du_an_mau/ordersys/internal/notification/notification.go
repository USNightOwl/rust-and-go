// Package notification: gửi "email" cho khách theo sự kiện đơn hàng.
package notification

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/example/ordersys/internal/db"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/worker"
)

const (
	Consumer       = "notification-worker"
	ProcessedTable = "notification.processed_messages"
)

var DDL = `CREATE SCHEMA IF NOT EXISTS notification;
CREATE TABLE IF NOT EXISTS notification.sent (
	id         bigserial PRIMARY KEY,
	message_id text NOT NULL,
	email      text NOT NULL,
	subject    text NOT NULL,
	sent_at    timestamptz NOT NULL DEFAULT now()
);
` + db.ProcessedDDL("notification")

// ErrSMTPTemporary giả lập lỗi tạm thời của máy chủ mail (mã 451).
var ErrSMTPTemporary = errors.New("smtp: 451 4.3.0 temporary failure")

type Handler struct {
	DB  *sql.DB
	Log *slog.Logger
}

func (h *Handler) Handle(ctx context.Context, m worker.Message) error {
	var email, subject string
	switch m.Type {
	case events.OrderCreated:
		var ev events.OrderCreatedEvent
		if err := json.Unmarshal(m.Body, &ev); err != nil {
			return worker.Permanent(err)
		}
		email = ev.CustomerEmail
		subject = fmt.Sprintf("Đã nhận đơn %s (%d cent)", short(ev.OrderID),
			ev.TotalCents)
	case events.StockReserved, events.StockRejected:
		var ev events.StockEvent
		if err := json.Unmarshal(m.Body, &ev); err != nil {
			return worker.Permanent(err)
		}
		email = ev.CustomerEmail
		subject = "Đơn " + short(ev.OrderID) + " đã được xác nhận"
		if m.Type == events.StockRejected {
			subject = "Đơn " + short(ev.OrderID) + " bị từ chối: " + ev.Reason
		}
	default:
		return worker.Permanent(fmt.Errorf("unexpected type %q", m.Type))
	}

	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	fresh, err := worker.MarkProcessed(ctx, tx, ProcessedTable, Consumer, m.ID)
	if err != nil {
		return err
	}
	if !fresh {
		h.Log.Info("duplicate message, skipped", "msg_id", m.ID,
			"trace_id", m.TraceID)
		return nil
	}
	// Gửi mail khi đang giữ transaction: nếu gửi lỗi, rollback xóa luôn dấu
	// "đã xử lý" để lần thử lại gửi tiếp. Gửi thành công nhưng commit lỗi => có
	// thể gửi trùng: email là hiệu ứng ngoài DB, chỉ đạt at-least-once.
	if err := sendEmail(email, subject, m.Retry); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO notification.sent
		(message_id, email, subject) VALUES ($1, $2, $3)`,
		m.ID, email, subject); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	h.Log.Info("email sent", "to", email, "subject", subject, "trace_id", m.TraceID)
	return nil
}

// sendEmail giả lập SMTP: địa chỉ bắt đầu bằng "fail" luôn lỗi tạm thời,
// "flaky" lỗi ở 2 lần đầu rồi thành công.
func sendEmail(to, subject string, retry int) error {
	switch {
	case strings.HasPrefix(to, "fail"):
		return ErrSMTPTemporary
	case strings.HasPrefix(to, "flaky") && retry < 2:
		return ErrSMTPTemporary
	}
	_ = subject
	return nil
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
