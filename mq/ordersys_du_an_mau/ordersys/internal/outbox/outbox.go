// Package outbox: ghi sự kiện cùng transaction nghiệp vụ, relay publish có confirm.
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/rabbitmq"
)

type Event struct {
	MessageID  string // khóa idempotency, ổn định qua mọi lần gửi lại
	RoutingKey string
	TraceID    string
	Payload    []byte // JSON
}

// Insert ghi event vào bảng outbox TRONG transaction tx của nghiệp vụ.
func Insert(ctx context.Context, tx *sql.Tx, table string, e Event) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO `+table+
		` (message_id, routing_key, event_type, trace_id, payload)
		 VALUES ($1, $2, $2, $3, $4)`,
		e.MessageID, e.RoutingKey, e.TraceID, e.Payload)
	return err
}

// Relay đọc các dòng chưa publish và đẩy lên RabbitMQ.
type Relay struct {
	DB       *sql.DB
	Table    string // ví dụ "orders.outbox"
	Pub      *rabbitmq.Client
	Log      *slog.Logger
	Metrics  *obs.Metrics
	Interval time.Duration
	Batch    int
}

// Run chạy tới khi ctx bị hủy. Nhiều instance chạy song song an toàn nhờ
// FOR UPDATE SKIP LOCKED.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		n, err := r.flush(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Warn("outbox flush failed", "err", err)
		}
		if n == r.Batch {
			continue // còn việc: chạy tiếp ngay
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Relay) flush(ctx context.Context) (int, error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // vô hại sau Commit

	rows, err := tx.QueryContext(ctx, `SELECT id, message_id, routing_key,
		event_type, trace_id, payload FROM `+r.Table+`
		WHERE published_at IS NULL ORDER BY id LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.Batch)
	if err != nil {
		return 0, err
	}
	type row struct {
		id                     int64
		msgID, key, typ, trace string
		payload                []byte
	}
	var batch []row
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.id, &x.msgID, &x.key, &x.typ, &x.trace,
			&x.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var done []int64
	for _, x := range batch {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.Pub.Publish(pctx, rabbitmq.EventsExchange, x.key, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    x.msgID,
			Type:         x.typ,
			Timestamp:    time.Now(),
			AppId:        r.Table,
			Headers:      amqp.Table{obs.HeaderTraceID: x.trace},
			Body:         x.payload,
		})
		cancel()
		if err != nil {
			r.Log.Warn("publish failed, will retry later", "msg_id", x.msgID,
				"err", err)
			break // giữ thứ tự: dừng lô ở message lỗi đầu tiên
		}
		done = append(done, x.id)
		r.Metrics.Inc("ordersys_outbox_published_total", "table", r.Table)
		r.Log.Info("outbox published", "msg_id", x.msgID, "routing_key", x.key,
			"trace_id", x.trace)
	}
	if len(done) == 0 {
		return 0, nil
	}
	// Crash tại đây => các message trên được gửi lại lần sau (at-least-once).
	if _, err := tx.ExecContext(ctx, `UPDATE `+r.Table+
		` SET published_at = now() WHERE id = ANY($1)`, pq.Array(done)); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	return len(done), tx.Commit()
}
