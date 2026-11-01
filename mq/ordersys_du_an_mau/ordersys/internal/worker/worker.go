// Package worker: khung xử lý message với retry (TTL+DLX), parking-lot và
// panic recovery. Idempotency do handler đảm nhận (cùng transaction với dữ liệu).
package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/rabbitmq"
)

// HeaderRetry đếm số lần đã thử lại (0 ở lần giao đầu tiên).
const HeaderRetry = "x-retry-count"

// ErrPermanent đánh dấu lỗi không thể khỏi khi thử lại (dữ liệu hỏng, vi phạm
// hợp đồng...). Message đi thẳng vào parking-lot.
var ErrPermanent = errors.New("permanent error")

func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// Message là dạng đã "giải nén" của amqp.Delivery mà handler cần.
type Message struct {
	ID      string
	Type    string
	TraceID string
	Body    []byte
	Retry   int // số lần đã thử lại
}

type Handler func(ctx context.Context, m Message) error

type Worker struct {
	Queues  rabbitmq.ServiceQueues
	Pub     *rabbitmq.Client // client publish (connection riêng với consumer)
	Log     *slog.Logger
	Metrics *obs.Metrics
	Handle  Handler
}

func retryCount(h amqp.Table) int {
	switch v := h[HeaderRetry].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	}
	return 0
}

// Process là rabbitmq.Handler: quyết định ack / retry / parking cho từng delivery.
func (w *Worker) Process(ctx context.Context, d amqp.Delivery) {
	m := Message{ID: d.MessageId, Type: d.Type, Body: d.Body,
		Retry: retryCount(d.Headers)}
	m.TraceID, _ = d.Headers[obs.HeaderTraceID].(string)
	ctx = obs.WithTraceID(ctx, m.TraceID)
	log := w.Log.With("msg_id", m.ID, "type", m.Type, "trace_id", m.TraceID,
		"retry", m.Retry)
	svc := w.Queues.Service

	err := w.safeHandle(ctx, m)
	switch {
	case err == nil:
		w.Metrics.Inc("ordersys_messages_total", "svc", svc, "result", "ok")
		w.ack(log, d)
	case errors.Is(err, ErrPermanent):
		log.Error("permanent failure -> parking-lot", "err", err)
		w.Metrics.Inc("ordersys_messages_total", "svc", svc, "result", "parked")
		w.reject(log, d)
	case m.Retry >= len(w.Queues.Delays):
		log.Error("retries exhausted -> parking-lot", "err", err)
		w.Metrics.Inc("ordersys_messages_total", "svc", svc, "result", "parked")
		w.reject(log, d)
	default:
		delay := w.Queues.Delays[m.Retry]
		if perr := w.scheduleRetry(ctx, d, m.Retry+1); perr != nil {
			// Không gửi được sang hàng đợi chờ: trả lại queue chính (thử lại ngay).
			log.Error("schedule retry failed, requeue", "err", perr)
			_ = d.Nack(false, true)
			return
		}
		log.Warn("transient failure, retry scheduled", "in", delay, "err", err)
		w.Metrics.Inc("ordersys_messages_total", "svc", svc, "result", "retried")
		w.ack(log, d)
	}
}

func (w *Worker) safeHandle(ctx context.Context, m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.Log.Error("panic in handler", "panic", r, "stack", string(debug.Stack()))
			err = Permanent(fmt.Errorf("panic: %v", r))
		}
	}()
	return w.Handle(ctx, m)
}

// scheduleRetry publish BẢN SAO vào <svc>.retry.<delay>; khi TTL hết hạn, broker
// dead-letter nó về <svc>.q. Phải có confirm trước khi ack bản gốc.
func (w *Worker) scheduleRetry(ctx context.Context, d amqp.Delivery, n int) error {
	h := amqp.Table{}
	for k, v := range d.Headers {
		h[k] = v
	}
	h[HeaderRetry] = int32(n)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return w.Pub.Publish(ctx, "", w.Queues.Retry(n-1), amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Type:         d.Type,
		Timestamp:    d.Timestamp,
		AppId:        d.AppId,
		Headers:      h,
		Body:         d.Body,
	})
}

func (w *Worker) ack(log *slog.Logger, d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		log.Warn("ack failed (message will be redelivered)", "err", err)
	}
}

// reject(requeue=false): broker dead-letter message sang parking-lot (x-death).
func (w *Worker) reject(log *slog.Logger, d amqp.Delivery) {
	if err := d.Reject(false); err != nil {
		log.Warn("reject failed", "err", err)
	}
}

// MarkProcessed ghi (consumer, message_id) trong tx. Trả false nếu đã xử lý rồi.
// Gọi ĐẦU TIÊN trong transaction nghiệp vụ: khóa chính chặn cả hai bản trùng
// chạy song song (bản thứ hai chờ khóa rồi nhận "đã tồn tại").
func MarkProcessed(ctx context.Context, tx *sql.Tx, table, consumer,
	msgID string) (bool, error) {
	if msgID == "" {
		return false, Permanent(errors.New("message without message_id"))
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO `+table+
		` (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumer, msgID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
