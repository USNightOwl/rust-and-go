package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// PermanentError đánh dấu lỗi không nên thử lại (dữ liệu sai, vi phạm nghiệp vụ)
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent bọc err thành lỗi vĩnh viễn
func Permanent(err error) error { return &PermanentError{Err: err} }

// Handler xử lý một record
type Handler func(ctx context.Context, r *kgo.Record) error

// RetryPolicy cấu hình thử lại tại chỗ
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// Run là vòng lặp consumer chuẩn: poll -> xử lý từng record (retry + DLQ) -> commit -> cho phép rebalance.
// Dừng khi ctx bị huỷ hoặc client đóng. dlqTopic rỗng nghĩa là không dùng DLQ (lỗi sẽ dừng vòng lặp).
func Run(ctx context.Context, cl *kgo.Client, log *slog.Logger, dlqTopic string, p RetryPolicy, h Handler) error {
	// Với BlockRebalanceOnPoll, rebalance (kể cả việc rời group khi Close) bị chặn
	// đến khi AllowRebalance được gọi -> luôn gọi khi thoát để Close không bị treo.
	defer cl.AllowRebalance()
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, part int32, err error) {
			log.Warn("lỗi fetch", "topic", t, "partition", part, "err", err)
		})
		var fatal error
		fetches.EachRecord(func(r *kgo.Record) {
			if fatal != nil {
				return
			}
			attempts, err := withRetry(ctx, r, p, h)
			if err == nil {
				return
			}
			if ctx.Err() != nil {
				fatal = ctx.Err()
				return
			}
			log.Error("xử lý thất bại, chuyển DLQ", "topic", r.Topic, "partition", r.Partition,
				"offset", r.Offset, "attempts", attempts, "err", err)
			if dlqTopic == "" {
				fatal = err
				return
			}
			if dErr := toDLQ(ctx, cl, dlqTopic, r, attempts, err); dErr != nil {
				fatal = fmt.Errorf("ghi DLQ: %w", dErr)
			}
		})
		if fatal != nil {
			// Không commit: lần chạy sau sẽ xử lý lại từ offset đã commit gần nhất
			cl.AllowRebalance()
			if errors.Is(fatal, context.Canceled) {
				return nil
			}
			return fatal
		}
		if err := cl.CommitUncommittedOffsets(context.WithoutCancel(ctx)); err != nil {
			log.Error("commit lỗi", "err", err)
		}
		cl.AllowRebalance()
	}
}

func withRetry(ctx context.Context, r *kgo.Record, p RetryPolicy, h Handler) (int, error) {
	backoff := p.Backoff
	for attempt := 1; ; attempt++ {
		err := h(ctx, r)
		var pe *PermanentError
		if err == nil || errors.As(err, &pe) || attempt >= p.MaxAttempts {
			return attempt, err
		}
		jitter := time.Duration(rand.Int64N(int64(backoff) + 1))
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			return attempt, ctx.Err()
		}
		backoff *= 2
	}
}

func toDLQ(ctx context.Context, cl *kgo.Client, topic string, r *kgo.Record, attempts int, cause error) error {
	headers := append([]kgo.RecordHeader{}, r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "dlq-error", Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: "dlq-original", Value: []byte(fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset))},
		kgo.RecordHeader{Key: "dlq-attempts", Value: []byte(strconv.Itoa(attempts))},
	)
	rec := &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Headers: headers}
	return cl.ProduceSync(ctx, rec).FirstErr()
}
