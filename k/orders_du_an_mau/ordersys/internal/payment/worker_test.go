package payment

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
)

func produceOrder(t *testing.T, ctx context.Context, cl *kgo.Client, eventID string, o events.OrderPlaced) {
	t.Helper()
	ev, _ := events.New(eventID, events.TypeOrderPlaced, time.Now(), o)
	b, _ := json.Marshal(ev)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: events.TopicOrders, Key: []byte(o.OrderID), Value: b}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestPaymentWorkerEndToEnd(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(3, events.TopicOrders, events.TopicPayments), kfake.SeedTopics(1, events.TopicOrdersDLQ))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addrs := c.ListenAddrs()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prod, _ := kafkax.NewProducer("test", addrs)
	defer prod.Close()
	produceOrder(t, ctx, prod, "e1", events.OrderPlaced{OrderID: "o1", UserID: "u1", Amount: 100})
	produceOrder(t, ctx, prod, "e2", events.OrderPlaced{OrderID: "o2", UserID: "u2", Amount: 999})       // vượt hạn mức
	produceOrder(t, ctx, prod, "e1", events.OrderPlaced{OrderID: "o1", UserID: "u1", Amount: 100})       // bản trùng
	prod.ProduceSync(ctx, &kgo.Record{Topic: events.TopicOrders, Key: []byte("x"), Value: []byte("hỏng")}) // poison pill

	gw := NewFakeGateway(500)
	w := NewWorker(gw, NewMemoryDeduper(), prod)
	cons, _ := kafkax.NewConsumer("payment-test", addrs, []string{events.TopicOrders})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- kafkax.Run(runCtx, cons, slog.New(slog.NewTextHandler(io.Discard, nil)), events.TopicOrdersDLQ,
			kafkax.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}, w.Handle)
	}()

	// Đọc kết quả trên topic payments và DLQ
	out, _ := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.ConsumeTopics(events.TopicPayments, events.TopicOrdersDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer out.Close()
	var results []string
	dlq := 0
	for len(results) < 2 || dlq < 1 {
		fs := out.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("hết thời gian: results=%v dlq=%d", results, dlq)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Topic == events.TopicOrdersDLQ {
				dlq++
				return
			}
			var p events.PaymentResult
			if _, err := events.Decode(r.Value, events.TypePaymentResult, &p); err != nil {
				t.Errorf("giải mã: %v", err)
			}
			results = append(results, p.OrderID+"="+p.Status)
		})
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Run trả lỗi: %v", err)
	}
	cons.CloseAllowingRebalance()

	sort.Strings(results)
	if len(results) != 2 || results[0] != "o1=PAID" || results[1] != "o2=FAILED" {
		t.Errorf("kết quả = %v", results)
	}
	if gw.Total() != 100 { // bản trùng e1 không bị thu tiền lần hai
		t.Errorf("tổng tiền đã thu = %d, muốn 100", gw.Total())
	}
	if dlq != 1 {
		t.Errorf("số message DLQ = %d, muốn 1", dlq)
	}
}
