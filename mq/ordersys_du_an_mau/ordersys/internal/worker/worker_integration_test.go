package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/rabbitmq"
	"github.com/example/ordersys/internal/worker"
)

// Chạy: AMQP_URL=amqp://admin:admin@localhost:5672/p2b go test ./internal/worker/
func TestRetryThenParkingLot(t *testing.T) {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL chưa đặt: bỏ qua test tích hợp")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	q := rabbitmq.ServiceQueues{
		Service:  fmt.Sprintf("itest-%d", time.Now().UnixNano()),
		Bindings: []string{"itest.#"},
		Delays:   []time.Duration{200 * time.Millisecond, 400 * time.Millisecond},
	}
	pub := rabbitmq.New(rabbitmq.Config{URL: url, Name: "itest-pub",
		Topology: q.Declare, Logger: log})
	defer pub.Close()
	cons := rabbitmq.New(rabbitmq.Config{URL: url, Name: "itest-consume",
		Topology: q.Declare, Logger: log})
	defer cons.Close()

	var transientCalls atomic.Int32
	w := &worker.Worker{Queues: q, Pub: pub, Log: log, Metrics: obs.NewMetrics(),
		Handle: func(_ context.Context, m worker.Message) error {
			switch m.Type {
			case "transient":
				transientCalls.Add(1)
				return errors.New("downstream unavailable")
			case "poison":
				return worker.Permanent(errors.New("cannot decode"))
			}
			return nil
		}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		opt := rabbitmq.ConsumeOptions{Queue: q.Main(), Prefetch: 10}
		_ = cons.Consume(ctx, opt, w.Process)
	}()

	for _, typ := range []string{"ok", "transient", "poison"} {
		err := pub.Publish(ctx, rabbitmq.EventsExchange, "itest.event", amqp.Publishing{
			MessageId: typ + "-1", Type: typ, Body: []byte("{}")})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Đọc parking-lot bằng basic.get (pull) cho tới khi đủ 2 message.
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, _ := conn.Channel()
	parked := map[string]amqp.Delivery{}
	deadline := time.Now().Add(10 * time.Second)
	for len(parked) < 2 && time.Now().Before(deadline) {
		d, ok, err := ch.Get(q.Parking(), true)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		parked[d.MessageId] = d
	}
	if len(parked) != 2 {
		t.Fatalf("parking-lot có %d message, muốn 2", len(parked))
	}
	if got := parked["transient-1"].Headers[worker.HeaderRetry]; got != int32(2) {
		t.Errorf("transient: x-retry-count=%v, muốn 2", got)
	}
	if n := transientCalls.Load(); n != 3 {
		t.Errorf("handler được gọi %d lần cho transient, muốn 3 (1 + 2 retry)", n)
	}
	if _, ok := parked["poison-1"].Headers[worker.HeaderRetry]; ok {
		t.Errorf("poison không được qua retry")
	}
	t.Logf("parked: transient (retry=%v), poison; handler calls=%d",
		parked["transient-1"].Headers[worker.HeaderRetry], transientCalls.Load())

	cancel()
	for _, name := range []string{q.Main(), q.Parking(), q.Retry(0), q.Retry(1)} {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
}
