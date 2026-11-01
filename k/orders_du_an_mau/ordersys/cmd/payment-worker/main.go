// payment-worker: đọc order.placed, thu tiền, phát payment.result.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
	"github.com/example/ordersys/internal/payment"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "payment-worker")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	brokers := kafkax.Brokers()

	producer, err := kafkax.NewProducer("payment-worker", brokers)
	if err != nil {
		log.Error("producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()
	consumer, err := kafkax.NewConsumer("payment-worker", brokers, []string{events.TopicOrders})
	if err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}

	w := payment.NewWorker(payment.NewFakeGateway(50_000_000), payment.NewMemoryDeduper(), producer)
	policy := kafkax.RetryPolicy{MaxAttempts: 5, Backoff: 200 * time.Millisecond}
	log.Info("bắt đầu xử lý")
	err = kafkax.Run(ctx, consumer, log, events.TopicOrdersDLQ, policy, w.Handle)
	consumer.CloseAllowingRebalance() // commit đã làm sau mỗi lô; rời group để rebalance nhanh
	if err != nil {
		log.Error("dừng do lỗi", "err", err)
		os.Exit(1)
	}
	log.Info("đã tắt")
}
