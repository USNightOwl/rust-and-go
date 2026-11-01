// notification-worker: đọc payment.result và gửi thông báo.
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
	"github.com/example/ordersys/internal/notification"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "notification-worker")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := kafkax.NewConsumer("notification-worker", kafkax.Brokers(), []string{events.TopicPayments})
	if err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}
	w := notification.NewWorker(notification.LogNotifier{Log: log})
	policy := kafkax.RetryPolicy{MaxAttempts: 3, Backoff: 500 * time.Millisecond}
	err = kafkax.Run(ctx, consumer, log, events.TopicPaymentsDLQ, policy, w.Handle)
	consumer.CloseAllowingRebalance()
	if err != nil {
		log.Error("dừng do lỗi", "err", err)
		os.Exit(1)
	}
}
