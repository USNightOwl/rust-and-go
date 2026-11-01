// notification-worker: tiêu thụ order.created và stock.*, gửi email cho khách.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/ordersys/internal/app"
	"github.com/example/ordersys/internal/config"
	"github.com/example/ordersys/internal/db"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/notification"
	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/rabbitmq"
	"github.com/example/ordersys/internal/worker"
)

func main() {
	log := obs.NewLogger("notification")
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config.Load(":8082"), log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	if err := db.Migrate(ctx, database, notification.DDL); err != nil {
		return err
	}
	metrics := obs.NewMetrics()
	queues := rabbitmq.ServiceQueues{
		Service:  "notification",
		Bindings: []string{events.OrderCreated, "stock.*"},
		Delays:   []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second},
	}
	pub := rabbitmq.New(rabbitmq.Config{URL: cfg.AMQPURL, Name: "notification-pub",
		Topology: queues.Declare, Logger: log, MaxBackoff: 10 * time.Second})
	defer pub.Close()
	cons := rabbitmq.New(rabbitmq.Config{URL: cfg.AMQPURL,
		Name: "notification-consume", Topology: queues.Declare, Logger: log,
		MaxBackoff: 10 * time.Second})
	defer cons.Close()

	w := &worker.Worker{Queues: queues, Pub: pub, Log: log, Metrics: metrics,
		Handle: (&notification.Handler{DB: database, Log: log}).Handle}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.Handle("GET /healthz", app.Health(database, pub, cons))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := app.ServeHTTP(ctx, cfg.HTTPAddr, mux, log); err != nil {
			log.Error("http", "err", err)
		}
	}()
	err = cons.Consume(ctx, rabbitmq.ConsumeOptions{Queue: queues.Main(),
		Prefetch: 20, Workers: 4}, w.Process)
	<-done
	return err
}
