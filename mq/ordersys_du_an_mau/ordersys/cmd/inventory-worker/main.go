// inventory-worker: tiêu thụ order.created, giữ hàng, phát stock.* qua outbox.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/example/ordersys/internal/app"
	"github.com/example/ordersys/internal/config"
	"github.com/example/ordersys/internal/db"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/inventory"
	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/outbox"
	"github.com/example/ordersys/internal/rabbitmq"
	"github.com/example/ordersys/internal/worker"
)

func main() {
	log := obs.NewLogger("inventory")
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config.Load(":8081"), log); err != nil {
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
	if err := db.Migrate(ctx, database, inventory.DDL); err != nil {
		return err
	}
	metrics := obs.NewMetrics()
	queues := rabbitmq.ServiceQueues{
		Service:  "inventory",
		Bindings: []string{events.OrderCreated},
		Delays:   []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second},
	}
	// Hai connection: publish (outbox, retry) và consume tách nhau (Chương 6).
	pub := rabbitmq.New(rabbitmq.Config{URL: cfg.AMQPURL, Name: "inventory-pub",
		Topology: queues.Declare, Logger: log, MaxBackoff: 10 * time.Second})
	defer pub.Close()
	cons := rabbitmq.New(rabbitmq.Config{URL: cfg.AMQPURL, Name: "inventory-consume",
		Topology: queues.Declare, Logger: log, MaxBackoff: 10 * time.Second})
	defer cons.Close()

	relay := &outbox.Relay{DB: database, Table: inventory.OutboxTable, Pub: pub,
		Log: log, Metrics: metrics, Interval: 500 * time.Millisecond, Batch: 100}
	w := &worker.Worker{Queues: queues, Pub: pub, Log: log, Metrics: metrics,
		Handle: (&inventory.Handler{DB: database, Log: log}).Handle}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.Handle("GET /healthz", app.Health(database, pub, cons))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); relay.Run(ctx) }()
	go func() {
		defer wg.Done()
		if err := app.ServeHTTP(ctx, cfg.HTTPAddr, mux, log); err != nil {
			log.Error("http", "err", err)
		}
	}()
	err = cons.Consume(ctx, rabbitmq.ConsumeOptions{Queue: queues.Main(),
		Prefetch: 20, Workers: 4}, w.Process)
	wg.Wait()
	return err
}
