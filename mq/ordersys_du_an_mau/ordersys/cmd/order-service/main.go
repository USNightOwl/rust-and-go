// order-service: HTTP API nhận đơn hàng, ghi PostgreSQL + outbox, relay publish
// sự kiện order.created lên exchange orders.events.
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
	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/order"
	"github.com/example/ordersys/internal/outbox"
	"github.com/example/ordersys/internal/rabbitmq"
)

func main() {
	log := obs.NewLogger("order-service")
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config.Load(":8080"), log); err != nil {
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
	if err := db.Migrate(ctx, database, order.DDL); err != nil {
		return err
	}
	metrics := obs.NewMetrics()

	pub := rabbitmq.New(rabbitmq.Config{URL: cfg.AMQPURL, Name: "order-service-pub",
		Topology: rabbitmq.DeclareEvents, Logger: log, MaxBackoff: 10 * time.Second})
	defer pub.Close()

	relay := &outbox.Relay{DB: database, Table: order.OutboxTable, Pub: pub,
		Log: log, Metrics: metrics, Interval: 500 * time.Millisecond, Batch: 100}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); relay.Run(ctx) }()

	svc := &order.Service{DB: database, Log: log}
	mux := http.NewServeMux()
	svc.Routes(mux)
	mux.Handle("GET /metrics", metrics)
	mux.Handle("GET /healthz", app.Health(database, pub))

	err = app.ServeHTTP(ctx, cfg.HTTPAddr, order.Middleware(log, metrics, mux), log)
	wg.Wait() // relay dừng khi ctx bị hủy
	return err
}
