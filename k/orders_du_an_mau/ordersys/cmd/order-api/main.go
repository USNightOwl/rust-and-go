// order-api: REST API nhận đơn hàng, phát event order.placed.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/api"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "order-api")
	if err := run(log); err != nil {
		log.Error("dừng do lỗi", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	brokers := kafkax.Brokers()

	producer, err := kafkax.NewProducer("order-api", brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := kafkax.EnsureTopics(ctx, producer, kafkax.AllTopics...); err != nil {
		return err
	}

	// Client đọc trực tiếp (không group) để dựng read model
	viewClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(events.TopicOrders, events.TopicPayments),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer viewClient.Close()
	view := api.NewProjection()

	gin.SetMode(gin.ReleaseMode)
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewHandler(producer, view, log).Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Go(func() { view.Run(ctx, viewClient) })
	errc := make(chan error, 1)
	go func() {
		log.Info("HTTP lắng nghe", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	log.Info("đang tắt")
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("shutdown HTTP", "err", err)
	}
	if err := producer.Flush(sctx); err != nil {
		log.Error("flush producer", "err", err)
	}
	wg.Wait()
	return nil
}
