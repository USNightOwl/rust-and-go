package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/kafkax"
)

func TestPlaceAndGetOrder(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, events.TopicOrders, events.TopicPayments))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prod, _ := kafkax.NewProducer("api-test", c.ListenAddrs())
	defer prod.Close()
	viewCl, _ := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(events.TopicOrders, events.TopicPayments), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer viewCl.Close()
	view := NewProjection()
	go view.Run(ctx, viewCl)

	router := NewHandler(prod, view, slog.New(slog.NewTextHandler(io.Discard, nil))).Router()
	do := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		return w
	}

	if w := do(http.MethodPost, "/api/v1/orders", `{"user_id":"u1","amount":0}`); w.Code != http.StatusBadRequest {
		t.Fatalf("amount 0: code = %d", w.Code)
	}
	w := do(http.MethodPost, "/api/v1/orders", `{"user_id":"u1","amount":120000}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body)
	}
	var resp struct {
		OrderID string `json:"order_id"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)

	// Read model cập nhật bất đồng bộ: chờ đến khi thấy đơn hàng
	for {
		if ctx.Err() != nil {
			t.Fatal("không thấy đơn hàng trong read model")
		}
		if w := do(http.MethodGet, "/api/v1/orders/"+resp.OrderID, ""); w.Code == http.StatusOK {
			var v OrderView
			json.Unmarshal(w.Body.Bytes(), &v)
			if v.Status != "PENDING" || v.Amount != 120000 {
				t.Fatalf("view = %+v", v)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}
