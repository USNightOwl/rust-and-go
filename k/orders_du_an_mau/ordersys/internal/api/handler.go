// Package api: REST API đặt hàng viết bằng Gin.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/ordersys/internal/events"
)

type PlaceOrderRequest struct {
	UserID string `json:"user_id" binding:"required"`
	Amount int64  `json:"amount" binding:"required,gt=0"`
}

type Handler struct {
	kafka *kgo.Client
	view  *Projection
	log   *slog.Logger
	now   func() time.Time
}

func NewHandler(kafka *kgo.Client, view *Projection, log *slog.Logger) *Handler {
	return &Handler{kafka: kafka, view: view, log: log, now: time.Now}
}

// Router đăng ký các route
func (h *Handler) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", h.ready)
	v1 := r.Group("/api/v1")
	v1.POST("/orders", h.placeOrder)
	v1.GET("/orders/:id", h.getOrder)
	return r
}

// placeOrder phát order.placed bằng ProduceSync (event quan trọng) rồi trả 202
func (h *Handler) placeOrder(c *gin.Context) {
	var req PlaceOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id bắt buộc, amount phải lớn hơn 0"})
		return
	}
	orderID := uuid.NewV7().String()
	ev, err := events.New(uuid.NewV7().String(), events.TypeOrderPlaced, h.now(),
		events.OrderPlaced{OrderID: orderID, UserID: req.UserID, Amount: req.Amount})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "lỗi hệ thống"})
		return
	}
	b, _ := json.Marshal(ev)
	rec := &kgo.Record{
		Topic:   events.TopicOrders,
		Key:     []byte(orderID), // mọi event của một đơn hàng vào cùng partition
		Value:   b,
		Headers: []kgo.RecordHeader{{Key: "event-type", Value: []byte(ev.Type)}},
	}
	// Context riêng có timeout, KHÔNG phụ thuộc vòng đời request
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 3*time.Second)
	defer cancel()
	if err := h.kafka.ProduceSync(ctx, rec).FirstErr(); err != nil {
		h.log.Error("phát order.placed thất bại", "order_id", orderID, "err", err)
		c.Header("Retry-After", "5")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "hệ thống đang bận, vui lòng thử lại"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"order_id": orderID, "status": "PENDING"})
}

func (h *Handler) getOrder(c *gin.Context) {
	v, ok := h.view.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "không tìm thấy đơn hàng"})
		return
	}
	c.JSON(http.StatusOK, v)
}

func (h *Handler) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := h.kafka.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"kafka": "unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"kafka": "ok"})
}
