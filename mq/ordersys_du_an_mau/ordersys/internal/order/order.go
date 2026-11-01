// Package order: HTTP API đặt hàng, ghi đơn + outbox trong một transaction.
package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/mail"
	"time"

	"github.com/example/ordersys/internal/db"
	"github.com/example/ordersys/internal/events"
	"github.com/example/ordersys/internal/obs"
	"github.com/example/ordersys/internal/outbox"
)

const OutboxTable = "orders.outbox"

var DDL = `CREATE SCHEMA IF NOT EXISTS orders;
CREATE TABLE IF NOT EXISTS orders.orders (
	id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	customer_email text NOT NULL,
	status         text NOT NULL,
	total_cents    bigint NOT NULL,
	created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS orders.order_items (
	order_id    uuid NOT NULL REFERENCES orders.orders(id),
	sku         text NOT NULL,
	qty         int  NOT NULL CHECK (qty > 0),
	price_cents bigint NOT NULL,
	PRIMARY KEY (order_id, sku)
);
` + db.OutboxDDL("orders")

// catalog: bảng giá (cent). Thực tế lấy từ service catalog/DB.
var catalog = map[string]int64{"BOOK-1": 1250, "PEN-2": 120, "MUG-3": 790}

type CreateRequest struct {
	CustomerEmail string        `json:"customer_email"`
	Items         []events.Item `json:"items"`
}

type Order struct {
	ID            string        `json:"id"`
	CustomerEmail string        `json:"customer_email"`
	Status        string        `json:"status"`
	TotalCents    int64         `json:"total_cents"`
	Items         []events.Item `json:"items,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
}

var errValidation = errors.New("validation")

func (r CreateRequest) validate() (int64, error) {
	if _, err := mail.ParseAddress(r.CustomerEmail); err != nil {
		return 0, fmt.Errorf("%w: invalid customer_email", errValidation)
	}
	if len(r.Items) == 0 {
		return 0, fmt.Errorf("%w: items must not be empty", errValidation)
	}
	var total int64
	seen := map[string]bool{}
	for _, it := range r.Items {
		price, ok := catalog[it.SKU]
		switch {
		case !ok:
			return 0, fmt.Errorf("%w: unknown sku %q", errValidation, it.SKU)
		case it.Qty <= 0:
			return 0, fmt.Errorf("%w: qty must be > 0", errValidation)
		case seen[it.SKU]:
			return 0, fmt.Errorf("%w: duplicate sku %q", errValidation, it.SKU)
		}
		seen[it.SKU] = true
		total += price * int64(it.Qty)
	}
	return total, nil
}

type Service struct {
	DB  *sql.DB
	Log *slog.Logger
}

// Create ghi đơn hàng, các dòng hàng và sự kiện order.created trong MỘT transaction.
func (s *Service) Create(ctx context.Context, req CreateRequest) (Order, error) {
	total, err := req.validate()
	if err != nil {
		return Order{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	o := Order{CustomerEmail: req.CustomerEmail, Status: "PENDING",
		TotalCents: total, Items: req.Items}
	err = tx.QueryRowContext(ctx, `INSERT INTO orders.orders
		(customer_email, status, total_cents) VALUES ($1, $2, $3)
		RETURNING id, created_at`, o.CustomerEmail, o.Status, o.TotalCents,
	).Scan(&o.ID, &o.CreatedAt)
	if err != nil {
		return Order{}, err
	}
	for _, it := range req.Items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO orders.order_items
			(order_id, sku, qty, price_cents) VALUES ($1, $2, $3, $4)`,
			o.ID, it.SKU, it.Qty, catalog[it.SKU]); err != nil {
			return Order{}, err
		}
	}
	payload, _ := json.Marshal(events.OrderCreatedEvent{OrderID: o.ID,
		CustomerEmail: o.CustomerEmail, Items: o.Items, TotalCents: o.TotalCents,
		CreatedAt: o.CreatedAt})
	if err := outbox.Insert(ctx, tx, OutboxTable, outbox.Event{
		MessageID:  events.OrderCreated + ":" + o.ID, // ổn định => khử trùng được
		RoutingKey: events.OrderCreated,
		TraceID:    obs.TraceID(ctx),
		Payload:    payload,
	}); err != nil {
		return Order{}, err
	}
	return o, tx.Commit()
}

func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	var o Order
	err := s.DB.QueryRowContext(ctx, `SELECT id, customer_email, status,
		total_cents, created_at FROM orders.orders WHERE id = $1`, id,
	).Scan(&o.ID, &o.CustomerEmail, &o.Status, &o.TotalCents, &o.CreatedAt)
	if err != nil {
		return Order{}, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT sku, qty FROM orders.order_items
		WHERE order_id = $1 ORDER BY sku`, id)
	if err != nil {
		return Order{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var it events.Item
		if err := rows.Scan(&it.SKU, &it.Qty); err != nil {
			return Order{}, err
		}
		o.Items = append(o.Items, it)
	}
	return o, rows.Err()
}

// ---------------------------------------------------------------- HTTP

func (s *Service) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", s.handleCreate)
	mux.HandleFunc("GET /orders/{id}", s.handleGet)
}

func (s *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	o, err := s.Create(r.Context(), req)
	switch {
	case errors.Is(err, errValidation):
		writeJSON(w, http.StatusUnprocessableEntity,
			map[string]string{"error": err.Error()})
	case err != nil:
		s.Log.Error("create order", "err", err, "trace_id", obs.TraceID(r.Context()))
		writeJSON(w, http.StatusInternalServerError,
			map[string]string{"error": "internal error"})
	default:
		s.Log.Info("order created", "order_id", o.ID, "total_cents", o.TotalCents,
			"trace_id", obs.TraceID(r.Context()))
		writeJSON(w, http.StatusCreated, o)
	}
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	o, err := s.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case err != nil:
		// id không phải uuid hợp lệ cũng rơi vào đây (lỗi 22P02 từ PostgreSQL)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusOK, o)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Middleware gắn trace id (nhận từ header hoặc sinh mới), log và đếm request.
func Middleware(log *slog.Logger, m *obs.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(obs.HeaderTraceID)
		if id == "" {
			id = obs.NewTraceID()
		}
		w.Header().Set(obs.HeaderTraceID, id)
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		req := r.WithContext(obs.WithTraceID(r.Context(), id))
		next.ServeHTTP(rec, req) // ServeMux ghi req.Pattern, ví dụ "POST /orders"
		m.Inc("ordersys_http_requests_total", "route", req.Pattern,
			"code", fmt.Sprint(rec.code))
		log.Info("http", "method", r.Method, "path", r.URL.Path, "code", rec.code,
			"dur", time.Since(start).Round(time.Microsecond), "trace_id", id)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}
