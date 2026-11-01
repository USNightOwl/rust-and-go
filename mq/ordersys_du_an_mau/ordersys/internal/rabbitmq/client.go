// Package rabbitmq là một client AMQP 0-9-1 tự kết nối lại, dựng trên amqp091-go.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrClosed: Client đã bị đóng bằng Close.
	ErrClosed = errors.New("rmq: client closed")
	// ErrNack: broker trả basic.nack cho message.
	ErrNack = errors.New("rmq: message nacked by broker")
)

// Config cấu hình Client.
type Config struct {
	URL        string
	Name       string                       // connection_name hiện trên UI
	Topology   func(ch *amqp.Channel) error // chạy lại sau MỖI lần kết nối
	MinBackoff time.Duration                // mặc định 500ms
	MaxBackoff time.Duration                // mặc định 30s
	Logger     *slog.Logger
}

// session gói một *amqp.Connection còn sống cùng channel publish của nó.
type session struct {
	conn  *amqp.Connection
	done  chan struct{} // bị đóng khi connection này chết
	pubMu sync.Mutex
	pubCh *amqp.Channel // channel ở confirm mode, dùng chung có khóa
	chErr chan *amqp.Error
}

// Client an toàn khi dùng từ nhiều goroutine.
type Client struct {
	cfg     Config
	log     *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	blocked atomic.Bool

	mu    sync.Mutex
	cur   *session      // nil khi đang mất kết nối
	ready chan struct{} // bị đóng khi cur != nil
}

// New tạo Client và bắt đầu kết nối ở goroutine nền. Hàm không chặn.
func New(cfg Config) *Client {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg: cfg, log: cfg.Logger, ctx: ctx, cancel: cancel,
		ready: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

// run là vòng đời kết nối: dial -> topology -> chờ chết -> backoff -> lặp lại.
func (c *Client) run() {
	defer c.wg.Done()
	attempt := 0
	for c.ctx.Err() == nil {
		s, err := c.connect()
		if err != nil {
			d := backoff(c.cfg.MinBackoff, c.cfg.MaxBackoff, attempt)
			attempt++
			c.log.Warn("connect failed", "attempt", attempt,
				"retry_in", d.Round(time.Millisecond), "err", err)
			select {
			case <-time.After(d):
				continue
			case <-c.ctx.Done():
				return
			}
		}
		attempt = 0
		closeCh := s.conn.NotifyClose(make(chan *amqp.Error, 1))
		blockCh := s.conn.NotifyBlocked(make(chan amqp.Blocking, 1))
		c.setSession(s)
		c.log.Info("connected", "name", c.cfg.Name)
		c.watch(s, closeCh, blockCh)
	}
}

// watch chặn cho tới khi connection chết hoặc Client bị đóng.
func (c *Client) watch(s *session, closeCh <-chan *amqp.Error,
	blockCh <-chan amqp.Blocking) {
	for {
		select {
		case b, ok := <-blockCh:
			if !ok {
				blockCh = nil // connection đã đóng, kênh này bị close
				continue
			}
			c.blocked.Store(b.Active)
			if b.Active {
				c.log.Warn("connection blocked by broker", "reason", b.Reason)
			} else {
				c.log.Info("connection unblocked")
			}
		case err := <-closeCh:
			c.clearSession(s)
			c.log.Warn("connection lost", "err", err)
			return
		case <-c.ctx.Done():
			c.clearSession(s)
			_ = s.conn.Close()
			return
		}
	}
}

func (c *Client) connect() (*session, error) {
	props := amqp.NewConnectionProperties()
	props.SetClientConnectionName(c.cfg.Name)
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat:  10 * time.Second,
		Properties: props,
	})
	if err != nil {
		return nil, err
	}
	if c.cfg.Topology != nil {
		ch, err := conn.Channel()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if err := c.cfg.Topology(ch); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("declare topology: %w", err)
		}
		_ = ch.Close()
	}
	return &session{conn: conn, done: make(chan struct{})}, nil
}

func (c *Client) setSession(s *session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked.Store(false)
	c.cur = s
	close(c.ready)
}

func (c *Client) clearSession(s *session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur == s {
		c.cur = nil
		c.ready = make(chan struct{})
		close(s.done)
	}
}

// waitSession trả về session hiện tại, chờ nếu đang kết nối lại.
func (c *Client) waitSession(ctx context.Context) (*session, error) {
	for {
		c.mu.Lock()
		s, ready := c.cur, c.ready
		c.mu.Unlock()
		if s != nil {
			return s, nil
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, ErrClosed
		}
	}
}

// Healthy trả nil nếu đang kết nối và không bị broker chặn (dùng cho /healthz).
func (c *Client) Healthy() error {
	c.mu.Lock()
	s := c.cur
	c.mu.Unlock()
	switch {
	case c.ctx.Err() != nil:
		return ErrClosed
	case s == nil:
		return errors.New("rmq: not connected")
	case c.blocked.Load():
		return errors.New("rmq: connection blocked by broker")
	}
	return nil
}

// Close dừng vòng kết nối lại và đóng connection.
func (c *Client) Close() {
	c.cancel()
	c.wg.Wait()
}

// backoff kiểu "equal jitter": nửa cố định + nửa ngẫu nhiên của base*2^attempt.
func backoff(base, limit time.Duration, attempt int) time.Duration {
	d := base << min(attempt, 20)
	if d <= 0 || d > limit {
		d = limit
	}
	half := d / 2
	return half + rand.N(half+1)
}

// ---------------------------------------------------------------- publish

// Publish gửi msg và CHỜ publisher confirm. Nếu connection rớt trước khi có
// confirm, Publish thử lại trên connection mới cho tới khi ctx hết hạn. Đây là
// at-least-once: có thể sinh bản trùng, hãy đặt MessageId để consumer khử trùng.
func (c *Client) Publish(ctx context.Context, exchange, key string,
	msg amqp.Publishing) error {
	for {
		s, err := c.waitSession(ctx)
		if err != nil {
			return err
		}
		if err := c.waitUnblocked(ctx); err != nil {
			return err
		}
		dc, err := s.publish(ctx, exchange, key, msg)
		if err == nil {
			var ok bool
			ok, err = dc.WaitContext(ctx)
			if err == nil && ok {
				return nil
			}
			if err == nil { // ack=false: nack thật hoặc channel/connection chết
				err = s.closeReason()
				if err == nil {
					return ErrNack
				}
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ae *amqp.Error
		if errors.As(err, &ae) && isChannelError(ae.Code) {
			return err // lỗi do chính message/topology: thử lại cũng vô ích
		}
		c.log.Warn("publish failed, will retry", "err", err)
		select { // chờ session cũ bị dọn để không quay vòng nóng
		case <-s.done:
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *session) publish(ctx context.Context, exchange, key string,
	msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	if s.pubCh == nil || s.pubCh.IsClosed() {
		ch, err := s.conn.Channel()
		if err != nil {
			return nil, err
		}
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, err
		}
		s.pubCh = ch
		s.chErr = ch.NotifyClose(make(chan *amqp.Error, 1))
	}
	return s.pubCh.PublishWithDeferredConfirmWithContext(
		ctx, exchange, key, false, false, msg)
}

// closeReason trả lỗi nếu connection hoặc channel publish đã đóng, nil nếu còn sống.
func (s *session) closeReason() error {
	if s.conn.IsClosed() {
		return amqp.ErrClosed
	}
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	select {
	case e, ok := <-s.chErr:
		if ok && e != nil {
			return e
		}
		return amqp.ErrClosed
	default:
		return nil
	}
}

// isChannelError: các mã lỗi do ứng dụng gây ra, không nên tự thử lại.
func isChannelError(code int) bool {
	switch code {
	case amqp.AccessRefused, amqp.NotFound, amqp.ResourceLocked,
		amqp.PreconditionFailed:
		return true
	}
	return false
}

func (c *Client) waitUnblocked(ctx context.Context) error {
	for c.blocked.Load() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("rmq: connection blocked: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}

// ---------------------------------------------------------------- consume

// Handler xử lý một delivery và TỰ ack/nack nó.
type Handler func(ctx context.Context, d amqp.Delivery)

// ConsumeOptions mô tả một consumer.
type ConsumeOptions struct {
	Queue    string
	Prefetch int // bắt buộc > 0 với quorum queue/stream; nên luôn đặt
	Workers  int // số goroutine xử lý song song trên cùng channel, mặc định 1
	Args     amqp.Table
}

// Consume chạy tới khi ctx bị hủy và tự consume lại sau mỗi lần kết nối lại.
// Khi ctx bị hủy: gửi basic.cancel, chờ các handler đang chạy xong rồi mới về.
func (c *Client) Consume(ctx context.Context, opt ConsumeOptions, h Handler) error {
	if opt.Workers <= 0 {
		opt.Workers = 1
	}
	for {
		s, err := c.waitSession(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		err = c.consumeOnce(ctx, s, opt, h)
		if ctx.Err() != nil {
			return nil
		}
		c.log.Warn("consumer stopped, will resume", "queue", opt.Queue, "err", err)
		select {
		case <-s.done:
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *Client) consumeOnce(ctx context.Context, s *session, opt ConsumeOptions,
	h Handler) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() // message chưa ack sẽ được broker trả lại queue
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	if err := ch.Qos(opt.Prefetch, 0, false); err != nil {
		return err
	}
	tag := fmt.Sprintf("%s.%s.%d", c.cfg.Name, opt.Queue, time.Now().UnixNano())
	msgs, err := ch.Consume(opt.Queue, tag, false, false, false, false, opt.Args)
	if err != nil {
		return err
	}
	c.log.Info("consuming", "queue", opt.Queue, "prefetch", opt.Prefetch,
		"workers", opt.Workers)
	stop := context.AfterFunc(ctx, func() { _ = ch.Cancel(tag, false) })
	defer stop()

	// Handler không nhận ctx gốc: khi shutdown, việc đang làm dở vẫn được làm xong.
	hctx := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	for range opt.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range msgs {
				h(hctx, d)
			}
		}()
	}
	wg.Wait()
	select {
	case e := <-closeCh:
		if e != nil {
			return e
		}
	default:
	}
	return errors.New("delivery channel closed")
}
