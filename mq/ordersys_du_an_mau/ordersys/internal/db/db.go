// Package db mở kết nối PostgreSQL và chạy DDL khởi tạo.
package db

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq" // driver "postgres"
)

func Open(ctx context.Context, url string) (*sql.DB, error) {
	d, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(10)
	d.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.PingContext(ctx); err != nil {
		_ = d.Close()
		return nil, err
	}
	return d, nil
}

// Migrate chạy DDL idempotent (CREATE ... IF NOT EXISTS). Production: dùng công cụ
// migration có version (goose, golang-migrate, atlas...).
func Migrate(ctx context.Context, d *sql.DB, ddl string) error {
	_, err := d.ExecContext(ctx, ddl)
	return err
}

// OutboxDDL trả về DDL bảng outbox cho một schema.
func OutboxDDL(schema string) string {
	return `CREATE TABLE IF NOT EXISTS ` + schema + `.outbox (
	id           bigserial PRIMARY KEY,
	message_id   text NOT NULL UNIQUE,
	routing_key  text NOT NULL,
	event_type   text NOT NULL,
	trace_id     text NOT NULL DEFAULT '',
	payload      jsonb NOT NULL,
	created_at   timestamptz NOT NULL DEFAULT now(),
	published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished
	ON ` + schema + `.outbox (id) WHERE published_at IS NULL;`
}

// ProcessedDDL trả về DDL bảng khử trùng (idempotency) cho một schema.
func ProcessedDDL(schema string) string {
	return `CREATE TABLE IF NOT EXISTS ` + schema + `.processed_messages (
	consumer     text NOT NULL,
	message_id   text NOT NULL,
	processed_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (consumer, message_id)
);`
}
