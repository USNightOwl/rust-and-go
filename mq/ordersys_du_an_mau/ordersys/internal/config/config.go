// Package config đọc cấu hình từ biến môi trường (12-factor).
package config

import "os"

type Config struct {
	AMQPURL     string
	DatabaseURL string
	HTTPAddr    string
}

func Load(defaultAddr string) Config {
	return Config{
		AMQPURL: get("AMQP_URL", "amqp://admin:admin@localhost:5672/p2b"),
		DatabaseURL: get("DATABASE_URL",
			"postgres://postgres@localhost:5432/mq_p2b?sslmode=disable"),
		HTTPAddr: get("HTTP_ADDR", defaultAddr),
	}
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
