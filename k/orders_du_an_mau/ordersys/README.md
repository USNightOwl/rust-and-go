# ordersys — dự án mẫu Kafka + Go (Phần 2, chương 14)

Hệ thống xử lý đơn hàng hướng sự kiện đi kèm bộ tài liệu Kafka:

```
order-api (Gin)  ──▶  topic orders  ──▶  payment-worker  ──▶  topic payments  ──▶  notification-worker
     ▲                                        │ lỗi vĩnh viễn / quá số lần thử
     └── GET /api/v1/orders/:id               ▼
                                         topic DLQ
```

- **Thư viện:** Go 1.27, Gin v1.12, franz-go v1.22 (kgo, kadm), kfake cho test.
- **Kỹ thuật:** producer idempotent, consumer group commit thủ công, retry có backoff + dead-letter topic, xử lý idempotent, graceful shutdown, health/readiness kiểm tra Kafka.

## Chạy

```bash
go mod tidy              # tạo go.sum (cần trước khi build Docker)
make test                # unit test + test với cụm Kafka giả lập kfake, không cần Docker
make up                  # Kafka 4.4 + 3 dịch vụ bằng Docker Compose
curl -X POST localhost:8080/api/v1/orders -H 'Content-Type: application/json' \
  -d '{"user_id":"user-42","amount":459000}'          # trả 202 Accepted kèm order_id
```

Hoặc chạy từng dịch vụ trên máy (Kafka ở `localhost:9092`, xem `.env.example`): `make run-api`, `make run-payment`, `make run-notification`.

Giải thích chi tiết từng file nằm ở chương 14 của `Kafka_P2_Lap_trinh_Go.docx`.
