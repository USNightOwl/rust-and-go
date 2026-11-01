# Programming Handbooks: Rust, Go, RabbitMQ & Kafka

A collection of Vietnamese-language study handbooks for **Rust**, **Go**, **RabbitMQ** and **Apache Kafka**. The language handbooks cover fundamentals, data structures and algorithms, and backend development; the RabbitMQ handbook covers broker fundamentals and programming RabbitMQ with Go; the Kafka handbook covers Kafka internals, programming Kafka with Go, and event-driven architecture and operations. The handbooks are published as a static website on **GitHub Pages**.

## Live site

🌐 **https://usnightowl.github.io/rust-and-go/**

When you push to `main`, GitHub Actions deploys the site automatically. The landing page (`index.html`) links to each handbook:

| Handbook | Topics |
|---|---|
| [Sổ tay Rust All-in-One](r/Sổ%20tay%20Rust%20All-in-One.html) | Language fundamentals, data structures & algorithms, backend with Axum + SQLx |
| [Sổ tay học Go & Gin](g/So_tay_hoc_Go_Gin.html) | Go fundamentals, data structures & algorithms, backend with Gin |
| [Sổ tay RabbitMQ Toàn tập](mq/So_tay_RabbitMQ_Toan_tap.html) | AMQP model, exchanges, queue types, reliability, DLX/retry, streams, clustering, ops; RabbitMQ with Go (amqp091-go) |
| [Sổ tay học Kafka](k/So_tay_hoc_Kafka.html) | Kafka 4.4 fundamentals, Kafka programming in Go (franz-go), event-driven architecture & operations |

## Features

All four handbooks are single, self-contained HTML files with interactive checklists. You can tick off sections and exercises as you study, or mark a whole chapter done in one click. Each handbook shows overall, per-part and per-chapter progress, and follows your system's light or dark mode.

The handbooks come in two styles:

| | Rust · RabbitMQ | Go · Kafka |
|---|---|---|
| Progress counts | Chapter sections only (exercises and practice milestones are saved but not counted) | Sections **and** exercises |
| Search | Search across all chapters, filter by part, hide finished chapters | Search within the chapter list |
| Saved view settings | Selected part, "hide finished", open chapters | — |
| Move progress to another device | — | Progress code in the *Lộ trình* tab (copy, then paste on the other device) |
| Home button (**← Trang chủ**) | ✅ | Go ✅ · Kafka ❌ (not added yet) |

### How progress is saved

Progress is stored in the browser's `localStorage`, so it survives a page reload (F5) or closing the browser. Each handbook uses its own key, so they never overwrite each other:

| Handbook | `localStorage` key |
|---|---|
| Rust | `rust-aio-progress-v1` (view settings: `rust-aio-ui-v1`) |
| Go | `go-gin-sotay-v1` |
| RabbitMQ | `rabbitmq-aio-progress-v1` (view settings: `rabbitmq-aio-ui-v1`) |
| Kafka | `kafka-sotay-v1` |

> **Note:** Progress is stored per browser. Clearing site data, or opening the page in a different browser or in private mode, starts you from zero. On the Go and Kafka handbooks, use the progress code to back up or move your progress.
>
> The Rust and RabbitMQ handbooks can also sync progress to your account when opened as a Claude artifact. On GitHub Pages they show *"Lưu trên trình duyệt này"* and save locally instead.

## Repository structure

```
.
├── index.html                  # Landing page linking to all handbooks
├── .github/workflows/
│   └── deploy.yml              # GitHub Pages deployment workflow
├── r/                          # Rust materials
│   ├── Sổ tay Rust All-in-One.html
│   ├── Rust_P1_Nen_tang_ngon_ngu.docx     # Part 1: Language fundamentals
│   ├── Rust_P2_CTDL_Giai_thuat.docx       # Part 2: Data structures & algorithms
│   ├── Rust_P3_Backend_Axum_SQLx.docx     # Part 3: Backend with Axum + SQLx
│   └── taskapi_du_an_mau/taskapi/         # Sample project (see below)
├── g/                          # Go materials
│   ├── So_tay_hoc_Go_Gin.html
│   ├── Phan1_Golang_Nen_Tang.docx         # Part 1: Go fundamentals
│   ├── Phan2_CTDL_Giai_Thuat_Go.docx      # Part 2: Data structures & algorithms
│   └── Phan3_Backend_Go_Gin.docx          # Part 3: Backend with Gin
├── mq/                         # RabbitMQ materials
│   ├── So_tay_RabbitMQ_Toan_tap.html
│   ├── RabbitMQ_P1_Nen_tang_Kien_truc.docx  # Part 1: RabbitMQ fundamentals & architecture
│   ├── RabbitMQ_P2_Lap_trinh_Go.docx        # Part 2: Programming RabbitMQ with Go
│   └── ordersys_du_an_mau/ordersys/         # Sample Go microservice project
└── k/                          # Kafka materials
    ├── So_tay_hoc_Kafka.html
    ├── Kafka_P1_Nen_tang.docx               # Part 1: Kafka fundamentals (KRaft, storage, replication, clients)
    ├── Kafka_P2_Lap_trinh_Go.docx           # Part 2: Programming Kafka with Go (franz-go)
    ├── Kafka_P3_Kien_truc_Van_hanh.docx     # Part 3: Event-driven architecture & operations
    └── orders_du_an_mau/ordersys/           # Sample Go + Kafka project
```

## Sample project: `taskapi` (Rust)

`r/taskapi_du_an_mau/taskapi` is a reference task-management REST API that goes with the Rust handbook. It shows a production-style backend layout:

- **Web framework:** Axum 0.8, with Tower / tower-http middleware (tracing, CORS, timeouts, request IDs)
- **Database:** PostgreSQL via SQLx, with migrations
- **Auth:** JWT (`jsonwebtoken`) and Argon2 password hashing
- **API docs:** OpenAPI via `utoipa`, served with Swagger UI
- **Validation & errors:** `validator`, `thiserror`, `anyhow`
- **Observability:** `tracing` with structured JSON logs
- **Deployment:** Dockerfile, docker-compose, Kubernetes manifests, an nginx config, and a systemd unit
- **Layered architecture:** `routes` → `services` → `repository` → `models`

To run it locally (Rust 1.88+, Docker):

```bash
cd r/taskapi_du_an_mau/taskapi
cp .env.example .env
docker compose up -d     # start PostgreSQL (and the app, if configured)
cargo run
cargo test
```

## Sample project: `ordersys` (Go + RabbitMQ)

`mq/ordersys_du_an_mau/ordersys` is the order-processing system from Part 2, Chapter 9 of the RabbitMQ handbook:

- **order-service:** HTTP API (`net/http`, Go 1.22+ routing) that writes orders and an outbox table to PostgreSQL, then relays events to a topic exchange with publisher confirms
- **inventory-worker / notification-worker:** consumers with manual ack, retry queues (TTL + DLX, 2s/5s/10s), a parking-lot queue and idempotent processing
- **Libraries:** `github.com/rabbitmq/amqp091-go` v1.16 and `github.com/lib/pq` only
- **Ops:** reconnecting client, simple metrics, trace-id propagation through message headers, Dockerfile and docker-compose (not run in the authoring sandbox)

To run it locally (Go 1.24+, RabbitMQ 4.x, PostgreSQL):

```bash
cd mq/ordersys_du_an_mau/ordersys
docker compose up -d rabbitmq postgres   # only the broker and database
# (or `docker compose up -d` to run all three services in containers)
go run ./cmd/order-service
go run ./cmd/inventory-worker
go run ./cmd/notification-worker
```

## Sample project: `ordersys` (Kafka + Go)

`k/orders_du_an_mau/ordersys` is the event-driven order-processing system from chapter 14 of the Kafka Part 2 handbook: `order-api` (Gin) publishes to the `orders` topic, `payment-worker` consumes it and publishes to `payments`, and `notification-worker` consumes payments. It uses franz-go v1.22 with manual offset commits, retry with backoff plus a dead-letter topic, idempotent processing and graceful shutdown. Tests run against an in-memory Kafka (`kfake`), so they need no Docker.

```bash
cd k/orders_du_an_mau/ordersys
go mod tidy       # creates go.sum
make test         # unit + kfake tests
make up           # Kafka 4.4 + the three services via Docker Compose
```

## Deployment

The workflow at `.github/workflows/deploy.yml`:

1. Runs on every push to `main`, or manually from the **Actions** tab.
2. Copies `index.html`, `r/`, `g/`, `mq/` and `k/` into a `_site` folder.
3. Uploads `_site` and deploys it to GitHub Pages.

**One-time setup:** go to **Settings → Pages → Build and deployment → Source** and select **GitHub Actions**.

### Troubleshooting

- **404 on a handbook page:** check that the HTML file was actually committed and pushed. For example: `git add g/So_tay_hoc_Go_Gin.html`. If `git status` doesn't list it, run `git check-ignore -v <file>` to see whether a `.gitignore` rule is excluding it.
- **Changes don't show up after a deploy:** GitHub Pages lets browsers cache pages for about 10 minutes. Do a hard reload (**Ctrl + F5**, or **Cmd + Shift + R** on macOS), or open the page in a private window.
- **Linking to files with spaces or Vietnamese characters:** URL-encode the path. For example, `Sổ tay Rust All-in-One.html` becomes `S%E1%BB%95%20tay%20Rust%20All-in-One.html`.

## Viewing locally

The handbooks are self-contained HTML files. Open `index.html` directly in a browser, or serve the folder:

```bash
python -m http.server 8000
# then visit http://localhost:8000
```

## Language

The handbook content is written in **Vietnamese**.
