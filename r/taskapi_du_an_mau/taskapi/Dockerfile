# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Build đa tầng (multi-stage) với cargo-chef để cache phần biên dịch dependency.
# ---------------------------------------------------------------------------
ARG RUST_VERSION=1.99

# Tầng 0: image Rust có sẵn cargo-chef.
FROM rust:${RUST_VERSION}-slim-trixie AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Tầng 1: "planner" - rút ra recipe.json (chỉ mô tả dependency, không có code).
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Tầng 2: "builder" - biên dịch dependency (được cache) rồi mới tới code của ta.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
# Không bao giờ kết nối DB lúc build (cần thư mục .sqlx nếu dùng query!).
ENV SQLX_OFFLINE=true
RUN cargo build --release --locked --bin taskapi

# Tầng 3: runtime tối giản, chỉ chứa binary + chứng chỉ CA.
FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin taskapi
COPY --from=builder /app/target/release/taskapi /usr/local/bin/taskapi
USER 10001:10001
ENV APP_HOST=0.0.0.0 \
    APP_PORT=3000 \
    LOG_FORMAT=json \
    RUST_LOG=taskapi=info,tower_http=info,sqlx=warn,info
EXPOSE 3000
# Dạng exec (mảng JSON): taskapi là PID 1 và nhận SIGTERM trực tiếp.
ENTRYPOINT ["/usr/local/bin/taskapi"]
CMD ["serve"]
