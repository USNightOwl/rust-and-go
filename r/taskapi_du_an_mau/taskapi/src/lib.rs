//! Task Manager API - dự án mẫu Phần 3, Chương 17.

pub mod auth;
pub mod config;
pub mod error;
pub mod extract;
pub mod models;
pub mod openapi;
pub mod repository;
pub mod routes;
pub mod services;
pub mod state;
pub mod telemetry;

use std::time::Duration;

use anyhow::Context;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderName, HeaderValue, Method, Request, StatusCode, header},
};
use secrecy::ExposeSecret;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{config::Settings, error::AppError, openapi::ApiDoc, state::AppState};

const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const MAX_BODY_BYTES: usize = 64 * 1024;

/// Lắp ráp toàn bộ ứng dụng. Tách khỏi `run` để test gọi trực tiếp
/// bằng `tower::ServiceExt::oneshot` mà không cần mở cổng mạng.
pub fn build_app(state: AppState, settings: &Settings) -> Router {
    let api = Router::new()
        .nest("/auth", routes::auth::router())
        .nest("/tasks", routes::tasks::router());

    // Layer khai báo trước bọc NGOÀI layer khai báo sau (request đi từ trên xuống).
    let middleware = ServiceBuilder::new()
        .layer(SetRequestIdLayer::new(REQUEST_ID, MakeRequestUuid))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(http_span)
                // Mỗi response sinh một dòng log INFO kèm status và latency.
                .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
        )
        .layer(PropagateRequestIdLayer::new(REQUEST_ID))
        .layer(cors_layer(&settings.cors_origins))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            settings.request_timeout,
        ))
        // Giới hạn body cho extractor (Json...). Vượt quá -> JsonRejection 413,
        // nên lỗi vẫn đi qua AppError và có định dạng JSON thống nhất.
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES));

    Router::new()
        .merge(routes::health::router())
        .nest("/api/v1", api)
        .merge(
            SwaggerUi::new("/swagger-ui")
                .url("/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .fallback(|| async { AppError::NotFound("route") })
        .layer(middleware)
        .with_state(state)
}

/// Span bao quanh mỗi request; mọi log bên trong handler đều mang theo các field này.
fn http_span<B>(req: &Request<B>) -> tracing::Span {
    let request_id = req
        .headers()
        .get(REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    tracing::info_span!(
        "http",
        method = %req.method(),
        uri = %req.uri(),
        request_id = %request_id,
    )
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    let allow_origin = if origins.iter().any(|o| o == "*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(origins.iter().filter_map(|o| HeaderValue::from_str(o).ok()))
    };
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .expose_headers([REQUEST_ID, header::LOCATION])
        .max_age(Duration::from_secs(3600))
}

pub async fn connect_db(settings: &Settings) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(settings.database_max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(settings.database_url.expose_secret())
        .await
        .context("không kết nối được PostgreSQL")
}

/// Chạy migration (nhúng vào binary lúc biên dịch bằng `sqlx::migrate!`).
pub async fn run_migrations(db: &PgPool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .context("chạy migration thất bại")?;
    tracing::info!("database migrations are up to date");
    Ok(())
}

/// Khởi động HTTP server và chờ đến khi nhận tín hiệu tắt.
pub async fn run(settings: Settings) -> anyhow::Result<()> {
    let db = connect_db(&settings).await?;
    if settings.run_migrations {
        run_migrations(&db).await?;
    }

    let app = build_app(AppState::new(db.clone(), &settings), &settings);
    let listener = TcpListener::bind(settings.addr()?).await?;
    tracing::info!(addr = %listener.local_addr()?, "taskapi listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Mọi request đang xử lý đã xong: đóng pool một cách gọn gàng.
    db.close().await;
    tracing::info!("server stopped gracefully");
    Ok(())
}

/// Hoàn thành khi nhận Ctrl+C (SIGINT) hoặc SIGTERM (Docker/Kubernetes/systemd).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received, draining in-flight requests");
}
