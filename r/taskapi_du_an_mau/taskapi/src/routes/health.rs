use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde::Serialize;
use sqlx::PgPool;
use utoipa::ToSchema;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

impl HealthResponse {
    fn new(status: &'static str) -> Self {
        Self {
            status,
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

/// Liveness: tiến trình còn sống và trả lời được HTTP. Không chạm DB.
#[utoipa::path(get, path = "/health", tag = "health",
    responses((status = 200, description = "Process is alive", body = HealthResponse)))]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse::new("ok"))
}

/// Readiness: sẵn sàng nhận traffic (kết nối được PostgreSQL).
#[utoipa::path(get, path = "/ready", tag = "health",
    responses(
        (status = 200, description = "Ready to serve traffic", body = HealthResponse),
        (status = 503, description = "Database unavailable", body = HealthResponse),
    ))]
pub async fn ready(State(db): State<PgPool>) -> (StatusCode, Json<HealthResponse>) {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&db)
        .await
    {
        Ok(_) => (StatusCode::OK, Json(HealthResponse::new("ready"))),
        Err(err) => {
            tracing::warn!(error = %err, "readiness check failed");
            let body = Json(HealthResponse::new("unavailable"));
            (StatusCode::SERVICE_UNAVAILABLE, body)
        }
    }
}
