//! Trạng thái dùng chung cho mọi handler.

use std::sync::Arc;

use axum::extract::FromRef;
use sqlx::PgPool;

use crate::{auth::JwtKeys, config::Settings};

/// `Clone` rẻ: `PgPool` và `Arc` bên trong đều chỉ tăng bộ đếm tham chiếu.
/// `FromRef` cho phép handler lấy riêng từng phần: `State<PgPool>`.
#[derive(Clone, FromRef)]
pub struct AppState {
    pub db: PgPool,
    pub jwt: Arc<JwtKeys>,
}

impl AppState {
    pub fn new(db: PgPool, settings: &Settings) -> Self {
        let jwt =
            JwtKeys::new(&settings.jwt_secret, &settings.jwt_issuer, settings.jwt_ttl);
        Self {
            db,
            jwt: Arc::new(jwt),
        }
    }
}
