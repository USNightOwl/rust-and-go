//! Extractor `AuthUser`: handler nào khai báo tham số này thì bắt buộc
//! phải có header `Authorization: Bearer <token>` hợp lệ.

use std::sync::Arc;

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header::AUTHORIZATION, request::Parts},
};
use uuid::Uuid;

use super::jwt::JwtKeys;
use crate::error::AppError;

#[derive(Debug, Clone, Copy)]
pub struct AuthUser {
    pub id: Uuid,
}

impl<S> FromRequestParts<S> for AuthUser
where
    Arc<JwtKeys>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, AppError> {
        let header = parts
            .headers
            .get(AUTHORIZATION)
            .ok_or(AppError::Unauthorized("missing authorization header"))?
            .to_str()
            .map_err(|_| AppError::Unauthorized("invalid authorization header"))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or(AppError::Unauthorized("expected a Bearer token"))?;

        let keys = Arc::<JwtKeys>::from_ref(state);
        let claims = keys.verify(token.trim())?;
        Ok(Self { id: claims.sub })
    }
}
