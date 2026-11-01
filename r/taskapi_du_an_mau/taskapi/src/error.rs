//! Kiểu lỗi thống nhất của API và cách chuyển lỗi thành HTTP response.

use std::collections::BTreeMap;

use axum::{
    Json,
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;
use validator::ValidationErrors;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("request validation failed")]
    Validation(#[from] ValidationErrors),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Unauthorized(&'static str),
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("{0}")]
    Conflict(&'static str),
    #[error(transparent)]
    Json(#[from] JsonRejection),
    #[error(transparent)]
    Query(#[from] QueryRejection),
    #[error(transparent)]
    Path(#[from] PathRejection),
    #[error("database error: {0}")]
    Database(sqlx::Error),
    #[error("internal error: {0:#}")]
    Internal(#[from] anyhow::Error),
}

/// Thân JSON của mọi response lỗi.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorBody {
    /// Mã HTTP, lặp lại để client tiện log.
    pub status: u16,
    /// Mã lỗi ổn định cho máy đọc, ví dụ `validation_error`.
    pub code: &'static str,
    /// Mô tả cho con người.
    pub message: String,
    /// Chi tiết theo từng trường (chỉ có với lỗi validation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<BTreeMap<String, Vec<String>>>,
}

impl AppError {
    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            Self::Validation(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "validation_error")
            }
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            Self::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::Json(r) => (r.status(), "invalid_body"),
            Self::Query(r) => (r.status(), "invalid_query"),
            Self::Path(r) => (r.status(), "invalid_path"),
            Self::Database(_) | Self::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();
        let message = match &self {
            // Không bao giờ lộ chi tiết lỗi nội bộ ra ngoài: chỉ ghi log.
            Self::Database(_) | Self::Internal(_) => {
                tracing::error!(error = %self, "unhandled internal error");
                "internal server error".to_string()
            }
            Self::Json(r) => r.body_text(),
            Self::Query(r) => r.body_text(),
            Self::Path(r) => r.body_text(),
            other => other.to_string(),
        };
        let details = match &self {
            Self::Validation(errors) => Some(validation_details(errors)),
            _ => None,
        };
        let body = ErrorBody {
            status: status.as_u16(),
            code,
            message,
            details,
        };
        (status, Json(body)).into_response()
    }
}

/// `{"title": ["length must be between 1 and 200"], ...}`
fn validation_details(errors: &ValidationErrors) -> BTreeMap<String, Vec<String>> {
    errors
        .field_errors()
        .into_iter()
        .map(|(field, errs)| {
            let messages = errs
                .iter()
                .map(|e| match &e.message {
                    Some(msg) => msg.to_string(),
                    None => e.code.to_string(),
                })
                .collect();
            (field.to_string(), messages)
        })
        .collect()
}

/// Ánh xạ lỗi SQLx sang lỗi nghiệp vụ. Mã SQLSTATE của PostgreSQL:
/// 23505 = unique_violation, 23503 = foreign_key_violation.
impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db_err) = &err {
            match db_err.code().as_deref() {
                Some("23505") => {
                    return match db_err.constraint() {
                        Some("users_email_key") => {
                            Self::Conflict("email is already registered")
                        }
                        _ => Self::Conflict("resource already exists"),
                    };
                }
                Some("23503") => return Self::BadRequest("invalid reference".into()),
                _ => {}
            }
        }
        if let sqlx::Error::RowNotFound = err {
            return Self::NotFound("resource");
        }
        Self::Database(err)
    }
}
