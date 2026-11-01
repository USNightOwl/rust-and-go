use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use sqlx::PgPool;

use crate::{
    auth::AuthUser,
    error::{AppError, AppResult, ErrorBody},
    extract::ValidatedJson,
    models::{LoginRequest, RegisterRequest, TokenResponse, UserResponse},
    repository::user_repo,
    services::auth_service,
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/register", post(register))
        .route("/login", post(login))
        .route("/me", get(me))
}

/// Đăng ký tài khoản mới.
#[utoipa::path(post, path = "/api/v1/auth/register", tag = "auth",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User created", body = UserResponse),
        (status = 409, description = "Email already registered", body = ErrorBody),
        (status = 422, description = "Validation error", body = ErrorBody),
    ))]
pub async fn register(
    State(db): State<PgPool>,
    ValidatedJson(req): ValidatedJson<RegisterRequest>,
) -> AppResult<(StatusCode, Json<UserResponse>)> {
    let user = auth_service::register(&db, req).await?;
    Ok((StatusCode::CREATED, Json(user.into())))
}

/// Đăng nhập, nhận access token (JWT).
#[utoipa::path(post, path = "/api/v1/auth/login", tag = "auth",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Logged in", body = TokenResponse),
        (status = 401, description = "Invalid credentials", body = ErrorBody),
    ))]
pub async fn login(
    State(state): State<AppState>,
    ValidatedJson(req): ValidatedJson<LoginRequest>,
) -> AppResult<Json<TokenResponse>> {
    let token = auth_service::login(&state.db, &state.jwt, req).await?;
    Ok(Json(token))
}

/// Thông tin người dùng hiện tại.
#[utoipa::path(get, path = "/api/v1/auth/me", tag = "auth",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current user", body = UserResponse),
        (status = 401, description = "Missing or invalid token", body = ErrorBody),
    ))]
pub async fn me(
    auth: AuthUser,
    State(db): State<PgPool>,
) -> AppResult<Json<UserResponse>> {
    // Token còn hạn nhưng tài khoản đã bị xóa -> coi như token không hợp lệ.
    let user = user_repo::find_by_id(&db, auth.id)
        .await?
        .ok_or(AppError::Unauthorized("user no longer exists"))?;
    Ok(Json(user.into()))
}
