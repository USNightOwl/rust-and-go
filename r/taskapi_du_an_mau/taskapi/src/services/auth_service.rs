use sqlx::PgPool;

use crate::{
    auth::{JwtKeys, password},
    error::{AppError, AppResult},
    models::{LoginRequest, RegisterRequest, TokenResponse, User},
    repository::user_repo,
};

/// Chuẩn hóa email để ràng buộc UNIQUE hoạt động đúng ("A@x.com" == "a@x.com").
fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

pub async fn register(db: &PgPool, req: RegisterRequest) -> AppResult<User> {
    let email = normalize_email(&req.email);
    let hash = password::hash_password(req.password).await?;
    // Không SELECT trước để kiểm tra trùng (sẽ có race condition):
    // cứ INSERT, ràng buộc UNIQUE + mã lỗi 23505 sẽ thành 409 Conflict.
    let user = user_repo::create(db, &email, req.name.trim(), &hash).await?;
    tracing::info!(user_id = %user.id, "user registered");
    Ok(user)
}

pub async fn login(
    db: &PgPool,
    jwt: &JwtKeys,
    req: LoginRequest,
) -> AppResult<TokenResponse> {
    // Cùng một thông báo cho "không có email" và "sai mật khẩu" để kẻ tấn công
    // không dò được email nào đã đăng ký.
    const INVALID: AppError = AppError::Unauthorized("invalid email or password");

    let email = normalize_email(&req.email);
    let user = user_repo::find_by_email(db, &email).await?.ok_or(INVALID)?;
    if !password::verify_password(req.password, user.password_hash).await? {
        return Err(INVALID);
    }

    Ok(TokenResponse {
        access_token: jwt.issue(user.id)?,
        token_type: "Bearer",
        expires_in: jwt.ttl().as_secs(),
    })
}
