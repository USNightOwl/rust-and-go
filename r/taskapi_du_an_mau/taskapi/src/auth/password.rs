//! Băm và kiểm tra mật khẩu bằng Argon2id (argon2 0.6).
//!
//! Argon2 cố ý tốn CPU và RAM, nên luôn chạy trong `spawn_blocking`
//! để không chặn worker thread của Tokio (xem Phần 3, Chương 3).

use anyhow::{Context, anyhow};
use argon2::{
    Argon2,
    password_hash::{
        Error as HashError, PasswordHasher, PasswordVerifier, phc::PasswordHash,
    },
};

use crate::error::AppResult;

/// Trả về chuỗi PHC dạng `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`.
pub async fn hash_password(password: String) -> AppResult<String> {
    let phc = tokio::task::spawn_blocking(move || {
        Argon2::default()
            .hash_password(password.as_bytes())
            .map(|hash| hash.to_string())
            .map_err(|e| anyhow!("failed to hash password: {e}"))
    })
    .await
    .context("password hashing task panicked")??;
    Ok(phc)
}

/// `Ok(true)` nếu khớp, `Ok(false)` nếu sai mật khẩu, `Err` nếu hash hỏng.
pub async fn verify_password(password: String, phc: String) -> AppResult<bool> {
    let matched = tokio::task::spawn_blocking(move || {
        let parsed = PasswordHash::new(&phc)
            .map_err(|e| anyhow!("invalid password hash: {e}"))?;
        match Argon2::default().verify_password(password.as_bytes(), &parsed) {
            Ok(()) => Ok(true),
            Err(HashError::PasswordInvalid) => Ok(false),
            Err(e) => Err(anyhow!("failed to verify password: {e}")),
        }
    })
    .await
    .context("password verification task panicked")??;
    Ok(matched)
}
