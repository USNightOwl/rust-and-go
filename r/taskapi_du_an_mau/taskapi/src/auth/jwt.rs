//! Phát hành và kiểm tra JWT (HS256) bằng jsonwebtoken 11.

use std::time::Duration;

use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode,
    errors::ErrorKind,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{AppError, AppResult};

/// Payload của access token.
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject: id người dùng.
    pub sub: Uuid,
    /// Issuer: ai phát hành token.
    pub iss: String,
    /// Issued at (giây Unix).
    pub iat: u64,
    /// Expiration (giây Unix).
    pub exp: u64,
}

/// Khóa và tham số JWT, tạo MỘT lần khi khởi động rồi chia sẻ qua `Arc`.
pub struct JwtKeys {
    encoding: EncodingKey,
    decoding: DecodingKey,
    validation: Validation,
    issuer: String,
    ttl: Duration,
}

impl JwtKeys {
    pub fn new(secret: &SecretString, issuer: &str, ttl: Duration) -> Self {
        let secret = secret.expose_secret().as_bytes();
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[issuer]);
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        validation.leeway = 30; // chấp nhận lệch đồng hồ tối đa 30 giây
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            validation,
            issuer: issuer.to_string(),
            ttl,
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Tạo access token cho `user_id`.
    pub fn issue(&self, user_id: Uuid) -> AppResult<String> {
        let now = jsonwebtoken::get_current_timestamp();
        let claims = Claims {
            sub: user_id,
            iss: self.issuer.clone(),
            iat: now,
            exp: now + self.ttl.as_secs(),
        };
        encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|e| AppError::Internal(e.into()))
    }

    /// Giải mã và kiểm tra chữ ký, `exp`, `iss`.
    pub fn verify(&self, token: &str) -> AppResult<Claims> {
        decode::<Claims>(token, &self.decoding, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| match e.kind() {
                ErrorKind::ExpiredSignature => AppError::Unauthorized("token expired"),
                _ => AppError::Unauthorized("invalid token"),
            })
    }
}
