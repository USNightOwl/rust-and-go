//! Cấu hình ứng dụng đọc từ biến môi trường (12-factor).

use std::{env, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{Context, bail};
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone)]
pub struct Settings {
    pub host: String,
    pub port: u16,
    pub database_url: SecretString,
    pub database_max_connections: u32,
    pub jwt_secret: SecretString,
    pub jwt_issuer: String,
    pub jwt_ttl: Duration,
    pub cors_origins: Vec<String>,
    pub request_timeout: Duration,
    pub run_migrations: bool,
}

impl Settings {
    /// Đọc và kiểm tra toàn bộ cấu hình. Lỗi ở đây làm ứng dụng dừng ngay
    /// khi khởi động (fail fast) thay vì hỏng âm thầm lúc đang chạy.
    pub fn from_env() -> anyhow::Result<Self> {
        let settings = Self {
            host: optional("APP_HOST", "0.0.0.0".to_string())?,
            port: optional("APP_PORT", 3000)?,
            database_url: required("DATABASE_URL")?.into(),
            database_max_connections: optional("DATABASE_MAX_CONNECTIONS", 10)?,
            jwt_secret: required("JWT_SECRET")?.into(),
            jwt_issuer: optional("JWT_ISSUER", "taskapi".to_string())?,
            jwt_ttl: Duration::from_secs(60 * optional("JWT_TTL_MINUTES", 60)?),
            cors_origins: optional("CORS_ORIGINS", "*".to_string())?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            request_timeout: Duration::from_secs(optional("REQUEST_TIMEOUT_SECS", 15)?),
            run_migrations: optional("RUN_MIGRATIONS", true)?,
        };
        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.jwt_secret.expose_secret().len() < 32 {
            bail!("JWT_SECRET phải dài tối thiểu 32 ký tự");
        }
        if self.database_max_connections == 0 {
            bail!("DATABASE_MAX_CONNECTIONS phải lớn hơn 0");
        }
        Ok(())
    }

    pub fn addr(&self) -> anyhow::Result<SocketAddr> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .context("APP_HOST/APP_PORT không hợp lệ")
    }
}

fn required(key: &str) -> anyhow::Result<String> {
    env::var(key).with_context(|| format!("thiếu biến môi trường {key}"))
}

fn optional<T>(key: &str, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|e| anyhow::anyhow!("biến {key}={raw:?} không hợp lệ: {e}")),
        Err(_) => Ok(default),
    }
}
