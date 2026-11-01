use taskapi::{config::Settings, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Nạp .env nếu có (chỉ tiện cho dev; production dùng biến môi trường thật).
    dotenvy::dotenv().ok();
    telemetry::init();

    let settings = Settings::from_env()?;
    match std::env::args().nth(1).as_deref() {
        // `taskapi migrate`: chỉ chạy migration rồi thoát (dùng khi deploy).
        Some("migrate") => {
            let db = taskapi::connect_db(&settings).await?;
            taskapi::run_migrations(&db).await
        }
        None | Some("serve") => taskapi::run(settings).await,
        Some(other) => {
            anyhow::bail!("lệnh không hợp lệ: {other} (dùng: serve | migrate)")
        }
    }
}
