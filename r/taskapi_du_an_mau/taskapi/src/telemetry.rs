//! Khởi tạo tracing-subscriber: mức log lấy từ RUST_LOG, định dạng từ LOG_FORMAT.

use std::io::IsTerminal;

use tracing_subscriber::{
    EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

const DEFAULT_FILTER: &str = "taskapi=debug,tower_http=info,sqlx=warn,info";

pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let json =
        std::env::var("LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"));

    let registry = tracing_subscriber::registry().with(filter);
    if json {
        // Production: mỗi dòng là một object JSON, dễ đưa vào Loki/ELK.
        registry
            .with(fmt::layer().json().flatten_event(true))
            .init();
    } else {
        // Chỉ tô màu khi in ra terminal; file log/journald không cần mã ANSI.
        let ansi = std::io::stdout().is_terminal();
        registry.with(fmt::layer().compact().with_ansi(ansi)).init();
    }
}
