//! Tầng truy cập dữ liệu: chỉ chứa SQL, trả về `sqlx::Error`.
//! Việc ánh xạ sang lỗi HTTP do `AppError::from` đảm nhiệm.

pub mod task_repo;
pub mod user_repo;
