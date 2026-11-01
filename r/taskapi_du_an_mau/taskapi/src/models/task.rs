use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::FromRow;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use validator::{Validate, ValidationError};

/// Ánh xạ tới kiểu ENUM `task_status` của PostgreSQL.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, ToSchema,
)]
#[sqlx(type_name = "task_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Todo,
    InProgress,
    Done,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, ToSchema,
)]
#[sqlx(type_name = "task_priority", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum TaskPriority {
    Low,
    Medium,
    High,
}

/// Bản ghi `tasks`. Dùng luôn làm response vì không có trường nhạy cảm;
/// riêng `user_id` được ẩn khỏi JSON.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct Task {
    pub id: Uuid,
    #[serde(skip)]
    pub user_id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub status: TaskStatus,
    pub priority: TaskPriority,
    pub due_date: Option<NaiveDate>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(example = json!({
    "title": "Viết chương 17", "priority": "high", "due_date": "2026-11-15"
}))]
pub struct CreateTaskRequest {
    #[validate(
        length(min = 1, max = 200, message = "must be 1-200 characters"),
        custom(function = "not_blank")
    )]
    pub title: String,
    #[validate(length(max = 2000, message = "must be at most 2000 characters"))]
    pub description: Option<String>,
    pub status: Option<TaskStatus>,
    pub priority: Option<TaskPriority>,
    pub due_date: Option<NaiveDate>,
}

/// PATCH: trường vắng mặt = giữ nguyên; `null` = xóa giá trị (với trường
/// cho phép NULL). Vì vậy `description`, `due_date` là `Option<Option<T>>`.
#[derive(Debug, Deserialize, Validate, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateTaskRequest {
    #[validate(
        length(min = 1, max = 200, message = "must be 1-200 characters"),
        custom(function = "not_blank")
    )]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    #[schema(value_type = Option<String>)]
    #[validate(length(max = 2000, message = "must be at most 2000 characters"))]
    pub description: Option<Option<String>>,
    pub status: Option<TaskStatus>,
    pub priority: Option<TaskPriority>,
    #[serde(default, deserialize_with = "double_option")]
    #[schema(value_type = Option<NaiveDate>)]
    pub due_date: Option<Option<NaiveDate>>,
}

impl UpdateTaskRequest {
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.description.is_none()
            && self.status.is_none()
            && self.priority.is_none()
            && self.due_date.is_none()
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortField {
    #[default]
    CreatedAt,
    UpdatedAt,
    DueDate,
    Priority,
    Title,
}

impl SortField {
    /// Tên cột lấy từ danh sách cố định: không bao giờ nối chuỗi từ input.
    pub fn column(self) -> &'static str {
        match self {
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
            Self::DueDate => "due_date",
            Self::Priority => "priority",
            Self::Title => "title",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    #[default]
    Desc,
}

impl SortOrder {
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Deserialize, Validate, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListTasksQuery {
    /// Trang hiện tại, bắt đầu từ 1.
    #[serde(default = "default_page")]
    #[validate(range(min = 1, max = 10_000, message = "must be between 1 and 10000"))]
    #[param(default = 1, minimum = 1)]
    pub page: i64,
    /// Số phần tử mỗi trang (1-100).
    #[serde(default = "default_per_page")]
    #[validate(range(min = 1, max = 100, message = "must be between 1 and 100"))]
    #[param(default = 20, minimum = 1, maximum = 100)]
    pub per_page: i64,
    pub status: Option<TaskStatus>,
    pub priority: Option<TaskPriority>,
    /// Tìm trong title và description (không phân biệt hoa thường).
    #[validate(length(min = 1, max = 100, message = "must be 1-100 characters"))]
    pub q: Option<String>,
    #[serde(default)]
    #[param(inline)]
    pub sort: SortField,
    #[serde(default)]
    #[param(inline)]
    pub order: SortOrder,
}

/// Một trang kết quả kèm metadata phân trang.
#[derive(Debug, Serialize, ToSchema)]
pub struct TaskPage {
    pub items: Vec<Task>,
    pub page: i64,
    pub per_page: i64,
    pub total: i64,
    pub total_pages: i64,
}

fn default_page() -> i64 {
    1
}

fn default_per_page() -> i64 {
    20
}

fn not_blank(value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        let mut err = ValidationError::new("blank");
        err.message = Some("must not be blank".into());
        return Err(err);
    }
    Ok(())
}

/// Phân biệt "vắng mặt" (None, nhờ `#[serde(default)]`) với `null` (Some(None)).
fn double_option<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}
