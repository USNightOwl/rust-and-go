use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthUser,
    error::{AppError, AppResult, ErrorBody},
    extract::{AppPath, ValidatedJson, ValidatedQuery},
    models::{CreateTaskRequest, ListTasksQuery, Task, TaskPage, UpdateTaskRequest},
    repository::task_repo,
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_tasks).post(create_task))
        .route(
            "/{id}",
            get(get_task).patch(update_task).delete(delete_task),
        )
}

/// Liệt kê task của người dùng hiện tại (phân trang, lọc, tìm kiếm, sắp xếp).
#[utoipa::path(get, path = "/api/v1/tasks", tag = "tasks",
    params(ListTasksQuery),
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "A page of tasks", body = TaskPage),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 422, description = "Invalid query", body = ErrorBody),
    ))]
pub async fn list_tasks(
    auth: AuthUser,
    State(db): State<PgPool>,
    ValidatedQuery(query): ValidatedQuery<ListTasksQuery>,
) -> AppResult<Json<TaskPage>> {
    let (items, total) = task_repo::list(&db, auth.id, &query).await?;
    let total_pages = (total + query.per_page - 1) / query.per_page;
    Ok(Json(TaskPage {
        items,
        page: query.page,
        per_page: query.per_page,
        total,
        total_pages,
    }))
}

/// Tạo task mới.
#[utoipa::path(post, path = "/api/v1/tasks", tag = "tasks",
    request_body = CreateTaskRequest,
    security(("bearer_auth" = [])),
    responses(
        (status = 201, description = "Task created", body = Task),
        (status = 401, description = "Unauthorized", body = ErrorBody),
        (status = 422, description = "Validation error", body = ErrorBody),
    ))]
pub async fn create_task(
    auth: AuthUser,
    State(db): State<PgPool>,
    ValidatedJson(req): ValidatedJson<CreateTaskRequest>,
) -> AppResult<impl IntoResponse> {
    let task = task_repo::create(&db, auth.id, &req).await?;
    tracing::info!(task_id = %task.id, user_id = %auth.id, "task created");
    let location = format!("/api/v1/tasks/{}", task.id);
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(task),
    ))
}

/// Xem chi tiết một task.
#[utoipa::path(get, path = "/api/v1/tasks/{id}", tag = "tasks",
    params(("id" = Uuid, Path, description = "Task id")),
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "The task", body = Task),
        (status = 404, description = "Not found or not yours", body = ErrorBody),
    ))]
pub async fn get_task(
    auth: AuthUser,
    State(db): State<PgPool>,
    AppPath(id): AppPath<Uuid>,
) -> AppResult<Json<Task>> {
    let task = task_repo::find(&db, auth.id, id)
        .await?
        .ok_or(AppError::NotFound("task"))?;
    Ok(Json(task))
}

/// Cập nhật một phần task.
#[utoipa::path(patch, path = "/api/v1/tasks/{id}", tag = "tasks",
    params(("id" = Uuid, Path, description = "Task id")),
    request_body = UpdateTaskRequest,
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Updated task", body = Task),
        (status = 400, description = "Empty patch", body = ErrorBody),
        (status = 404, description = "Not found or not yours", body = ErrorBody),
        (status = 422, description = "Validation error", body = ErrorBody),
    ))]
pub async fn update_task(
    auth: AuthUser,
    State(db): State<PgPool>,
    AppPath(id): AppPath<Uuid>,
    ValidatedJson(patch): ValidatedJson<UpdateTaskRequest>,
) -> AppResult<Json<Task>> {
    if patch.is_empty() {
        return Err(AppError::BadRequest("no fields to update".into()));
    }
    let task = task_repo::update(&db, auth.id, id, &patch)
        .await?
        .ok_or(AppError::NotFound("task"))?;
    Ok(Json(task))
}

/// Xóa task.
#[utoipa::path(delete, path = "/api/v1/tasks/{id}", tag = "tasks",
    params(("id" = Uuid, Path, description = "Task id")),
    security(("bearer_auth" = [])),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found or not yours", body = ErrorBody),
    ))]
pub async fn delete_task(
    auth: AuthUser,
    State(db): State<PgPool>,
    AppPath(id): AppPath<Uuid>,
) -> AppResult<StatusCode> {
    if task_repo::delete(&db, auth.id, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound("task"))
    }
}
