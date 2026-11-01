use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::models::{
    CreateTaskRequest, ListTasksQuery, Task, TaskPriority, TaskStatus,
    UpdateTaskRequest,
};

pub async fn create(
    db: &PgPool,
    user_id: Uuid,
    req: &CreateTaskRequest,
) -> Result<Task, sqlx::Error> {
    sqlx::query_as::<_, Task>(
        "INSERT INTO tasks (id, user_id, title, description, status, priority, due_date)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, user_id, title, description, status, priority, due_date,
                   created_at, updated_at",
    )
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(req.title.trim())
    .bind(req.description.as_deref())
    .bind(req.status.unwrap_or(TaskStatus::Todo))
    .bind(req.priority.unwrap_or(TaskPriority::Medium))
    .bind(req.due_date)
    .fetch_one(db)
    .await
}

/// Luôn lọc theo `user_id`: task của người khác coi như không tồn tại.
pub async fn find(
    db: &PgPool,
    user_id: Uuid,
    id: Uuid,
) -> Result<Option<Task>, sqlx::Error> {
    sqlx::query_as::<_, Task>(
        "SELECT id, user_id, title, description, status, priority, due_date,
                created_at, updated_at
         FROM tasks WHERE id = $1 AND user_id = $2",
    )
    .bind(id)
    .bind(user_id)
    .fetch_optional(db)
    .await
}

/// Cập nhật một phần trong MỘT câu lệnh (nguyên tử, không cần transaction).
/// `COALESCE($n, cột)`: NULL nghĩa là giữ nguyên. Với cột cho phép NULL,
/// cờ boolean cho biết client có gửi trường đó hay không.
pub async fn update(
    db: &PgPool,
    user_id: Uuid,
    id: Uuid,
    patch: &UpdateTaskRequest,
) -> Result<Option<Task>, sqlx::Error> {
    sqlx::query_as::<_, Task>(
        "UPDATE tasks SET
             title       = COALESCE($3, title),
             description = CASE WHEN $4 THEN $5 ELSE description END,
             status      = COALESCE($6, status),
             priority    = COALESCE($7, priority),
             due_date    = CASE WHEN $8 THEN $9 ELSE due_date END
         WHERE id = $1 AND user_id = $2
         RETURNING id, user_id, title, description, status, priority, due_date,
                   created_at, updated_at",
    )
    .bind(id)
    .bind(user_id)
    .bind(patch.title.as_deref().map(str::trim))
    .bind(patch.description.is_some())
    .bind(patch.description.clone().flatten())
    .bind(patch.status)
    .bind(patch.priority)
    .bind(patch.due_date.is_some())
    .bind(patch.due_date.flatten())
    .fetch_optional(db)
    .await
}

/// Trả về `true` nếu có bản ghi bị xóa.
pub async fn delete(db: &PgPool, user_id: Uuid, id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM tasks WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(user_id)
        .execute(db)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Trả về (các task của trang hiện tại, tổng số task khớp bộ lọc).
pub async fn list(
    db: &PgPool,
    user_id: Uuid,
    query: &ListTasksQuery,
) -> Result<(Vec<Task>, i64), sqlx::Error> {
    let mut count = QueryBuilder::<Postgres>::new("SELECT COUNT(*) FROM tasks");
    push_filters(&mut count, user_id, query);
    let total: i64 = count.build_query_scalar().fetch_one(db).await?;

    let mut select = QueryBuilder::<Postgres>::new(
        "SELECT id, user_id, title, description, status, priority, due_date,
                created_at, updated_at
         FROM tasks",
    );
    push_filters(&mut select, user_id, query);
    let (column, order) = (query.sort.column(), query.order.keyword());
    // Tên cột/ASC/DESC lấy từ whitelist nên push thẳng vào SQL là an toàn.
    // Thêm `id` để thứ tự ổn định khi nhiều dòng có cùng giá trị sắp xếp.
    select.push(format_args!(
        " ORDER BY {column} {order} NULLS LAST, id {order}"
    ));
    select
        .push(" LIMIT ")
        .push_bind(query.per_page)
        .push(" OFFSET ")
        .push_bind((query.page - 1) * query.per_page);
    let items = select.build_query_as::<Task>().fetch_all(db).await?;

    Ok((items, total))
}

/// Mọi giá trị do người dùng nhập đều đi qua `push_bind` (tham số $n).
fn push_filters(
    qb: &mut QueryBuilder<Postgres>,
    user_id: Uuid,
    query: &ListTasksQuery,
) {
    qb.push(" WHERE user_id = ").push_bind(user_id);
    if let Some(status) = query.status {
        qb.push(" AND status = ").push_bind(status);
    }
    if let Some(priority) = query.priority {
        qb.push(" AND priority = ").push_bind(priority);
    }
    if let Some(q) = query.q.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
        let pattern = format!("%{}%", escape_like(q));
        qb.push(" AND (title ILIKE ")
            .push_bind(pattern.clone())
            .push(" OR description ILIKE ")
            .push_bind(pattern)
            .push(")");
    }
}

/// Vô hiệu hóa ký tự đại diện của LIKE trong chuỗi người dùng nhập.
fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
