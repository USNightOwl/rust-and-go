//! Tài liệu OpenAPI sinh tự động từ code bằng utoipa.

use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

use crate::routes::{auth, health, tasks};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Task Manager API",
        version = "1.0.0",
        description = "Dự án mẫu Chương 17 - Rust All-in-One"
    ),
    paths(
        health::health,
        health::ready,
        auth::register,
        auth::login,
        auth::me,
        tasks::list_tasks,
        tasks::create_task,
        tasks::get_task,
        tasks::update_task,
        tasks::delete_task,
    ),
    modifiers(&BearerAuth),
    tags(
        (name = "health", description = "Liveness & readiness"),
        (name = "auth", description = "Đăng ký, đăng nhập"),
        (name = "tasks", description = "Quản lý công việc"),
    )
)]
pub struct ApiDoc;

/// Khai báo security scheme `bearer_auth` mà các path tham chiếu tới.
struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}
