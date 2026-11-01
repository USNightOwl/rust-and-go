//! Integration test: mỗi test nhận một database PostgreSQL MỚI (do
//! `#[sqlx::test]` tạo, chạy migrations, rồi xóa khi test thành công).
//! Cần biến môi trường DATABASE_URL trỏ tới một server PostgreSQL.

use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use taskapi::{config::Settings, state::AppState};
use tower::ServiceExt; // cho `oneshot`

fn test_settings() -> Settings {
    Settings {
        host: "127.0.0.1".into(),
        port: 0,
        database_url: "unused-in-tests".into(),
        database_max_connections: 5,
        jwt_secret: "test-secret-that-is-at-least-32-characters".into(),
        jwt_issuer: "taskapi-test".into(),
        jwt_ttl: Duration::from_secs(600),
        cors_origins: vec!["*".into()],
        request_timeout: Duration::from_secs(10),
        run_migrations: false,
    }
}

fn app(db: PgPool) -> Router {
    let settings = test_settings();
    taskapi::build_app(AppState::new(db, &settings), &settings)
}

/// Gửi một request qua Router (không mở socket), trả về (status, JSON).
async fn send(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let req = match body {
        Some(json) => req
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json.to_string())),
        None => req.body(Body::empty()),
    }
    .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

/// Đăng ký + đăng nhập, trả về access token.
async fn signup(app: &Router, email: &str) -> String {
    let body = json!({ "email": email, "name": "Tester", "password": "password123" });
    let (status, _) =
        send(app, Method::POST, "/api/v1/auth/register", None, Some(body)).await;
    assert_eq!(status, StatusCode::CREATED);

    let body = json!({ "email": email, "password": "password123" });
    let (status, json) =
        send(app, Method::POST, "/api/v1/auth/login", None, Some(body)).await;
    assert_eq!(status, StatusCode::OK);
    json["access_token"].as_str().unwrap().to_string()
}

#[sqlx::test(migrations = "./migrations")]
async fn health_and_ready(db: PgPool) {
    let app = app(db);
    let (status, json) = send(&app, Method::GET, "/health", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");

    let (status, json) = send(&app, Method::GET, "/ready", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ready");
}

#[sqlx::test(migrations = "./migrations")]
async fn register_login_and_me(db: PgPool) {
    let app = app(db);
    let token = signup(&app, "Alice@Example.com").await;

    let (status, json) =
        send(&app, Method::GET, "/api/v1/auth/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["email"], "alice@example.com"); // đã chuẩn hóa chữ thường
    assert!(json.get("password_hash").is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn duplicate_email_returns_409(db: PgPool) {
    let app = app(db);
    signup(&app, "bob@example.com").await;

    let body =
        json!({ "email": "BOB@example.com", "name": "Bob", "password": "12345678" });
    let (status, json) = send(
        &app,
        Method::POST,
        "/api/v1/auth/register",
        None,
        Some(body),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["code"], "conflict");
}

#[sqlx::test(migrations = "./migrations")]
async fn invalid_payload_returns_422_with_details(db: PgPool) {
    let app = app(db);
    let body = json!({ "email": "not-an-email", "name": "", "password": "short" });
    let (status, json) = send(
        &app,
        Method::POST,
        "/api/v1/auth/register",
        None,
        Some(body),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["code"], "validation_error");
    for field in ["email", "name", "password"] {
        assert!(
            json["details"][field].is_array(),
            "missing details for {field}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn wrong_password_and_bad_tokens_return_401(db: PgPool) {
    let app = app(db);
    signup(&app, "carol@example.com").await;

    let body = json!({ "email": "carol@example.com", "password": "wrong-password" });
    let (status, _) =
        send(&app, Method::POST, "/api/v1/auth/login", None, Some(body)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = send(&app, Method::GET, "/api/v1/tasks", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, json) =
        send(&app, Method::GET, "/api/v1/tasks", Some("not.a.jwt"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json["message"], "invalid token");
}

#[sqlx::test(migrations = "./migrations")]
async fn task_crud_lifecycle(db: PgPool) {
    let app = app(db);
    let token = signup(&app, "dave@example.com").await;
    let t = Some(token.as_str());

    let body = json!({ "title": "Write tests", "description": "with sqlx::test",
                       "due_date": "2026-12-01" });
    let (status, task) = send(&app, Method::POST, "/api/v1/tasks", t, Some(body)).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(task["status"], "todo");
    assert_eq!(task["priority"], "medium");
    let uri = format!("/api/v1/tasks/{}", task["id"].as_str().unwrap());

    let (status, got) = send(&app, Method::GET, &uri, t, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got["title"], "Write tests");

    // PATCH: đổi status, xóa description bằng null, giữ nguyên due_date.
    let patch = json!({ "status": "done", "description": null });
    let (status, updated) = send(&app, Method::PATCH, &uri, t, Some(patch)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["status"], "done");
    assert!(updated["description"].is_null());
    assert_eq!(updated["due_date"], "2026-12-01");

    let (status, _) = send(&app, Method::PATCH, &uri, t, Some(json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, body) = send(&app, Method::DELETE, &uri, t, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_null());

    let (status, _) = send(&app, Method::GET, &uri, t, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn list_supports_filter_search_sort_and_pagination(db: PgPool) {
    let app = app(db);
    let token = signup(&app, "erin@example.com").await;
    let t = Some(token.as_str());

    for (title, status, priority) in [
        ("Buy milk", "todo", "low"),
        ("Fix bug #42", "in_progress", "high"),
        ("Deploy 100% safely", "todo", "high"),
        ("Write report", "done", "medium"),
    ] {
        let body = json!({ "title": title, "status": status, "priority": priority });
        let (status, _) =
            send(&app, Method::POST, "/api/v1/tasks", t, Some(body)).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (_, page) = send(&app, Method::GET, "/api/v1/tasks?per_page=3", t, None).await;
    assert_eq!(page["total"], 4);
    assert_eq!(page["total_pages"], 2);
    assert_eq!(page["items"].as_array().unwrap().len(), 3);

    let (_, page) = send(&app, Method::GET, "/api/v1/tasks?status=todo", t, None).await;
    assert_eq!(page["total"], 2);

    // '%' phải được hiểu là ký tự thường, không phải wildcard.
    let (_, page) = send(&app, Method::GET, "/api/v1/tasks?q=100%25", t, None).await;
    assert_eq!(page["total"], 1);
    assert_eq!(page["items"][0]["title"], "Deploy 100% safely");

    let uri = "/api/v1/tasks?sort=title&order=asc&page=2&per_page=2";
    let (_, page) = send(&app, Method::GET, uri, t, None).await;
    let titles: Vec<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["title"].as_str().unwrap())
        .collect();
    assert_eq!(titles, ["Fix bug #42", "Write report"]);

    let (status, json) =
        send(&app, Method::GET, "/api/v1/tasks?per_page=500", t, None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json["details"]["per_page"].is_array());

    let (status, _) =
        send(&app, Method::GET, "/api/v1/tasks?status=bogus", t, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn users_cannot_see_each_others_tasks(db: PgPool) {
    let app = app(db);
    let owner = signup(&app, "owner@example.com").await;
    let intruder = signup(&app, "intruder@example.com").await;

    let body = json!({ "title": "Private task" });
    let (_, task) = send(
        &app,
        Method::POST,
        "/api/v1/tasks",
        Some(&owner),
        Some(body),
    )
    .await;
    let uri = format!("/api/v1/tasks/{}", task["id"].as_str().unwrap());

    // 404 (không phải 403) để không tiết lộ rằng task đó tồn tại.
    let (status, _) = send(&app, Method::GET, &uri, Some(&intruder), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let patch = json!({ "title": "hacked" });
    let (status, _) =
        send(&app, Method::PATCH, &uri, Some(&intruder), Some(patch)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, Method::DELETE, &uri, Some(&intruder), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, page) =
        send(&app, Method::GET, "/api/v1/tasks", Some(&intruder), None).await;
    assert_eq!(page["total"], 0);

    let (status, _) = send(&app, Method::GET, &uri, Some(&owner), None).await;
    assert_eq!(status, StatusCode::OK);
}
