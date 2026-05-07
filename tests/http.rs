use axum::body::{to_bytes, Body};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceExt;
use webhook_listener::db;
use webhook_listener::routes::build_router;
use webhook_listener::state::AppState;

fn with_connect_info(mut req: Request<Body>) -> Request<Body> {
    let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

async fn test_state() -> Arc<AppState> {
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
    })
}

#[tokio::test]
async fn ingest_post_to_existing_endpoint_stores_webhook() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Test", "").await.unwrap();
    let app = build_router(state.clone(), "u", "p");

    let req = with_connect_info(
        Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{}?foo=bar", endpoint.id))
            .header("content-type", "application/json")
            .header("x-test", "yes")
            .body(Body::from(r#"{"hello":"world"}"#))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 1024).await.unwrap();
    assert!(body.is_empty());

    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    let saved = db::get_webhook(&state.pool, list[0].id).await.unwrap().unwrap();
    assert_eq!(saved.method, "POST");
    assert_eq!(saved.path, format!("/webhooks/{}", endpoint.id));
    assert_eq!(saved.query, "foo=bar");
    assert_eq!(saved.body, br#"{"hello":"world"}"#.to_vec());
    assert_eq!(saved.body_size, r#"{"hello":"world"}"#.len() as i64);
    assert!(saved.headers_json.contains("x-test"));
    assert_eq!(saved.source_ip, "127.0.0.1");
}

#[tokio::test]
async fn ingest_to_unknown_endpoint_returns_404() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = with_connect_info(
        Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{}", uuid::Uuid::new_v4()))
            .body(Body::from("x"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ingest_accepts_get_method_too() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Test", "").await.unwrap();
    let app = build_router(state.clone(), "u", "p");
    let req = with_connect_info(
        Request::builder()
            .method("GET")
            .uri(format!("/webhooks/{}", endpoint.id))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].method, "GET");
}

// --- Dashboard tests ---

fn auth_header(user: &str, pass: &str) -> String {
    use base64::{engine::general_purpose, Engine as _};
    let raw = format!("{user}:{pass}");
    format!("Basic {}", general_purpose::STANDARD.encode(raw))
}

#[tokio::test]
async fn dashboard_index_without_auth_returns_401() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key("www-authenticate"));
}

#[tokio::test]
async fn dashboard_index_with_bad_password_returns_401() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri("/")
        .header("authorization", auth_header("u", "wrong"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn dashboard_index_with_auth_returns_html() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri("/")
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("Webhook Listener"));
    assert!(body_str.contains("Create endpoint"));
}

#[tokio::test]
async fn create_endpoint_redirects_to_detail_and_persists() {
    let state = test_state().await;
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri("/endpoints")
        .header("authorization", auth_header("u", "p"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("label=GitHub&description=PRs"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection(), "got {}", resp.status());
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(location.starts_with("/endpoints/"), "{location}");
    let list = db::list_endpoints(&state.pool).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].label, "GitHub");
}
