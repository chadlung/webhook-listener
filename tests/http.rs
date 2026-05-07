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
