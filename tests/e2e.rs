use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use webhook_listener::db;
use webhook_listener::routes::build_router;
use webhook_listener::state::AppState;

#[tokio::test]
async fn end_to_end_create_endpoint_send_webhook_observe_in_list() {
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let state = Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
        body_limit_bytes: 1_048_576,
        session_token: "e2e-session-token".to_string(),
        dashboard_user: "u".to_string(),
        dashboard_password: "p".to_string(),
    });
    let app = build_router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let base = format!("http://{addr}");

    // 1. Log in via the dashboard form to obtain a session cookie.
    let resp = client
        .post(format!("{base}/login"))
        .form(&[("username", "u"), ("password", "p")])
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_redirection(), "login got {}", resp.status());
    let set_cookie = resp
        .headers()
        .get("set-cookie")
        .expect("login should set a session cookie")
        .to_str()
        .unwrap();
    // Keep only the "session=<token>" name=value pair (drop attributes).
    let session_cookie = set_cookie.split(';').next().unwrap().to_string();
    assert!(session_cookie.starts_with("session="), "{session_cookie}");

    // 2. Create endpoint via dashboard form (authenticated with the session cookie).
    let resp = client
        .post(format!("{base}/endpoints"))
        .header("cookie", &session_cookie)
        .form(&[("label", "E2E"), ("description", "test")])
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_redirection(), "got {}", resp.status());
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    let id = location.trim_start_matches("/endpoints/").to_string();

    // 3. Send a webhook (no auth required on the ingest endpoint).
    let resp = client
        .post(format!("{base}/webhooks/{id}?run=42"))
        .header("X-Custom", "hello")
        .json(&serde_json::json!({"event": "ping"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "got {}", resp.status());

    // 4. Poll the partial endpoint until the row appears.
    let partial_url = format!("{base}/endpoints/{id}/list");
    let mut found = false;
    for _ in 0..20 {
        let r = client
            .get(&partial_url)
            .header("cookie", &session_cookie)
            .send()
            .await
            .unwrap();
        let body = r.text().await.unwrap();
        if body.contains("X-Custom") || body.contains("POST") {
            assert!(body.contains("POST"));
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "webhook did not appear in list partial");

    handle.abort();
}
