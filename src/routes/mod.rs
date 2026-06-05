pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{any, get, post};
use axum_extra::extract::CookieJar;
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;


const STYLES_CSS: &str = include_str!("../../static/styles.css");
const HTMX_JS: &str = include_str!("../../static/htmx.min.js");

async fn health() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/plain")], "ok")
}

async fn serve_styles() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLES_CSS)
}

async fn serve_htmx() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        HTMX_JS,
    )
}

async fn require_session(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    req: Request,
    next: Next,
) -> Response {
    let valid = jar
        .get("session")
        .map(|c| c.value() == state.session_token)
        .unwrap_or(false);

    if valid {
        next.run(req).await
    } else {
        Redirect::to("/login").into_response()
    }
}

/// Fallback for any unmatched route. This is a test application, so unknown
/// paths answer 200 instead of 404.
async fn ok_fallback() -> impl IntoResponse {
    StatusCode::OK
}

/// Test-application behavior: never surface an error status. Any 4xx/5xx
/// produced by a handler or a layer (e.g. the body-size limit's 413, a caught
/// panic's 500, an extractor's 400, or `AppError::NotFound`'s 404) is rewritten
/// to 200. Redirects (3xx) are left intact so the dashboard login/logout/create
/// flows keep working.
async fn force_ok_on_errors(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    if response.status().is_client_error() || response.status().is_server_error() {
        *response.status_mut() = StatusCode::OK;
    }
    response
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let body_limit = state.body_limit_bytes;

    let public = Router::new()
        .route("/health", get(health))
        .route("/login", get(dashboard::login_page).post(dashboard::login_post))
        .route("/logout", post(dashboard::logout))
        .route("/webhooks/{endpoint_id}", any(ingest::ingest))
        .with_state(state.clone());

    let protected = Router::new()
        .route("/", get(dashboard::index))
        .route("/endpoints", post(dashboard::create_endpoint))
        .route("/endpoints/{id}", get(dashboard::endpoint_detail))
        .route("/endpoints/{id}/list", get(dashboard::endpoint_list_partial))
        .route("/endpoints/{id}/clear", post(dashboard::clear_endpoint))
        .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
        .route("/webhooks/view/{id}", get(dashboard::webhook_detail))
        .route("/webhooks/view/{id}/delete", post(dashboard::delete_webhook))
        .route("/static/styles.css", get(serve_styles))
        .route("/static/htmx.min.js", get(serve_htmx))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        .with_state(state);

    public
        .merge(protected)
        .fallback(ok_fallback)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        // Outermost layer: wraps routing and every layer above, so it also
        // catches the body-limit 413 and caught-panic 500.
        .layer(middleware::from_fn(force_ok_on_errors))
}
