pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{any, get, post};
use axum_extra::extract::CookieJar;
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

async fn health() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/plain")], "ok")
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
        .nest_service("/static", ServeDir::new("static"))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        .with_state(state);

    public
        .merge(protected)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
}
