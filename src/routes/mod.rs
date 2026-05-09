pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::Router;
use axum::routing::{any, get, post};
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
#[allow(deprecated)]
use tower_http::{
    auth::require_authorization::Basic, validate_request::ValidateRequestHeaderLayer,
};

pub fn build_router(state: Arc<AppState>, user: &str, password: &str) -> Router {
    let body_limit = state.body_limit_bytes;

    let public = Router::new()
        .route("/webhooks/{endpoint_id}", any(ingest::ingest))
        .with_state(state.clone());

    let dashboard = Router::new()
        .route("/", get(dashboard::index))
        .route("/endpoints", post(dashboard::create_endpoint))
        .route("/endpoints/{id}", get(dashboard::endpoint_detail))
        .route(
            "/endpoints/{id}/list",
            get(dashboard::endpoint_list_partial),
        )
        .route("/endpoints/{id}/clear", post(dashboard::clear_endpoint))
        .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
        .route("/webhooks/view/{id}", get(dashboard::webhook_detail))
        .route(
            "/webhooks/view/{id}/delete",
            post(dashboard::delete_webhook),
        )
        .nest_service("/static", ServeDir::new("static"))
        .layer(
            #[allow(deprecated)]
            ValidateRequestHeaderLayer::<Basic<axum::body::Body>>::basic(user, password),
        )
        .with_state(state);

    public
        .merge(dashboard)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
}
