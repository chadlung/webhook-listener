pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::routing::{any, get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::services::ServeDir;
#[allow(deprecated)]
use tower_http::{
    auth::require_authorization::Basic, validate_request::ValidateRequestHeaderLayer,
};

pub fn build_router(state: Arc<AppState>, user: &str, password: &str) -> Router {
    let public = Router::new()
        .route("/webhooks/{endpoint_id}", any(ingest::ingest))
        .with_state(state.clone());

    let dashboard = Router::new()
        .route("/", get(dashboard::index))
        .route("/endpoints", post(dashboard::create_endpoint))
        .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
        .nest_service("/static", ServeDir::new("static"))
        .layer(
            #[allow(deprecated)]
            ValidateRequestHeaderLayer::<Basic<axum::body::Body>>::basic(user, password),
        )
        .with_state(state);

    public.merge(dashboard)
}
