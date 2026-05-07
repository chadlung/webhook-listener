pub mod ingest;

use crate::state::AppState;
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

pub fn build_router(state: Arc<AppState>, _user: &str, _password: &str) -> Router {
    let public = Router::new().route("/webhooks/{endpoint_id}", any(ingest::ingest));
    public.with_state(state)
}
