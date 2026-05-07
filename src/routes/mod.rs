use crate::state::AppState;
use axum::Router;
use std::sync::Arc;

pub fn build_router(state: Arc<AppState>, _user: &str, _password: &str) -> Router {
    // Will be filled in starting Task 7. For now an empty router with state.
    Router::new().with_state(state)
}
