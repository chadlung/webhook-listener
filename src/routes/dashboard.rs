use crate::db;
use crate::error::AppError;
use crate::state::AppState;
use askama::Template;
use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;
use std::sync::Arc;

fn host_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string()
}

fn render<T: Template>(t: T) -> Result<Response, AppError> {
    let body = t.render()?;
    Ok(Html(body).into_response())
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    endpoints: Vec<db::EndpointSummary>,
    base_url: String,
}

pub async fn index(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let endpoints = db::list_endpoints(&state.pool).await?;
    let base_url = format!("http://{}", host_from_headers(&headers));
    render(IndexTemplate { endpoints, base_url })
}

#[derive(Deserialize)]
pub struct CreateEndpointForm {
    pub label: String,
    #[serde(default)]
    pub description: String,
}

pub async fn create_endpoint(
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateEndpointForm>,
) -> Result<Redirect, AppError> {
    let label = form.label.trim();
    if label.is_empty() {
        return Err(AppError::BadRequest("label is required".into()));
    }
    let endpoint = db::create_endpoint(&state.pool, label, form.description.trim()).await?;
    Ok(Redirect::to(&format!("/endpoints/{}", endpoint.id)))
}

pub async fn delete_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let _ = db::delete_endpoint(&state.pool, id).await?;
    Ok((StatusCode::SEE_OTHER, [("location", "/")]))
}
