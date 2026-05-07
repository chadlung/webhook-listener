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

fn fmt_ms(ms: i64) -> String {
    use time::format_description::well_known::Rfc3339;
    let nanos = (ms as i128) * 1_000_000;
    match time::OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => dt.format(&Rfc3339).unwrap_or_else(|_| ms.to_string()),
        Err(_) => ms.to_string(),
    }
}

struct WebhookRow {
    id: i64,
    received_at: String,
    method: String,
    path: String,
    source_ip: String,
    body_size: i64,
}

fn rows(items: Vec<db::WebhookSummary>) -> Vec<WebhookRow> {
    items
        .into_iter()
        .map(|w| WebhookRow {
            id: w.id,
            received_at: fmt_ms(w.received_at),
            method: w.method,
            path: w.path,
            source_ip: w.source_ip,
            body_size: w.body_size,
        })
        .collect()
}

struct EndpointRow {
    id: uuid::Uuid,
    label: String,
    description: String,
    webhook_count: i64,
    last_received_at: Option<String>,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    endpoints: Vec<EndpointRow>,
    base_url: String,
}

pub async fn index(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let raw = db::list_endpoints(&state.pool).await?;
    let endpoints = raw
        .into_iter()
        .map(|s| EndpointRow {
            id: s.id,
            label: s.label,
            description: s.description,
            webhook_count: s.webhook_count,
            last_received_at: s.last_received_at.map(fmt_ms),
        })
        .collect();
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string();
    let base_url = format!("http://{host}");
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

#[derive(Template)]
#[template(path = "endpoint.html")]
struct EndpointPageTemplate {
    endpoint: db::Endpoint,
    base_url: String,
    rows: Vec<WebhookRow>,
}

#[derive(Template)]
#[template(path = "_list.html")]
struct ListPartialTemplate {
    rows: Vec<WebhookRow>,
}

pub async fn endpoint_detail(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let endpoint = db::get_endpoint(&state.pool, id).await?.ok_or(AppError::NotFound)?;
    let summaries = db::list_webhooks_for_endpoint(&state.pool, id, 250).await?;
    let host = host_from_headers(&headers);
    render(EndpointPageTemplate {
        endpoint,
        base_url: format!("http://{host}"),
        rows: rows(summaries),
    })
}

pub async fn endpoint_list_partial(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<Response, AppError> {
    if db::get_endpoint(&state.pool, id).await?.is_none() {
        return Err(AppError::NotFound);
    }
    let summaries = db::list_webhooks_for_endpoint(&state.pool, id, 250).await?;
    render(ListPartialTemplate { rows: rows(summaries) })
}

pub async fn clear_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<Redirect, AppError> {
    db::clear_endpoint(&state.pool, id).await?;
    Ok(Redirect::to(&format!("/endpoints/{id}")))
}

struct DecodedBody {
    pretty: Option<String>,
    text: Option<String>,
    hex: Option<String>,
    label: String,
}

fn decode_body(body: &[u8], headers_json: &str) -> DecodedBody {
    let content_type = serde_json::from_str::<serde_json::Value>(headers_json)
        .ok()
        .and_then(|v| {
            v.get("content-type")
                .and_then(|c| c.get(0))
                .and_then(|s| s.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default();

    let is_json = content_type.contains("application/json")
        || content_type.contains("+json");

    match std::str::from_utf8(body) {
        Ok(text) => {
            if is_json
                && let Ok(v) = serde_json::from_str::<serde_json::Value>(text)
                && let Ok(pretty) = serde_json::to_string_pretty(&v)
            {
                return DecodedBody {
                    pretty: Some(pretty),
                    text: None,
                    hex: None,
                    label: "JSON".into(),
                };
            }
            DecodedBody {
                pretty: None,
                text: Some(text.to_string()),
                hex: None,
                label: if is_json { "JSON (unparsed)".into() } else { "text".into() },
            }
        }
        Err(_) => {
            let preview = body.iter().take(4096).copied().collect::<Vec<_>>();
            let hex = preview
                .chunks(16)
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect::<Vec<_>>()
                .join("\n");
            DecodedBody {
                pretty: None,
                text: None,
                hex: Some(hex),
                label: format!("binary ({} B, first 4 KiB shown)", body.len()),
            }
        }
    }
}

fn parse_headers(headers_json: &str) -> Vec<(String, String)> {
    let v: serde_json::Value = match serde_json::from_str(headers_json) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    let map = match v.as_object() {
        Some(m) => m,
        None => return vec![],
    };
    let mut out = Vec::new();
    for (name, values) in map {
        if let Some(arr) = values.as_array() {
            for val in arr {
                if let Some(s) = val.as_str() {
                    out.push((name.clone(), s.to_string()));
                }
            }
        }
    }
    out
}

#[derive(Template)]
#[template(path = "webhook_detail.html")]
struct WebhookDetailTemplate {
    webhook_id: i64,
    endpoint_id: uuid::Uuid,
    endpoint_label: String,
    received_at: String,
    method: String,
    path: String,
    query: String,
    source_ip: String,
    headers: Vec<(String, String)>,
    body_size: i64,
    body: DecodedBody,
}

pub async fn webhook_detail(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Response, AppError> {
    let webhook = db::get_webhook(&state.pool, id).await?.ok_or(AppError::NotFound)?;
    let endpoint = db::get_endpoint(&state.pool, webhook.endpoint_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let body = decode_body(&webhook.body, &webhook.headers_json);
    let headers = parse_headers(&webhook.headers_json);
    render(WebhookDetailTemplate {
        webhook_id: webhook.id,
        endpoint_id: endpoint.id,
        endpoint_label: endpoint.label,
        received_at: fmt_ms(webhook.received_at),
        method: webhook.method,
        path: webhook.path,
        query: webhook.query,
        source_ip: webhook.source_ip,
        headers,
        body_size: webhook.body_size,
        body,
    })
}
