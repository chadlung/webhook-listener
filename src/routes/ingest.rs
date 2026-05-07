use crate::db;
use crate::error::AppError;
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

pub async fn ingest(
    State(state): State<Arc<AppState>>,
    Path(endpoint_id): Path<Uuid>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, AppError> {
    if db::get_endpoint(&state.pool, endpoint_id).await?.is_none() {
        return Err(AppError::NotFound);
    }

    let received_at_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers.iter() {
        let entry = grouped.entry(name.as_str().to_ascii_lowercase()).or_default();
        let v = match value.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => "<binary>".to_string(),
        };
        entry.push(v);
    }
    let headers_json = serde_json::to_string(&grouped)
        .map_err(|e| AppError::Internal(format!("serializing headers: {e}")))?;

    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    let source_ip = addr.ip().to_string();

    db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id,
            received_at: received_at_ms,
            method: method.as_str(),
            path: &path,
            query: &query,
            source_ip: &source_ip,
            headers_json: &headers_json,
            body: &body,
        },
        state.retain_per_endpoint,
    )
    .await?;

    tracing::info!(
        endpoint_id = %endpoint_id,
        method = %method,
        body_size = body.len(),
        source_ip = %source_ip,
        "webhook received"
    );

    Ok(StatusCode::OK)
}
