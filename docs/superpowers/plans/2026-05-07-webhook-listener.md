# Webhook Listener Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a small Rust HTTP service that captures incoming webhooks at user-created UUID-keyed endpoints and presents them on a basic-auth-protected dashboard, persisting to SQLite.

**Architecture:** Single binary, axum + tokio. One `Arc<AppState>` shared by two router branches: a public `ANY /webhooks/{uuid}` ingest endpoint and a basic-auth-protected dashboard. Storage in SQLite via sqlx. Templates rendered with askama. Live updates on the endpoint detail page via HTMX polling every 5 seconds.

**Tech Stack:** axum 0.8, tokio 1, sqlx 0.8 (sqlite), askama 0.12 + askama_axum 0.4, tower-http 0.6, tower 0.5, htmx 2.0, thiserror 2, anyhow 1, clap 4, tracing 0.1, time 0.3, uuid 1, serde 1, serde_json 1.

**Reference spec:** `docs/superpowers/specs/2026-05-07-webhook-listener-design.md`

---

## File Structure

Files this plan will create:

```
webhook-listener/
├── Cargo.toml                                      # Task 1
├── .gitignore                                      # Task 1
├── migrations/
│   └── 0001_init.sql                               # Task 2
├── static/
│   ├── htmx.min.js                                 # Task 8
│   └── styles.css                                  # Task 8
├── templates/
│   ├── layout.html                                 # Task 8
│   ├── index.html                                  # Task 8
│   ├── endpoint.html                               # Task 9
│   ├── _list.html                                  # Task 9
│   └── webhook_detail.html                         # Task 11
├── src/
│   ├── main.rs                                     # Task 6 (overwrite scaffold)
│   ├── config.rs                                   # Task 4
│   ├── state.rs                                    # Task 6
│   ├── error.rs                                    # Task 5
│   ├── db.rs                                       # Tasks 2, 3
│   └── routes/
│       ├── mod.rs                                  # Task 6
│       ├── ingest.rs                               # Task 7
│       └── dashboard.rs                            # Tasks 8, 9, 10, 11, 12
└── tests/
    ├── http.rs                                     # Tasks 7, 8, 9, 10, 11, 12
    └── e2e.rs                                      # Task 14
```

Boundaries:
- **`db.rs`** owns all SQL. Returns typed structs. Handlers never write SQL.
- **`routes/ingest.rs`** is one handler — capture path only.
- **`routes/dashboard.rs`** holds dashboard handlers + form parsing. May grow to ~400 lines; if it exceeds 600, split into `endpoints.rs` + `webhooks.rs` submodules.
- **`error.rs`** has the only `IntoResponse` impl for app errors.
- **`config.rs`** is the only place that reads env vars.

---

## Task 1: Cargo manifest, gitignore, directory skeleton

**Files:**
- Modify: `Cargo.toml`
- Modify: `.gitignore`
- Create directories: `migrations/`, `static/`, `templates/`, `src/routes/`, `tests/`

- [ ] **Step 1: Replace `Cargo.toml` content**

```toml
[package]
name = "webhook-listener"
version = "0.1.0"
edition = "2024"

[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["trace", "limit", "validate-request", "fs", "catch-panic"] }
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio-rustls", "sqlite", "macros", "migrate"] }
askama = "0.12"
askama_axum = "0.4"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
time = { version = "0.3", features = ["serde", "formatting", "macros"] }
clap = { version = "4", features = ["derive", "env"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
anyhow = "1"
bytes = "1"

[dev-dependencies]
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
```

- [ ] **Step 2: Replace `.gitignore` content**

```
/target
*.db
*.db-shm
*.db-wal
```

- [ ] **Step 3: Create empty directories**

```bash
mkdir -p migrations static templates src/routes tests
```

- [ ] **Step 4: Verify cargo build (will compile the existing hello-world `main.rs` with new deps)**

```bash
cargo build
```

Expected: builds successfully. Will take a while for first build (pulling many crates).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock .gitignore migrations static templates src/routes tests
git commit -m "Add cargo dependencies and project skeleton"
```

Note: empty directories will not be committed by git directly. The next tasks add files to them, which is fine.

---

## Task 2: Migration + `db.rs` endpoint operations (TDD)

**Files:**
- Create: `migrations/0001_init.sql`
- Create: `src/db.rs`

- [ ] **Step 1: Create migration `migrations/0001_init.sql`**

```sql
CREATE TABLE endpoints (
    id           TEXT PRIMARY KEY,
    label        TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL
);

CREATE TABLE webhooks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_id  TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    received_at  INTEGER NOT NULL,
    method       TEXT NOT NULL,
    path         TEXT NOT NULL,
    query        TEXT NOT NULL DEFAULT '',
    source_ip    TEXT NOT NULL,
    headers      TEXT NOT NULL,
    body         BLOB NOT NULL,
    body_size    INTEGER NOT NULL
);

CREATE INDEX webhooks_endpoint_received
    ON webhooks(endpoint_id, received_at DESC);
```

- [ ] **Step 2: Create `src/db.rs` with the pool helper, types, and endpoint operations**

```rust
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub id: Uuid,
    pub label: String,
    pub description: String,
    pub created_at: i64,
}

pub async fn open_pool(path: &str) -> Result<SqlitePool, sqlx::Error> {
    let opts = SqliteConnectOptions::from_str(path)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new().connect_with(opts).await
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

pub async fn create_endpoint(
    pool: &SqlitePool,
    label: &str,
    description: &str,
) -> Result<Endpoint, sqlx::Error> {
    let id = Uuid::new_v4();
    let id_str = id.to_string();
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    sqlx::query(
        "INSERT INTO endpoints (id, label, description, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(&id_str)
    .bind(label)
    .bind(description)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(Endpoint {
        id,
        label: label.to_string(),
        description: description.to_string(),
        created_at: now,
    })
}

pub async fn get_endpoint(pool: &SqlitePool, id: Uuid) -> Result<Option<Endpoint>, sqlx::Error> {
    let id_str = id.to_string();
    let row: Option<(String, String, String, i64)> = sqlx::query_as(
        "SELECT id, label, description, created_at FROM endpoints WHERE id = ?",
    )
    .bind(&id_str)
    .fetch_optional(pool)
    .await?;
    row.map(|(id, label, description, created_at)| {
        Ok(Endpoint {
            id: Uuid::parse_str(&id).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            label,
            description,
            created_at,
        })
    })
    .transpose()
}

#[derive(Debug, Clone)]
pub struct EndpointSummary {
    pub id: Uuid,
    pub label: String,
    pub description: String,
    pub webhook_count: i64,
    pub last_received_at: Option<i64>,
}

pub async fn list_endpoints(pool: &SqlitePool) -> Result<Vec<EndpointSummary>, sqlx::Error> {
    let rows: Vec<(String, String, String, i64, Option<i64>)> = sqlx::query_as(
        "SELECT e.id, e.label, e.description,
                COUNT(w.id) AS cnt,
                MAX(w.received_at) AS last
         FROM endpoints e
         LEFT JOIN webhooks w ON w.endpoint_id = e.id
         GROUP BY e.id
         ORDER BY e.created_at DESC",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(id, label, description, cnt, last)| {
            Ok(EndpointSummary {
                id: Uuid::parse_str(&id).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
                label,
                description,
                webhook_count: cnt,
                last_received_at: last,
            })
        })
        .collect()
}

pub async fn delete_endpoint(pool: &SqlitePool, id: Uuid) -> Result<bool, sqlx::Error> {
    let id_str = id.to_string();
    let result = sqlx::query("DELETE FROM endpoints WHERE id = ?")
        .bind(&id_str)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_pool() -> SqlitePool {
        let pool = open_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn create_and_get_endpoint_round_trip() {
        let pool = test_pool().await;
        let created = create_endpoint(&pool, "GitHub PRs", "all PR events")
            .await
            .unwrap();
        let fetched = get_endpoint(&pool, created.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.label, "GitHub PRs");
        assert_eq!(fetched.description, "all PR events");
        assert_eq!(fetched.created_at, created.created_at);
    }

    #[tokio::test]
    async fn get_endpoint_returns_none_when_missing() {
        let pool = test_pool().await;
        let result = get_endpoint(&pool, Uuid::new_v4()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn list_endpoints_orders_by_created_desc_with_zero_counts() {
        let pool = test_pool().await;
        let a = create_endpoint(&pool, "A", "").await.unwrap();
        // Sleep so the unix_timestamp differs.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let b = create_endpoint(&pool, "B", "").await.unwrap();
        let list = list_endpoints(&pool).await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, b.id);
        assert_eq!(list[1].id, a.id);
        assert_eq!(list[0].webhook_count, 0);
        assert_eq!(list[0].last_received_at, None);
    }

    #[tokio::test]
    async fn delete_endpoint_removes_row_and_reports_affected() {
        let pool = test_pool().await;
        let e = create_endpoint(&pool, "X", "").await.unwrap();
        assert!(delete_endpoint(&pool, e.id).await.unwrap());
        assert!(!delete_endpoint(&pool, e.id).await.unwrap());
        assert!(get_endpoint(&pool, e.id).await.unwrap().is_none());
    }
}
```

- [ ] **Step 3: Wire `db` module into `main.rs` so it compiles** (we'll replace `main.rs` properly in Task 6 — for now keep hello-world but expose the module)

Replace `src/main.rs` with:

```rust
mod db;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 4: Run the unit tests**

```bash
cargo test --lib
```

Expected: 4 tests pass (`create_and_get_endpoint_round_trip`, `get_endpoint_returns_none_when_missing`, `list_endpoints_orders_by_created_desc_with_zero_counts`, `delete_endpoint_removes_row_and_reports_affected`).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock migrations/0001_init.sql src/db.rs src/main.rs
git commit -m "Add SQLite schema and endpoint CRUD with unit tests"
```

---

## Task 3: `db.rs` webhook operations + eviction (TDD)

**Files:**
- Modify: `src/db.rs`

- [ ] **Step 1: Add `Webhook`, `WebhookSummary`, `NewWebhook` types and operations to `src/db.rs`**

Add the following at the end of `src/db.rs`, before the `#[cfg(test)] mod tests` block:

```rust
#[derive(Debug, Clone)]
pub struct NewWebhook<'a> {
    pub endpoint_id: Uuid,
    pub received_at: i64, // ms
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub source_ip: &'a str,
    pub headers_json: &'a str,
    pub body: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct Webhook {
    pub id: i64,
    pub endpoint_id: Uuid,
    pub received_at: i64,
    pub method: String,
    pub path: String,
    pub query: String,
    pub source_ip: String,
    pub headers_json: String,
    pub body: Vec<u8>,
    pub body_size: i64,
}

#[derive(Debug, Clone)]
pub struct WebhookSummary {
    pub id: i64,
    pub received_at: i64,
    pub method: String,
    pub path: String,
    pub source_ip: String,
    pub body_size: i64,
}

pub async fn insert_webhook(
    pool: &SqlitePool,
    new: &NewWebhook<'_>,
    retain_per_endpoint: i64,
) -> Result<i64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let endpoint_id_str = new.endpoint_id.to_string();
    let inserted_id: i64 = sqlx::query_scalar(
        "INSERT INTO webhooks
            (endpoint_id, received_at, method, path, query, source_ip, headers, body, body_size)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING id",
    )
    .bind(&endpoint_id_str)
    .bind(new.received_at)
    .bind(new.method)
    .bind(new.path)
    .bind(new.query)
    .bind(new.source_ip)
    .bind(new.headers_json)
    .bind(new.body)
    .bind(new.body.len() as i64)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query(
        "DELETE FROM webhooks
         WHERE endpoint_id = ?
           AND id NOT IN (
               SELECT id FROM webhooks
               WHERE endpoint_id = ?
               ORDER BY received_at DESC, id DESC
               LIMIT ?
           )",
    )
    .bind(&endpoint_id_str)
    .bind(&endpoint_id_str)
    .bind(retain_per_endpoint)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(inserted_id)
}

pub async fn list_webhooks_for_endpoint(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    limit: i64,
) -> Result<Vec<WebhookSummary>, sqlx::Error> {
    let endpoint_id_str = endpoint_id.to_string();
    let rows: Vec<(i64, i64, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, received_at, method, path, source_ip, body_size
         FROM webhooks
         WHERE endpoint_id = ?
         ORDER BY received_at DESC, id DESC
         LIMIT ?",
    )
    .bind(&endpoint_id_str)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, received_at, method, path, source_ip, body_size)| WebhookSummary {
            id,
            received_at,
            method,
            path,
            source_ip,
            body_size,
        })
        .collect())
}

pub async fn get_webhook(pool: &SqlitePool, id: i64) -> Result<Option<Webhook>, sqlx::Error> {
    let row: Option<(i64, String, i64, String, String, String, String, String, Vec<u8>, i64)> =
        sqlx::query_as(
            "SELECT id, endpoint_id, received_at, method, path, query, source_ip, headers, body, body_size
             FROM webhooks WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;
    row.map(|(id, endpoint_id, received_at, method, path, query, source_ip, headers_json, body, body_size)| {
        Ok(Webhook {
            id,
            endpoint_id: Uuid::parse_str(&endpoint_id)
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            received_at,
            method,
            path,
            query,
            source_ip,
            headers_json,
            body,
            body_size,
        })
    })
    .transpose()
}

pub async fn delete_webhook(pool: &SqlitePool, id: i64) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let endpoint_id: Option<String> =
        sqlx::query_scalar("SELECT endpoint_id FROM webhooks WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    sqlx::query("DELETE FROM webhooks WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    endpoint_id
        .map(|s| Uuid::parse_str(&s).map_err(|e| sqlx::Error::Decode(Box::new(e))))
        .transpose()
}

pub async fn clear_endpoint(pool: &SqlitePool, endpoint_id: Uuid) -> Result<u64, sqlx::Error> {
    let endpoint_id_str = endpoint_id.to_string();
    let result = sqlx::query("DELETE FROM webhooks WHERE endpoint_id = ?")
        .bind(&endpoint_id_str)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
```

- [ ] **Step 2: Add unit tests for webhook operations and eviction inside the `tests` module in `src/db.rs`**

Add inside the existing `mod tests`:

```rust
fn make_new<'a>(endpoint_id: Uuid, ts: i64, body: &'a [u8]) -> NewWebhook<'a> {
    NewWebhook {
        endpoint_id,
        received_at: ts,
        method: "POST",
        path: "/webhooks/x",
        query: "",
        source_ip: "127.0.0.1",
        headers_json: "{}",
        body,
    }
}

#[tokio::test]
async fn insert_below_cap_keeps_all_webhooks() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    for i in 0..10 {
        insert_webhook(&pool, &make_new(e.id, i, b"x"), 250)
            .await
            .unwrap();
    }
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    assert_eq!(list.len(), 10);
}

#[tokio::test]
async fn insert_at_cap_evicts_oldest() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    let cap = 5;
    for i in 0..(cap + 3) {
        insert_webhook(&pool, &make_new(e.id, i, b"x"), cap)
            .await
            .unwrap();
    }
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    assert_eq!(list.len(), cap as usize);
    // Most recent first; oldest 3 evicted, surviving received_at = [7,6,5,4,3].
    let received: Vec<i64> = list.iter().map(|w| w.received_at).collect();
    assert_eq!(received, vec![7, 6, 5, 4, 3]);
}

#[tokio::test]
async fn eviction_is_scoped_per_endpoint() {
    let pool = test_pool().await;
    let a = create_endpoint(&pool, "A", "").await.unwrap();
    let b = create_endpoint(&pool, "B", "").await.unwrap();
    let cap = 2;
    for i in 0..5 {
        insert_webhook(&pool, &make_new(a.id, i, b"x"), cap)
            .await
            .unwrap();
    }
    insert_webhook(&pool, &make_new(b.id, 100, b"y"), cap)
        .await
        .unwrap();
    let list_a = list_webhooks_for_endpoint(&pool, a.id, 1000).await.unwrap();
    let list_b = list_webhooks_for_endpoint(&pool, b.id, 1000).await.unwrap();
    assert_eq!(list_a.len(), 2);
    assert_eq!(list_b.len(), 1);
}

#[tokio::test]
async fn same_millisecond_ties_break_on_id_desc() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    let cap = 2;
    let id1 = insert_webhook(&pool, &make_new(e.id, 1000, b"a"), cap)
        .await
        .unwrap();
    let id2 = insert_webhook(&pool, &make_new(e.id, 1000, b"b"), cap)
        .await
        .unwrap();
    let id3 = insert_webhook(&pool, &make_new(e.id, 1000, b"c"), cap)
        .await
        .unwrap();
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    let ids: Vec<i64> = list.iter().map(|w| w.id).collect();
    assert_eq!(ids, vec![id3, id2]);
    assert!(id1 < id2 && id2 < id3);
}

#[tokio::test]
async fn delete_endpoint_cascades_to_webhooks() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    insert_webhook(&pool, &make_new(e.id, 1, b"x"), 250)
        .await
        .unwrap();
    assert!(delete_endpoint(&pool, e.id).await.unwrap());
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
async fn clear_endpoint_keeps_endpoint_drops_webhooks() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    insert_webhook(&pool, &make_new(e.id, 1, b"x"), 250)
        .await
        .unwrap();
    let n = clear_endpoint(&pool, e.id).await.unwrap();
    assert_eq!(n, 1);
    assert!(get_endpoint(&pool, e.id).await.unwrap().is_some());
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
async fn delete_webhook_returns_endpoint_id() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    let id = insert_webhook(&pool, &make_new(e.id, 1, b"x"), 250)
        .await
        .unwrap();
    let returned = delete_webhook(&pool, id).await.unwrap();
    assert_eq!(returned, Some(e.id));
    let again = delete_webhook(&pool, id).await.unwrap();
    assert_eq!(again, None);
}

#[tokio::test]
async fn list_webhooks_summary_orders_recent_first() {
    let pool = test_pool().await;
    let e = create_endpoint(&pool, "E", "").await.unwrap();
    insert_webhook(&pool, &make_new(e.id, 1, b"x"), 250)
        .await
        .unwrap();
    insert_webhook(&pool, &make_new(e.id, 2, b"y"), 250)
        .await
        .unwrap();
    let list = list_webhooks_for_endpoint(&pool, e.id, 1000).await.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].received_at, 2);
    assert_eq!(list[1].received_at, 1);
}
```

- [ ] **Step 3: Run the unit tests**

```bash
cargo test --lib
```

Expected: all tests pass (12 total: 4 from Task 2 + 8 new).

- [ ] **Step 4: Commit**

```bash
git add src/db.rs
git commit -m "Add webhook insert with eviction and supporting queries"
```

---

## Task 4: Config module (TDD)

**Files:**
- Create: `src/config.rs`
- Modify: `src/main.rs` (add `mod config;`)

- [ ] **Step 1: Create `src/config.rs`**

```rust
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "webhook-listener", version)]
pub struct CliArgs {
    /// Bind address.
    #[arg(long, env = "WEBHOOK_BIND", default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// SQLite database file path.
    #[arg(long, env = "WEBHOOK_DB_PATH", default_value = "webhooks.db")]
    pub db_path: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub db_path: String,
    pub body_limit_bytes: usize,
    pub retain_per_endpoint: i64,
    pub dashboard_user: String,
    pub dashboard_password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("environment variable {0} is required and must be non-empty")]
    MissingEnv(&'static str),
    #[error("environment variable {var} has invalid value: {message}")]
    InvalidEnv { var: &'static str, message: String },
}

fn require_env(name: &'static str) -> Result<String, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(ConfigError::MissingEnv(name)),
    }
}

fn optional_usize_env(
    name: &'static str,
    default: usize,
) -> Result<usize, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.parse::<usize>().map_err(|e| ConfigError::InvalidEnv {
            var: name,
            message: e.to_string(),
        }),
        _ => Ok(default),
    }
}

fn optional_i64_env(name: &'static str, default: i64) -> Result<i64, ConfigError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.parse::<i64>().map_err(|e| ConfigError::InvalidEnv {
            var: name,
            message: e.to_string(),
        }),
        _ => Ok(default),
    }
}

impl Config {
    pub fn from_args_and_env(args: CliArgs) -> Result<Self, ConfigError> {
        let dashboard_user = require_env("WEBHOOK_DASHBOARD_USER")?;
        let dashboard_password = require_env("WEBHOOK_DASHBOARD_PASSWORD")?;
        let body_limit_bytes = optional_usize_env("WEBHOOK_BODY_LIMIT_BYTES", 1_048_576)?;
        let retain_per_endpoint = optional_i64_env("WEBHOOK_RETAIN_PER_ENDPOINT", 250)?;
        if retain_per_endpoint < 1 {
            return Err(ConfigError::InvalidEnv {
                var: "WEBHOOK_RETAIN_PER_ENDPOINT",
                message: "must be >= 1".into(),
            });
        }
        Ok(Self {
            bind: args.bind,
            db_path: args.db_path,
            body_limit_bytes,
            retain_per_endpoint,
            dashboard_user,
            dashboard_password,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> CliArgs {
        CliArgs {
            bind: "127.0.0.1:8080".into(),
            db_path: "webhooks.db".into(),
        }
    }

    // Note: env-var tests must serialize because std::env::set_var is process-global.
    // We use a simple Mutex from std for this.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    fn clear_all() {
        for v in [
            "WEBHOOK_DASHBOARD_USER",
            "WEBHOOK_DASHBOARD_PASSWORD",
            "WEBHOOK_BODY_LIMIT_BYTES",
            "WEBHOOK_RETAIN_PER_ENDPOINT",
        ] {
            // SAFETY: tests are serialized via env_lock().
            unsafe { std::env::remove_var(v) };
        }
    }

    #[test]
    fn missing_user_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::MissingEnv("WEBHOOK_DASHBOARD_USER") => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_password_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::MissingEnv("WEBHOOK_DASHBOARD_PASSWORD") => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn defaults_apply_when_only_credentials_set() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        let cfg = Config::from_args_and_env(args()).unwrap();
        assert_eq!(cfg.dashboard_user, "u");
        assert_eq!(cfg.dashboard_password, "p");
        assert_eq!(cfg.body_limit_bytes, 1_048_576);
        assert_eq!(cfg.retain_per_endpoint, 250);
    }

    #[test]
    fn invalid_retain_zero_is_error() {
        let _g = env_lock().lock().unwrap();
        clear_all();
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_USER", "u") };
        unsafe { std::env::set_var("WEBHOOK_DASHBOARD_PASSWORD", "p") };
        unsafe { std::env::set_var("WEBHOOK_RETAIN_PER_ENDPOINT", "0") };
        let err = Config::from_args_and_env(args()).unwrap_err();
        match err {
            ConfigError::InvalidEnv { var: "WEBHOOK_RETAIN_PER_ENDPOINT", .. } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Add `mod config;` to `src/main.rs`**

```rust
mod config;
mod db;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test --lib
```

Expected: 16 tests pass (12 db + 4 config).

- [ ] **Step 4: Commit**

```bash
git add src/config.rs src/main.rs
git commit -m "Add config parsing from CLI args and env vars"
```

---

## Task 5: AppError type

**Files:**
- Create: `src/error.rs`
- Modify: `src/main.rs` (add `mod error;`)

- [ ] **Step 1: Create `src/error.rs`**

```rust
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Template(#[from] askama::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            AppError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            AppError::Database(e) => {
                tracing::error!(error = %e, "database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".to_string())
            }
            AppError::Template(e) => {
                tracing::error!(error = %e, "template error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".to_string())
            }
            AppError::Internal(m) => {
                tracing::error!(message = %m, "internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".to_string())
            }
        };
        (status, msg).into_response()
    }
}
```

- [ ] **Step 2: Add `mod error;` to `src/main.rs`**

```rust
mod config;
mod db;
mod error;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 3: Verify compilation**

```bash
cargo build
```

Expected: builds clean.

- [ ] **Step 4: Commit**

```bash
git add src/error.rs src/main.rs
git commit -m "Add AppError type with IntoResponse mapping"
```

---

## Task 6: AppState, router skeleton, and runnable `main.rs`

**Files:**
- Create: `src/state.rs`
- Create: `src/routes/mod.rs`
- Modify: `src/main.rs` (replace fully)

- [ ] **Step 1: Create `src/state.rs`**

```rust
use sqlx::SqlitePool;

pub struct AppState {
    pub pool: SqlitePool,
    pub retain_per_endpoint: i64,
}
```

- [ ] **Step 2: Create `src/routes/mod.rs`**

```rust
use crate::state::AppState;
use axum::Router;
use std::sync::Arc;

pub fn build_router(state: Arc<AppState>, _user: &str, _password: &str) -> Router {
    // Will be filled in starting Task 7. For now an empty router with state.
    Router::new().with_state(state)
}
```

- [ ] **Step 3: Replace `src/main.rs` fully**

```rust
mod config;
mod db;
mod error;
mod routes;
mod state;

use crate::config::{CliArgs, Config};
use crate::state::AppState;
use anyhow::Context;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config = Config::from_args_and_env(args).context("invalid configuration")?;

    let pool = db::open_pool(&config.db_path)
        .await
        .with_context(|| format!("opening database at {}", config.db_path))?;
    db::run_migrations(&pool)
        .await
        .context("running migrations")?;

    let state = Arc::new(AppState {
        pool,
        retain_per_endpoint: config.retain_per_endpoint,
    });

    let app = routes::build_router(state, &config.dashboard_user, &config.dashboard_password);

    let addr: SocketAddr = config
        .bind
        .parse()
        .with_context(|| format!("invalid bind address: {}", config.bind))?;
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
```

- [ ] **Step 4: Verify the binary compiles and starts (then stop it)**

```bash
cargo build
WEBHOOK_DASHBOARD_USER=test WEBHOOK_DASHBOARD_PASSWORD=test cargo run -- --bind 127.0.0.1:18080 --db-path /tmp/wh-test.db &
sleep 2
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:18080/
kill %1
rm -f /tmp/wh-test.db /tmp/wh-test.db-shm /tmp/wh-test.db-wal
```

Expected: `404` (no routes mounted yet; axum returns 404 for unknown paths).

- [ ] **Step 5: Run all tests**

```bash
cargo test
```

Expected: all 16 unit tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/state.rs src/routes/mod.rs src/main.rs
git commit -m "Wire up runtime: tokio main, pool, migrations, empty router"
```

---

## Task 7: Ingest handler (TDD via integration test)

**Files:**
- Create: `src/routes/ingest.rs`
- Modify: `src/routes/mod.rs`
- Create: `tests/http.rs`

- [ ] **Step 1: Create `tests/http.rs` with the failing test**

```rust
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;
use webhook_listener::db;
use webhook_listener::routes::build_router;
use webhook_listener::state::AppState;

async fn test_state() -> Arc<AppState> {
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
    })
}

#[tokio::test]
async fn ingest_post_to_existing_endpoint_stores_webhook() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Test", "").await.unwrap();
    let app = build_router(state.clone(), "u", "p");

    let req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/{}?foo=bar", endpoint.id))
        .header("content-type", "application/json")
        .header("x-test", "yes")
        .body(Body::from(r#"{"hello":"world"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 1024).await.unwrap();
    assert!(body.is_empty());

    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    let saved = db::get_webhook(&state.pool, list[0].id).await.unwrap().unwrap();
    assert_eq!(saved.method, "POST");
    assert_eq!(saved.path, format!("/webhooks/{}", endpoint.id));
    assert_eq!(saved.query, "foo=bar");
    assert_eq!(saved.body, br#"{"hello":"world"}"#.to_vec());
    assert_eq!(saved.body_size, r#"{"hello":"world"}"#.len() as i64);
    assert!(saved.headers_json.contains("x-test"));
}

#[tokio::test]
async fn ingest_to_unknown_endpoint_returns_404() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/{}", uuid::Uuid::new_v4()))
        .body(Body::from("x"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ingest_accepts_get_method_too() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Test", "").await.unwrap();
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/webhooks/{}", endpoint.id))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].method, "GET");
}
```

- [ ] **Step 2: Expose modules from a library target**

To make modules visible to integration tests, add a `lib.rs` alongside `main.rs`. Create `src/lib.rs`:

```rust
pub mod config;
pub mod db;
pub mod error;
pub mod routes;
pub mod state;
```

And update `src/main.rs` to use the library:

```rust
use anyhow::Context;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use webhook_listener::config::{CliArgs, Config};
use webhook_listener::{db, routes};
use webhook_listener::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config = Config::from_args_and_env(args).context("invalid configuration")?;

    let pool = db::open_pool(&config.db_path)
        .await
        .with_context(|| format!("opening database at {}", config.db_path))?;
    db::run_migrations(&pool).await.context("running migrations")?;

    let state = Arc::new(AppState {
        pool,
        retain_per_endpoint: config.retain_per_endpoint,
    });

    let app = routes::build_router(state, &config.dashboard_user, &config.dashboard_password);

    let addr: SocketAddr = config
        .bind
        .parse()
        .with_context(|| format!("invalid bind address: {}", config.bind))?;
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
```

Then add `[lib]` and `[[bin]]` sections to `Cargo.toml` (after `[dependencies]` block, BEFORE `[dev-dependencies]`):

```toml
[lib]
name = "webhook_listener"
path = "src/lib.rs"

[[bin]]
name = "webhook-listener"
path = "src/main.rs"
```

- [ ] **Step 3: Run the failing tests to confirm they fail**

```bash
cargo test --test http
```

Expected: tests fail (router has no `/webhooks/{id}` route yet — all return 404, including the success case).

- [ ] **Step 4: Create `src/routes/ingest.rs`**

```rust
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
```

- [ ] **Step 5: Wire ingest into the router in `src/routes/mod.rs`**

Replace `src/routes/mod.rs` with:

```rust
pub mod ingest;

use crate::state::AppState;
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

pub fn build_router(state: Arc<AppState>, _user: &str, _password: &str) -> Router {
    let public = Router::new().route("/webhooks/{endpoint_id}", any(ingest::ingest));
    public.with_state(state)
}
```

- [ ] **Step 6: Run integration tests**

```bash
cargo test --test http
```

Expected: 3 tests pass.

- [ ] **Step 7: Run all tests**

```bash
cargo test
```

Expected: 16 unit tests + 3 integration tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/lib.rs src/main.rs src/routes/mod.rs src/routes/ingest.rs tests/http.rs Cargo.toml Cargo.lock
git commit -m "Add webhook ingest endpoint with integration tests"
```

---

## Task 8: Auth-protected dashboard index page

**Files:**
- Create: `static/htmx.min.js`
- Create: `static/styles.css`
- Create: `templates/layout.html`
- Create: `templates/index.html`
- Create: `src/routes/dashboard.rs`
- Modify: `src/routes/mod.rs`
- Modify: `tests/http.rs`

- [ ] **Step 1: Download `static/htmx.min.js`**

```bash
curl -fsSL -o static/htmx.min.js https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js
ls -l static/htmx.min.js
```

Expected: file ~50 KB.

- [ ] **Step 2: Create `static/styles.css`**

```css
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, sans-serif; max-width: 1100px; margin: 1rem auto; padding: 0 1rem; color: #1c1c1c; }
header { display: flex; justify-content: space-between; align-items: baseline; border-bottom: 1px solid #ddd; padding-bottom: 0.5rem; margin-bottom: 1rem; }
header h1 { margin: 0; font-size: 1.4rem; }
a { color: #1d4ed8; text-decoration: none; }
a:hover { text-decoration: underline; }
form.inline { display: inline; }
input[type=text], input[type=password], textarea { width: 100%; padding: 0.4rem 0.6rem; border: 1px solid #ccc; border-radius: 4px; font: inherit; }
button { padding: 0.4rem 0.8rem; border: 1px solid #888; background: #f5f5f5; border-radius: 4px; cursor: pointer; font: inherit; }
button.danger { background: #fee2e2; border-color: #f87171; color: #991b1b; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 0.4rem 0.6rem; border-bottom: 1px solid #eee; text-align: left; font-size: 0.95rem; }
th { background: #f9fafb; font-weight: 600; }
.url { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.9rem; background: #f1f5f9; padding: 0.1rem 0.3rem; border-radius: 3px; }
.muted { color: #6b7280; font-size: 0.85rem; }
.endpoint-card { padding: 0.8rem 0; border-bottom: 1px solid #eee; }
.endpoint-card .actions { float: right; }
pre.body { background: #f9fafb; border: 1px solid #e5e7eb; padding: 0.6rem; border-radius: 4px; overflow-x: auto; font-size: 0.9rem; max-height: 600px; }
.kv { display: grid; grid-template-columns: 160px 1fr; gap: 0.3rem 1rem; font-size: 0.95rem; }
.kv dt { font-weight: 600; color: #4b5563; }
.kv dd { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
```

- [ ] **Step 3: Create `templates/layout.html`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{% block title %}Webhook Listener{% endblock %}</title>
<link rel="stylesheet" href="/static/styles.css">
<script src="/static/htmx.min.js"></script>
</head>
<body>
<header>
<h1><a href="/">Webhook Listener</a></h1>
<span class="muted">{% block header_extra %}{% endblock %}</span>
</header>
<main>
{% block content %}{% endblock %}
</main>
</body>
</html>
```

- [ ] **Step 4: Create `templates/index.html`**

```html
{% extends "layout.html" %}
{% block title %}Endpoints — Webhook Listener{% endblock %}
{% block content %}

<section>
  <h2>Create endpoint</h2>
  <form method="post" action="/endpoints">
    <p><label>Label<br><input type="text" name="label" required></label></p>
    <p><label>Description (optional)<br><input type="text" name="description"></label></p>
    <p><button type="submit">Create</button></p>
  </form>
</section>

<section>
  <h2>Endpoints ({{ endpoints.len() }})</h2>
  {% if endpoints.is_empty() %}
    <p class="muted">No endpoints yet. Create one above to get a webhook URL.</p>
  {% else %}
    {% for e in endpoints %}
    <div class="endpoint-card">
      <div class="actions">
        <form class="inline" method="post" action="/endpoints/{{ e.id }}/delete"
              onsubmit="return confirm('Delete endpoint and all its webhooks?');">
          <button type="submit" class="danger">Delete</button>
        </form>
      </div>
      <div><strong><a href="/endpoints/{{ e.id }}">{{ e.label }}</a></strong></div>
      <div><span class="url">{{ base_url }}/webhooks/{{ e.id }}</span></div>
      {% if !e.description.is_empty() %}<div class="muted">{{ e.description }}</div>{% endif %}
      <div class="muted">
        {{ e.webhook_count }} webhooks
        {% match e.last_received_at %}
        {% when Some with (ts) %} · last {{ ts }}
        {% when None %}
        {% endmatch %}
      </div>
    </div>
    {% endfor %}
  {% endif %}
</section>

{% endblock %}
```

- [ ] **Step 5: Create `src/routes/dashboard.rs`**

```rust
use crate::db;
use crate::error::AppError;
use crate::state::AppState;
use askama::Template;
use askama_axum::IntoResponse;
use axum::extract::{Form, State};
use axum::http::HeaderMap;
use axum::response::{Redirect, Response};
use serde::Deserialize;
use std::sync::Arc;

fn host_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string()
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
    Ok(IndexTemplate { endpoints, base_url }.into_response())
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
) -> Result<Redirect, AppError> {
    let _ = db::delete_endpoint(&state.pool, id).await?;
    Ok(Redirect::to("/"))
}
```

- [ ] **Step 6: Update `src/routes/mod.rs` to mount dashboard routes with auth and serve static files**

Replace `src/routes/mod.rs` with:

```rust
pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::routing::{any, get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::services::ServeDir;
use tower_http::validate_request::ValidateRequestHeaderLayer;

pub fn build_router(state: Arc<AppState>, user: &str, password: &str) -> Router {
    let public = Router::new()
        .route("/webhooks/{endpoint_id}", any(ingest::ingest))
        .with_state(state.clone());

    let dashboard = Router::new()
        .route("/", get(dashboard::index))
        .route("/endpoints", post(dashboard::create_endpoint))
        .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
        .nest_service("/static", ServeDir::new("static"))
        .layer(ValidateRequestHeaderLayer::basic(user, password))
        .with_state(state);

    public.merge(dashboard)
}
```

- [ ] **Step 7: Add integration tests for auth + index + create**

Append to `tests/http.rs`:

```rust
// --- Dashboard tests ---

fn auth_header(user: &str, pass: &str) -> String {
    use base64::{engine::general_purpose, Engine as _};
    let raw = format!("{user}:{pass}");
    format!("Basic {}", general_purpose::STANDARD.encode(raw))
}

#[tokio::test]
async fn dashboard_index_without_auth_returns_401() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key("www-authenticate"));
}

#[tokio::test]
async fn dashboard_index_with_bad_password_returns_401() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri("/")
        .header("authorization", auth_header("u", "wrong"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn dashboard_index_with_auth_returns_html() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri("/")
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("Webhook Listener"));
    assert!(body_str.contains("Create endpoint"));
}

#[tokio::test]
async fn create_endpoint_redirects_to_detail_and_persists() {
    let state = test_state().await;
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri("/endpoints")
        .header("authorization", auth_header("u", "p"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("label=GitHub&description=PRs"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection(), "got {}", resp.status());
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(location.starts_with("/endpoints/"), "{location}");
    let list = db::list_endpoints(&state.pool).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].label, "GitHub");
}
```

Add `base64` as a dev-dependency in `Cargo.toml`:

```toml
base64 = "0.22"
```

- [ ] **Step 8: Run tests**

```bash
cargo test
```

Expected: all tests pass (unit + 3 ingest + 4 dashboard tests).

- [ ] **Step 9: Commit**

```bash
git add static templates src/routes/mod.rs src/routes/dashboard.rs tests/http.rs Cargo.toml Cargo.lock
git commit -m "Add basic-auth dashboard index and endpoint creation"
```

---

## Task 9: Endpoint detail page + HTMX list partial

**Files:**
- Create: `templates/endpoint.html`
- Create: `templates/_list.html`
- Modify: `src/routes/dashboard.rs`
- Modify: `src/routes/mod.rs`
- Modify: `tests/http.rs`

- [ ] **Step 1: Add a date-formatting helper and a render-summary type**

In `src/routes/dashboard.rs`, add at the top:

```rust
fn fmt_ms(ms: i64) -> String {
    use time::format_description::well_known::Rfc3339;
    let nanos = (ms as i128) * 1_000_000;
    match time::OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => dt.format(&Rfc3339).unwrap_or_else(|_| ms.to_string()),
        Err(_) => ms.to_string(),
    }
}
```

Add a struct used by the templates:

```rust
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
```

- [ ] **Step 2: Add detail-page handler and partial-list handler in `src/routes/dashboard.rs`**

Add these handlers:

```rust
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
    headers: axum::http::HeaderMap,
) -> Result<Response, AppError> {
    let endpoint = db::get_endpoint(&state.pool, id).await?.ok_or(AppError::NotFound)?;
    let summaries = db::list_webhooks_for_endpoint(&state.pool, id, 250).await?;
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string();
    Ok(EndpointPageTemplate {
        endpoint,
        base_url: format!("http://{host}"),
        rows: rows(summaries),
    }
    .into_response())
}

pub async fn endpoint_list_partial(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<Response, AppError> {
    if db::get_endpoint(&state.pool, id).await?.is_none() {
        return Err(AppError::NotFound);
    }
    let summaries = db::list_webhooks_for_endpoint(&state.pool, id, 250).await?;
    Ok(ListPartialTemplate { rows: rows(summaries) }.into_response())
}

pub async fn clear_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<Redirect, AppError> {
    db::clear_endpoint(&state.pool, id).await?;
    Ok(Redirect::to(&format!("/endpoints/{id}")))
}
```

Also fix `index` to use the same `headers` approach (replace the `Host` extractor entirely so the file is consistent):

```rust
pub async fn index(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, AppError> {
    let endpoints = db::list_endpoints(&state.pool).await?;
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string();
    let base_url = format!("http://{host}");
    Ok(IndexTemplate { endpoints, base_url }.into_response())
}
```

Remove the now-unused `axum::extract::Host` import if you had added it.

Update `IndexTemplate` to also use formatted timestamps. Since the index template references `e.last_received_at` as a number, swap that for a formatted string. Replace the `IndexTemplate` and the `index` body so we provide a formatted summary:

```rust
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
    headers: axum::http::HeaderMap,
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
    Ok(IndexTemplate { endpoints, base_url }.into_response())
}
```

No change is needed in `templates/index.html` — `last_received_at` switched from `Option<i64>` to `Option<String>`, but the existing `{% match %}` block prints whichever type without modification.

- [ ] **Step 3: Create `templates/_list.html`**

```html
{% if rows.is_empty() %}
  <tr><td colspan="6" class="muted">No webhooks yet.</td></tr>
{% else %}
  {% for r in rows %}
  <tr>
    <td>{{ r.received_at }}</td>
    <td>{{ r.method }}</td>
    <td><a href="/webhooks/view/{{ r.id }}">{{ r.path }}</a></td>
    <td>{{ r.source_ip }}</td>
    <td>{{ r.body_size }} B</td>
    <td>
      <form class="inline" method="post" action="/webhooks/view/{{ r.id }}/delete"
            onsubmit="return confirm('Delete this webhook?');">
        <button type="submit" class="danger">×</button>
      </form>
    </td>
  </tr>
  {% endfor %}
{% endif %}
```

- [ ] **Step 4: Create `templates/endpoint.html`**

```html
{% extends "layout.html" %}
{% block title %}{{ endpoint.label }} — Webhook Listener{% endblock %}
{% block content %}

<p><a href="/">← Back to all endpoints</a></p>

<h2>{{ endpoint.label }}</h2>
{% if !endpoint.description.is_empty() %}<p class="muted">{{ endpoint.description }}</p>{% endif %}
<p class="url">{{ base_url }}/webhooks/{{ endpoint.id }}</p>

<form class="inline" method="post" action="/endpoints/{{ endpoint.id }}/clear"
      onsubmit="return confirm('Delete all webhooks for this endpoint?');">
  <button type="submit" class="danger">Clear all webhooks</button>
</form>

<h3>Recent webhooks (most recent 250)</h3>

<table>
  <thead>
    <tr>
      <th>Received</th><th>Method</th><th>Path</th><th>From</th><th>Size</th><th></th>
    </tr>
  </thead>
  <tbody hx-get="/endpoints/{{ endpoint.id }}/list"
         hx-trigger="every 5s"
         hx-swap="innerHTML">
    {% include "_list.html" %}
  </tbody>
</table>

{% endblock %}
```

- [ ] **Step 5: Mount the new routes in `src/routes/mod.rs`**

Update the `dashboard` router to include detail, list partial, and clear:

```rust
let dashboard = Router::new()
    .route("/", get(dashboard::index))
    .route("/endpoints", post(dashboard::create_endpoint))
    .route("/endpoints/{id}", get(dashboard::endpoint_detail))
    .route("/endpoints/{id}/list", get(dashboard::endpoint_list_partial))
    .route("/endpoints/{id}/clear", post(dashboard::clear_endpoint))
    .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
    .nest_service("/static", ServeDir::new("static"))
    .layer(ValidateRequestHeaderLayer::basic(user, password))
    .with_state(state);
```

- [ ] **Step 6: Add integration tests**

Append to `tests/http.rs`:

```rust
#[tokio::test]
async fn endpoint_detail_renders_full_page() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Hooks", "").await.unwrap();
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri(format!("/endpoints/{}", endpoint.id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let s = std::str::from_utf8(&body).unwrap();
    assert!(s.contains("<!doctype html>"));
    assert!(s.contains("Hooks"));
    assert!(s.contains(&endpoint.id.to_string()));
}

#[tokio::test]
async fn endpoint_list_partial_returns_rows_only_no_html_doctype() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "Hooks", "").await.unwrap();
    db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "",
            source_ip: "1.2.3.4",
            headers_json: "{}",
            body: b"hi",
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri(format!("/endpoints/{}/list", endpoint.id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let s = std::str::from_utf8(&body).unwrap();
    assert!(!s.contains("<!doctype"), "partial leaked full layout");
    assert!(s.contains("<tr>"));
    assert!(s.contains("1.2.3.4"));
}

#[tokio::test]
async fn endpoint_list_partial_for_unknown_endpoint_returns_404() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri(format!("/endpoints/{}/list", uuid::Uuid::new_v4()))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 7: Run all tests**

```bash
cargo test
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add templates src/routes/dashboard.rs src/routes/mod.rs tests/http.rs
git commit -m "Add endpoint detail page with HTMX list partial"
```

---

## Task 10: Endpoint clear + delete actions (tests)

**Files:**
- Modify: `tests/http.rs`

(Handlers already exist from Tasks 8 and 9. This task adds tests covering them.)

- [ ] **Step 1: Append integration tests to `tests/http.rs`**

```rust
#[tokio::test]
async fn clear_endpoint_keeps_endpoint_drops_webhooks() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "E", "").await.unwrap();
    db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "",
            source_ip: "127.0.0.1",
            headers_json: "{}",
            body: b"x",
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri(format!("/endpoints/{}/clear", endpoint.id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert!(list.is_empty());
    assert!(db::get_endpoint(&state.pool, endpoint.id).await.unwrap().is_some());
}

#[tokio::test]
async fn delete_endpoint_removes_endpoint_and_webhooks_via_cascade() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "E", "").await.unwrap();
    db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "",
            source_ip: "127.0.0.1",
            headers_json: "{}",
            body: b"x",
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri(format!("/endpoints/{}/delete", endpoint.id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert!(db::get_endpoint(&state.pool, endpoint.id).await.unwrap().is_none());
    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert!(list.is_empty());
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test
```

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add tests/http.rs
git commit -m "Test endpoint clear and delete actions"
```

---

## Task 11: Webhook detail page (with body decoding)

**Files:**
- Create: `templates/webhook_detail.html`
- Modify: `src/routes/dashboard.rs`
- Modify: `src/routes/mod.rs`
- Modify: `tests/http.rs`

- [ ] **Step 1: Add body-decoding helper and detail handler in `src/routes/dashboard.rs`**

Add after the existing helpers:

```rust
struct DecodedBody {
    pretty: Option<String>, // pretty-printed JSON if applicable
    text: Option<String>,   // raw UTF-8 text
    hex: Option<String>,    // hex preview if non-UTF-8
    label: String,          // human-readable category for the heading
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
            if is_json {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                        return DecodedBody {
                            pretty: Some(pretty),
                            text: None,
                            hex: None,
                            label: "JSON".into(),
                        };
                    }
                }
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
    Ok(WebhookDetailTemplate {
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
    }
    .into_response())
}
```

- [ ] **Step 2: Create `templates/webhook_detail.html`**

```html
{% extends "layout.html" %}
{% block title %}Webhook #{{ webhook_id }} — Webhook Listener{% endblock %}
{% block content %}

<p><a href="/endpoints/{{ endpoint_id }}">← Back to {{ endpoint_label }}</a></p>

<h2>Webhook #{{ webhook_id }}</h2>

<dl class="kv">
  <dt>Received</dt><dd>{{ received_at }}</dd>
  <dt>Method</dt><dd>{{ method }}</dd>
  <dt>Path</dt><dd>{{ path }}</dd>
  <dt>Query</dt><dd>{% if query.is_empty() %}<span class="muted">(none)</span>{% else %}{{ query }}{% endif %}</dd>
  <dt>Source IP</dt><dd>{{ source_ip }}</dd>
  <dt>Body size</dt><dd>{{ body_size }} bytes</dd>
</dl>

<h3>Headers</h3>
{% if headers.is_empty() %}
  <p class="muted">(none)</p>
{% else %}
  <dl class="kv">
  {% for h in headers %}
    <dt>{{ h.0 }}</dt><dd>{{ h.1 }}</dd>
  {% endfor %}
  </dl>
{% endif %}

<h3>Body — {{ body.label }}</h3>
{% match body.pretty %}
{% when Some with (s) %}<pre class="body">{{ s }}</pre>
{% when None %}{% endmatch %}
{% match body.text %}
{% when Some with (s) %}<pre class="body">{{ s }}</pre>
{% when None %}{% endmatch %}
{% match body.hex %}
{% when Some with (s) %}<pre class="body">{{ s }}</pre>
{% when None %}{% endmatch %}

<form class="inline" method="post" action="/webhooks/view/{{ webhook_id }}/delete"
      onsubmit="return confirm('Delete this webhook?');">
  <button type="submit" class="danger">Delete this webhook</button>
</form>

{% endblock %}
```

- [ ] **Step 3: Mount the route in `src/routes/mod.rs`**

Add to the dashboard router:

```rust
.route("/webhooks/view/{id}", get(dashboard::webhook_detail))
```

- [ ] **Step 4: Add tests**

Append to `tests/http.rs`:

```rust
#[tokio::test]
async fn webhook_detail_renders_with_pretty_json_body() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "E", "").await.unwrap();
    let id = db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "k=v",
            source_ip: "1.2.3.4",
            headers_json: r#"{"content-type":["application/json"]}"#,
            body: br#"{"a":1,"b":[2,3]}"#,
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri(format!("/webhooks/view/{}", id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let s = std::str::from_utf8(&body).unwrap();
    assert!(s.contains("Webhook #"));
    assert!(s.contains("\"a\": 1"), "expected pretty-printed JSON: {s}");
    assert!(s.contains("content-type"));
    assert!(s.contains("k=v"));
}

#[tokio::test]
async fn webhook_detail_falls_back_to_hex_for_non_utf8_body() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "E", "").await.unwrap();
    let id = db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "",
            source_ip: "1.2.3.4",
            headers_json: "{}",
            body: &[0xff, 0xfe, 0xfd],
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri(format!("/webhooks/view/{}", id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 65536).await.unwrap();
    let s = std::str::from_utf8(&body).unwrap();
    assert!(s.contains("ff fe fd"), "expected hex output: {s}");
    assert!(s.contains("binary"));
}

#[tokio::test]
async fn webhook_detail_for_unknown_id_returns_404() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .uri("/webhooks/view/999999")
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add templates/webhook_detail.html src/routes/dashboard.rs src/routes/mod.rs tests/http.rs
git commit -m "Add webhook detail page with body decoding"
```

---

## Task 12: Single-webhook delete

**Files:**
- Modify: `src/routes/dashboard.rs`
- Modify: `src/routes/mod.rs`
- Modify: `tests/http.rs`

- [ ] **Step 1: Add the delete handler in `src/routes/dashboard.rs`**

```rust
pub async fn delete_webhook(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Redirect, AppError> {
    match db::delete_webhook(&state.pool, id).await? {
        Some(endpoint_id) => Ok(Redirect::to(&format!("/endpoints/{endpoint_id}"))),
        None => Err(AppError::NotFound),
    }
}
```

- [ ] **Step 2: Mount the route in `src/routes/mod.rs`**

Add to the dashboard router:

```rust
.route("/webhooks/view/{id}/delete", post(dashboard::delete_webhook))
```

- [ ] **Step 3: Add the test**

Append to `tests/http.rs`:

```rust
#[tokio::test]
async fn delete_single_webhook_redirects_to_endpoint_detail() {
    let state = test_state().await;
    let endpoint = db::create_endpoint(&state.pool, "E", "").await.unwrap();
    let id = db::insert_webhook(
        &state.pool,
        &db::NewWebhook {
            endpoint_id: endpoint.id,
            received_at: 1,
            method: "POST",
            path: "/webhooks/x",
            query: "",
            source_ip: "127.0.0.1",
            headers_json: "{}",
            body: b"x",
        },
        250,
    )
    .await
    .unwrap();
    let app = build_router(state.clone(), "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/view/{}/delete", id))
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert_eq!(location, format!("/endpoints/{}", endpoint.id));
    let list = db::list_webhooks_for_endpoint(&state.pool, endpoint.id, 1000)
        .await
        .unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
async fn delete_unknown_webhook_returns_404() {
    let state = test_state().await;
    let app = build_router(state, "u", "p");
    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/view/999999/delete")
        .header("authorization", auth_header("u", "p"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/routes/dashboard.rs src/routes/mod.rs tests/http.rs
git commit -m "Add single-webhook delete handler"
```

---

## Task 13: Outer middleware (trace, body limit, panic catch) + graceful shutdown

**Files:**
- Modify: `src/routes/mod.rs`
- Modify: `src/main.rs`
- Modify: `src/state.rs`

- [ ] **Step 1: Plumb body limit into `AppState` so the router can apply it**

Replace `src/state.rs`:

```rust
use sqlx::SqlitePool;

pub struct AppState {
    pub pool: SqlitePool,
    pub retain_per_endpoint: i64,
    pub body_limit_bytes: usize,
}
```

Update both places that construct `AppState`:

In `src/main.rs`:

```rust
let state = Arc::new(AppState {
    pool,
    retain_per_endpoint: config.retain_per_endpoint,
    body_limit_bytes: config.body_limit_bytes,
});
```

In `tests/http.rs` `test_state()`:

```rust
async fn test_state() -> Arc<AppState> {
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
        body_limit_bytes: 1_048_576,
    })
}
```

- [ ] **Step 2: Apply outer middleware in `src/routes/mod.rs`**

Update `build_router` to wrap the merged router with trace, panic catch, and body limit:

```rust
pub mod dashboard;
pub mod ingest;

use crate::state::AppState;
use axum::routing::{any, get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tower_http::validate_request::ValidateRequestHeaderLayer;

pub fn build_router(state: Arc<AppState>, user: &str, password: &str) -> Router {
    let body_limit = state.body_limit_bytes;

    let public = Router::new()
        .route("/webhooks/{endpoint_id}", any(ingest::ingest))
        .with_state(state.clone());

    let dashboard = Router::new()
        .route("/", get(dashboard::index))
        .route("/endpoints", post(dashboard::create_endpoint))
        .route("/endpoints/{id}", get(dashboard::endpoint_detail))
        .route("/endpoints/{id}/list", get(dashboard::endpoint_list_partial))
        .route("/endpoints/{id}/clear", post(dashboard::clear_endpoint))
        .route("/endpoints/{id}/delete", post(dashboard::delete_endpoint))
        .route("/webhooks/view/{id}", get(dashboard::webhook_detail))
        .route("/webhooks/view/{id}/delete", post(dashboard::delete_webhook))
        .nest_service("/static", ServeDir::new("static"))
        .layer(ValidateRequestHeaderLayer::basic(user, password))
        .with_state(state);

    public
        .merge(dashboard)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
}
```

- [ ] **Step 3: Add graceful shutdown in `src/main.rs`**

Replace the `axum::serve(...).await?` block with:

```rust
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ... unchanged setup ...
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}
```

The full updated `src/main.rs`:

```rust
use anyhow::Context;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use webhook_listener::config::{CliArgs, Config};
use webhook_listener::state::AppState;
use webhook_listener::{db, routes};

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config = Config::from_args_and_env(args).context("invalid configuration")?;

    let pool = db::open_pool(&config.db_path)
        .await
        .with_context(|| format!("opening database at {}", config.db_path))?;
    db::run_migrations(&pool).await.context("running migrations")?;

    let state = Arc::new(AppState {
        pool,
        retain_per_endpoint: config.retain_per_endpoint,
        body_limit_bytes: config.body_limit_bytes,
    });

    let app = routes::build_router(state, &config.dashboard_user, &config.dashboard_password);

    let addr: SocketAddr = config
        .bind
        .parse()
        .with_context(|| format!("invalid bind address: {}", config.bind))?;
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}
```

- [ ] **Step 4: Add an integration test for the body-size limit**

Append to `tests/http.rs`:

```rust
#[tokio::test]
async fn ingest_body_over_limit_returns_413() {
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let small_state = Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
        body_limit_bytes: 64,
    });
    let endpoint = db::create_endpoint(&small_state.pool, "E", "").await.unwrap();
    let app = build_router(small_state, "u", "p");
    let big = vec![b'x'; 200];
    let req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/{}", endpoint.id))
        .body(Body::from(big))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs src/routes/mod.rs src/state.rs tests/http.rs
git commit -m "Add outer middleware stack and graceful shutdown"
```

---

## Task 14: End-to-end sanity test

**Files:**
- Create: `tests/e2e.rs`

- [ ] **Step 1: Create `tests/e2e.rs`**

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use webhook_listener::db;
use webhook_listener::routes::build_router;
use webhook_listener::state::AppState;

#[tokio::test]
async fn end_to_end_create_endpoint_send_webhook_observe_in_list() {
    // Start the same router the binary uses, on an OS-assigned port.
    let pool = db::open_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let state = Arc::new(AppState {
        pool,
        retain_per_endpoint: 250,
        body_limit_bytes: 1_048_576,
    });
    let app = build_router(state.clone(), "u", "p");

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

    // Give the server a moment to be ready.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let base = format!("http://{addr}");

    // 1. Create endpoint via dashboard form (with auth).
    let resp = client
        .post(format!("{base}/endpoints"))
        .basic_auth("u", Some("p"))
        .form(&[("label", "E2E"), ("description", "test")])
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_redirection(), "got {}", resp.status());
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    let id = location.trim_start_matches("/endpoints/").to_string();

    // 2. Send a webhook (no auth).
    let resp = client
        .post(format!("{base}/webhooks/{id}?run=42"))
        .header("X-Custom", "hello")
        .json(&serde_json::json!({"event": "ping"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "got {}", resp.status());

    // 3. Poll the partial endpoint until the row appears.
    let partial_url = format!("{base}/endpoints/{id}/list");
    let mut found = false;
    for _ in 0..20 {
        let r = client
            .get(&partial_url)
            .basic_auth("u", Some("p"))
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
```

- [ ] **Step 2: Run the e2e test**

```bash
cargo test --test e2e
```

Expected: passes.

- [ ] **Step 3: Run full test suite**

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e.rs
git commit -m "Add end-to-end sanity test for full ingest + dashboard flow"
```

---

## Task 15: Lint, format, manual smoke test

**Files:**
- Modify: any with clippy-flagged issues

- [ ] **Step 1: Format the code**

```bash
cargo fmt
```

- [ ] **Step 2: Run clippy with warnings denied**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: no warnings. If clippy flags something, fix it (the most likely items are unused imports, redundant clones, or `&str` vs `String` choices).

- [ ] **Step 3: Manual smoke test**

```bash
cargo build --release
WEBHOOK_DASHBOARD_USER=admin WEBHOOK_DASHBOARD_PASSWORD=hunter2 \
  ./target/release/webhook-listener --bind 127.0.0.1:8080 --db-path /tmp/wh-smoke.db &
SERVER_PID=$!
sleep 1

# Open dashboard (will prompt for auth)
echo "Visit http://127.0.0.1:8080/ with admin / hunter2"
echo "Then create an endpoint and:"
echo "  curl -X POST -H 'Content-Type: application/json' -d '{\"hi\":1}' http://127.0.0.1:8080/webhooks/<UUID>"
echo "Press Enter when done."
read

kill $SERVER_PID
rm -f /tmp/wh-smoke.db /tmp/wh-smoke.db-shm /tmp/wh-smoke.db-wal
```

- [ ] **Step 4: Commit any fmt/clippy fixes**

```bash
git status
# If anything changed:
git add -u
git commit -m "Apply cargo fmt and clippy fixes"
```

---

## Definition of done

- `cargo test` passes (unit tests in `src/db.rs` and `src/config.rs`, integration in `tests/http.rs`, e2e in `tests/e2e.rs`).
- `cargo clippy --all-targets --all-features -- -D warnings` passes.
- `cargo fmt --check` passes.
- Manual smoke test from Task 15 confirms: dashboard auth works, endpoint creation works, sending a webhook lands in the dashboard within 5 seconds (HTMX poll).
