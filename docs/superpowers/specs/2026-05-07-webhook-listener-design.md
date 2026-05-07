# Webhook Listener — Design

**Date:** 2026-05-07
**Status:** Approved (pending implementation plan)

## Goal

A small, self-hosted Rust HTTP service that captures incoming webhooks at user-created UUID-keyed endpoints (e.g. `/webhooks/4e4d03c2-8168-4514-a2ca-880e40ccc711`) and presents them on a basic-auth-protected dashboard. SQLite for storage. Targeted at low-traffic local testing — not production grade.

## Out of scope

- HTTPS termination (run behind a reverse proxy if needed).
- Multi-user accounts, password hashing, account management.
- Webhook replay or forwarding.
- Webhook signature verification.
- Configurable per-endpoint response codes/bodies.
- Time-based retention or unbounded retention.
- Browser/UI end-to-end tests.
- Load and fuzz testing.
- `.env` file loading.
- CLI for endpoint management (dashboard only).

## Stack

| Concern | Choice |
|---------|--------|
| HTTP framework | `axum` |
| Async runtime | `tokio` |
| DB driver | `sqlx` with `sqlite` + `runtime-tokio-rustls`, `migrate` features |
| Templates | `askama` (compile-time HTML) |
| Live updates | `htmx` (one `<script>` tag, polled partial) |
| Middleware | `tower-http` (`TraceLayer`, `RequestBodyLimitLayer`, `ValidateRequestHeaderLayer::basic`, `CatchPanicLayer`, `ServeDir`) |
| Logging | `tracing` + `tracing-subscriber` |
| Serde | `serde`, `serde_json` |
| UUID | `uuid` (`v4`, `serde`) |
| Time | `time` (with `serde`, `formatting`) |
| CLI | `clap` (derive) |
| Errors | `thiserror` for `AppError`, `anyhow` only in `main()` for startup |

## Functional behavior

### Endpoints

- Endpoints are user-created, identified by a UUID v4.
- Created via dashboard form submission (`POST /endpoints`) with `label` (required) and `description` (optional).
- Listed on the dashboard index with their full receive URL, webhook count, and last-received timestamp.
- Deletable via dashboard (cascades to webhooks).

### Webhook capture

- Single handler bound to `ANY /webhooks/{endpoint_id}` — accepts all HTTP methods (some providers use GET for verification handshakes).
- Captures, per request:
  - `received_at` (unix epoch milliseconds, UTC)
  - HTTP method
  - Full request path (the literal `/webhooks/<uuid>`)
  - Query string (raw; empty if absent)
  - Source IP (from `ConnectInfo<SocketAddr>`; documented as proxy-IP if behind reverse proxy)
  - All request headers as JSON: `{ "header-name": ["value-1", "value-2"], ... }`. Non-UTF-8 header values are replaced with the literal string `<binary>`.
  - Raw request body bytes (BLOB), and `body_size` denormalized for fast list rendering.
- Returns `200 OK` with empty body on success.
- Returns `404 Not Found` if `endpoint_id` doesn't match an existing endpoint.
- Returns `413 Payload Too Large` (auto, via middleware) if body exceeds size limit.
- No webhook signature verification of any kind.

### Retention

- Hard cap of 250 webhooks per endpoint (configurable via `WEBHOOK_RETAIN_PER_ENDPOINT`).
- Oldest evicted on insert, in the same DB transaction as the insert. Tie-breaker on `id DESC` to be deterministic when multiple webhooks land in the same millisecond.
- Manual deletion supported in the dashboard at three levels: single webhook, clear-all-for-endpoint, and delete endpoint (cascades).

### Dashboard

- All dashboard pages and partials require HTTP basic auth.
- Credentials supplied exclusively via env vars `WEBHOOK_DASHBOARD_USER` and `WEBHOOK_DASHBOARD_PASSWORD`. Missing or empty → process exits with a clear error before binding the port.
- Pages:
  1. `GET /` — endpoint list + create-endpoint form.
  2. `GET /endpoints/{id}` — detail page; live-polled `<tbody>` of recent webhooks.
  3. `GET /webhooks/view/{webhook_id}` — webhook detail (timestamp, method, path, query, source IP, headers, body).
- Live updates via HTMX polling: the table body has `hx-get="/endpoints/{id}/list" hx-trigger="every 5s, load" hx-swap="innerHTML"`.
- Static assets (`htmx.min.js`, `styles.css`) served via `tower_http::services::ServeDir` from `static/`.
- Body display:
  - If valid UTF-8: render text. If `Content-Type` indicates JSON, attempt `serde_json` pretty-print on success; fall back to raw text.
  - If non-UTF-8: show the first 4 KiB as hex.

## Architecture

Single binary, single tokio runtime, single SQLite file. Two logical surfaces sharing one `Arc<AppState>`:

```
                   ┌──────────────────────────┐
                   │ Outer middleware:         │
                   │  TraceLayer               │
                   │  CatchPanicLayer          │
                   │  RequestBodyLimitLayer    │
                   └─────────────┬─────────────┘
                                 │
                ┌────────────────┴────────────────┐
                ▼                                  ▼
   ANY /webhooks/{uuid}           Dashboard branch (basic-auth layer)
   (no auth)                       GET  /
                                   POST /endpoints
                                   GET  /endpoints/{id}
                                   GET  /endpoints/{id}/list
                                   POST /endpoints/{id}/clear
                                   POST /endpoints/{id}/delete
                                   GET  /webhooks/view/{webhook_id}
                                   POST /webhooks/view/{webhook_id}/delete
                                   GET  /static/*file
                                 │
                                 ▼
                       AppState { SqlitePool, retain_per_endpoint }
```

### Module layout

```
src/
  main.rs        # entrypoint: tracing init, config parse, migrate, build router, serve
  config.rs      # clap + env parsing → Config struct
  state.rs       # AppState
  db.rs          # pool setup, migrations, queries (create/insert/list/delete/evict)
  routes/
    mod.rs       # build_router(state) -> Router
    ingest.rs    # ANY /webhooks/{id}
    dashboard.rs # all dashboard handlers
  templates/     # askama: layout, index, endpoint, webhook_detail, _list (partial)
  static/        # htmx.min.js, styles.css
  error.rs       # AppError + IntoResponse
migrations/
  0001_init.sql
tests/
  http.rs        # integration tests via tower::ServiceExt::oneshot
  e2e.rs         # binary-on-127.0.0.1:0 sanity test
```

## Data model

SQLite, WAL mode, foreign keys on.

```sql
CREATE TABLE endpoints (
    id           TEXT PRIMARY KEY,    -- UUID v4 string
    label        TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL     -- unix epoch seconds
);

CREATE TABLE webhooks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_id  TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    received_at  INTEGER NOT NULL,    -- unix epoch milliseconds
    method       TEXT NOT NULL,
    path         TEXT NOT NULL,       -- full path including the UUID
    query        TEXT NOT NULL DEFAULT '',
    source_ip    TEXT NOT NULL,
    headers      TEXT NOT NULL,       -- JSON object {name: [values...]}
    body         BLOB NOT NULL,
    body_size    INTEGER NOT NULL
);

CREATE INDEX webhooks_endpoint_received
    ON webhooks(endpoint_id, received_at DESC);
```

### Insert + evict (single transaction)

```sql
BEGIN;
INSERT INTO webhooks (endpoint_id, received_at, method, path, query,
                      source_ip, headers, body, body_size)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
DELETE FROM webhooks
WHERE endpoint_id = ?
  AND id NOT IN (
      SELECT id FROM webhooks
      WHERE endpoint_id = ?
      ORDER BY received_at DESC, id DESC
      LIMIT ?    -- retain_per_endpoint
  );
COMMIT;
```

## Routes

### Public (no auth)

| Method | Path                      | Behavior |
|--------|---------------------------|----------|
| ANY    | `/webhooks/{endpoint_id}` | Validate endpoint, capture, evict. 200 OK empty body on success; 404 if endpoint missing; 413 if body over limit. |

### Dashboard (basic-auth)

| Method | Path                                  | Behavior |
|--------|---------------------------------------|----------|
| GET    | `/`                                   | Render index.html with endpoints + create form. |
| POST   | `/endpoints`                          | `application/x-www-form-urlencoded` body: `label`, optional `description`. Generate UUID v4. 303 → `/endpoints/{id}`. |
| GET    | `/endpoints/{id}`                     | Render endpoint.html (URL, controls, initial webhook table). |
| GET    | `/endpoints/{id}/list`                | Render `_list.html` partial (rows only). HTMX target. |
| POST   | `/endpoints/{id}/clear`               | Delete all webhooks for endpoint. 303 → `/endpoints/{id}`. |
| POST   | `/endpoints/{id}/delete`              | Delete endpoint (cascades). 303 → `/`. |
| GET    | `/webhooks/view/{webhook_id}`         | Render webhook_detail.html. |
| POST   | `/webhooks/view/{webhook_id}/delete`  | Delete one webhook. 303 → `/endpoints/{endpoint_id}`. |
| GET    | `/static/*file`                       | `ServeDir("static")`. |

Note on path namespace: receive endpoints are `/webhooks/{uuid}`; dashboard webhook detail is `/webhooks/view/{id}`. The `view/` literal segment makes them unambiguous and preserves the requested receive URL format.

### Middleware stack

Outer to inner (applied to the merged router):
1. `TraceLayer::new_for_http()` — request/response logs.
2. `CatchPanicLayer::new()` — panics → 500.
3. `RequestBodyLimitLayer::new(body_limit_bytes)` — global body cap.
4. `ValidateRequestHeaderLayer::basic(user, password)` — applied only to the dashboard sub-router.

## Configuration

CLI args (clap) take precedence over env vars. Credentials are required.

| Setting              | CLI flag    | Env var                       | Default          | Required |
|----------------------|-------------|-------------------------------|------------------|----------|
| Bind address         | `--bind`    | `WEBHOOK_BIND`                | `127.0.0.1:8080` | no       |
| SQLite file path     | `--db-path` | `WEBHOOK_DB_PATH`             | `webhooks.db`    | no       |
| Body size limit (B)  | —           | `WEBHOOK_BODY_LIMIT_BYTES`    | `1048576`        | no       |
| Per-endpoint cap     | —           | `WEBHOOK_RETAIN_PER_ENDPOINT` | `250`            | no       |
| Dashboard username   | —           | `WEBHOOK_DASHBOARD_USER`      | —                | **yes**  |
| Dashboard password   | —           | `WEBHOOK_DASHBOARD_PASSWORD`  | —                | **yes**  |
| Log filter           | —           | `RUST_LOG`                    | `info`           | no       |

No `.env` file loading — env vars must be set in the shell or by the launcher.

## Startup sequence (`main.rs`)

1. `tracing_subscriber` init (reads `RUST_LOG`).
2. Parse `Config` from clap + env. Fail fast if credentials missing.
3. Open `SqlitePool` with `SqliteConnectOptions`:
   - `filename(config.db_path)`
   - `create_if_missing(true)`
   - `journal_mode(Wal)`
   - `synchronous(Normal)`
   - `foreign_keys(true)`
   - `busy_timeout(Duration::from_secs(5))`.
4. `sqlx::migrate!("./migrations").run(&pool).await?`.
5. Build `Arc<AppState>` and the router.
6. Bind TCP listener; run `axum::serve(...).with_graceful_shutdown(ctrl_c)`.

## Error handling

Single typed error in handler code; `anyhow` only in `main()` for startup.

```rust
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
```

`IntoResponse` mapping:

| Variant     | Status | Notes |
|-------------|--------|-------|
| `NotFound`  | 404    |       |
| `BadRequest`| 400    | Includes `msg`. |
| `Database`  | 500    | Logged at `error`; client gets generic message. |
| `Template`  | 500    | Logged at `error`; client gets generic message. |
| `Internal`  | 500    | Logged at `error`; client gets generic message. |

External middleware mappings:
- Body too large → 413 (`RequestBodyLimitLayer`).
- Missing/invalid basic auth → 401 with `WWW-Authenticate` (`ValidateRequestHeaderLayer::basic`).
- Panic in handler → 500 (`CatchPanicLayer`).

## Concurrency

Per the domain-web rule "shared state must be thread-safe":
- `AppState` is shared as `Arc<AppState>` through axum's `State` extractor.
- `SqlitePool` is `Send + Sync`; cloning the `Arc` does not clone the pool.
- All DB calls go through the pool — SQLite WAL mode allows concurrent readers and a single writer, which fits this workload.

## Logging

- One line per request via `TraceLayer` at `info`.
- Webhook ingest logs at `info` with `endpoint_id`, `method`, `body_size`, `source_ip`.
- Dashboard handlers log at `debug`.
- `AppError` variants log at `error` (DB / template / internal) before being returned.

## Testing

### Unit tests (`db.rs`)

In-memory SQLite per test (`sqlite::memory:`), migrations applied each time:

- `create_endpoint` → `get_endpoint` round-trips fields.
- `insert_webhook` eviction:
  - Below cap → keeps all.
  - At cap+1 → keeps cap, oldest dropped.
  - Multiple endpoints → eviction scoped per `endpoint_id`.
  - Same-millisecond ties resolved by `id DESC`.
- `delete_endpoint` cascades.
- `clear_endpoint` keeps endpoint, removes webhooks.

### Integration tests (`tests/http.rs`)

Full router + in-memory pool, requests via `tower::ServiceExt::oneshot`:

- `POST /webhooks/{valid}` → 200, row landed correctly (method/headers/body).
- `POST /webhooks/{nonexistent}` → 404.
- Body over limit → 413.
- `GET /` no auth → 401 with `WWW-Authenticate`.
- `GET /` valid auth → 200 with expected content.
- `GET /` bad password → 401.
- `POST /endpoints` form → 303 to `/endpoints/{id}`; row in DB.
- `GET /endpoints/{id}/list` returns just rows (no `<html>` doctype).
- `POST /endpoints/{id}/clear` empties webhooks, endpoint stays.
- `POST /endpoints/{id}/delete` removes endpoint and webhooks.

### Sanity test (`tests/e2e.rs`)

Spawns the binary on `127.0.0.1:0`, creates an endpoint with auth, posts a webhook via `reqwest`, polls `/endpoints/{id}/list` until the row appears, asserts headers/body match. Catches askama template path / sqlx migration discovery issues that unit tests miss.

### Tooling

- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --check`

## Known limitations / explicitly accepted

- Source IP is the immediate peer's IP. Behind a reverse proxy this is the proxy's IP. Not parsed from `X-Forwarded-For`.
- No HTTPS — bind to localhost or terminate TLS upstream.
- Body cap is global, not per-endpoint.
- Header values that are non-UTF-8 are replaced (not preserved as bytes).
- Single global retention cap — not configurable per endpoint.
- Dashboard form posts use simple `confirm()` for delete actions; no CSRF tokens (basic-auth + localhost is the threat model).
