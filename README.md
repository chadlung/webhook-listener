# webhook-listener

A self-hosted webhook capture and inspection tool built with Rust. Create named endpoints, send HTTP requests to them from any source, and inspect the captured payloads through a browser dashboard.

## How it works

```
Sender → POST /webhooks/{endpoint-id} → stored in SQLite → dashboard UI
```

**Ingest path** — any HTTP method to `/webhooks/{endpoint-id}` is accepted and stored: method, path, query string, source IP, headers (as JSON), and raw body. No authentication required on this path. A per-endpoint cap evicts the oldest webhooks once the limit is reached.

**Dashboard** — a server-rendered HTML UI (Askama templates + htmx for live polling) protected by HTTP Basic Auth. From it you can:

- Create and delete named endpoints
- View the webhook list for an endpoint, with auto-refresh via htmx polling
- Inspect individual webhook payloads — pretty-printed JSON, plain text, or hex for binary bodies
- Clear all webhooks on an endpoint, or delete individual ones

**Storage** — SQLite with WAL mode. Migrations run automatically on startup via sqlx-migrate.

## Prerequisites

- Rust (edition 2024, stable)
- No external database required — SQLite is embedded

## Configuration

Two CLI flags (also settable as env vars):

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--bind` | `WEBHOOK_BIND` | `127.0.0.1:8080` | Listen address |
| `--db-path` | `WEBHOOK_DB_PATH` | `webhooks.db` | SQLite file path |

Required environment variables (no defaults — server refuses to start without them):

| Env var | Description |
|---------|-------------|
| `WEBHOOK_DASHBOARD_USER` | HTTP Basic Auth username for the dashboard |
| `WEBHOOK_DASHBOARD_PASSWORD` | HTTP Basic Auth password for the dashboard |

Optional environment variables:

| Env var | Default | Description |
|---------|---------|-------------|
| `WEBHOOK_BODY_LIMIT_BYTES` | `1048576` (1 MiB) | Maximum request body size |
| `WEBHOOK_RETAIN_PER_ENDPOINT` | `250` | Max webhooks kept per endpoint (oldest evicted) |

Log level is controlled via the standard `RUST_LOG` env var (e.g. `RUST_LOG=debug`).

## Running

```bash
export WEBHOOK_DASHBOARD_USER=admin
export WEBHOOK_DASHBOARD_PASSWORD=secret

cargo run --release
```

Then open `http://127.0.0.1:8080` in a browser and log in with the credentials above.

Create an endpoint in the dashboard, then send webhooks to it:

```bash
curl -X POST http://127.0.0.1:8080/webhooks/<endpoint-id> \
  -H 'Content-Type: application/json' \
  -d '{"event": "push", "repo": "my-repo"}'
```

## Running tests

```bash
cargo test
```

The test suite includes unit tests for the config, database layer, and HTTP handlers, plus an end-to-end test that exercises the full ingest → dashboard flow against an in-memory SQLite database.

## Project layout

```
src/
  main.rs          — startup: config, DB pool, router, graceful shutdown
  config.rs        — CLI args + env-var config with validation
  db.rs            — SQLite queries and migration runner
  error.rs         — AppError type mapping to HTTP responses
  state.rs         — shared AppState (pool + runtime config)
  routes/
    mod.rs         — router construction, middleware stack
    ingest.rs      — webhook capture handler
    dashboard.rs   — dashboard handlers (index, endpoint detail, webhook detail)
templates/         — Askama HTML templates
static/            — htmx and CSS served at /static
migrations/        — sqlx migration SQL files
tests/
  http.rs          — handler-level integration tests
  e2e.rs           — full stack end-to-end test
```

## Tech stack

| Crate | Role |
|-------|------|
| axum 0.8 | HTTP server and routing |
| sqlx 0.8 (SQLite) | Async database access and migrations |
| askama 0.12 | Compile-time HTML templating |
| htmx | Live dashboard updates without a JS build step |
| tower-http | Tracing, body size limit, Basic Auth, static files, panic recovery |
| clap 4 | CLI argument parsing |
| tracing | Structured logging |
| tokio | Async runtime |
