use sqlx::SqlitePool;

pub struct AppState {
    pub pool: SqlitePool,
    pub retain_per_endpoint: i64,
    pub body_limit_bytes: usize,
    pub session_token: String,
    pub dashboard_user: String,
    pub dashboard_password: String,
}
