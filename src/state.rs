#![allow(dead_code)]

use sqlx::SqlitePool;

pub struct AppState {
    pub pool: SqlitePool,
    pub retain_per_endpoint: i64,
}
