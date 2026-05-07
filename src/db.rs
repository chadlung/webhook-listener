#![allow(dead_code)]

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
         GROUP BY e.id, e.label, e.description, e.created_at
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
