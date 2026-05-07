#![allow(dead_code)]

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
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
    sqlx::query("INSERT INTO endpoints (id, label, description, created_at) VALUES (?, ?, ?, ?)")
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
    let row: Option<(String, String, String, i64)> =
        sqlx::query_as("SELECT id, label, description, created_at FROM endpoints WHERE id = ?")
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
        .map(
            |(id, received_at, method, path, source_ip, body_size)| WebhookSummary {
                id,
                received_at,
                method,
                path,
                source_ip,
                body_size,
            },
        )
        .collect())
}

pub async fn get_webhook(pool: &SqlitePool, id: i64) -> Result<Option<Webhook>, sqlx::Error> {
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, String, i64, String, String, String, String, String, Vec<u8>, i64)> =
        sqlx::query_as(
            "SELECT id, endpoint_id, received_at, method, path, query, source_ip, headers, body, body_size
             FROM webhooks WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;
    row.map(
        |(
            id,
            endpoint_id,
            received_at,
            method,
            path,
            query,
            source_ip,
            headers_json,
            body,
            body_size,
        )| {
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
        },
    )
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
}
