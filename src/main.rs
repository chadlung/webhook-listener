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
