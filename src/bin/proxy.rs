//! Caching Range-aware proxy entry point.
//!
//! Env:
//!   UPSTREAM_BASE  e.g. http://127.0.0.1:9000/   (loopback only, must end /)
//!   LISTEN_ADDR    e.g. 127.0.0.1:8000           (default 127.0.0.1:8000)
//!   CACHE_DIR      e.g. ./cache-data
//!   ALLOW_NON_LOOPBACK=1 to disable the loopback-only guard (discouraged)

use range_cache_proxy::ProxyConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,range_cache_proxy=debug".parse().unwrap()),
        )
        .init();

    let upstream_base = std::env::var("UPSTREAM_BASE")
        .map_err(|_| anyhow::anyhow!("UPSTREAM_BASE is required"))?;
    let listen = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".into());
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or_else(|_| "./cache-data".into());

    let mut config = ProxyConfig::new(upstream_base, cache_dir);
    if std::env::var("ALLOW_NON_LOOPBACK").as_deref() == Ok("1") {
        config.require_loopback_upstream = false;
    }

    let (app, state) = range_cache_proxy::build_app(config).await?;
    tracing::info!(upstream = %state.config.upstream_base, "proxy starting");

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(%listen, "listening");
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
