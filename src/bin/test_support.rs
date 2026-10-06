//! Standalone launcher for the local test upstream.
//!
//! Requires the `test-support` feature:
//!   cargo run --features test-support --bin test-support -- 127.0.0.1:9000

#[cfg(feature = "test-support")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    if !listen.starts_with("127.0.0.1") && !listen.starts_with("localhost") {
        anyhow::bail!("the test upstream must only bind loopback: {listen}");
    }
    let app = range_cache_proxy::support::app();
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    eprintln!("test upstream listening on http://{listen}/");
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

#[cfg(not(feature = "test-support"))]
fn main() {
    eprintln!("rebuild with --features test-support");
}
