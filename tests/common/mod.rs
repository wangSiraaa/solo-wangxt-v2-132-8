//! Integration test harness: spawns the test upstream and the proxy on
//! loopback ephemeral ports.

#![cfg(feature = "test-support")]

use std::net::SocketAddr;

use range_cache_proxy::support;
use range_cache_proxy::ProxyConfig;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

pub struct Env {
    pub client: reqwest::Client,
    pub proxy: String,
    /// Base URL WITHOUT trailing slash (e.g. http://127.0.0.1:41234).
    pub upstream: String,
    #[allow(dead_code)]
    upstream_base: String,
    #[allow(dead_code)]
    pub cache_dir: TempDir,
}

pub async fn spawn_env() -> Env {
    let upstream_base = support::spawn().await; // ends with '/'
    let upstream = upstream_base.trim_end_matches('/').to_string();
    let cache_dir = TempDir::new().unwrap();
    let config = ProxyConfig::new(upstream_base.clone(), cache_dir.path().to_path_buf());
    let (app, _) = range_cache_proxy::build_app(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    Env {
        client,
        proxy: format!("http://{addr}"),
        upstream,
        upstream_base,
        cache_dir,
    }
}

pub fn sha256_hex(b: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(b);
    let d = h.finalize();
    let mut s = String::with_capacity(64);
    for byte in d {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// One stats line for an object, as a map of metric -> u64.
pub async fn stats_for(env: &Env, name: &str) -> std::collections::HashMap<String, u64> {
    let txt = env
        .client
        .get(format!("{upstream}/stats", upstream = env.upstream))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for line in txt.lines() {
        if line.contains(&format!("\"name\":\"{name}\"")) {
            return parse_json_object(line);
        }
    }
    std::collections::HashMap::new()
}

pub async fn reset_stats(env: &Env) {
    env.client
        .get(format!("{upstream}/stats/reset", upstream = env.upstream))
        .send()
        .await
        .unwrap();
}

fn parse_json_object(s: &str) -> std::collections::HashMap<String, u64> {
    let mut m = std::collections::HashMap::new();
    let inner = s.trim().trim_start_matches('{').trim_end_matches('}');
    for part in inner.split(',') {
        if let Some((k, v)) = part.split_once(':') {
            let k = k.trim().trim_matches('"').to_string();
            if let Ok(n) = v.trim().parse::<u64>() {
                m.insert(k, n);
            }
        }
    }
    m
}
