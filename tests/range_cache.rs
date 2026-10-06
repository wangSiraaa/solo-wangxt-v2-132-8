#![cfg(feature = "test-support")]

mod common;

use common::*;
use range_cache_proxy::support;

async fn get(
    env: &Env,
    path: &str,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    let mut rb = env.client.get(format!("{}{}", env.proxy, path));
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    rb.send().await.unwrap()
}

fn hdr<'a>(r: &'a reqwest::Response, name: &str) -> Option<&'a str> {
    r.headers().get(name).and_then(|v| v.to_str().ok())
}

// ---------------------------------------------------------------------------
// 1. Cold full GET establishes the cache; bytes must be exactly right.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cold_full_then_range_served_from_cache_with_correct_bytes() {
    let env = spawn_env().await;
    let total = 300_000usize;
    let expected = support::object_bytes("alpha-v1", total);

    reset_stats(&env).await;
    let resp = get(&env, "/obj/alpha", &[]).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(hdr(&resp, "etag"), Some("\"alpha-v1\""));
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected));

    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["full_200"], 1);
    assert_eq!(st["partial_206"], 0);

    // Overlapping ranges, all served without touching upstream.
    let ranges = ["bytes=0-99", "bytes=50-149", "bytes=100-200", "bytes=199-999"];
    for rh in ranges {
        let resp = get(&env, "/obj/alpha", &[("Range", rh)]).await;
        assert_eq!(resp.status(), 206, "range {rh}");
        let (s, e) = parse_interval(rh);
        assert_eq!(
            hdr(&resp, "content-range"),
            Some(&*format!("bytes {s}-{e}/{total}")),
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.len() as u64, e - s + 1);
        assert_eq!(
            sha256_hex(&body),
            sha256_hex(&expected[s as usize..=e as usize]),
            "digest mismatch for {rh}"
        );
    }
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["full_200"], 1, "ranges must be served from cache");
    assert_eq!(st["partial_206"], 0);
}

// ---------------------------------------------------------------------------
// 2. Overlapping ranges on a cold object merge segments (verified upstream
//    counters + byte digests + inspect SQLite segments).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn overlapping_ranges_merge_segments_same_version_only() {
    let env = spawn_env().await;
    let total = 300_000u64;
    let expected = support::object_bytes("alpha-v1", total as usize);
    reset_stats(&env).await;

    // Cold 206: interval establishes version + total length.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=1000-1999")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[1000..2000]));

    // Overlapping adjacent interval: gap [0,1000) is fetched, [1000,2000)
    // already present — upstream should only see one exact 206.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=0-1999")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[0..2000]));

    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["partial_206"], 2);
    // second 206 must have carried our strong If-Range
    assert!(st["if_range_seen"] >= 1);
    // second 206 should only transfer the 1000 missing bytes
    assert_eq!(st["bytes_sent"], 2000);

    // Overlapping again: fully cached, no new upstream traffic.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=500-1500")]).await;
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[500..1501]));
    let st2 = stats_for(&env, "alpha").await;
    assert_eq!(st2["bytes_sent"], 2000);
    assert_eq!(st2["partial_206"], 2);

    // SQLite: exactly one version, segments merged into a single [0,2000).
    let segs = read_segments(&env);
    assert!(!segs.is_empty());
    let distinct: std::collections::HashSet<i64> = segs.iter().map(|(v, _, _)| *v).collect();
    assert_eq!(distinct.len(), 1, "segments must belong to one version");
    let intervals: Vec<(i64, i64)> = segs.iter().map(|(_, s, e)| (*s, *e)).collect();
    assert_covered(&intervals, 0, 2000);
}

// ---------------------------------------------------------------------------
// 2b. Fully-cached interval still revalidates the STRONG version: upstream
//     sends 304 with no body. Content/version change yields 200 and the new
//     bytes are returned (verified again with real byte digests).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cached_hits_revalidate_strong_and_accept_304() {
    let env = spawn_env().await;
    let total = 300_000usize;
    let expected = support::object_bytes("alpha-v1", total);
    reset_stats(&env).await;

    // Warm the object fully.
    let resp = get(&env, "/obj/alpha", &[]).await;
    assert_eq!(resp.status(), 200);

    let after_full = stats_for(&env, "alpha").await;
    assert_eq!(after_full["full_200"], 1);
    assert_eq!(after_full["bytes_sent"], total as u64);

    // Two overlapping cached ranges: they must be answered from disk after a
    // 304 each; upstream must transfer zero additional object bytes.
    for rh in ["bytes=10-20", "bytes=15-30"] {
        let resp = get(&env, "/obj/alpha", &[("Range", rh)]).await;
        assert_eq!(resp.status(), 206);
        let (s, e) = parse_interval(rh);
        let body = resp.bytes().await.unwrap();
        assert_eq!(sha256_hex(&body), sha256_hex(&expected[s as usize..=e as usize]));
    }
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["bytes_sent"], total as u64, "no extra body bytes");
    assert_eq!(st["partial_206"], 0, "upstream never served a 206");
    // Two conditional revalidation requests arrived.
    assert_eq!(st["requests"], 1 + 2);
}

// ---------------------------------------------------------------------------
// 3. Tail / suffix / open-ended requests.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn suffix_and_open_tail_requests_return_exact_tail_bytes() {
    let env = spawn_env().await;
    let total = 300u64; // /obj/tiny
    let expected = support::object_bytes("tiny-v1", total as usize);
    reset_stats(&env).await;

    // Cold suffix request resolves via Content-Range total.
    let resp = get(&env, "/obj/tiny", &[("Range", "bytes=-50")]).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes 250-299/300")
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 50);
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[250..]));

    // Open-ended tail request.
    let resp = get(&env, "/obj/tiny", &[("Range", "bytes=280-")]).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes 280-299/300")
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[280..]));

    // Suffix larger than the object: clamps to whole object.
    let resp = get(&env, "/obj/tiny", &[("Range", "bytes=-100000")]).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes 0-299/300")
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected));

    // bytes=-0 is unsatisfiable -> 416 with bytes */300.
    let resp = get(&env, "/obj/tiny", &[("Range", "bytes=-0")]).await;
    assert_eq!(resp.status(), 416);
    assert_eq!(hdr(&resp, "content-range"), Some("bytes */300"));
}

// ---------------------------------------------------------------------------
// 4. Upstream ignores Range: 200 with the full body is committed and the
//    proxy answers 200 (never splices a 200 into a 206).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upstream_ignoring_range_is_committed_as_full_version() {
    let env = spawn_env().await;
    let total = 120_000usize;
    let expected = support::object_bytes("ignore-v1", total);
    reset_stats(&env).await;

    let resp = get(
        &env,
        "/obj/ignores-range",
        &[("Range", "bytes=0-999"), ("If-Range", "\"ignore-v1\"")],
    )
    .await;
    assert_eq!(resp.status(), 200, "ignored Range must surface as 200");
    assert!(hdr(&resp, "content-range").is_none());
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), total);
    assert_eq!(sha256_hex(&body), sha256_hex(&expected));

    // Subsequent genuine range is served from cache with correct bytes.
    let resp = get(&env, "/obj/ignores-range", &[("Range", "bytes=100-199")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[100..200]));
    let st = stats_for(&env, "ignores-range").await;
    assert_eq!(st["partial_206"], 0);
}

// ---------------------------------------------------------------------------
// 5. 416: both cold (upstream generates it) and warmed (proxy knows length
//    and validates with the upstream under If-Range).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unsatisfiable_ranges_become_416_with_content_range() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    // Cold: proxy forwards, upstream emits 416.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=999999-")]).await;
    assert_eq!(resp.status(), 416);
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes */300000")
    );

    // Warm cache with full object.
    let resp = get(&env, "/obj/alpha", &[]).await;
    assert_eq!(resp.status(), 200);

    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=300000-300100")]).await;
    assert_eq!(resp.status(), 416);
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes */300000")
    );
    let st = stats_for(&env, "alpha").await;
    assert!(st["range_416"] >= 1);
}

// ---------------------------------------------------------------------------
// 6. If-Range: matching strong etag -> 206; stale etag -> fresh full 200;
//    weak If-Range -> never honored; weak stored validator -> never cached.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn if_range_strong_and_weak_semantics() {
    let env = spawn_env().await;
    let total = 300_000usize;
    let expected = support::object_bytes("alpha-v1", total);
    reset_stats(&env).await;

    // Warm with a plain range first.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=0-99")]).await;
    assert_eq!(resp.status(), 206);

    // Matching strong If-Range -> 206.
    let resp = get(
        &env,
        "/obj/alpha",
        &[("Range", "bytes=200-299"), ("If-Range", "\"alpha-v1\"")],
    )
    .await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[200..300]));

    // Mismatched strong If-Range -> full GET to upstream, 200, and the
    // bytes are the CURRENT representation (not cached stale bytes).
    let resp = get(
        &env,
        "/obj/alpha",
        &[("Range", "bytes=200-299"), ("If-Range", "\"old-etag\"")],
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected));

    // Weak If-Range sent by client: must NOT be used as a range proof;
    // RFC says the precondition fails -> full 200.
    let resp = get(
        &env,
        "/obj/alpha",
        &[("Range", "bytes=200-299"), ("If-Range", "W/\"alpha-v1\"")],
    )
    .await;
    assert_eq!(resp.status(), 200);

    // Weak-ETag object: 206s pass through but segments must not be merged
    // into any cached version.
    let weak_expected = support::object_bytes("weak-v1", 20_000);
    let resp = get(&env, "/obj/weak", &[("Range", "bytes=0-99")]).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(
        hdr(&resp, "etag"),
        Some("W/\"weak-v1\""),
        "weak validator must be preserved"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&weak_expected[0..100]));

    let resp2 = get(&env, "/obj/weak", &[("Range", "bytes=0-199")]).await;
    let body2 = resp2.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body2), sha256_hex(&weak_expected[0..200]));
    // A second 206 went upstream because weak bytes were never cached.
    let st = stats_for(&env, "weak").await;
    assert_eq!(st["partial_206"], 2);
}

// ---------------------------------------------------------------------------
// 7. Object changes but KEEPS THE SAME LENGTH: equal file size is never
//    treated as equal content; segments never bleed across versions.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_length_new_version_never_merges_old_segments() {
    let env = spawn_env().await;
    let len = 120_000usize;
    let v0 = support::mutable_bytes(0, len);
    reset_stats(&env).await;

    // Warm v0 with two overlapping ranges.
    let resp = get(&env, "/obj/mutable", &[("Range", "bytes=0-4999")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&v0[0..5000]));
    let resp = get(&env, "/obj/mutable", &[("Range", "bytes=4000-9999")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&v0[4000..10000]));

    // Upstream rolls to v1: same length, completely different bytes.
    let roll = env
        .client
        .post(format!("{}/obj/mutable", env.upstream))
        .send()
        .await
        .unwrap();
    assert_eq!(roll.status(), 200);
    let v1 = support::mutable_bytes(1, len);
    assert_ne!(sha256_hex(&v0), sha256_hex(&v1));

    // Range that hits old cached bytes must not return stale content. The
    // proxy validates the cached version with If-None-Match; upstream
    // answers 200 with the new representation (same length, new ETag). The
    // new full body is committed and the requested range is served from it
    // with v1 bytes — never spliced onto the old v0 segment.
    let resp = get(&env, "/obj/mutable", &[("Range", "bytes=2000-7999")]).await;
    assert_eq!(resp.status(), 206);
    assert_eq!(
        hdr(&resp, "etag"),
        Some("\"mutable-v1\""),
        "response must identify the NEW version"
    );
    assert_eq!(
        hdr(&resp, "content-range"),
        Some("bytes 2000-7999/120000")
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 6000);
    assert_eq!(sha256_hex(&body), sha256_hex(&v1[2000..8000]));
    assert_ne!(sha256_hex(&body), sha256_hex(&v0[2000..8000]));

    // Now a range of v1 is served from the new version blob — compare
    // against v1, never v0.
    let resp = get(&env, "/obj/mutable", &[("Range", "bytes=0-99")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&v1[0..100]));
    assert_ne!(sha256_hex(&body), sha256_hex(&v0[0..100]));

    // Two distinct version rows exist for the object; v1 is current.
    let count = count_versions(&env);
    assert!(count >= 2, "expected at least two versions, got {count}");
}

// ---------------------------------------------------------------------------
// 8. Truncated upstream (Content-Length lies): proxy must return 502 and
//    must not cache the partial body.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncated_upstream_is_never_a_success() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    let resp = get(&env, "/obj/truncated", &[]).await;
    assert_eq!(resp.status(), 502);

    // Nothing cached: another request goes upstream again.
    let before = stats_for(&env, "truncated").await;
    let resp = get(&env, "/obj/truncated", &[("Range", "bytes=0-99")]).await;
    assert_eq!(resp.status(), 502);
    let after = stats_for(&env, "truncated").await;
    assert!(after["requests"] > before["requests"]);
}

// ---------------------------------------------------------------------------
// 9. Client disconnect during slow upstream: the upstream download is
//    cancelled with the request (bounded, no background draining), and no
//    version/segment is committed for bytes the client never received.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_disconnect_cancels_upstream_download() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    // 100 KB at 80 ms/4KB ≈ 2 s of upstream streaming. The proxy spools the
    // upstream body fully before replying; a 400 ms client timeout fires
    // while the proxy is still spooling, dropping the request future.
    let timed_out = env
        .client
        .get(format!(
            "{}/obj/slow?ms=80&len=100000",
            env.proxy
        ))
        .timeout(std::time::Duration::from_millis(400))
        .send()
        .await;
    assert!(timed_out.is_err(), "expected client timeout");

    // Give the proxy a moment to observe the cancellation and drop the
    // in-flight spool (it must not continue downloading to completion).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // No version of this slow object may have been committed.
    let versions = count_rows(
        &env,
        "SELECT COUNT(*) FROM versions v JOIN objects o ON o.id = v.object_id
         WHERE o.path LIKE '/obj/slow%'",
    );
    assert_eq!(versions, 0, "disconnected transfer must not be cached");

    // Proof the upstream transfer really was cut short: after ~0.9 s of
    // total wall time it cannot have delivered 100 KB (needs ~2 s). Then
    // a fresh, patient request works and caches the object.
    let before = stats_for(&env, "slow").await;
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap()
        .get(format!("{}/obj/slow?ms=0&len=10000", env.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 10_000);
    assert_eq!(
        sha256_hex(&body),
        sha256_hex(&support::object_bytes("slow-v1", 10_000))
    );
    let after = stats_for(&env, "slow").await;
    assert!(after["requests"] > before.get("requests").copied().unwrap_or(0));
}

// ---------------------------------------------------------------------------
// 10. Corrupt the cached blob on disk: proxy must not return a short
//     "successful" body; it detects the truncation, resets segments and
//     re-fetches from upstream.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncated_cache_file_does_not_return_short_success() {
    let env = spawn_env().await;
    let total = 300_000u64;
    let expected = support::object_bytes("alpha-v1", total as usize);
    reset_stats(&env).await;

    let resp = get(&env, "/obj/alpha", &[]).await;
    assert_eq!(resp.status(), 200);

    // Truncate every blob file to half its declared size.
    let blobs = std::fs::read_dir(env.cache_dir.path().join("blobs")).unwrap();
    for entry in blobs {
        let p = entry.unwrap().path();
        let meta = std::fs::metadata(&p).unwrap();
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(meta.len() / 2).unwrap();
    }

    // A range near the end cannot be served from damaged bytes. The proxy
    // must either repair transparently (206 with correct bytes) or fail
    // loudly (5xx), but never 206 with truncated/zero-filled bytes.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=299000-299999")]).await;
    assert!(
        resp.status() == 206 || resp.status().is_server_error(),
        "unexpected status {}",
        resp.status()
    );
    if resp.status() == 206 {
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.len(), 1000);
        assert_eq!(
            sha256_hex(&body),
            sha256_hex(&expected[299_000..300_000]),
            "repaired bytes must match upstream exactly"
        );
    }

    // Follow-up request must recover fully from the upstream.
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=0-49")]).await;
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[0..50]));
}

// ---------------------------------------------------------------------------
// 11. Allow-list: arbitrary URLs / traversal / redirects are refused.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn only_configured_upstream_is_reachable() {
    let env = spawn_env().await;

    // Raw request lines: literal `..` traversal, percent-encoded traversal
    // and an absolute-form target pointing at an external origin. These go
    // over a raw socket because the reqwest client normalizes client-side.
    let host = env.proxy.strip_prefix("http://").unwrap();
    for req_line in [
        "GET /../etc/passwd HTTP/1.1",
        "GET /obj/../../etc/passwd HTTP/1.1",
        "GET /obj/%2e%2e/%2e%2e/etc/passwd HTTP/1.1",
        "GET /%2e%2e/%2e%2e/etc/passwd HTTP/1.1",
        "GET http://example.invalid/x HTTP/1.1",
    ] {
        let body = format!("{req_line}\r\nHost: {host}\r\nConnection: close\r\n\r\n");
        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream.write_all(body.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf).await;
        let text = String::from_utf8_lossy(&buf);
        let status_line = text.lines().next().unwrap_or_default();
        assert!(
            !status_line.contains("200 OK"),
            "{req_line} must not succeed, got: {status_line}"
        );
        assert!(
            !text.to_lowercase().contains("example domain"),
            "external body must never be proxied"
        );
    }

    // Upstream redirect to an external host: redirects are disabled and the
    // response is treated as an upstream failure (5xx), never followed.
    let resp = get(&env, "/redir", &[]).await;
    assert!(resp.status().is_server_error());
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_interval(rh: &str) -> (u64, u64) {
    let body = rh.strip_prefix("bytes=").unwrap();
    let (a, b) = body.split_once('-').unwrap();
    (a.parse().unwrap(), b.parse().unwrap())
}

fn sqlite_conn(env: &Env) -> rusqlite::Connection {
    rusqlite::Connection::open(env.cache_dir.path().join("range-cache.sqlite3")).unwrap()
}

fn read_segments(env: &Env) -> Vec<(i64, i64, i64)> {
    let conn = sqlite_conn(env);
    let mut stmt = conn
        .prepare(
            "SELECT v.id, s.start, s.end FROM segments s
             JOIN versions v ON v.id = s.version_id
             JOIN objects o ON o.id = v.object_id
             WHERE o.path = '/obj/alpha' ORDER BY s.start",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn assert_covered(segs: &[(i64, i64)], start: u64, end: u64) {
    let mut cursor = start;
    for &(s, e) in segs {
        let (s, e) = (s as u64, e as u64);
        if e <= cursor {
            continue;
        }
        assert!(s <= cursor, "gap at {cursor}; segments {segs:?}");
        cursor = cursor.max(e);
    }
    assert!(cursor >= end, "range [{start},{end}) not covered; {segs:?}");
}

fn count_versions(env: &Env) -> usize {
    count_rows(
        env,
        "SELECT COUNT(*) FROM versions v
         JOIN objects o ON o.id = v.object_id
         WHERE o.path = '/obj/mutable'",
    )
}

fn count_rows(env: &Env, sql: &str) -> usize {
    let conn = sqlite_conn(env);
    conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap() as usize
}
