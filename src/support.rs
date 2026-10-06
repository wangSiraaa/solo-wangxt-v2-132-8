//! The local test upstream, compiled behind the `test-support` feature.
//!
//! It deliberately exercises every proxy behavior:
//!   /obj/alpha          300 000 deterministic bytes, strong ETag, Range
//!   /obj/tiny           300 bytes
//!   /obj/weak           strong bytes but a W/ ETag
//!   /obj/noetag         200 always, no ETag (pass-through without caching)
//!   /obj/slow           streamed slowly (client disconnect tests)
//!   /obj/truncated      Content-Length lies; the body ends early
//!   /obj/ignores-range  always answers 200 even when Range is sent
//!   /obj/mutable        fixed-length body whose content/ETag change via POST
//!   /redir              302 to an EXTERNAL url (allow-list/no-redirect test)
//!   /stats, /stats/reset
//!
//! The test upstream only ever binds loopback.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct UpState {
    stats: Arc<Mutex<HashMap<String, Stat>>>,
    mutable_version: Arc<Mutex<u64>>,
}

#[derive(Default, Clone, Copy)]
struct Stat {
    requests: u64,
    full_200: u64,
    partial_206: u64,
    range_416: u64,
    if_range_seen: u64,
    bytes_sent: u64,
}

impl Stat {
    fn json(&self, name: &str) -> String {
        format!(
            "{{\"name\":\"{name}\",\"requests\":{},\"full_200\":{},\"partial_206\":{},\
             \"range_416\":{},\"if_range_seen\":{},\"bytes_sent\":{}}}",
            self.requests,
            self.full_200,
            self.partial_206,
            self.range_416,
            self.if_range_seen,
            self.bytes_sent
        )
    }
}

/// Deterministic pseudo-object bytes: `seed` names the object and each
/// 32-byte block is SHA-256 over the extending counter, giving
/// non-repetitive content whose digest tests can recompute exactly.
pub fn object_bytes(seed: &str, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut counter = 0u64;
    while out.len() < len {
        let mut h = Sha256::new();
        h.update(seed.as_bytes());
        h.update(b":");
        h.update(counter.to_le_bytes());
        out.extend_from_slice(&h.finalize());
        counter += 1;
    }
    out.truncate(len);
    out
}

/// Bytes of `/obj/mutable` at a given version. Length is fixed across
/// versions; content and ETag change.
pub fn mutable_bytes(version: u64, len: usize) -> Vec<u8> {
    object_bytes(&format!("mutable-v{version}"), len)
}

#[derive(Clone)]
struct ObjectSpec {
    length: usize,
    etag_seed: String,
    weak: bool,
    honors_range: bool,
    last_modified: i64,
}

fn spec_for(name: &str) -> Option<ObjectSpec> {
    Some(match name {
        "alpha" => ObjectSpec {
            length: 300_000,
            etag_seed: "alpha-v1".into(),
            weak: false,
            honors_range: true,
            last_modified: 784_111_777,
        },
        "tiny" => ObjectSpec {
            length: 300,
            etag_seed: "tiny-v1".into(),
            weak: false,
            honors_range: true,
            last_modified: 784_111_777,
        },
        "weak" => ObjectSpec {
            length: 20_000,
            etag_seed: "weak-v1".into(),
            weak: true,
            honors_range: true,
            last_modified: 784_111_777,
        },
        "noetag" => ObjectSpec {
            length: 50_000,
            etag_seed: String::new(),
            weak: false,
            honors_range: true,
            last_modified: 784_111_777,
        },
        "ignores-range" => ObjectSpec {
            length: 120_000,
            etag_seed: "ignore-v1".into(),
            weak: false,
            honors_range: false,
            last_modified: 784_111_777,
        },
        "mutable" => ObjectSpec {
            length: 120_000,
            etag_seed: String::new(), // resolved per request
            weak: false,
            honors_range: true,
            last_modified: 900_000_000,
        },
        _ => return None,
    })
}

fn etag_value(seed: &str, weak: bool) -> HeaderValue {
    let wire = if weak {
        format!("W/\"{seed}\"")
    } else {
        format!("\"{seed}\"")
    };
    HeaderValue::from_str(&wire).unwrap()
}

fn base_headers(spec: &ObjectSpec, etag_seed: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if !etag_seed.is_empty() {
        h.insert(axum::http::header::ETAG, etag_value(etag_seed, spec.weak));
    }
    h.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    if spec.last_modified > 0 {
        h.insert(
            axum::http::header::LAST_MODIFIED,
            HeaderValue::from_str(&crate::httpdate::imf_date(spec.last_modified)).unwrap(),
        );
    }
    h
}

fn bump(stats: &Arc<Mutex<HashMap<String, Stat>>>, name: &str, f: impl FnOnce(&mut Stat)) {
    let mut g = stats.lock().unwrap();
    f(g.entry(name.to_string()).or_default());
}

async fn get_object(
    State(st): State<UpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut spec = match spec_for(&name) {
        Some(s) => s,
        None => return StatusCode::NOT_FOUND.into_response(),
    };

    let mut version = 0u64;
    if name == "mutable" {
        version = *st.mutable_version.lock().unwrap();
        spec.etag_seed = format!("mutable-v{version}");
    }

    bump(&st.stats, &name, |s| s.requests += 1);
    if headers.contains_key(axum::http::header::IF_RANGE) {
        bump(&st.stats, &name, |s| s.if_range_seen += 1);
    }

    let data: Vec<u8> = if name == "mutable" {
        mutable_bytes(version, spec.length)
    } else {
        object_bytes(&spec.etag_seed, spec.length)
    };
    let len = data.len() as u64;

    // A fixture object that ignores Range answers 200 regardless.
    let range = if spec.honors_range {
        headers
            .get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(crate::range::parse_range)
    } else {
        None
    };

    // Evaluate If-Range like a compliant upstream, so the proxy's own
    // If-Range handling on gap fills is testable.
    let current_etag = crate::etag::ETag::parse(
        etag_value(&spec.etag_seed, spec.weak)
            .to_str()
            .unwrap(),
    );
    let range_allowed = match headers
        .get(axum::http::header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        None => true,
        Some(ir) => {
            crate::httpdate::if_range_matches(ir, current_etag.as_ref(), Some(spec.last_modified))
        }
    };

    // Conditional revalidation (proxy checks the strong validator before
    // answering a fully-cached request). Weak current validators never
    // satisfy a strong If-None-Match here.
    if let Some(inm) = headers
        .get(axum::http::header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        let mut match_current = false;
        for tag in inm.split(',').map(str::trim) {
            if tag == "*" {
                match_current = true;
                break;
            }
            if let Some(parsed) = crate::etag::ETag::parse(tag) {
                if let Some(cur) = &current_etag {
                    if crate::etag::strong_equal(&parsed, cur) {
                        match_current = true;
                        break;
                    }
                }
            }
        }
        if match_current && !spec.weak && !spec.etag_seed.is_empty() {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            resp.headers_mut().extend(base_headers(&spec, &spec.etag_seed).drain());
            return resp;
        }
    }

    match (&range, range_allowed) {
        (Some(crate::range::RangeSpec::Single(iv)), true) => {
            match crate::range::resolve(*iv, len) {
                Ok((s, e_incl)) => {
                    bump(&st.stats, &name, |x| {
                        x.partial_206 += 1;
                        x.bytes_sent += e_incl + 1 - s;
                    });
                    let slice = data[s as usize..=e_incl as usize].to_vec();
                    let mut h = base_headers(&spec, &spec.etag_seed);
                    h.insert(
                        axum::http::header::CONTENT_RANGE,
                        HeaderValue::from_str(&crate::range::content_range(s, e_incl, len))
                            .unwrap(),
                    );
                    h.insert(
                        axum::http::header::CONTENT_LENGTH,
                        HeaderValue::from_str(&slice.len().to_string()).unwrap(),
                    );
                    let mut resp = Response::new(Body::from(slice));
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp.headers_mut().extend(h.drain());
                    resp
                }
                Err(crate::range::Unsatisfiable) => {
                    bump(&st.stats, &name, |x| x.range_416 += 1);
                    let mut h = base_headers(&spec, &spec.etag_seed);
                    h.insert(
                        axum::http::header::CONTENT_RANGE,
                        HeaderValue::from_str(&crate::range::unsatisfiable_content_range(len))
                            .unwrap(),
                    );
                    let mut resp = Response::new(Body::empty());
                    *resp.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                    resp.headers_mut().extend(h.drain());
                    resp
                }
            }
        }
        _ => {
            // Full 200: ignored multipart Range, failed If-Range or no Range.
            bump(&st.stats, &name, |x| {
                x.full_200 += 1;
                x.bytes_sent += len;
            });
            let mut h = base_headers(&spec, &spec.etag_seed);
            h.insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&len.to_string()).unwrap(),
            );
            let mut resp = Response::new(Body::from(data));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut().extend(h.drain());
            resp
        }
    }
}

/// Deliberately slow object: streamed in 4 KB chunks with a delay. Query
/// params `ms` (delay per chunk, default 60) and `len` (size, default
/// 30 000) let the proxy tests arrange for the client to disconnect while
/// the proxy is still spooling the upstream body.
async fn get_slow(
    State(st): State<UpState>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    let q: HashMap<String, String> = uri
        .query()
        .map(|qs| {
            qs.split('&')
                .filter_map(|pair| pair.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let delay_ms: u64 = q.get("ms").and_then(|s| s.parse().ok()).unwrap_or(60);
    let len: usize = q
        .get("len")
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    let data = object_bytes("slow-v1", len);
    bump(&st.stats, "slow", |s| s.requests += 1);
    if headers.contains_key(axum::http::header::IF_RANGE) {
        bump(&st.stats, "slow", |s| s.if_range_seen += 1);
    }

    let interval = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(crate::range::parse_range);
    if let Some(crate::range::RangeSpec::Single(iv)) = interval {
        if let Ok((s, e)) = crate::range::resolve(iv, len as u64) {
            bump(&st.stats, "slow", |x| {
                x.partial_206 += 1;
                x.bytes_sent += e + 1 - s;
            });
            let slice = data[s as usize..=e as usize].to_vec();
            let n = slice.len() as u64;
            let body = slow_stream(slice, delay_ms);
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            let h = resp.headers_mut();
            h.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            h.insert(axum::http::header::ETAG, etag_value("slow-v1", false));
            h.insert(
                axum::http::header::CONTENT_RANGE,
                HeaderValue::from_str(&crate::range::content_range(s, e, len as u64)).unwrap(),
            );
            h.insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&n.to_string()).unwrap(),
            );
            return resp;
        }
    }

    bump(&st.stats, "slow", |x| {
        x.full_200 += 1;
        x.bytes_sent += len as u64;
    });
    let body = slow_stream(data, delay_ms);
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::OK;
    let h = resp.headers_mut();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    h.insert(axum::http::header::ETAG, etag_value("slow-v1", false));
    h.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).unwrap(),
    );
    resp
}

fn slow_stream(data: Vec<u8>, delay_ms: u64) -> Body {
    let chunks: Vec<Bytes> = data
        .chunks(4096)
        .map(Bytes::copy_from_slice)
        .collect();
    let s = stream::unfold((0usize, chunks), move |(i, chunks)| async move {
        if i >= chunks.len() {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        let item: std::result::Result<Bytes, std::io::Error> = Ok(chunks[i].clone());
        Some((item, (i + 1, chunks)))
    });
    Body::from_stream(s)
}

/// Announces 50 000 bytes but only sends 10 000 before closing.
async fn get_truncated(State(st): State<UpState>) -> Response {
    const ADVERTISE: usize = 50_000;
    const ACTUAL: usize = 10_000;
    bump(&st.stats, "truncated", |s| {
        s.requests += 1;
        s.full_200 += 1;
        s.bytes_sent += ACTUAL as u64;
    });
    let data = object_bytes("truncated-v1", ACTUAL);
    let mut resp = Response::new(Body::from(data));
    *resp.status_mut() = StatusCode::OK;
    let h = resp.headers_mut();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    h.insert(axum::http::header::ETAG, etag_value("truncated-v1", false));
    h.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&ADVERTISE.to_string()).unwrap(),
    );
    resp
}

/// 302 to an external host: the proxy must neither follow nor fetch it.
async fn redir() -> Response {
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::FOUND;
    resp.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_static("http://example.invalid/escaped"),
    );
    resp
}

/// Roll the mutable object to the next version (same length, new content).
async fn post_mutable(State(st): State<UpState>) -> Response {
    let v = {
        let mut g = st.mutable_version.lock().unwrap();
        *g += 1;
        *g
    };
    bump(&st.stats, "mutable", |s| s.requests += 1);
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        format!("{{\"version\":{v}}}"),
    )
        .into_response()
}

async fn stats(State(st): State<UpState>) -> Response {
    let g = st.stats.lock().unwrap();
    let mut names: Vec<&String> = g.keys().collect();
    names.sort();
    let body = names
        .iter()
        .map(|n| g[*n].json(n))
        .collect::<Vec<_>>()
        .join("\n");
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

async fn stats_reset(State(st): State<UpState>) -> &'static str {
    st.stats.lock().unwrap().clear();
    "ok"
}

/// Build the test upstream router.
pub fn app() -> axum::Router {
    let st = UpState {
        stats: Arc::new(Mutex::new(HashMap::new())),
        mutable_version: Arc::new(Mutex::new(0)),
    };
    axum::Router::new()
        .route(
            "/obj/{name}",
            axum::routing::get(get_object).post(post_mutable),
        )
        .route("/obj/slow", axum::routing::get(get_slow))
        .route("/obj/truncated", axum::routing::get(get_truncated))
        .route("/redir", axum::routing::get(redir))
        .route("/stats", axum::routing::get(stats))
        .route("/stats/reset", axum::routing::get(stats_reset))
        .with_state(st)
}

/// Spawn the test upstream on a loopback ephemeral port, returning its
/// base URL (e.g. `http://127.0.0.1:41234/`).
pub async fn spawn() -> String {
    let app = app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });
    format!("http://{addr}/")
}
