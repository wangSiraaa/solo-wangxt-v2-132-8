//! The proxy handler.
//!
//! Request strategy (GET only, pinned to one upstream):
//!
//! * No `Range` (or one we cannot parse): plain GET. A strong-ETag 200
//!   replaces the cached version wholesale; weak/missing validators are
//!   passed through without caching.
//! * Single resolved interval with a known strong cached version: serve
//!   from cached bytes; fetch only missing intervals, each carrying our own
//!   `If-Range: "<strong etag>"`. A changed object (upstream 200) is
//!   detected before any bytes can be spliced into the old version.
//! * Unknown length / unsatisfiable / first range request: forward the
//!   client's Range as-is. A strong 206 establishes the version (and its
//!   total length from Content-Range) and the request is re-planned; a weak
//!   206 is passed through but never merged; multipart 206 is never cached.
//!
//! Optional client version pinning with `If-Match`: a request carrying
//! exactly one STRONG entity-tag in `If-Match` is a version lock. The proxy
//! first confirms the upstream's current validator with a bodyless HEAD,
//! then serves only that pinned representation (full body or one range) from
//! its own cache or by filling gaps. Any other outcome is reported without
//! returning an object body:
//!
//! * current strong ETag differs from the lock, or the upstream proves the
//!   object changed while the request ran → 412 Precondition Failed;
//! * upstream's current validator is weak or missing → 412 (the pinned
//!   bytes cannot be guaranteed);
//! * weak tag, a tag list, `*` or unparseable `If-Match` → 400.
//!
//! Requests without `If-Match` follow the ordinary strategy above.
//!
//! Upstream bodies are spooled to a temp file in full before any bytes are
//! committed to the cache or sent to the client. A length mismatch becomes
//! 502 — a truncated response is never reported as complete. The spool
//! lives in the request future, so when the client disconnects the future
//! (and therefore the upstream download) is cancelled with it.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::config::ProxyConfig;
use crate::db::VersionRow;
use crate::error::{ProxyError, Result};
use crate::etag::{ETag, LockError};
use crate::range::{self, RangeSpec};
use crate::store::{BlobStore, Spool};

const MAX_PLAN_ITERATIONS: u32 = 4;
/// How many times a damaged blob (file shorter than metadata) may be reset
/// and re-fetched within one request.
const MAX_CACHE_REPAIRS: u32 = 2;

pub struct ProxyState {
    pub config: ProxyConfig,
    base_url: url::Url,
    db: Arc<Mutex<Connection>>,
    pub store: BlobStore,
    http: reqwest::Client,
    locks: tokio::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

fn inner_router(state: Arc<ProxyState>) -> axum::Router {
    axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .fallback(axum::routing::any(proxy_entry))
        .with_state(state)
}

async fn proxy_entry(
    axum::extract::State(state): axum::extract::State<Arc<ProxyState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    match serve(state, &method, &uri, &headers).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(error = %e, "proxy error");
            (e.status(), e.to_string()).into_response()
        }
    }
}

/// Build the proxy app. Fails when the configured upstream is not an
/// http(s) loopback origin.
pub async fn build_app(config: ProxyConfig) -> anyhow::Result<(axum::Router, Arc<ProxyState>)> {
    let base_url = url::Url::parse(&config.upstream_base)?;
    if !matches!(base_url.scheme(), "http" | "https") {
        anyhow::bail!("upstream must be http(s): {}", config.upstream_base);
    }
    if config.require_loopback_upstream {
        match base_url.host_str().and_then(|h| h.parse::<IpAddr>().ok()) {
            Some(ip) if ip.is_loopback() => {}
            other => anyhow::bail!("upstream must be a loopback IP, got {:?}", other),
        }
    }
    if !config.upstream_base.ends_with('/') {
        anyhow::bail!("UPSTREAM_BASE must end with '/'");
    }

    tokio::fs::create_dir_all(&config.cache_dir).await?;
    let conn = crate::db::open(&config.cache_dir.join("range-cache.sqlite3"))?;
    let store = BlobStore::new(&config.cache_dir).await?;
    let http = reqwest::Client::builder()
        // The allow-list is enforced by URL construction; never let a 3xx
        // silently take us to some other host.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()?;

    let state = Arc::new(ProxyState {
        config,
        base_url,
        db: Arc::new(Mutex::new(conn)),
        store,
        http,
        locks: tokio::sync::Mutex::new(HashMap::new()),
    });
    let app: axum::Router = inner_router(state.clone());
    Ok((app, state))
}

async fn lock_for(state: &Arc<ProxyState>, key: &str) -> Arc<Mutex<()>> {
    let mut map = state.locks.lock().await;
    map.entry(key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

// ---------------------------------------------------------------------------
// Allow-list / URL construction
// ---------------------------------------------------------------------------

/// Reconstruct the single allowed upstream target URL from the request URI.
fn resolve_target(state: &ProxyState, uri: &Uri) -> Result<url::Url> {
    let raw_path = uri.path();
    if !raw_path.starts_with('/') {
        return Err(ProxyError::PathEscape);
    }
    let base_path = state.base_url.path();

    // The request must lexically stay inside the upstream prefix. Both raw
    // and percent-decoded forms are checked so "%2e%2e" cannot escape.
    for segment in raw_path.split('/') {
        if matches!(segment, ".." | ".") {
            return Err(ProxyError::PathEscape);
        }
        let dec = percent_decode_str(segment).decode_utf8_lossy();
        if dec == ".." || dec == "." || dec.contains('\\') || dec.contains('\0') {
            return Err(ProxyError::PathEscape);
        }
    }
    if base_path != "/" && !raw_path.starts_with(base_path) {
        return Err(ProxyError::PathEscape);
    }

    let rel = if base_path == "/" {
        raw_path.to_string()
    } else {
        format!("/{}", raw_path[base_path.len()..].trim_start_matches('/'))
    };

    let mut target = state
        .base_url
        .join(&rel)
        .map_err(|_| ProxyError::UpstreamNotAllowed)?;
    target.set_query(uri.query());

    if target.scheme() != state.base_url.scheme()
        || target.host_str() != state.base_url.host_str()
        || target.port_or_known_default() != state.base_url.port_or_known_default()
    {
        return Err(ProxyError::UpstreamNotAllowed);
    }
    Ok(target)
}

fn target_key(target: &url::Url) -> String {
    match target.query() {
        Some(q) => format!("{}?{q}", target.path()),
        None => target.path().to_string(),
    }
}

// ---------------------------------------------------------------------------
// Upstream capture
// ---------------------------------------------------------------------------

struct Captured {
    spool: Spool,
    len: u64,
    etag: Option<ETag>,
    last_modified: Option<i64>,
    content_type: Option<String>,
    content_range: Option<String>,
}

/// Run one upstream request, spooling the body into a temp file.
/// Content-Length is checked strictly: a body that ends early is a
/// truncation error, never a successful capture.
async fn capture_upstream(
    state: &ProxyState,
    target: &url::Url,
    range: Option<&str>,
    if_range: Option<&str>,
) -> Result<(reqwest::StatusCode, Captured)> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    if let Some(r) = range {
        headers.insert(reqwest::header::RANGE, HeaderValue::from_str(r).unwrap());
    }
    if let Some(ir) = if_range {
        if let Ok(v) = HeaderValue::from_str(ir) {
            headers.insert(reqwest::header::IF_RANGE, v);
        }
    }

    let resp = state.http.get(target.clone()).headers(headers).send().await?;
    let status = resp.status();
    // 304 carries no body and no Content-Length; drain whatever bytes the
    // connection ends (normally none) and return without length checking.
    if status == reqwest::StatusCode::NOT_MODIFIED {
        let header_str = |name: HeaderName| -> Option<String> {
            resp.headers()
                .get(&name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let etag = header_str(reqwest::header::ETAG).and_then(|s| ETag::parse(&s));
        let last_modified = header_str(reqwest::header::LAST_MODIFIED)
            .and_then(|s| crate::httpdate::parse_http_date(&s));
        let content_type = header_str(reqwest::header::CONTENT_TYPE);
        let content_range = header_str(reqwest::header::CONTENT_RANGE);
        // Consume (close) the response body so the connection can be reused.
        let mut stream = resp.bytes_stream();
        while stream.next().await.is_some() {}
        let spool = state.store.new_spool().await?;
        return Ok((
            status,
            Captured {
                spool,
                len: 0,
                etag,
                last_modified,
                content_type,
                content_range,
            },
        ));
    }
    let declared = if status == reqwest::StatusCode::PARTIAL_CONTENT
        || status == reqwest::StatusCode::OK
    {
        resp.content_length()
    } else {
        None
    };
    let header_str = |name: HeaderName| -> Option<String> {
        resp.headers()
            .get(&name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let etag = header_str(reqwest::header::ETAG).and_then(|s| ETag::parse(&s));
    let last_modified = header_str(reqwest::header::LAST_MODIFIED)
        .and_then(|s| crate::httpdate::parse_http_date(&s));
    let content_type = header_str(reqwest::header::CONTENT_TYPE);
    let content_range = header_str(reqwest::header::CONTENT_RANGE);

    let mut spool = state.store.new_spool().await?;
    let mut len = 0u64;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        spool.write_all(&chunk).await?;
        len += chunk.len() as u64;
    }
    spool.flush().await?;
    if let Some(want) = declared {
        if want != len {
            return Err(ProxyError::TruncatedUpstream);
        }
    }

    Ok((
        status,
        Captured {
            spool,
            len,
            etag,
            last_modified,
            content_type,
            content_range,
        },
    ))
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn common_headers(v: &VersionRow) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_str(v.content_type.as_deref().unwrap_or("application/octet-stream"))
            .unwrap(),
    );
    if let Some(lm) = v.last_modified {
        if let Ok(hv) = HeaderValue::from_str(&crate::httpdate::imf_date(lm)) {
            h.insert(axum::http::header::LAST_MODIFIED, hv);
        }
    }
    h.insert(
        axum::http::header::ETAG,
        HeaderValue::from_str(
            &ETag {
                weak: v.weak,
                raw_tag: v.etag_tag.clone(),
            }
            .to_wire(),
        )
        .unwrap(),
    );
    h.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    h
}

fn build_response(status: StatusCode, mut headers: HeaderMap, body: Bytes) -> Response {
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).unwrap(),
    );
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = status;
    resp.headers_mut().extend(headers.drain());
    resp
}

fn passthrough_headers(c: &Captured) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_str(c.content_type.as_deref().unwrap_or("application/octet-stream"))
            .unwrap(),
    );
    if let Some(tag) = &c.etag {
        h.insert(
            axum::http::header::ETAG,
            HeaderValue::from_str(&tag.to_wire()).unwrap(),
        );
    }
    if let Some(lm) = c.last_modified {
        if let Ok(hv) = HeaderValue::from_str(&crate::httpdate::imf_date(lm)) {
            h.insert(axum::http::header::LAST_MODIFIED, hv);
        }
    }
    if let Some(cr) = &c.content_range {
        if let Ok(hv) = HeaderValue::from_str(cr) {
            h.insert(axum::http::header::CONTENT_RANGE, hv);
        }
    }
    h.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    h
}

async fn serve(
    state: Arc<ProxyState>,
    method: &Method,
    uri: &Uri,
    req_headers: &HeaderMap,
) -> Result<Response> {
    if *method != Method::GET {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }
    let target = resolve_target(&state, uri)?;
    let key = target_key(&target);

    let obj_lock = lock_for(&state, &key).await;
    let _guard = obj_lock.lock().await;

    let range_raw = req_headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let if_range = req_headers
        .get(axum::http::header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // Optional client version lock. A valid lock is exactly one strong
    // entity-tag; every other If-Match shape is rejected up front. Its
    // presence changes the whole strategy: the pinned representation is
    // confirmed upstream first and nothing else is ever returned.
    let lock = match parse_client_lock(req_headers) {
        Ok(lock) => lock,
        Err(resp) => return Ok(resp),
    };

    let single = match range_raw.as_deref().map(range::parse_range) {
        None | Some(None) => match lock {
            None => full_get(&state, &target).await,
            Some(tag) => locked_full_get(&state, &target, &key, tag).await,
        },
        Some(Some(RangeSpec::Multiple)) => match lock {
            None => {
                multipart_forward(&state, &target, range_raw.as_deref(), if_range.as_deref())
                    .await
            }
            Some(tag) => {
                locked_multipart_forward(
                    &state,
                    &target,
                    range_raw.as_deref(),
                    if_range.as_deref(),
                    &tag,
                )
                .await
            }
        },
        Some(Some(RangeSpec::Single(iv))) => match lock {
            None => {
                serve_range(
                    &state,
                    &target,
                    &key,
                    iv,
                    range_raw.as_deref(),
                    if_range.as_deref(),
                )
                .await
            }
            Some(tag) => {
                serve_locked_range(
                    &state,
                    &target,
                    &key,
                    iv,
                    range_raw.as_deref().unwrap(),
                    if_range.as_deref(),
                    &tag,
                )
                .await
            }
        },
    };
    single
}

/// Extract and strictly validate the client's `If-Match` version lock.
///
/// Returns `Ok(None)` when no lock was supplied (ordinary request), `Ok(Some)`
/// for the single accepted strong tag, and an error *response* (400) for every
/// shape that cannot pin one exact representation.
fn parse_client_lock(req_headers: &HeaderMap) -> std::result::Result<Option<ETag>, Response> {
    let mut values = req_headers.get_all(axum::http::header::IF_MATCH).iter();
    let first = match values.next() {
        None => return Ok(None),
        Some(v) => v,
    };
    // Several If-Match header lines form one combined list (RFC 9110 §13.1.3)
    // — it can therefore never be one single tag.
    let joined = match values.next() {
        None => first.to_str().map_err(|_| bad_lock_response(LockError::Malformed))?,
        Some(second) => {
            let mut s = first
                .to_str()
                .map_err(|_| bad_lock_response(LockError::Malformed))?
                .to_string();
            s.push(',');
            s.push_str(
                second
                    .to_str()
                    .map_err(|_| bad_lock_response(LockError::Malformed))?,
            );
            for v in values {
                s.push(',');
                s.push_str(
                    v.to_str()
                        .map_err(|_| bad_lock_response(LockError::Malformed))?,
                );
            }
            return Err(bad_lock_response_value(&s));
        }
    };
    match crate::etag::parse_if_match_lock(joined) {
        Ok(tag) => Ok(Some(tag)),
        Err(e) => Err(bad_lock_response(e)),
    }
}

/// 400 explaining which shape of lock value is accepted.
fn bad_lock_response(e: LockError) -> Response {
    let detail = match e {
        LockError::Malformed => {
            "If-Match must be exactly one quoted strong entity-tag (e.g. \"v1\"); \
             lists and * do not pin one version"
        }
        LockError::Multiple => {
            "If-Match version lock accepts exactly one entity-tag, not a list"
        }
        LockError::Weak => {
            "a weak (W/) ETag in If-Match cannot pin exact bytes; use a strong ETag"
        }
    };
    (
        StatusCode::BAD_REQUEST,
        [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        detail,
    )
        .into_response()
}

/// 400 for a raw combined header value (used when multiple If-Match header
/// lines were present, which can only ever form a list).
fn bad_lock_response_value(raw: &str) -> Response {
    match crate::etag::parse_if_match_lock(raw) {
        Err(e) => bad_lock_response(e),
        // A value that parses as one tag can only reach here across multiple
        // header lines, which is still a combined list.
        Ok(_) => bad_lock_response(LockError::Multiple),
    }
}

// ---------------------------------------------------------------------------
// Version locking (client If-Match)
// ---------------------------------------------------------------------------

/// What a bodyless HEAD probe found about the upstream's current validator.
enum CurrentValidator {
    /// Upstream currently serves exactly this strong entity-tag.
    Strong(ETag),
    /// Upstream serves only a weak validator — byte identity unprovable.
    Weak,
    /// Upstream provides no validator at all.
    Missing,
}

/// Confirm the upstream's *current* version with a bodyless HEAD before any
/// object bytes are returned for a locked request.
async fn probe_current_validator(
    state: &ProxyState,
    target: &url::Url,
) -> Result<CurrentValidator> {
    let resp = state
        .http
        .head(target.clone())
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .send()
        .await?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(ProxyError::TruncatedUpstream);
    }
    // Read the validator before consuming (moving) the response.
    let etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .and_then(ETag::parse);
    // Consume whatever the connection carries (normally nothing for HEAD).
    let mut stream = resp.bytes_stream();
    while stream.next().await.is_some() {}
    Ok(match etag {
        Some(t) if !t.weak => CurrentValidator::Strong(t),
        Some(_) => CurrentValidator::Weak,
        None => CurrentValidator::Missing,
    })
}

/// Run the current-version probe and compare it against the lock.
/// `Ok(None)` means the lock is confirmed; `Ok(Some(resp))` is the 412 to
/// return when the pinned version cannot be guaranteed.
async fn confirm_lock(
    state: &ProxyState,
    target: &url::Url,
    lock: &ETag,
) -> Result<Option<Response>> {
    Ok(match probe_current_validator(state, target).await? {
        CurrentValidator::Strong(current) => {
            if crate::etag::strong_equal(&current, lock) {
                None
            } else {
                Some(precondition_failed(
                    Some(&current),
                    "the object's current version differs from the If-Match lock; \
                     refusing to return a different representation",
                ))
            }
        }
        CurrentValidator::Weak => Some(precondition_failed(
            None,
            "upstream now provides only a weak ETag; the pinned exact version cannot \
             be guaranteed",
        )),
        CurrentValidator::Missing => Some(precondition_failed(
            None,
            "upstream provides no ETag; the pinned exact version cannot be guaranteed",
        )),
    })
}

/// 412 Precondition Failed. Never carries an object body; when the upstream's
/// current strong validator is known it is echoed back so the client can see
/// what replaced its lock.
fn precondition_failed(current: Option<&ETag>, reason: &'static str) -> Response {
    let mut resp = (
        StatusCode::PRECONDITION_FAILED,
        reason,
    )
        .into_response();
    if let Some(tag) = current {
        if let Ok(hv) = HeaderValue::from_str(&tag.to_wire()) {
            resp.headers_mut().insert(axum::http::header::ETAG, hv);
        }
    }
    resp
}

/// Full GET pinned to `lock`: confirm the current version upstream, then
/// answer with exactly that representation — the whole body if the client
/// sent no usable Range.
async fn locked_full_get(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    lock: ETag,
) -> Result<Response> {
    if let Some(resp) = confirm_lock(state, target, &lock).await? {
        return Ok(resp);
    }

    // Fast path: the pinned representation is already fully cached.
    let key_find = key.to_string();
    let tag_find = lock.raw_tag.clone();
    if let Some(v) = with_db(state, move |c| {
        crate::db::find_version(c, &key_find, &tag_find)
    })
    .await?
    {
        if let Some(len) = v.total_length {
            match read_checked(state, v.id, 0, len).await {
                Ok(body) => return Ok(build_response(StatusCode::OK, common_headers(&v), body)),
                Err(ProxyError::BlobTruncated) => {
                    repair_version(state, v.id).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    // Otherwise fetch the whole representation and demand the locked tag.
    let (status, cap) = capture_upstream(state, target, None, None).await?;
    if status != reqwest::StatusCode::OK {
        return Err(ProxyError::TruncatedUpstream);
    }
    match classify_locked_capture(&cap, &lock) {
        Ok(()) => match commit_200(state, key, cap).await? {
            CommitOutcome::Strong(v) => {
                let len = v.total_length.unwrap();
                let body = read_checked(state, v.id, 0, len).await?;
                Ok(build_response(StatusCode::OK, common_headers(&v), body))
            }
            CommitOutcome::Uncached(_) => Err(ProxyError::TruncatedUpstream),
        },
        Err(resp) => Ok(resp),
    }
}

/// Demand that a captured response prove the pinned representation: its
/// validator must be present, strong and equal to the lock. Used on paths
/// where a version swap must become 412 rather than an implicit fallback.
fn classify_locked_capture(cap: &Captured, lock: &ETag) -> std::result::Result<(), Response> {
    match &cap.etag {
        Some(tag) if crate::etag::strong_equal(tag, lock) => Ok(()),
        Some(current) => Err(precondition_failed(
            Some(current),
            "object changed while the locked request was being served; \
             refusing to return a different representation",
        )),
        None => Err(precondition_failed(
            None,
            "upstream response carried no strong ETag; the locked version \
             cannot be guaranteed",
        )),
    }
}

/// Single-range GET pinned to `lock`.
#[allow(clippy::too_many_arguments)]
async fn serve_locked_range(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    single: range::RawInterval,
    range_raw: &str,
    if_range: Option<&str>,
    lock: &ETag,
) -> Result<Response> {
    // 1) Confirm the current upstream version before trusting anything.
    if let Some(resp) = confirm_lock(state, target, lock).await? {
        return Ok(resp);
    }

    let mut repairs = 0;
    for _ in 0..MAX_PLAN_ITERATIONS {
        // Plan against the PINNED version specifically; a newer cached
        // version must never satisfy the lock.
        let key_for_db = key.to_string();
        let tag_for_db = lock.raw_tag.clone();
        let pinned = with_db(state, move |c| {
            crate::db::find_version(c, &key_for_db, &tag_for_db)
        })
        .await?;
        let total = match pinned.as_ref().and_then(|v| v.total_length) {
            Some(t) => t,
            None => {
                match locked_cold_range(
                    state,
                    target,
                    key,
                    range_raw,
                    if_range,
                    lock,
                )
                .await?
                {
                    ColdOutcome::Reply(resp) => return Ok(resp),
                    ColdOutcome::Replan => continue,
                }
            }
        };
        let ver = pinned.expect("total length implies a version row");

        let (start, end_inclusive) = match range::resolve(single, total) {
            Ok(v) => v,
            Err(range::Unsatisfiable) => {
                return locked_unsatisfiable(state, target, &ver, total, range_raw, lock).await;
            }
        };
        let end_excl = end_inclusive + 1;

        // Honor the client's own If-Range against the pinned representation.
        // Mismatch falls back to the locked full body — never an unpinned one.
        if let Some(ir) = if_range {
            let cur = ETag {
                weak: false,
                raw_tag: ver.etag_tag.clone(),
            };
            if !crate::httpdate::if_range_matches(ir, Some(&cur), ver.last_modified) {
                return locked_full_get(state, target, key, lock.clone()).await;
            }
        }

        let vid = ver.id;
        let covered =
            with_db(state, move |c| crate::db::covered_segments(c, vid)).await?;
        let gaps = range::missing_within(start, end_excl, &covered);

        if !gaps.is_empty() {
            // Every gap is fetched under the locked strong tag.
            for &(gs, ge) in &gaps {
                let range_hdr = format!("bytes={gs}-{}", ge - 1);
                let ir = ETag {
                    weak: false,
                    raw_tag: ver.etag_tag.clone(),
                }
                .to_wire();
                let (status, cap) =
                    capture_upstream(state, target, Some(&range_hdr), Some(&ir)).await?;
                match status {
                    reqwest::StatusCode::PARTIAL_CONTENT => {
                        match store_proven_206(
                            state,
                            key,
                            Some(&ver),
                            cap,
                            Some((gs, ge)),
                            Some(total),
                        )
                        .await?
                        {
                            ProveOutcome::Stored(_) => {}
                            // Validator/Content-Range contradiction under the
                            // lock: an upstream fault, never a passthrough.
                            ProveOutcome::Passthrough(_) => {
                                return Err(ProxyError::TruncatedUpstream)
                            }
                        }
                    }
                    reqwest::StatusCode::OK => {
                        // A 200 to our strong If-Range means the object moved.
                        // With the lock still current the only legal 200 is a
                        // Range-ignoring upstream serving the pinned bytes;
                        // anything else is a precondition failure with no body.
                        match classify_locked_capture(&cap, lock) {
                            Ok(()) => match commit_200(state, key, cap).await? {
                                CommitOutcome::Strong(v) => {
                                    let len = v.total_length.unwrap();
                                    let body = read_checked(state, v.id, 0, len).await?;
                                    return Ok(build_response(
                                        StatusCode::OK,
                                        common_headers(&v),
                                        body,
                                    ));
                                }
                                CommitOutcome::Uncached(_) => {
                                    return Err(ProxyError::TruncatedUpstream)
                                }
                            },
                            Err(resp) => return Ok(resp),
                        }
                    }
                    _ => return Err(ProxyError::TruncatedUpstream),
                }
            }
        } else {
            // Fully on disk: still prove the pinned version is current before
            // returning any cached bytes.
            match revalidate_cached(state, target, key, &ver).await? {
                Revalidation::Fresh => {}
                Revalidation::Changed(v) => {
                    if crate::etag::strong_equal(
                        &ETag { weak: false, raw_tag: v.etag_tag.clone() },
                        lock,
                    ) {
                        // Same tag (e.g. refreshed metadata): re-plan.
                        continue;
                    }
                    return Ok(precondition_failed(
                        Some(&ETag { weak: false, raw_tag: v.etag_tag }),
                        "the object's current version differs from the If-Match lock; \
                         refusing to return a different representation",
                    ));
                }
                Revalidation::Unverifiable => {
                    return Ok(precondition_failed(
                        None,
                        "upstream no longer provides a strong ETag; the pinned exact \
                         version cannot be guaranteed",
                    ));
                }
            }
        }

        // All requested bytes cached for the one proven pinned version.
        let read_ver = ver.clone();
        match read_partial(state, &read_ver, start, end_excl, total).await {
            Ok(resp) => return Ok(resp),
            Err(ProxyError::BlobTruncated) => {
                if repairs >= MAX_CACHE_REPAIRS {
                    return Err(ProxyError::BlobTruncated);
                }
                repairs += 1;
                repair_version(state, ver.id).await?;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ProxyError::State("upstream version churn; giving up".into()))
}

/// Cold path for a locked range: the pinned version is not cached yet (or its
/// length is unknown). Forward the Range; prove the response is the pinned
/// representation before establishing the cache entry.
async fn locked_cold_range(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    range_hdr: &str,
    if_range: Option<&str>,
    lock: &ETag,
) -> Result<ColdOutcome> {
    // Send the pinned tag ourselves when the client gave no If-Range, so a
    // compliant upstream answers 206 for this exact representation.
    let own_if_range = lock.to_wire();
    let effective_if_range = if_range.unwrap_or(&own_if_range);
    let (status, cap) =
        capture_upstream(state, target, Some(range_hdr), Some(effective_if_range)).await?;
    match status {
        reqwest::StatusCode::OK => Ok(ColdOutcome::Reply(match classify_locked_capture(&cap, lock)
        {
            Ok(()) => match commit_200(state, key, cap).await? {
                CommitOutcome::Strong(v) => {
                    let len = v.total_length.unwrap();
                    let body = read_checked(state, v.id, 0, len).await?;
                    build_response(StatusCode::OK, common_headers(&v), body)
                }
                CommitOutcome::Uncached(_) => return Err(ProxyError::TruncatedUpstream),
            },
            Err(resp) => resp,
        })),
        reqwest::StatusCode::PARTIAL_CONTENT => match classify_locked_capture(&cap, lock) {
            Ok(()) => match store_proven_206(state, key, None, cap, None, None).await? {
                ProveOutcome::Stored(_) => Ok(ColdOutcome::Replan),
                // Tag matched but Content-Range was unusable: upstream fault.
                ProveOutcome::Passthrough(_) => return Err(ProxyError::TruncatedUpstream),
            },
            Err(resp) => Ok(ColdOutcome::Reply(resp)),
        },
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
            let h = passthrough_headers(&cap);
            Ok(ColdOutcome::Reply(build_response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                h,
                Bytes::new(),
            )))
        }
        _ => Err(ProxyError::TruncatedUpstream),
    }
}

/// Locked request whose range is unsatisfiable against the pinned length:
/// re-check with the upstream under the locked strong tag.
async fn locked_unsatisfiable(
    state: &ProxyState,
    target: &url::Url,
    ver: &VersionRow,
    total: u64,
    range_hdr: &str,
    lock: &ETag,
) -> Result<Response> {
    let ir = ETag {
        weak: false,
        raw_tag: ver.etag_tag.clone(),
    }
    .to_wire();
    let (status, cap) = capture_upstream(state, target, Some(range_hdr), Some(&ir)).await?;
    match status {
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
            let h = HeaderMap::from_iter([(
                axum::http::header::CONTENT_RANGE,
                HeaderValue::from_str(&range::unsatisfiable_content_range(total)).unwrap(),
            )]);
            Ok(build_response(StatusCode::RANGE_NOT_SATISFIABLE, h, Bytes::new()))
        }
        reqwest::StatusCode::OK => Ok(match classify_locked_capture(&cap, lock) {
            Ok(()) => match commit_200(state, &target_key(target), cap).await? {
                CommitOutcome::Strong(v) => {
                    let body =
                        read_checked(state, v.id, 0, v.total_length.unwrap()).await?;
                    build_response(StatusCode::OK, common_headers(&v), body)
                }
                CommitOutcome::Uncached(_) => return Err(ProxyError::TruncatedUpstream),
            },
            Err(resp) => resp,
        }),
        _ => Err(ProxyError::TruncatedUpstream),
    }
}

/// Multipart (or any multi-interval) locked request: forward raw and demand
/// the locked validator on the response; multipart bodies are never merged.
async fn locked_multipart_forward(
    state: &ProxyState,
    target: &url::Url,
    range_hdr: Option<&str>,
    if_range: Option<&str>,
    lock: &ETag,
) -> Result<Response> {
    let own_if_range = lock.to_wire();
    let effective_if_range = if_range.unwrap_or(&own_if_range);
    let (status, cap) =
        capture_upstream(state, target, range_hdr, Some(effective_if_range)).await?;
    let sc = match status {
        reqwest::StatusCode::OK => StatusCode::OK,
        reqwest::StatusCode::PARTIAL_CONTENT => StatusCode::PARTIAL_CONTENT,
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => StatusCode::RANGE_NOT_SATISFIABLE,
        _ => return Err(ProxyError::TruncatedUpstream),
    };
    Ok(match classify_locked_capture(&cap, lock) {
        Ok(()) => {
            let body = cap.spool.read_bytes().await?;
            build_response(sc, passthrough_headers(&cap), body)
        }
        Err(resp) => resp,
    })
}

/// Known-length single interval, ordinary (unlocked) planning loop.
async fn serve_range(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    single: range::RawInterval,
    range_raw: Option<&str>,
    if_range: Option<&str>,
) -> Result<Response> {
    let mut repairs = 0;
    for _ in 0..MAX_PLAN_ITERATIONS {
        let key_for_db = key.to_string();
        let latest = with_db(state, move |c| {
            crate::db::latest_strong_version(c, &key_for_db)
        })
        .await?;
        let total = match latest.as_ref().and_then(|v| v.total_length) {
            Some(t) => t,
            // Cold / length unknown: forward the client's Range.
            None => {
                match cold_range(
                    &state,
                    &target,
                    key,
                    range_raw,
                    if_range,
                )
                .await?
                {
                    ColdOutcome::Reply(resp) => return Ok(resp),
                    ColdOutcome::Replan => continue,
                }
            }
        };
        let ver = latest.expect("total length implies a version row");

        let (start, end_inclusive) = match range::resolve(single, total) {
            Ok(v) => v,
            Err(crate::range::Unsatisfiable) => {
                return unsatisfiable(
                    &state,
                    &target,
                    key,
                    &ver,
                    total,
                    range_raw.unwrap(),
                )
                .await;
            }
        };
        // Half-open end used for byte arithmetic / segment coverage.
        let end_excl = end_inclusive + 1;

        // If-Range precondition against the current STRONG validator.
        // A weak validator here never matches; on mismatch the *current*
        // full representation must be returned, which means revalidating
        // upstream rather than trusting the cache.
        if let Some(ir) = if_range {
            let cur = ETag {
                weak: false,
                raw_tag: ver.etag_tag.clone(),
            };
            if !crate::httpdate::if_range_matches(ir, Some(&cur), ver.last_modified) {
                return full_get(&state, &target).await;
            }
        }

        let vid = ver.id;
        let covered =
            with_db(&state, move |c| crate::db::covered_segments(c, vid)).await?;
        let gaps = range::missing_within(start, end_excl, &covered);

        if !gaps.is_empty() {
            // Fill every gap; each fetch proves it still belongs to this
            // exact version via If-Range on the strong ETag.
            let mut changed_version: Option<VersionRow> = None;
            for &(gs, ge) in &gaps {
                let range_hdr = format!("bytes={gs}-{}", ge - 1);
                let ir = ETag {
                    weak: false,
                    raw_tag: ver.etag_tag.clone(),
                }
                .to_wire();
                let (status, cap) =
                    capture_upstream(&state, &target, Some(&range_hdr), Some(&ir)).await?;
                match status {
                    reqwest::StatusCode::PARTIAL_CONTENT => {
                        match store_proven_206(
                            &state,
                            key,
                            Some(&ver),
                            cap,
                            Some((gs, ge)),
                            Some(total),
                        )
                        .await?
                        {
                            ProveOutcome::Stored(_) => {}
                            // Validator/Content-Range contradiction.
                            ProveOutcome::Passthrough(_) => {
                                return Err(ProxyError::TruncatedUpstream)
                            }
                        }
                    }
                    reqwest::StatusCode::OK => {
                        // Object changed under the strong If-Range: the
                        // client's range is invalid against the new
                        // representation. Commit the new version and return
                        // the complete new body with 200 (RFC 9110 13.1.6),
                        // never splicing it into the old range response.
                        changed_version = Some(match commit_200(&state, key, cap).await? {
                            CommitOutcome::Strong(v) => v,
                            CommitOutcome::Uncached(_) => {
                                return Err(ProxyError::TruncatedUpstream)
                            }
                        });
                        break;
                    }
                    _ => return Err(ProxyError::TruncatedUpstream),
                }
            }
            if let Some(newver) = changed_version {
                let len = newver.total_length.unwrap();
                let body = read_checked(&state, newver.id, 0, len).await?;
                return Ok(build_response(
                    StatusCode::OK,
                    common_headers(&newver),
                    body,
                ));
            }
        } else {
            // The whole requested interval is on disk. Disk alone is not
            // proof: two representations may have identical lengths, so
            // confirm the cached version is still current with a strong
            // conditional request before returning any cached bytes.
            match revalidate_cached(&state, &target, key, &ver).await? {
                Revalidation::Fresh => {}
                Revalidation::Changed(_) => {
                    // New representation committed; re-plan against it.
                    continue;
                }
                Revalidation::Unverifiable => {
                    // The current response proves nothing about our cache;
                    // do not serve stale bytes.
                    return Err(ProxyError::TruncatedUpstream);
                }
            }
        }

        // All requested bytes cached for one proven version — read them.
        let read_ver = ver.clone();
        match read_partial(&state, &read_ver, start, end_excl, total).await {
            Ok(resp) => return Ok(resp),
            Err(ProxyError::BlobTruncated) => {
                // On-disk corruption: discard the bad metadata/bytes and
                // re-fetch. Never answer with a shorter "successful" body.
                if repairs >= MAX_CACHE_REPAIRS {
                    return Err(ProxyError::BlobTruncated);
                }
                repairs += 1;
                repair_version(&state, ver.id).await?;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ProxyError::State("upstream version churn; giving up".into()))
}

/// Result of validating a cached version against the upstream.
enum Revalidation {
    /// Upstream confirmed the strong validator (304).
    Fresh,
    /// A new strong full representation was committed.
    Changed(VersionRow),
    /// The upstream answered 200 without a usable strong validator: nothing
    /// can be concluded about the cached bytes.
    Unverifiable,
}

async fn revalidate_cached(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    cached: &VersionRow,
) -> Result<Revalidation> {
    let inm = ETag {
        weak: false,
        raw_tag: cached.etag_tag.clone(),
    }
    .to_wire();
    let resp = state
        .http
        .get(target.clone())
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .header(reqwest::header::IF_NONE_MATCH, &inm)
        .send()
        .await?;
    let status = resp.status();

    if status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(Revalidation::Fresh);
    }
    if status != reqwest::StatusCode::OK {
        return Err(ProxyError::TruncatedUpstream);
    }

    let declared = resp.content_length();
    let header_str = |name: reqwest::header::HeaderName| -> Option<String> {
        resp.headers()
            .get(&name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let etag = header_str(reqwest::header::ETAG).and_then(|s| ETag::parse(&s));
    let last_modified = header_str(reqwest::header::LAST_MODIFIED)
        .and_then(|s| crate::httpdate::parse_http_date(&s));
    let content_type = header_str(reqwest::header::CONTENT_TYPE);

    let mut spool = state.store.new_spool().await?;
    let mut len = 0u64;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        spool.write_all(&chunk).await?;
        len += chunk.len() as u64;
    }
    spool.flush().await?;
    if let Some(want) = declared {
        if want != len {
            return Err(ProxyError::TruncatedUpstream);
        }
    }

    let cap = Captured {
        spool,
        len,
        etag,
        last_modified,
        content_type,
        content_range: None,
    };
    match commit_200(state, key, cap).await? {
        CommitOutcome::Strong(v) => Ok(Revalidation::Changed(v)),
        // Upstream answered 200 without a strong validator: cannot prove
        // anything about the cached bytes. Callers refuse to serve them;
        // callers that hold a client lock turn this into 412 rather than 502.
        CommitOutcome::Uncached(cap) => {
            // The uncached capture is not retained; drop the spool here.
            drop(cap);
            Ok(Revalidation::Unverifiable)
        }
    }
}

async fn read_partial(
    state: &ProxyState,
    ver: &VersionRow,
    start: u64,
    end_excl: u64,
    total: u64,
) -> Result<Response> {
    let body = read_checked(state, ver.id, start, end_excl).await?;
    let mut h = common_headers(ver);
    h.insert(
        axum::http::header::CONTENT_RANGE,
        HeaderValue::from_str(&range::content_range(start, end_excl - 1, total)).unwrap(),
    );
    Ok(build_response(StatusCode::PARTIAL_CONTENT, h, body))
}

async fn read_checked(
    state: &ProxyState,
    version_id: i64,
    start: u64,
    end_excl: u64,
) -> Result<Bytes> {
    match state
        .store
        .open_range(version_id, start, end_excl)
        .await?
        .read_all_checked()
        .await
    {
        Ok(b) => Ok(b),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(ProxyError::BlobTruncated)
        }
        Err(e) => Err(e.into()),
    }
}

async fn repair_version(state: &ProxyState, version_id: i64) -> Result<()> {
    state.store.truncate_blob(version_id).await?;
    with_db_mut(state, move |c| crate::db::reset_segments(c, version_id)).await
}

/// Known length, unsatisfiable range: confirm with the upstream under our
/// strong If-Range rather than trusting a possibly stale cached length.
async fn unsatisfiable(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    ver: &VersionRow,
    total: u64,
    range_hdr: &str,
) -> Result<Response> {
    let ir = ETag {
        weak: false,
        raw_tag: ver.etag_tag.clone(),
    }
    .to_wire();
    let (status, cap) = capture_upstream(state, target, Some(range_hdr), Some(&ir)).await?;
    match status {
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
            let h = HeaderMap::from_iter([(
                axum::http::header::CONTENT_RANGE,
                HeaderValue::from_str(&range::unsatisfiable_content_range(total)).unwrap(),
            )]);
            Ok(build_response(StatusCode::RANGE_NOT_SATISFIABLE, h, Bytes::new()))
        }
        reqwest::StatusCode::OK => {
            // The object really changed: commit the new representation and
            // answer with the complete new body (Range invalid).
            match commit_200(state, key, cap).await? {
                CommitOutcome::Strong(v) => {
                    let body =
                        read_checked(state, v.id, 0, v.total_length.unwrap()).await?;
                    Ok(build_response(StatusCode::OK, common_headers(&v), body))
                }
                CommitOutcome::Uncached(_) => Err(ProxyError::TruncatedUpstream),
            }
        }
        _ => Err(ProxyError::TruncatedUpstream),
    }
}

enum ColdOutcome {
    Reply(Response),
    /// A strong 206 established a version; the caller re-plans.
    Replan,
}

/// No known length yet: forward the client's Range verbatim.
async fn cold_range(
    state: &ProxyState,
    target: &url::Url,
    key: &str,
    range_hdr: Option<&str>,
    if_range: Option<&str>,
) -> Result<ColdOutcome> {
    let (status, cap) = capture_upstream(state, target, range_hdr, if_range).await?;
    match status {
        reqwest::StatusCode::OK => Ok(ColdOutcome::Reply(match commit_200(state, key, cap).await? {
            CommitOutcome::Strong(v) => {
                // Upstream ignored Range / precondition failed: the strong
                // 200 becomes the cached current representation.
                let body = read_checked(state, v.id, 0, v.total_length.unwrap()).await?;
                build_response(StatusCode::OK, common_headers(&v), body)
            }
            CommitOutcome::Uncached(cap) => {
                let body = cap.spool.read_bytes().await?;
                build_response(StatusCode::OK, passthrough_headers(&cap), body)
            }
        })),
        reqwest::StatusCode::PARTIAL_CONTENT => {
            match store_proven_206(state, key, None, cap, None, None).await? {
                ProveOutcome::Stored(_) => Ok(ColdOutcome::Replan),
                ProveOutcome::Passthrough(cap) => {
                    // Weak/no validator, bad Content-Range or multipart:
                    // pass through, never merge into the cache.
                    let body = cap.spool.read_bytes().await?;
                    Ok(ColdOutcome::Reply(build_response(
                        StatusCode::PARTIAL_CONTENT,
                        passthrough_headers(&cap),
                        body,
                    )))
                }
            }
        }
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
            let h = passthrough_headers(&cap);
            Ok(ColdOutcome::Reply(build_response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                h,
                Bytes::new(),
            )))
        }
        _ => Err(ProxyError::TruncatedUpstream),
    }
}

async fn multipart_forward(
    state: &ProxyState,
    target: &url::Url,
    range_hdr: Option<&str>,
    if_range: Option<&str>,
) -> Result<Response> {
    let (status, cap) = capture_upstream(state, target, range_hdr, if_range).await?;
    let body = cap.spool.read_bytes().await?;
    let sc = match status {
        reqwest::StatusCode::OK => StatusCode::OK,
        reqwest::StatusCode::PARTIAL_CONTENT => StatusCode::PARTIAL_CONTENT,
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => StatusCode::RANGE_NOT_SATISFIABLE,
        _ => return Err(ProxyError::TruncatedUpstream),
    };
    Ok(build_response(sc, passthrough_headers(&cap), body))
}

/// Plain GET (no usable Range header). Always revalidates upstream.
async fn full_get(state: &ProxyState, target: &url::Url) -> Result<Response> {
    let key = target_key(target);
    let (status, cap) = capture_upstream(state, target, None, None).await?;
    match status {
        reqwest::StatusCode::OK => Ok(match commit_200(state, &key, cap).await? {
            CommitOutcome::Strong(v) => {
                let body = read_checked(state, v.id, 0, v.total_length.unwrap()).await?;
                build_response(StatusCode::OK, common_headers(&v), body)
            }
            CommitOutcome::Uncached(cap) => {
                // Weak/missing validator: pass through, do not cache.
                let body = cap.spool.read_bytes().await?;
                build_response(StatusCode::OK, passthrough_headers(&cap), body)
            }
        }),
        _ => Err(ProxyError::TruncatedUpstream),
    }
}

enum CommitOutcome {
    Strong(VersionRow),
    /// Validator weak/absent: capture handed back for uncached passthrough.
    Uncached(Captured),
}

/// Commit a strong-200 full capture as a fresh version.
///
/// Equal length is never treated as equal content: a new strong ETag gets a
/// distinct version row and its blob is physically replaced with the
/// freshly verified capture.
async fn commit_200(
    state: &ProxyState,
    key: &str,
    cap: Captured,
) -> Result<CommitOutcome> {
    let tag = match cap.etag {
        Some(t) if !t.weak => t,
        other => return Ok(CommitOutcome::Uncached(Captured {
            etag: other,
            ..cap
        })),
    };
    let key_s = key.to_string();
    let tag_s = tag.raw_tag.clone();
    let lm = cap.last_modified;
    let len = cap.len;
    let ct = cap.content_type.clone();
    let id = with_db_mut(state, move |c| {
        crate::db::upsert_full_version(c, &key_s, &tag_s, lm, len, ct.as_deref())
    })
    .await?;
    cap.spool
        .persist_rename(&state.store.blob_path_for(id))
        .await?;
    let key_find = key.to_string();
    let tag_find = tag.raw_tag.clone();
    let v = with_db(state, move |c| crate::db::find_version(c, &key_find, &tag_find))
        .await?
        .expect("version row exists after upsert");
    Ok(CommitOutcome::Strong(v))
}

enum ProveOutcome {
    #[allow(dead_code)]
    Stored(VersionRow),
    /// Cannot prove: weak/no ETag, malformed Content-Range or multipart.
    Passthrough(Captured),
}

/// Validate and record a 206 capture.
///
/// * `expected`: version the fetch was proving (gap fill). The response's
///   strong ETag must equal it; `None` is used on the cold path.
/// * `exact`: the Content-Range interval must equal it (gap fills are exact
///   fetches).
/// * `known_total`: Content-Range's total must agree with this.
///
/// Bytes are written to the version's own blob *before* the segment row is
/// committed.
async fn store_proven_206(
    state: &ProxyState,
    key: &str,
    expected: Option<&VersionRow>,
    cap: Captured,
    exact: Option<(u64, u64)>,
    known_total: Option<u64>,
) -> Result<ProveOutcome> {
    let tag = match cap.etag.clone() {
        Some(t) if !t.weak => t,
        other => {
            return Ok(ProveOutcome::Passthrough(Captured {
                etag: other,
                ..cap
            }))
        }
    };
    let multipart = cap
        .content_type
        .as_deref()
        .map(|ct| ct.to_ascii_lowercase().starts_with("multipart/byteranges"))
        .unwrap_or(false);
    let parsed_cr = cap.content_range.as_deref().and_then(range::parse_content_range);
    let (s, e, total) = match parsed_cr {
        Some(v) => v,
        None => return Ok(ProveOutcome::Passthrough(cap)),
    };

    let valid = !multipart
        && e >= s
        && e < total.unwrap_or(u64::MAX)
        && e + 1 - s == cap.len
        && exact.map(|(gs, ge)| s == gs && e + 1 == ge).unwrap_or(true)
        && known_total.map(|kt| total == Some(kt)).unwrap_or(true)
        && expected
            .map(|exp| tag.raw_tag == exp.etag_tag)
            .unwrap_or(true);
    if !valid {
        return Ok(ProveOutcome::Passthrough(cap));
    }

    let key_s = key.to_string();
    let tag_s = tag.raw_tag.clone();
    let lm = cap.last_modified;
    let ct = cap.content_type.clone();
    let id = with_db_mut(state, move |c| {
        crate::db::insert_partial_version(c, &key_s, &tag_s, lm, ct.as_deref())
    })
    .await?;

    cap.spool
        .copy_into_blob(&state.store.blob_path_for(id), s)
        .await?;

    with_db_mut(state, move |c| {
        crate::db::add_segment(c, id, s, e + 1)?;
        if let Some(t) = total {
            crate::db::set_total_length(c, id, t)?;
        }
        Ok::<_, rusqlite::Error>(())
    })
    .await?;

    let key_find = key.to_string();
    let tag_find = tag.raw_tag.clone();
    let v = with_db(state, move |c| crate::db::find_version(c, &key_find, &tag_find))
        .await?
        .expect("version row exists after insert");
    Ok(ProveOutcome::Stored(v))
}

// ---------------------------------------------------------------------------
// SQLite helpers
// ---------------------------------------------------------------------------

async fn with_db<T, F>(state: &ProxyState, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
{
    let db = state.db.clone();
    tokio::task::spawn_blocking(move || {
        let guard = db.blocking_lock();
        f(&guard)
    })
    .await
    .expect("blocking task panicked")
    .map_err(ProxyError::Db)
}

async fn with_db_mut<T, F>(state: &ProxyState, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
{
    let db = state.db.clone();
    tokio::task::spawn_blocking(move || {
        let mut guard = db.blocking_lock();
        f(&mut guard)
    })
    .await
    .expect("blocking task panicked")
    .map_err(ProxyError::Db)
}
