use axum::http::StatusCode;

/// Internal error type. Most variants map to a 502/500 response; protocol
/// level outcomes (416, upstream 200/206) are handled explicitly instead,
/// and the client version-lock variants carry their own 4xx statuses.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream I/O failure: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("upstream refused to serve bytes (truncated or malformed)")]
    TruncatedUpstream,
    /// A cached blob file was shorter than the SQLite segments claimed.
    #[error("cached blob was truncated on disk")]
    BlobTruncated,
    #[error("metadata store failure: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("blob file failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream is not the configured local test service")]
    UpstreamNotAllowed,
    #[error("requested path escapes the upstream prefix")]
    PathEscape,
    /// The client's `If-Match` version lock could not be honored: the
    /// current upstream version differs from the lock, or cannot be proven.
    /// The response carries no representation body.
    #[error("version lock not satisfied: {0}")]
    LockMismatch(String),
    /// The `If-Match` lock value itself is unusable (this proxy accepts
    /// exactly one strong entity-tag); rejected before any upstream traffic.
    #[error("unusable version lock: {0}")]
    LockMalformed(String),
    #[error("internal state error: {0}")]
    State(String),
}

impl ProxyError {
    pub fn status(&self) -> StatusCode {
        match self {
            // The configured upstream behaved wrongly (truncated body, sent
            // Content-Range that contradicts its length, ...). Never turn
            // that into a successful complete response.
            ProxyError::TruncatedUpstream
            | ProxyError::BlobTruncated
            | ProxyError::Upstream(_) => StatusCode::BAD_GATEWAY,
            ProxyError::UpstreamNotAllowed | ProxyError::PathEscape => StatusCode::FORBIDDEN,
            // The version lock precondition failed (or can never hold for
            // a weak validator): 412, with no representation body.
            ProxyError::LockMismatch(_) => StatusCode::PRECONDITION_FAILED,
            // The lock value itself is garbage: 400, before any upstream
            // request was made.
            ProxyError::LockMalformed(_) => StatusCode::BAD_REQUEST,
            ProxyError::Db(_) | ProxyError::Io(_) | ProxyError::State(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, ProxyError>;
