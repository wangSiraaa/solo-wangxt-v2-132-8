use std::path::PathBuf;

/// Static configuration for a proxy instance.
///
/// `upstream_base` is the *only* origin the proxy will ever talk to. Requests
/// are allowed only when the reconstructed target URL has exactly the same
/// scheme, host and port as this base; reqwest redirects are disabled as
/// well, so the allow-list cannot be bypassed.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// e.g. `http://127.0.0.1:9000/root/` — must end with `/`.
    pub upstream_base: String,
    /// Directory containing `range-cache.sqlite3` and the `blobs/` tree.
    pub cache_dir: PathBuf,
    /// When true (default), non-loopback upstream origins are refused at
    /// startup. This is an internal test tool, not an open relay.
    pub require_loopback_upstream: bool,
}

impl ProxyConfig {
    pub fn new(upstream_base: impl Into<String>, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            upstream_base: upstream_base.into(),
            cache_dir: cache_dir.into(),
            require_loopback_upstream: true,
        }
    }
}
