//! A caching reverse proxy that understands HTTP Range requests.
//!
//! The proxy is pinned to exactly one configured upstream base URL (a local
//! test service). See `proxy` for the request handling logic, `db` for the
//! SQLite metadata and `store` for the byte-backed blob files.

pub mod config;
pub mod db;
pub mod error;
pub mod etag;
pub mod httpdate;
pub mod proxy;
pub mod range;
pub mod store;

#[cfg(feature = "test-support")]
pub mod support;

pub use config::ProxyConfig;
pub use proxy::{build_app, ProxyState};
