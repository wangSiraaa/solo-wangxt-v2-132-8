//! SQLite metadata: objects, their versions (identified by strong ETags) and
//! the byte intervals of each version present in the local blob file.
//!
//! Segment rows are *never* shared across versions. Two versions of one
//! object only get the same row set when bytes were actually fetched under
//! each version's own strong validator, so equal file lengths can never
//! confuse versions.

use rusqlite::{params, Connection, OptionalExtension};

/// Metadata row for one stored representation.
#[derive(Debug, Clone)]
pub struct VersionRow {
    pub id: i64,
    /// ETag without quotes / `W/` prefix.
    pub etag_tag: String,
    pub weak: bool,
    pub last_modified: Option<i64>,
    /// Total representation length once the representation is complete;
    /// `None` while only partial segments are known.
    pub total_length: Option<u64>,
    pub content_type: Option<String>,
}

pub fn open(path: &std::path::Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS objects (
            id   INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS versions (
            id          INTEGER PRIMARY KEY,
            object_id   INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
            etag_tag    TEXT NOT NULL,
            weak        INTEGER NOT NULL DEFAULT 0,
            last_modified INTEGER,
            total_length INTEGER,
            content_type TEXT,
            created_at  INTEGER NOT NULL,
            UNIQUE(object_id, etag_tag)
        );
        CREATE TABLE IF NOT EXISTS segments (
            id         INTEGER PRIMARY KEY,
            version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
            start      INTEGER NOT NULL,
            end        INTEGER NOT NULL,
            UNIQUE(version_id, start)
        );
        "#,
    )?;
    Ok(conn)
}

fn object_id(conn: &Connection, path: &str) -> rusqlite::Result<i64> {
    conn.execute(
        "INSERT INTO objects(path) VALUES (?1) ON CONFLICT(path) DO NOTHING",
        params![path],
    )?;
    conn.query_row("SELECT id FROM objects WHERE path = ?1", params![path], |r| {
        r.get(0)
    })
}

fn row_from(r: &rusqlite::Row<'_>) -> rusqlite::Result<VersionRow> {
    let total: Option<i64> = r.get("total_length")?;
    Ok(VersionRow {
        id: r.get("id")?,
        etag_tag: r.get("etag_tag")?,
        weak: r.get::<_, i64>("weak")? != 0,
        last_modified: r.get("last_modified")?,
        total_length: total.map(|v| v as u64),
        content_type: r.get("content_type")?,
    })
}

/// Most recently seen *strong* version of an object. Weak versions are not
/// recorded, so they can never satisfy a later range request.
pub fn latest_strong_version(
    conn: &Connection,
    path: &str,
) -> rusqlite::Result<Option<VersionRow>> {
    conn.query_row(
        "SELECT v.* FROM versions v
         JOIN objects o ON o.id = v.object_id
         WHERE o.path = ?1 AND v.weak = 0
         ORDER BY v.id DESC LIMIT 1",
        params![path],
        row_from,
    )
    .optional()
}

/// Find a version by its exact ETag within one object.
pub fn find_version(
    conn: &Connection,
    path: &str,
    etag_tag: &str,
) -> rusqlite::Result<Option<VersionRow>> {
    conn.query_row(
        "SELECT v.* FROM versions v
         JOIN objects o ON o.id = v.object_id
         WHERE o.path = ?1 AND v.etag_tag = ?2",
        params![path, etag_tag],
        row_from,
    )
    .optional()
}

/// Insert (or reuse) a not-yet-complete version discovered via a 206.
pub fn insert_partial_version(
    conn: &mut Connection,
    path: &str,
    etag_tag: &str,
    last_modified: Option<i64>,
    content_type: Option<&str>,
) -> rusqlite::Result<i64> {
    let tx = conn.transaction()?;
    let oid = object_id(&tx, path)?;
    if let Some(existing) = tx
        .query_row(
            "SELECT id FROM versions WHERE object_id = ?1 AND etag_tag = ?2",
            params![oid, etag_tag],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
    {
        tx.commit()?;
        return Ok(existing);
    }
    tx.execute(
        "INSERT INTO versions
             (object_id, etag_tag, weak, last_modified, total_length, content_type, created_at)
         VALUES (?1, ?2, 0, ?3, NULL, ?4, strftime('%s','now'))",
        params![oid, etag_tag, last_modified, content_type],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

/// Record a complete representation: (re)create the version row and replace
/// its segment set with the single full interval `[0, len)`.
pub fn upsert_full_version(
    conn: &mut Connection,
    path: &str,
    etag_tag: &str,
    last_modified: Option<i64>,
    len: u64,
    content_type: Option<&str>,
) -> rusqlite::Result<i64> {
    let tx = conn.transaction()?;
    let oid = object_id(&tx, path)?;
    tx.execute(
        "INSERT INTO versions
             (object_id, etag_tag, weak, last_modified, total_length, content_type, created_at)
         VALUES (?1, ?2, 0, ?3, ?4, ?5, strftime('%s','now'))
         ON CONFLICT(object_id, etag_tag) DO UPDATE SET
             last_modified = excluded.last_modified,
             total_length   = excluded.total_length,
             content_type   = excluded.content_type",
        params![oid, etag_tag, last_modified, len as i64, content_type],
    )?;
    let id = tx.query_row(
        "SELECT id FROM versions WHERE object_id = ?1 AND etag_tag = ?2",
        params![oid, etag_tag],
        |r| r.get(0),
    )?;
    tx.execute("DELETE FROM segments WHERE version_id = ?1", params![id])?;
    tx.execute(
        "INSERT INTO segments(version_id, start, end) VALUES (?1, 0, ?2)",
        params![id, len as i64],
    )?;
    tx.commit()?;
    Ok(id)
}

/// Half-open intervals cached for a version, sorted and disjoint.
pub fn covered_segments(conn: &Connection, version_id: i64) -> rusqlite::Result<Vec<(u64, u64)>> {
    let mut rows: Vec<(u64, u64)> = conn
        .prepare("SELECT start, end FROM segments WHERE version_id = ?1 ORDER BY start")?
        .query_map(params![version_id], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64))
        })?
        .collect::<rusqlite::Result<_>>()?;
    rows.retain(|&(s, e)| e > s);
    Ok(rows)
}

/// Merge `[start, end)` into the cached intervals of one version.
pub fn add_segment(
    conn: &mut Connection,
    version_id: i64,
    start: u64,
    end: u64,
) -> rusqlite::Result<()> {
    if start >= end {
        return Ok(());
    }
    let mut intervals: Vec<(u64, u64)> = {
        let mut stmt = conn.prepare(
            "SELECT start, end FROM segments WHERE version_id = ?1 ORDER BY start",
        )?;
        let mut rows = stmt.query(params![version_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64));
        }
        out
    };
    intervals = crate::range::merge_interval(intervals, start, end);

    let tx = conn.transaction()?;
    tx.execute("DELETE FROM segments WHERE version_id = ?1", params![version_id])?;
    {
        let mut stmt =
            tx.prepare("INSERT INTO segments(version_id, start, end) VALUES (?1, ?2, ?3)")?;
        for (s, e) in &intervals {
            stmt.execute(params![version_id, *s as i64, *e as i64])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Declare the total length of a version whose segments already cover the
/// whole `[0, total)` interval.
pub fn set_total_length(
    conn: &mut Connection,
    version_id: i64,
    total: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE versions SET total_length = ?1 WHERE id = ?2",
        params![total as i64, version_id],
    )?;
    Ok(())
}

/// Drop all cached intervals of one version after its blob was found
/// damaged/truncated. The blob itself is also truncated by the caller.
pub fn reset_segments(conn: &mut Connection, version_id: i64) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE versions SET total_length = NULL WHERE id = ?1",
        params![version_id],
    )?;
    conn.execute("DELETE FROM segments WHERE version_id = ?1", params![version_id])?;
    Ok(())
}
