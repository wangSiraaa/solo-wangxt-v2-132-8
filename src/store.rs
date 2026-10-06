//! Byte storage.
//!
//! Every version has its own blob file `blobs/v<id>.bin`; byte offsets in
//! `segments` index into that file. Versions never share a blob, so merging
//! segments can only happen after strong ETags proved the bytes belong to
//! the same representation.
//!
//! Upstream bytes are first captured in a `Spool` temp file. They only land
//! in the real blob (and the segment is only recorded) after the upstream
//! response was received in full with the expected length. If the client
//! disconnects while spooling, the future is cancelled and the temp file is
//! removed — there is no background task that keeps downloading.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::Result;

#[derive(Clone)]
pub struct BlobStore {
    root: PathBuf,
    tmp: PathBuf,
    counter: Arc<AtomicU64>,
}

impl BlobStore {
    pub async fn new(cache_dir: &Path) -> Result<BlobStore> {
        let root = cache_dir.join("blobs");
        let tmp = cache_dir.join("tmp");
        fs::create_dir_all(&root).await?;
        fs::create_dir_all(&tmp).await?;
        Ok(BlobStore {
            root,
            tmp,
            counter: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn blob_path_for(&self, version_id: i64) -> PathBuf {
        self.root.join(format!("v{version_id}.bin"))
    }

    /// Truncate a damaged blob back to empty (paired with resetting the
    /// version's segments in SQLite).
    pub async fn truncate_blob(&self, version_id: i64) -> Result<()> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.blob_path_for(version_id))
            .await?;
        f.set_len(0).await?;
        Ok(())
    }

    /// Open a verified slice `[start, end)` of a version blob. The returned
    /// reader fails with `UnexpectedEof` if the file is shorter than the
    /// metadata claims — the handler treats that as cache corruption, never
    /// as a successful body.
    pub async fn open_range(
        &self,
        version_id: i64,
        start: u64,
        end: u64,
    ) -> Result<BlobReader> {
        let path = self.blob_path_for(version_id);
        let mut f = fs::File::open(&path).await?;
        f.seek(std::io::SeekFrom::Start(start)).await?;
        Ok(BlobReader {
            file: f,
            remaining: end - start,
        })
    }

    pub(crate) async fn new_spool(&self) -> Result<Spool> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "spool-{}-{}-{n}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let path = self.tmp.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .await?;
        Ok(Spool {
            path,
            file: Some(file),
            len: 0,
            consumed: false,
        })
    }
}

/// Reader over exactly `remaining` bytes of a blob.
pub struct BlobReader {
    file: fs::File,
    remaining: u64,
}

impl BlobReader {
    /// Read the whole slice into memory, verifying the on-disk length.
    /// Returns `UnexpectedEof` when the blob was truncated underneath us.
    pub async fn read_all_checked(mut self) -> std::io::Result<bytes::Bytes> {
        let want = self.remaining as usize;
        let mut buf = Vec::with_capacity(want.clamp(1, 4 * 1024 * 1024));
        let mut chunk = vec![0u8; 64 * 1024];
        let mut got = 0u64;
        while got < self.remaining {
            let max = std::cmp::min(chunk.len() as u64, self.remaining - got) as usize;
            let n = self.file.read(&mut chunk[..max]).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "cached blob shorter than recorded segments",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
            got += n as u64;
        }
        debug_assert_eq!(buf.len(), want);
        Ok(bytes::Bytes::from(buf))
    }
}

/// A temp file collecting one upstream response. Dropping it removes the
/// file, including when the owning request future is cancelled because the
/// client went away.
pub struct Spool {
    path: PathBuf,
    file: Option<fs::File>,
    len: u64,
    consumed: bool,
}

impl Spool {
    pub async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.file.as_mut().unwrap().write_all(data).await?;
        self.len += data.len() as u64;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.file.as_mut().unwrap().flush().await
    }

    /// Read the captured bytes back (passthrough responses are buffered in
    /// the temp file rather than held in RAM).
    pub async fn read_bytes(&self) -> std::io::Result<bytes::Bytes> {
        let mut f = fs::File::open(&self.path).await?;
        let mut buf = Vec::with_capacity(self.len as usize);
        f.read_to_end(&mut buf).await?;
        if buf.len() as u64 != self.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "spool shorter than expected",
            ));
        }
        Ok(bytes::Bytes::from(buf))
    }

    /// Atomically replace `dst` with this spool (complete representations).
    pub async fn persist_rename(self, dst: &Path) -> std::io::Result<()> {
        let mut this = self;
        let f = this.file.take().unwrap();
        f.sync_all().await?;
        drop(f);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&this.path, dst).await?;
        this.consumed = true;
        Ok(())
    }

    /// Copy the captured bytes into `dst` at absolute offset `start`,
    /// returning the number of bytes written. The spool is consumed.
    pub async fn copy_into_blob(self, dst: &Path, start: u64) -> std::io::Result<u64> {
        let mut this = self;
        let f = this.file.take().unwrap();
        f.sync_all().await?;
        drop(f);
        let len = this.len;
        let src = this.path.clone();
        let dst = dst.to_path_buf();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::{Read, Seek, Write};
            let mut src = std::fs::File::open(&src)?;
            src.seek(std::io::SeekFrom::Start(0))?;
            let mut dst = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&dst)?;
            dst.seek(std::io::SeekFrom::Start(start))?;
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let n = src.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                dst.write_all(&chunk[..n])?;
            }
            dst.sync_all()?;
            Ok(())
        })
        .await
        .expect("blocking task panicked")?;
        this.consumed = true;
        Ok(len)
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
