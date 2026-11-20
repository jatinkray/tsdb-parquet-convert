//! Where converted files end up: a local directory or an object store.
//!
//! Object stores are written through a local staging directory and a plain
//! `put` per file, which is atomic on S3/GCS/Azure and keeps one code path for
//! file assembly. Peak local disk use is one output file, not one run.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

/// Schemes forwarded to `object_store::parse_url`.
const REMOTE_SCHEMES: &[&str] = &["s3", "gs", "az", "https", "http"];

/// Where `convert --out` writes.
#[derive(Clone)]
pub enum Destination {
    Local(PathBuf),
    Remote {
        url: String,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}

impl std::fmt::Debug for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(path) => f.debug_tuple("Local").field(path).finish(),
            Self::Remote { url, prefix, .. } => f
                .debug_struct("Remote")
                .field("url", url)
                .field("prefix", prefix)
                .finish(),
        }
    }
}

impl Destination {
    /// Parses `--out`: a filesystem path, or an object-store URL.
    pub fn parse(value: &str) -> Result<Self> {
        if let Some((scheme, _)) = value.split_once("://") {
            if !REMOTE_SCHEMES.contains(&scheme) {
                anyhow::bail!(
                    "unsupported output scheme `{scheme}://`; use a directory or one of {}",
                    REMOTE_SCHEMES.join(", ")
                );
            }
            let url =
                url::Url::parse(value).with_context(|| format!("parsing output URL {value}"))?;
            let (store, prefix) = object_store::parse_url(&url)
                .with_context(|| format!("opening output store {value}"))?;
            return Ok(Self::Remote {
                url: value.trim_end_matches('/').to_string(),
                store: Arc::from(store),
                prefix,
            });
        }
        Ok(Self::Local(PathBuf::from(value)))
    }

    /// True when files must be uploaded after they are written.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }

    /// Human-readable location for logs and manifests.
    pub fn describe(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Remote { url, .. } => url.clone(),
        }
    }

    /// Copies one written file to its final place. Local destinations are
    /// already written in place, so this is only the upload path.
    pub fn publish(&self, local_path: &Path, filename: &str) -> Result<()> {
        let Self::Remote {
            store, prefix, url, ..
        } = self
        else {
            return Ok(());
        };
        let destination = prefix.clone().join(filename);
        let bytes = std::fs::read(local_path)
            .with_context(|| format!("reading staged file {}", local_path.display()))?;
        let size = bytes.len();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("building the upload runtime")?;
        runtime
            .block_on(store.put(&destination, bytes.into()))
            .with_context(|| format!("uploading {url}/{destination}"))?;
        tracing::debug!(file = %destination, size, "uploaded");
        Ok(())
    }

    /// Writes a small text object next to the data files (`manifest.json`, …).
    pub fn write_text(&self, name: &str, body: &str) -> Result<()> {
        match self {
            Self::Local(path) => {
                let target = path.join(name);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("creating {}", parent.display()))?;
                }
                std::fs::write(&target, body)
                    .with_context(|| format!("writing {}", target.display()))?;
                Ok(())
            }
            Self::Remote { store, prefix, .. } => {
                let destination = prefix.clone().join(name);
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("building the upload runtime")?;
                runtime
                    .block_on(store.put(&destination, body.to_string().into()))
                    .with_context(|| format!("writing {destination}"))?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filesystem_paths() -> Result<()> {
        let destination = Destination::parse("/tmp/out")?;
        assert!(!destination.is_remote());
        assert_eq!(destination.describe(), "/tmp/out");
        Ok(())
    }

    #[test]
    fn rejects_unknown_schemes() -> Result<()> {
        let error = match Destination::parse("ftp://example.com/out") {
            Ok(_) => anyhow::bail!("an ftp destination should not parse"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unsupported output scheme"));
        Ok(())
    }

    fn memory_destination() -> Destination {
        Destination::Remote {
            url: "memory://bucket/prefix".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: ObjectPath::from("prefix"),
        }
    }

    #[test]
    fn remote_publish_uploads_the_staged_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let staged = dir.path().join("data_0_1_00000.parquet");
        std::fs::write(&staged, b"parquet-bytes")?;
        let destination = memory_destination();
        destination.publish(&staged, "data_0_1_00000.parquet")?;
        let Destination::Remote { store, prefix, .. } = &destination else {
            anyhow::bail!("expected a remote destination");
        };
        let object = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(store.get(&prefix.clone().join("data_0_1_00000.parquet")))?;
        let bytes = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(object.bytes())?;
        assert_eq!(&bytes[..], b"parquet-bytes");
        Ok(())
    }

    #[test]
    fn remote_text_writes_support_nested_names() -> Result<()> {
        let destination = memory_destination();
        destination.write_text("manifests/run.json", "{}")?;
        let Destination::Remote { store, prefix, .. } = &destination else {
            anyhow::bail!("expected a remote destination");
        };
        let object = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(store.get(&prefix.clone().join("manifests/run.json")))?;
        assert_eq!(object.meta.size, 2);
        Ok(())
    }

    #[test]
    fn local_text_writes_land_in_the_directory() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let destination = Destination::parse(dir.path().to_str().context("utf-8 path")?)?;
        destination.write_text("manifest.json", "{}")?;
        assert!(dir.path().join("manifest.json").exists());
        Ok(())
    }
}
