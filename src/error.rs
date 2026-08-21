//! Central error type for the crate.
//!
//! Follows the pattern in [`stac-utils/rustac`](https://github.com/stac-utils/rustac):
//! one enum with `#[error(transparent)]` `#[from]` variants for upstream errors and
//! structured variants for domain-specific failures. Callers use [`crate::Result`].
//!
//! Context around an underlying error is added via [`ResultExt::context`], which
//! wraps in an [`Error::Context`] variant while preserving the source chain.

use std::path::PathBuf;

use thiserror::Error;

/// The crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    // ─── Upstream error wrappers ────────────────────────────────────────────
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    #[error(transparent)]
    ObjectStore(#[from] Box<object_store::Error>),

    #[error(transparent)]
    Parquet(#[from] Box<parquet::errors::ParquetError>),

    #[error(transparent)]
    Http(#[from] Box<reqwest::Error>),

    #[error(transparent)]
    Stac(#[from] Box<stac::Error>),

    #[error(transparent)]
    StacIo(#[from] Box<stac_io::Error>),

    // ─── Adds context to another error while keeping the source chain ───────
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<Error>,
    },

    // ─── Domain-specific variants ───────────────────────────────────────────
    #[error("URI must point at bucket root (no path segment): {0}")]
    UriHasPath(String),

    #[error("--schema-version is required when --release-version is provided")]
    SchemaVersionRequired,

    #[error("--release-version must be in format YYYY-MM-DD.N (got: {0})")]
    InvalidReleaseVersion(String),

    #[error("--schema-version must be in format X.Y.Z (got: {0})")]
    InvalidSchemaVersion(String),

    #[error("parse release date from {0}")]
    ParseReleaseDate(String),

    #[error("no `geo` metadata in {0}")]
    MissingGeoMetadata(String),

    #[error("no columns.geometry in geo metadata for {0}")]
    MissingGeometryColumn(String),

    #[error("no bbox in geometry metadata for {0}")]
    MissingBbox(String),

    #[error("bbox coord {index} not a number in {key}")]
    InvalidBboxCoord { key: String, index: usize },

    #[error("no id column or statistics in {0}")]
    MissingIdStatistics(String),

    #[error("could not read local dir {0}")]
    ReadDir(PathBuf),
}

// Boxed From conversions — thiserror only wires up `#[from]` on the boxed types,
// so add manual From impls for the un-boxed originals to keep call-site `?`
// ergonomics.
impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::ObjectStore(Box::new(e))
    }
}
impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(Box::new(e))
    }
}
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(Box::new(e))
    }
}
impl From<stac::Error> for Error {
    fn from(e: stac::Error) -> Self {
        Error::Stac(Box::new(e))
    }
}
impl From<stac_io::Error> for Error {
    fn from(e: stac_io::Error) -> Self {
        Error::StacIo(Box::new(e))
    }
}

/// Extension trait providing ergonomic `.context()` on any `Result<T, E: Into<Error>>`.
///
/// Mirrors `anyhow::Context` closely: wraps the inner error in [`Error::Context`],
/// preserving the source chain and prepending a message.
pub trait ResultExt<T> {
    fn context<C: Into<String>>(self, ctx: C) -> Result<T>;
    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T>;
}

impl<T, E: Into<Error>> ResultExt<T> for std::result::Result<T, E> {
    fn context<C: Into<String>>(self, ctx: C) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: ctx.into(),
            source: Box::new(e.into()),
        })
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: f().into(),
            source: Box::new(e.into()),
        })
    }
}
