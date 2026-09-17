//! Subcommand dispatch — clap arg structs and the async handler for each
//! `overture-stac <subcommand>`. `main.rs` builds a [`Cli`], matches the
//! variant, and calls the matching handler here.

use clap::Subcommand;

use overture_stac::stac::{validate_catalog, ValidateOptions};
use overture_stac::{Error, Result};

pub mod build;
pub mod list_releases;
pub mod reconcile;
pub mod validate;

pub use build::BuildArgs;
pub use list_releases::ListReleasesArgs;
pub use reconcile::ReconcileArgs;
pub use validate::ValidateArgs;

pub(crate) const PROD_ROOT_HREF: &str = "https://stac.overturemaps.org";
pub(crate) const PROD_DATA_URI: &str = "s3://overturemaps-us-west-2";
pub(crate) const PROD_EXTRAS_URI: &str = "s3://overturemaps-extras-us-west-2";
pub(crate) const PROD_CATALOG_URI: &str = "s3://overturemaps-extras-us-west-2/stac/";

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Build a STAC catalog.
    Build(BuildArgs),
    /// List release IDs currently in the data bucket, newest first.
    ListReleases(ListReleasesArgs),
    /// Compare the live STAC catalog against the data bucket and report drift.
    /// Read-only; exits non-zero when the catalog is out of sync.
    Reconcile(ReconcileArgs),
    /// Validate a built STAC catalog on disk (JSON schema, link integrity,
    /// Overture-specific rules). Exits non-zero on any failure.
    Validate(ValidateArgs),
}

impl Command {
    pub async fn run(self) -> Result<()> {
        match self {
            Command::Build(args) => build::run(args).await,
            Command::ListReleases(args) => list_releases::run(args).await,
            Command::Reconcile(args) => reconcile::run(args).await,
            Command::Validate(args) => validate::run(args).await,
        }
    }
}

pub(crate) fn default_concurrency() -> usize {
    (num_cpus::get() / 2).max(1)
}

pub(crate) async fn run_validation(dir: &std::path::Path, json: bool) -> Result<()> {
    let report = validate_catalog(dir, default_concurrency(), ValidateOptions::default()).await?;
    report.print(json);
    if !report.is_ok() {
        return Err(Error::ValidationFailed(report.failures.len()));
    }
    Ok(())
}
