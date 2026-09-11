//! `validate` — run JSON-schema + link-integrity + Overture-specific checks against
//! a built catalog. Target is one of: local dir, catalog root URL, or catalog object-store URI.

use std::path::PathBuf;

use overture_stac::stac::{validate_catalog, validate_catalog_uri, validate_url, ValidateOptions};
use overture_stac::{Error, Result};

use super::default_concurrency;

#[derive(clap::Args, Debug)]
#[command(group(clap::ArgGroup::new("target").required(true).args(["dir", "url", "catalog_uri"])))]
pub struct ValidateArgs {
    /// Local directory containing the built catalog (must have catalog.json at
    /// the root). Mutually exclusive with --url and --catalog-uri.
    #[arg(value_name = "DIR")]
    dir: Option<PathBuf>,

    /// Remote catalog root URL to fetch and crawl over HTTP (e.g.
    /// https://stac.overturemaps.org/catalog.json). Tests what the CDN serves
    /// to users. Intended for a scheduled prod health check that runs
    /// independently of publish.
    #[arg(long)]
    url: Option<String>,

    /// Object-store URI of the catalog storage (e.g.
    /// s3://overturemaps-extras-us-west-2/stac/). Tests the source of truth
    /// in the bucket, independent of CDN cache state. Anonymous S3 access is
    /// used when no AWS credentials are set in the environment.
    #[arg(long = "catalog-uri")]
    catalog_uri: Option<String>,

    /// Concurrent HTTP GETs / object-store GETs (remote/bucket, capped at 16)
    /// or file-check futures (local mode). Defaults to num_cpus / 2 (min 1).
    #[arg(long)]
    concurrency: Option<usize>,

    /// Emit machine-readable JSON summary instead of pretty text.
    #[arg(long, default_value_t = false)]
    json: bool,
}

pub async fn run(args: ValidateArgs) -> Result<()> {
    let concurrency = args.concurrency.unwrap_or_else(default_concurrency);
    let opts = ValidateOptions::default();
    let report = match (args.url, args.catalog_uri, args.dir) {
        (Some(url), _, _) => validate_url(&url, concurrency, opts).await?,
        (_, Some(uri), _) => validate_catalog_uri(&uri, concurrency, opts).await?,
        (_, _, Some(dir)) => validate_catalog(&dir, concurrency, opts).await?,
        (None, None, None) => unreachable!("clap's ArgGroup requires exactly one target"),
    };
    report.print(args.json);
    if !report.is_ok() {
        return Err(Error::ValidationFailed(report.failures.len()));
    }
    Ok(())
}
