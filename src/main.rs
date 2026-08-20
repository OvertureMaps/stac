use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use regex::Regex;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

use overture_stac::{
    catalog::{
        build_single_release, build_top_catalog, link_neighbor_releases, list_release_ids,
        save_absolute_published,
    },
    s3::Bucket,
};

const PROD_ROOT_HREF: &str = "https://stac.overturemaps.org";
const PROD_DATA_URI: &str = "s3://overturemaps-us-west-2";
const PROD_EXTRAS_URI: &str = "s3://overturemaps-extras-us-west-2";

/// Generate a STAC index for Overture Maps data from the public release bucket.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Build a STAC catalog.
    Build(BuildArgs),
    /// List release IDs currently in the data bucket, newest first.
    ListReleases(ListReleasesArgs),
}

#[derive(clap::Args, Debug)]
struct ListReleasesArgs {
    /// Object-store URI to the data bucket (s3://, gs://, az://, file:// ...).
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,
}

#[derive(clap::Args, Debug)]
struct BuildArgs {
    /// Output path for the catalog.
    #[arg(long, default_value = "public_releases")]
    output: PathBuf,

    /// Object-store URI to the data bucket (s3://, gs://, az://, file:// ...).
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,

    /// Object-store URI to the extras bucket (holds PMTiles). Pass an empty string to skip.
    #[arg(long = "extras-uri", default_value = PROD_EXTRAS_URI)]
    extras_uri: String,

    /// Sample mode — only 1 item per collection.
    #[arg(long, default_value_t = false)]
    debug: bool,

    /// Concurrent theme-processing futures. Defaults to `num_cpus / 2` (min 1).
    #[arg(long)]
    concurrency: Option<usize>,

    /// Release version to generate STAC for (e.g. 2026-05-20.0). When omitted, all releases are processed.
    #[arg(long = "release-version")]
    release_version: Option<String>,

    /// Schema version for the release (e.g. 1.17.0). Required when --release-version is provided.
    #[arg(long = "schema-version")]
    schema_version: Option<String>,

    /// Public root URL the catalog will be hosted at, used to build absolute
    /// 'self' links. Override for staging/testing, e.g. https://staging.overturemaps.org/stac/pr/123.
    #[arg(long = "root-href", default_value = PROD_ROOT_HREF)]
    root_href: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Build(args) => build(args).await,
        Command::ListReleases(args) => list_releases(args).await,
    }
}

fn default_concurrency() -> usize {
    (num_cpus::get() / 2).max(1)
}

async fn build(args: BuildArgs) -> Result<()> {
    let root_href = args.root_href.trim_end_matches('/').to_string();
    let concurrency = args.concurrency.unwrap_or_else(default_concurrency);

    if args.release_version.is_some() && args.schema_version.is_none() {
        bail!("--schema-version is required when --release-version is provided");
    }

    if let Some(r) = &args.release_version {
        let re = Regex::new(r"^\d{4}-\d{2}-\d{2}\.\d+$").unwrap();
        if !re.is_match(r) {
            bail!("--release-version must be in format YYYY-MM-DD.N (e.g. 2026-05-20.0)");
        }
    }
    if let Some(s) = &args.schema_version {
        let re = Regex::new(r"^\d+\.\d+\.\d+$").unwrap();
        if !re.is_match(s) {
            bail!("--schema-version must be in format X.Y.Z (e.g. 1.17.0)");
        }
    }

    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("creating output dir {}", args.output.display()))?;

    let bucket = Bucket::from_url(&args.data_uri)?;
    let extras_bucket = if args.extras_uri.is_empty() {
        None
    } else {
        Some(Bucket::from_url(&args.extras_uri)?)
    };

    if let Some(release) = args.release_version {
        let schema = args.schema_version.unwrap();
        let title = format!("{release} Overture Release");

        let mut catalog = build_single_release(
            &bucket,
            extras_bucket.as_ref(),
            &release,
            &schema,
            &title,
            args.debug,
            concurrency,
            &args.output,
        )
        .await?;

        let ids = list_release_ids(&bucket, "release").await?;
        link_neighbor_releases(&mut catalog, &ids, &root_href);

        let dest = args.output.join(&release);
        save_absolute_published(&catalog, &format!("{root_href}/{release}"), &dest)?;
        return Ok(());
    }

    // Multi-release path.
    let ids = list_release_ids(&bucket, "release").await?;
    let top = build_top_catalog(
        &bucket,
        extras_bucket.as_ref(),
        &ids,
        &root_href,
        args.debug,
        concurrency,
        &args.output,
    )
    .await?;
    save_absolute_published(&top, &root_href, &args.output)?;
    Ok(())
}

async fn list_releases(args: ListReleasesArgs) -> Result<()> {
    let bucket = Bucket::from_url(&args.data_uri)?;
    let ids = list_release_ids(&bucket, "release").await?;
    for id in ids {
        println!("{id}");
    }
    Ok(())
}
