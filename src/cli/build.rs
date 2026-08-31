//! `build` — generate a STAC catalog on disk from a data bucket.

use std::path::PathBuf;

use regex::Regex;

use overture_stac::stac::{
    build_single_release, build_top_catalog, link_neighbor_releases, list_release_ids,
    save_absolute_published,
};
use overture_stac::storage::Bucket;
use overture_stac::{Error, Result, ResultExt};

use super::{default_concurrency, run_validation, PROD_DATA_URI, PROD_EXTRAS_URI, PROD_ROOT_HREF};

#[derive(clap::Args, Debug)]
pub struct BuildArgs {
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

    /// Validate the catalog after writing. Fails the run on any failure.
    #[arg(long, default_value_t = false)]
    validate: bool,
}

pub async fn run(args: BuildArgs) -> Result<()> {
    let root_href = args.root_href.trim_end_matches('/').to_string();
    let concurrency = args.concurrency.unwrap_or_else(default_concurrency);

    if args.release_version.is_some() && args.schema_version.is_none() {
        return Err(Error::SchemaVersionRequired);
    }

    if let Some(r) = &args.release_version {
        let re = Regex::new(r"^\d{4}-\d{2}-\d{2}\.\d+$").unwrap();
        if !re.is_match(r) {
            return Err(Error::InvalidReleaseVersion(r.clone()));
        }
    }
    if let Some(s) = &args.schema_version {
        let re = Regex::new(r"^\d+\.\d+\.\d+$").unwrap();
        if !re.is_match(s) {
            return Err(Error::InvalidSchemaVersion(s.clone()));
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
        if args.validate {
            run_validation(&dest, false).await?;
        }
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
    if args.validate {
        run_validation(&args.output, false).await?;
    }
    Ok(())
}
