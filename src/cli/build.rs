//! `build` — generate a STAC catalog on disk from a data bucket.

use std::path::PathBuf;

use regex::Regex;

use overture_stac::stac::{
    build_single_release, build_top_catalog, link_neighbor_releases, list_release_ids, registry,
    save_root_catalog, save_sub_catalog, stamp_vcs, validate_release_id,
};
use overture_stac::storage::Bucket;
use overture_stac::{Error, Result, ResultExt};

use super::{
    default_concurrency, resolve_release_schema_version, run_validation, PROD_DATA_URI,
    PROD_EXTRAS_URI, PROD_ROOT_HREF,
};

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

    /// Schema version for the release (e.g. 1.17.0). Optional; when omitted,
    /// resolved from parquet metadata (once stamped) or a paired tag on
    /// OvertureMaps/schema. Left null when neither source has a match.
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

    if let Some(r) = &args.release_version {
        validate_release_id(r)?;
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
        let schema = match args.schema_version {
            Some(s) => s,
            None => resolve_release_schema_version(&release).await,
        };
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
        save_sub_catalog(&catalog, &root_href, &dest)?;
        if args.validate {
            run_validation(&dest, false).await?;
        }
        write_root(&args.output, &root_href, &bucket).await?;
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
    save_root_catalog(&top, &root_href, &args.output)?;
    if args.validate {
        run_validation(&args.output, false).await?;
    }
    Ok(())
}

/// Regenerate the root `catalog.json` from the release dirs currently in
/// `output`, plus the registry manifest from `bucket`. Idempotent.
async fn write_root(output: &std::path::Path, root_href: &str, bucket: &Bucket) -> Result<()> {
    // YYYY-MM-DD.N sorts lexicographically = chronologically, newest first.
    let mut releases: Vec<String> = std::fs::read_dir(output)
        .with_context(|| format!("listing {}", output.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| validate_release_id(n).is_ok())
        .collect();
    releases.sort_by(|a, b| b.cmp(a));

    let self_href = format!("{root_href}/catalog.json");
    let mut links = vec![serde_json::json!({
        "rel": "root",
        "href": self_href,
        "type": "application/json",
        "title": "Overture Releases",
    })];
    for (idx, r) in releases.iter().enumerate() {
        let mut link = serde_json::json!({
            "rel": "child",
            "href": format!("{root_href}/{r}/catalog.json"),
            "type": "application/json",
            "title": format!("{r} Overture Release"),
        });
        if idx == 0 {
            link.as_object_mut()
                .expect("json literal")
                .insert("latest".into(), serde_json::json!(true));
        }
        links.push(link);
    }
    links.push(serde_json::json!({
        "rel": "self",
        "href": self_href,
        "type": "application/json",
    }));

    let mut root = serde_json::json!({
        "type": "Catalog",
        "id": "Overture Releases",
        "title": "Overture Releases",
        "description": "All Overture Releases",
        "stac_version": "1.1.0",
        "links": links,
    });
    if let Some(latest) = releases.first() {
        root.as_object_mut()
            .expect("json literal")
            .insert("latest".into(), serde_json::json!(latest));
    }

    let manifest = registry::create_manifest(bucket)
        .await
        .context("scanning registry manifest")?;
    let registry_path = bucket
        .as_s3()
        .map(|name| format!("s3://{name}/registry"))
        .unwrap_or_else(|| format!("{}/registry", bucket.name));
    root.as_object_mut().expect("json literal").insert(
        "registry".into(),
        serde_json::json!({ "path": registry_path, "manifest": manifest }),
    );

    stamp_vcs(&mut root)?;

    let root_path = output.join("catalog.json");
    let bytes = serde_json::to_vec_pretty(&root).context("serializing root catalog")?;
    std::fs::write(&root_path, bytes)
        .with_context(|| format!("writing {}", root_path.display()))?;
    Ok(())
}
