use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

use overture_stac::{
    catalog::{
        build_single_release, build_top_catalog, link_neighbor_releases, list_release_ids,
        save_absolute_published,
    },
    s3::{delete_prefix, get_json, put_json, upload_directory, Bucket},
};

const PROD_ROOT_HREF: &str = "https://stac.overturemaps.org";
const PROD_DATA_URI: &str = "s3://overturemaps-us-west-2";
const PROD_EXTRAS_URI: &str = "s3://overturemaps-extras-us-west-2";
const PROD_CATALOG_URI: &str = "s3://overturemaps-extras-us-west-2/stac/";

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
    /// Compare the live STAC catalog against the data bucket and report drift.
    /// Read-only; exits non-zero when the catalog is out of sync.
    Reconcile(ReconcileArgs),
}

#[derive(clap::Args, Debug)]
struct ListReleasesArgs {
    /// Object-store URI to the data bucket (s3://, gs://, az://, file:// ...).
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,
}

#[derive(clap::Args, Debug)]
struct ReconcileArgs {
    /// Object-store URI to the catalog storage (may include a path prefix).
    /// Same URI is used for both read and write.
    #[arg(long = "catalog-uri", default_value = PROD_CATALOG_URI)]
    catalog_uri: String,

    /// Object-store URI to the data bucket (s3://, gs://, az://, file:// ...).
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,

    /// Object-store URI to the extras bucket (holds PMTiles). Only used when --apply
    /// builds a new release. Pass an empty string to skip PMTiles discovery.
    #[arg(long = "extras-uri", default_value = PROD_EXTRAS_URI)]
    extras_uri: String,

    /// Public root URL baked into `self:` links when apply builds new releases.
    #[arg(long = "root-href", default_value = PROD_ROOT_HREF)]
    root_href: String,

    /// Actually apply the diff (write to --catalog-uri). Without this flag the
    /// command is read-only and only reports drift.
    #[arg(long, default_value_t = false)]
    apply: bool,

    /// Before --apply mutates anything, copy the existing catalog.json to
    /// catalog.json.bak-YYYYMMDD-HHMMSS in the same location.
    #[arg(long = "backup-catalog", default_value_t = false)]
    backup_catalog: bool,

    /// Concurrent theme-processing futures used when building added releases.
    #[arg(long)]
    concurrency: Option<usize>,
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
        Command::Reconcile(args) => reconcile(args).await,
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

/// Diff between the catalog's known release IDs and the data bucket's actual release IDs.
struct Diff {
    to_add: Vec<String>,
    to_remove: Vec<String>,
}

impl Diff {
    fn compute(catalog_ids: &[String], bucket_ids: &[String]) -> Self {
        let cat: BTreeSet<String> = catalog_ids.iter().cloned().collect();
        let buk: BTreeSet<String> = bucket_ids.iter().cloned().collect();
        Diff {
            to_add: buk.difference(&cat).cloned().collect(),
            to_remove: cat.difference(&buk).cloned().collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.to_add.is_empty() && self.to_remove.is_empty()
    }
}

/// Read the root `catalog.json` from a catalog bucket and extract release IDs
/// from its `rel: child` links. Returns an empty list if the catalog is missing.
async fn read_catalog_children(
    catalog_bucket: &Bucket,
    catalog_prefix: &str,
) -> Result<Vec<String>> {
    let key = format!("{catalog_prefix}catalog.json");
    let root = get_json(catalog_bucket, &key).await?;
    Ok(children_from_root(&root))
}

fn children_from_root(root: &Value) -> Vec<String> {
    let Some(links) = root.get("links").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for link in links {
        if link.get("rel").and_then(|v| v.as_str()) != Some("child") {
            continue;
        }
        let Some(href) = link.get("href").and_then(|v| v.as_str()) else {
            continue;
        };
        if let Some(id) = release_id_from_href(href) {
            out.push(id);
        }
    }
    out
}

fn release_id_from_href(href: &str) -> Option<String> {
    href.trim_end_matches("/catalog.json")
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(String::from)
}

async fn reconcile(args: ReconcileArgs) -> Result<()> {
    let (catalog_bucket, catalog_prefix) = Bucket::from_url_with_prefix(&args.catalog_uri)?;
    let (data_bucket, data_prefix) = Bucket::from_url_with_prefix(&args.data_uri)?;
    let release_prefix = format!("{data_prefix}release");

    let (catalog_ids, bucket_ids) = tokio::try_join!(
        read_catalog_children(&catalog_bucket, &catalog_prefix),
        list_release_ids(&data_bucket, &release_prefix),
    )?;
    let diff = Diff::compute(&catalog_ids, &bucket_ids);

    print_diff_summary(
        &args.catalog_uri,
        &args.data_uri,
        &catalog_ids,
        &bucket_ids,
        &diff,
    );

    if !args.apply {
        if diff.is_empty() {
            println!("Catalog is in sync with the bucket.");
            Ok(())
        } else {
            println!(
                "Catalog drift: {} release(s) differ.",
                diff.to_add.len() + diff.to_remove.len()
            );
            println!("Re-run with --apply to fix.");
            std::process::exit(1);
        }
    } else if diff.is_empty() {
        println!("Catalog is in sync with the bucket. Nothing to apply.");
        Ok(())
    } else {
        let extras_bucket = if args.extras_uri.is_empty() {
            None
        } else {
            Some(Bucket::from_url(&args.extras_uri)?)
        };
        let concurrency = args.concurrency.unwrap_or_else(default_concurrency);
        let root_href = args.root_href.trim_end_matches('/').to_string();
        apply_diff(
            &catalog_bucket,
            &catalog_prefix,
            &data_bucket,
            extras_bucket.as_ref(),
            &root_href,
            &diff,
            concurrency,
            args.backup_catalog,
        )
        .await?;
        println!();
        println!(
            "Applied {} change(s) to {}.",
            diff.to_add.len() + diff.to_remove.len(),
            args.catalog_uri
        );
        Ok(())
    }
}

fn print_diff_summary(
    catalog_uri: &str,
    data_uri: &str,
    catalog_ids: &[String],
    bucket_ids: &[String],
    diff: &Diff,
) {
    println!(
        "Read  {catalog_uri}catalog.json ({} releases)",
        catalog_ids.len()
    );
    println!("Listed {data_uri} ({} releases)", bucket_ids.len());
    println!();

    if diff.to_add.is_empty() {
        println!("+ 0 releases to add (in bucket, not in catalog)");
    } else {
        println!(
            "+ {} release{} to add (in bucket, not in catalog):",
            diff.to_add.len(),
            if diff.to_add.len() == 1 { "" } else { "s" }
        );
        for id in &diff.to_add {
            println!("    + {id}");
        }
    }
    if diff.to_remove.is_empty() {
        println!("- 0 releases to remove (in catalog, not in bucket)");
    } else {
        println!(
            "- {} release{} to remove (in catalog, not in bucket):",
            diff.to_remove.len(),
            if diff.to_remove.len() == 1 { "" } else { "s" }
        );
        for id in &diff.to_remove {
            println!("    - {id}");
        }
    }
    println!();
}

/// Apply the diff to the catalog bucket. Removals happen first (root updated first,
/// then files deleted); additions happen after (files uploaded first, then root updated
/// last). This keeps the root catalog pointing only at complete releases even mid-run.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn apply_diff(
    catalog_bucket: &Bucket,
    catalog_prefix: &str,
    data_bucket: &Bucket,
    extras_bucket: Option<&Bucket>,
    root_href: &str,
    diff: &Diff,
    concurrency: usize,
    backup: bool,
) -> Result<()> {
    let root_key = format!("{catalog_prefix}catalog.json");
    let existing_root = get_json(catalog_bucket, &root_key).await.ok();

    if backup {
        if let Some(current) = existing_root.as_ref() {
            let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
            let backup_key = format!("{catalog_prefix}catalog.json.bak-{ts}");
            put_json(catalog_bucket, &backup_key, current).await?;
            println!("Backed up existing root → {backup_key}");
        } else {
            println!("No existing catalog.json to back up.");
        }
    }

    let mut root = existing_root.unwrap_or_else(build_empty_root);

    // Removals: update root first, then delete files.
    for release in &diff.to_remove {
        println!("- removing {release}");
        remove_child_link(&mut root, release);
        put_json(catalog_bucket, &root_key, &root).await?;
        let deleted = delete_prefix(catalog_bucket, &format!("{catalog_prefix}{release}/")).await?;
        println!("    dropped child link + deleted {deleted} object(s)");
    }

    // Additions: build locally, upload, update root last.
    for release in &diff.to_add {
        println!("+ adding {release}");
        let temp = tempfile::tempdir().context("creating temp dir for release build")?;
        let title = format!("{release} Overture Release");
        let release_catalog = build_single_release(
            data_bucket,
            extras_bucket,
            release,
            "",
            &title,
            false,
            concurrency,
            temp.path(),
        )
        .await
        .with_context(|| format!("building release {release}"))?;

        let release_dir = temp.path().join(release);
        save_absolute_published(
            &release_catalog,
            &format!("{root_href}/{release}"),
            &release_dir,
        )?;

        let uploaded = upload_directory(
            catalog_bucket,
            &format!("{catalog_prefix}{release}/"),
            &release_dir,
        )
        .await?;

        add_child_link(&mut root, release, root_href);
        put_json(catalog_bucket, &root_key, &root).await?;
        println!("    uploaded {uploaded} object(s) + added child link");
    }

    // Post-pass: refresh `latest` from the current set of children and stamp
    // the VCS extension so anyone reading the catalog can tell which build
    // wrote it.
    let current_children = children_from_root(&root);
    let mut sorted = current_children.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    if let Some(latest) = sorted.first() {
        root.as_object_mut()
            .expect("root is object")
            .insert("latest".into(), json!(latest));
    }
    stamp_vcs(&mut root);
    put_json(catalog_bucket, &root_key, &root).await?;

    Ok(())
}

const VCS_EXTENSION_URL: &str = "https://stac-extensions.github.io/vcs/v0.1.0/schema.json";

fn stamp_vcs(root: &mut Value) {
    let obj = root.as_object_mut().expect("root is object");
    let extensions = obj
        .entry("stac_extensions".to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("stac_extensions is array");
    if !extensions
        .iter()
        .any(|v| v.as_str() == Some(VCS_EXTENSION_URL))
    {
        extensions.push(json!(VCS_EXTENSION_URL));
    }
    obj.insert("vcs:type".into(), json!("git"));
    obj.insert("vcs:branch".into(), json!(env!("GIT_BRANCH")));
    obj.insert("vcs:commit".into(), json!(env!("GIT_COMMIT")));
}

fn build_empty_root() -> Value {
    json!({
        "type": "Catalog",
        "id": "Overture Releases",
        "description": "All Overture Releases",
        "stac_version": "1.0.0",
        "links": [],
    })
}

fn remove_child_link(root: &mut Value, release: &str) {
    let Some(links) = root.get_mut("links").and_then(|v| v.as_array_mut()) else {
        return;
    };
    links.retain(|link| match link.get("href").and_then(|v| v.as_str()) {
        Some(href) => release_id_from_href(href).as_deref() != Some(release),
        None => true,
    });
}

fn add_child_link(root: &mut Value, release: &str, root_href: &str) {
    let obj = root.as_object_mut().expect("catalog root is a JSON object");
    let links = obj
        .entry("links".to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("links is an array");
    // Idempotency: skip if a child link for this release already exists.
    let already_present = links.iter().any(|link| {
        link.get("rel").and_then(|v| v.as_str()) == Some("child")
            && link
                .get("href")
                .and_then(|v| v.as_str())
                .and_then(release_id_from_href)
                .as_deref()
                == Some(release)
    });
    if already_present {
        return;
    }
    links.push(json!({
        "rel": "child",
        "href": format!("{root_href}/{release}/catalog.json"),
        "type": "application/json",
        "title": format!("{release} Overture Release"),
    }));
}
