//! `reconcile` — compare the umbrella catalog against the data bucket and (optionally)
//! apply the diff. Additions build a fresh release into a temp dir, upload, then update
//! the root last; removals update the root first, then delete files. That ordering keeps
//! the root pointing only at complete releases even mid-run.

use std::collections::BTreeSet;

use serde_json::json;

use overture_stac::stac::{add_child_link, build_empty_root, remove_child_link};
use overture_stac::stac::{
    build_single_release, children_from_root, list_release_ids, read_catalog_children,
    save_absolute_published, stamp_vcs, validate_catalog_uri, ValidateOptions,
};
use overture_stac::storage::{
    delete_prefix, get_json_optional, put_json, upload_directory, Bucket,
};
use overture_stac::{Error, Result, ResultExt};

use super::{
    default_concurrency, run_validation, PROD_CATALOG_URI, PROD_DATA_URI, PROD_EXTRAS_URI,
    PROD_ROOT_HREF,
};

#[derive(clap::Args, Debug)]
pub struct ReconcileArgs {
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

    /// Validate each newly-built release locally before uploading AND run a
    /// bucket-mode validation of the whole catalog after apply completes
    /// (covers the mutated root). Fails the run on any failure.
    #[arg(long, default_value_t = false)]
    validate: bool,

    /// Concurrent theme-processing futures used when building added releases.
    #[arg(long)]
    concurrency: Option<usize>,
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

pub async fn run(args: ReconcileArgs) -> Result<()> {
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
            std::process::exit(10);
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
            args.validate,
        )
        .await?;
        println!();
        println!(
            "Applied {} change(s) to {}.",
            diff.to_add.len() + diff.to_remove.len(),
            args.catalog_uri
        );
        if args.validate {
            // Pre-upload check only covered each new release's temp dir; it
            // couldn't see the root catalog that apply_diff just mutated. Run
            // the same three checks against the post-apply bucket state to
            // close that gap.
            println!();
            println!("Validating post-apply bucket state...");
            let report = validate_catalog_uri(
                &args.catalog_uri,
                default_concurrency(),
                ValidateOptions::default(),
            )
            .await?;
            report.print(false);
            if !report.is_ok() {
                return Err(Error::ValidationFailed(report.failures.len()));
            }
        }
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
    validate: bool,
) -> Result<()> {
    let root_key = format!("{catalog_prefix}catalog.json");
    let existing_root = get_json_optional(catalog_bucket, &root_key).await?;

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

        if validate {
            run_validation(&release_dir, false)
                .await
                .with_context(|| format!("validating {release} before upload"))?;
        }

        let uploaded = upload_directory(
            catalog_bucket,
            &format!("{catalog_prefix}{release}/"),
            &release_dir,
        )
        .await?;

        add_child_link(&mut root, release, root_href)?;
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
            .ok_or_else(|| Error::MalformedCatalog("root is not a JSON object".into()))?
            .insert("latest".into(), json!(latest));
    }
    stamp_vcs(&mut root)?;
    put_json(catalog_bucket, &root_key, &root).await?;

    Ok(())
}
