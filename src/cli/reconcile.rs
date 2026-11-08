//! `reconcile` — compare the umbrella catalog against the data bucket and (optionally)
//! apply the diff. Additions build a fresh release into a temp dir, upload, then update
//! the root last; removals update the root first, then delete files. That ordering keeps
//! the root pointing only at complete releases even mid-run.

use std::collections::BTreeSet;

use serde_json::json;

use overture_stac::stac::registry;
use overture_stac::stac::{add_child_link, build_empty_root, remove_child_link};
use overture_stac::stac::{
    build_single_release, check_duckdb_cli, children_from_root, compute_neighbor_links,
    list_release_ids, read_catalog_children, refresh_latest, save_sub_catalog, schema_tag_url,
    stamp_vcs, sync_latest_duckdb, validate_catalog_uri, ValidateOptions,
};
use overture_stac::storage::{
    delete_prefix, get_json, get_json_optional, put_json, upload_directory, Bucket,
};
use overture_stac::{Error, Result, ResultExt};

use super::{
    default_concurrency, resolve_release_schema_version, run_validation, PROD_CATALOG_URI,
    PROD_DATA_URI, PROD_EXTRAS_URI, PROD_ROOT_HREF,
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

    /// With --apply, build and publish `latest.ddb` (needs the DuckDB CLI, checked before
    /// anything is written). Without it, the `latest.ddb` root link is removed when `latest`
    /// changes, so it never points at an older release.
    #[arg(long = "generate-latest-ddb", default_value_t = false)]
    generate_latest_ddb: bool,
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
    if args.apply && args.generate_latest_ddb {
        check_duckdb_cli()?;
    }
    // Both buckets are constructed with any URI path baked into a PrefixStore,
    // so downstream keys ("release/<id>", "catalog.json") stay layout-relative.
    let catalog_bucket = Bucket::from_url(&args.catalog_uri)?;
    let data_bucket = Bucket::from_url(&args.data_uri)?;

    let (catalog_ids, bucket_ids) = tokio::try_join!(
        read_catalog_children(&catalog_bucket),
        list_release_ids(&data_bucket, "release"),
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
        // A to-add release has no catalog.json yet, so scan only existing catalogs.
        let root_href = args.root_href.trim_end_matches('/').to_string();
        let scan_ids = sort_release_ids(&catalog_ids);
        let expected_ids = sort_release_ids(&bucket_ids);
        let stale =
            find_stale_neighbors(&catalog_bucket, &scan_ids, &expected_ids, &root_href).await?;
        for s in &stale {
            println!("~ {} has stale prev/next links", s.release_id);
        }
        let missing =
            find_missing_schema_versions(&catalog_bucket, &scan_ids, calendar_schema_version)
                .await?;
        for m in &missing {
            println!(
                "~ {} missing schema:version (release calendar has {})",
                m.release_id, m.schema
            );
        }

        if diff.is_empty() && stale.is_empty() && missing.is_empty() {
            println!("Catalog is in sync with the bucket.");
            Ok(())
        } else {
            let total = diff.to_add.len() + diff.to_remove.len() + stale.len() + missing.len();
            println!("Catalog drift: {total} item(s) differ. Re-run with --apply to fix.");
            std::process::exit(10);
        }
    } else if diff.is_empty() {
        // Past purges may still have left stale prev/next; re-check even when in sync.
        let root_href = args.root_href.trim_end_matches('/').to_string();
        let sorted = sort_release_ids(&bucket_ids);
        let stale = find_stale_neighbors(&catalog_bucket, &sorted, &sorted, &root_href).await?;
        let mut mutated = !stale.is_empty();
        if mutated {
            apply_stale_neighbors(&catalog_bucket, stale).await?;
            println!("Patched stale neighbour links.");
        }
        // Runs after the neighbour patch so it reads the patched docs back.
        let missing =
            find_missing_schema_versions(&catalog_bucket, &sorted, calendar_schema_version).await?;
        if !missing.is_empty() {
            apply_missing_schema_versions(&catalog_bucket, missing).await?;
            mutated = true;
        }
        // A missing `latest.ddb` link (e.g. first run after this feature ships) is repaired here.
        if let Some(root) = get_json_optional(&catalog_bucket, "catalog.json").await? {
            let mut refreshed = root.clone();
            refresh_latest(&mut refreshed);
            let data = args
                .generate_latest_ddb
                .then_some((&data_bucket, args.data_uri.as_str()));
            sync_latest_duckdb(&catalog_bucket, data, &root, &mut refreshed)
                .await
                .context("syncing latest.ddb")?;
            if refreshed != root {
                put_json(&catalog_bucket, "catalog.json", &refreshed).await?;
                mutated = true;
            }
        }
        if !mutated {
            println!("Catalog is in sync with the bucket. Nothing to apply.");
        }
        if mutated && args.validate {
            run_post_apply_validation(&args.catalog_uri).await?;
        }
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
            &data_bucket,
            &args.data_uri,
            extras_bucket.as_ref(),
            &root_href,
            &diff,
            concurrency,
            args.backup_catalog,
            args.validate,
            args.generate_latest_ddb,
        )
        .await?;
        println!();
        println!(
            "Applied {} change(s) to {}.",
            diff.to_add.len() + diff.to_remove.len(),
            args.catalog_uri
        );
        if args.validate {
            run_post_apply_validation(&args.catalog_uri).await?;
        }
        Ok(())
    }
}

/// Bucket-mode check after mutation; pre-upload validation misses the mutated files.
async fn run_post_apply_validation(catalog_uri: &str) -> Result<()> {
    println!();
    println!("Validating post-apply bucket state...");
    let report = validate_catalog_uri(
        catalog_uri,
        default_concurrency(),
        ValidateOptions::default(),
    )
    .await?;
    report.print(false);
    if !report.is_ok() {
        return Err(Error::ValidationFailed(report.failures.len()));
    }
    Ok(())
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
    data_bucket: &Bucket,
    data_uri: &str,
    extras_bucket: Option<&Bucket>,
    root_href: &str,
    diff: &Diff,
    concurrency: usize,
    backup: bool,
    validate: bool,
    generate_latest_ddb: bool,
) -> Result<()> {
    let root_key = "catalog.json";
    let existing_root = get_json_optional(catalog_bucket, root_key).await?;

    if backup {
        if let Some(current) = existing_root.as_ref() {
            let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
            let backup_key = format!("catalog.json.bak-{ts}");
            put_json(catalog_bucket, &backup_key, current).await?;
            println!("Backed up existing root → {backup_key}");
        } else {
            println!("No existing catalog.json to back up.");
        }
    }

    let mut root = existing_root.unwrap_or_else(build_empty_root);
    let initial_root = root.clone();

    // Removals: update root first, then delete files.
    for release in &diff.to_remove {
        println!("- removing {release}");
        remove_child_link(&mut root, release);
        put_json(catalog_bucket, root_key, &root).await?;
        let deleted = delete_prefix(catalog_bucket, &format!("{release}/")).await?;
        println!("    dropped child link + deleted {deleted} object(s)");
    }

    // Additions: build locally, upload, update root last.
    for release in &diff.to_add {
        println!("+ adding {release}");
        let temp = tempfile::tempdir().context("creating temp dir for release build")?;
        let title = format!("{release} Overture Release");
        let schema_version = resolve_release_schema_version(release).await;
        let release_catalog = build_single_release(
            data_bucket,
            extras_bucket,
            release,
            &schema_version,
            &title,
            false,
            concurrency,
            temp.path(),
        )
        .await
        .with_context(|| format!("building release {release}"))?;

        let release_dir = temp.path().join(release);
        save_sub_catalog(&release_catalog, root_href, &release_dir)?;

        if validate {
            run_validation(&release_dir, false)
                .await
                .with_context(|| format!("validating {release} before upload"))?;
        }

        let uploaded =
            upload_directory(catalog_bucket, &format!("{release}/"), &release_dir).await?;

        add_child_link(&mut root, release, root_href)?;
        put_json(catalog_bucket, root_key, &root).await?;
        println!("    uploaded {uploaded} object(s) + added child link");
    }

    let current_children = children_from_root(&root);
    let sorted = sort_release_ids(&current_children);

    // Any add/remove shifts the surviving releases' prev/next.
    reconcile_neighbor_links(catalog_bucket, &sorted, root_href).await?;
    let missing =
        find_missing_schema_versions(catalog_bucket, &sorted, calendar_schema_version).await?;
    apply_missing_schema_versions(catalog_bucket, missing).await?;

    // Post-pass: refresh `latest`, recompute the registry manifest (registry
    // parquet files get rewritten on release day, so this belongs on the same
    // apply that publishes the release), and stamp the VCS extension so anyone
    // reading the catalog can tell which build wrote it.
    refresh_latest(&mut root);
    // Upload the database before the root that links to it.
    let data = generate_latest_ddb.then_some((data_bucket, data_uri));
    sync_latest_duckdb(catalog_bucket, data, &initial_root, &mut root)
        .await
        .context("syncing latest.ddb")?;
    let manifest = registry::create_manifest(data_bucket)
        .await
        .context("refreshing registry manifest")?;
    let registry_path = format!(
        "{}/registry",
        data_bucket
            .as_s3()
            .map(|name| format!("s3://{name}"))
            .unwrap_or_else(|| data_bucket.name.clone())
    );
    root.as_object_mut()
        .ok_or_else(|| Error::MalformedCatalog("root is not a JSON object".into()))?
        .insert(
            "registry".into(),
            json!({
                "path": registry_path,
                "manifest": manifest,
            }),
        );
    stamp_vcs(&mut root)?;
    put_json(catalog_bucket, root_key, &root).await?;

    Ok(())
}

/// Reverse alphabetical works for `YYYY-MM-DD.N` release IDs.
fn sort_release_ids(ids: &[String]) -> Vec<String> {
    let mut out = ids.to_vec();
    out.sort_by(|a, b| b.cmp(a));
    out
}

struct StaleNeighbor {
    release_id: String,
    doc: serde_json::Value,
    expected: Vec<stac::Link>,
}

/// `scan_ids` are what we read; `expected_ids` is what neighbours are computed against.
async fn find_stale_neighbors(
    catalog_bucket: &Bucket,
    scan_ids: &[String],
    expected_ids: &[String],
    root_href: &str,
) -> Result<Vec<StaleNeighbor>> {
    let mut out = Vec::new();
    for id in scan_ids {
        let key = format!("{id}/catalog.json");
        let doc = get_json(catalog_bucket, &key)
            .await
            .with_context(|| format!("reading {key} for neighbour-link check"))?;
        let expected = compute_neighbor_links(id, expected_ids, root_href);
        let actual = extract_neighbor_links(&doc);
        if !links_equivalent(actual.as_deref(), &expected) {
            out.push(StaleNeighbor {
                release_id: id.clone(),
                doc,
                expected,
            });
        }
    }
    Ok(out)
}

async fn apply_stale_neighbors(catalog_bucket: &Bucket, stale: Vec<StaleNeighbor>) -> Result<()> {
    for mut s in stale {
        replace_neighbor_links(&mut s.doc, &s.expected)?;
        let key = format!("{}/catalog.json", s.release_id);
        put_json(catalog_bucket, &key, &s.doc)
            .await
            .with_context(|| format!("writing patched {key}"))?;
        println!("~ patched neighbour links on {}", s.release_id);
    }
    Ok(())
}

async fn reconcile_neighbor_links(
    catalog_bucket: &Bucket,
    current_ids: &[String],
    root_href: &str,
) -> Result<()> {
    let stale = find_stale_neighbors(catalog_bucket, current_ids, current_ids, root_href).await?;
    apply_stale_neighbors(catalog_bucket, stale).await
}

struct MissingSchema {
    release_id: String,
    doc: serde_json::Value,
    schema: String,
}

async fn calendar_schema_version(release: String) -> String {
    resolve_release_schema_version(&release).await
}

/// Release catalogs without `schema:version` that `resolve` can now fill, e.g. when the
/// release calendar was updated after the release was first published.
async fn find_missing_schema_versions<F, Fut>(
    catalog_bucket: &Bucket,
    ids: &[String],
    resolve: F,
) -> Result<Vec<MissingSchema>>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = String>,
{
    let mut out = Vec::new();
    for id in ids {
        let key = format!("{id}/catalog.json");
        let doc = get_json(catalog_bucket, &key)
            .await
            .with_context(|| format!("reading {key} for schema:version check"))?;
        if doc.get("schema:version").and_then(|v| v.as_str()).is_some() {
            continue;
        }
        let schema = resolve(id.clone()).await;
        if schema.is_empty() {
            continue;
        }
        out.push(MissingSchema {
            release_id: id.clone(),
            doc,
            schema,
        });
    }
    Ok(out)
}

async fn apply_missing_schema_versions(
    catalog_bucket: &Bucket,
    missing: Vec<MissingSchema>,
) -> Result<()> {
    for mut m in missing {
        stamp_schema_version(&mut m.doc, &m.schema)?;
        let key = format!("{}/catalog.json", m.release_id);
        put_json(catalog_bucket, &key, &m.doc)
            .await
            .with_context(|| format!("writing {key} with schema:version"))?;
        println!("Backfilled schema:version {} on {}", m.schema, m.release_id);
    }
    Ok(())
}

fn stamp_schema_version(doc: &mut serde_json::Value, schema: &str) -> Result<()> {
    let obj = doc
        .as_object_mut()
        .ok_or_else(|| Error::MalformedCatalog("release catalog is not a JSON object".into()))?;
    obj.insert("schema:version".into(), json!(schema));
    obj.insert("schema:tag".into(), json!(schema_tag_url(schema)));
    Ok(())
}

/// `None` if any prev/next entry fails to deserialize (forces a rewrite).
fn extract_neighbor_links(doc: &serde_json::Value) -> Option<Vec<stac::Link>> {
    let Some(links) = doc.get("links").and_then(|v| v.as_array()) else {
        return Some(Vec::new());
    };
    let mut out = Vec::new();
    for raw in links {
        let rel = raw.get("rel").and_then(|v| v.as_str());
        if !matches!(rel, Some("prev") | Some("next")) {
            continue;
        }
        out.push(serde_json::from_value::<stac::Link>(raw.clone()).ok()?);
    }
    Some(out)
}

/// Order-insensitive full-field equality (rel, href, type, title, ...).
fn links_equivalent(actual: Option<&[stac::Link]>, expected: &[stac::Link]) -> bool {
    let Some(actual) = actual else { return false };
    if actual.len() != expected.len() {
        return false;
    }
    actual.iter().all(|a| expected.iter().any(|e| e == a))
}

/// Preserves every non-prev/next link.
fn replace_neighbor_links(doc: &mut serde_json::Value, new_links: &[stac::Link]) -> Result<()> {
    let obj = doc
        .as_object_mut()
        .ok_or_else(|| Error::MalformedCatalog("release catalog is not a JSON object".into()))?;
    let entry = obj.entry("links").or_insert_with(|| json!([]));
    let arr = entry
        .as_array_mut()
        .ok_or_else(|| Error::MalformedCatalog("links is not a JSON array".into()))?;
    arr.retain(|l| {
        l.get("rel")
            .and_then(|v| v.as_str())
            .map(|r| r != "prev" && r != "next")
            .unwrap_or(true)
    });
    for l in new_links {
        arr.push(serde_json::to_value(l).map_err(Error::from)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_catalog(dir: &std::path::Path, id: &str, doc: serde_json::Value) {
        let path = dir.join(id).join("catalog.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec(&doc).unwrap()).unwrap();
    }

    fn read_catalog(dir: &std::path::Path, id: &str) -> serde_json::Value {
        let bytes = std::fs::read(dir.join(id).join("catalog.json")).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn stub_resolver(release: String) -> String {
        match release.as_str() {
            "2026-02-01.0" | "2026-03-01.0" => "2.0.0".into(),
            _ => String::new(),
        }
    }

    #[tokio::test]
    async fn backfills_only_missing_schema_versions_the_resolver_knows() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // Already stamped: left alone even though the resolver has a value.
        write_catalog(
            dir,
            "2026-03-01.0",
            json!({"id": "2026-03-01.0", "schema:version": "1.9.0"}),
        );
        // Missing: filled.
        write_catalog(dir, "2026-02-01.0", json!({"id": "2026-02-01.0"}));
        // Missing but unknown to the resolver: left without the field.
        write_catalog(dir, "2026-01-01.0", json!({"id": "2026-01-01.0"}));

        let bucket = Bucket::from_url(&format!("file://{}/", dir.display())).unwrap();
        let ids: Vec<String> = ["2026-03-01.0", "2026-02-01.0", "2026-01-01.0"]
            .map(String::from)
            .to_vec();

        let missing = find_missing_schema_versions(&bucket, &ids, stub_resolver)
            .await
            .unwrap();
        let found: Vec<&str> = missing.iter().map(|m| m.release_id.as_str()).collect();
        assert_eq!(found, vec!["2026-02-01.0"]);

        apply_missing_schema_versions(&bucket, missing)
            .await
            .unwrap();

        let filled = read_catalog(dir, "2026-02-01.0");
        assert_eq!(filled["schema:version"], "2.0.0");
        assert_eq!(
            filled["schema:tag"],
            "https://github.com/OvertureMaps/schema/releases/tag/v2.0.0"
        );
        assert_eq!(filled["id"], "2026-02-01.0");
        assert_eq!(read_catalog(dir, "2026-03-01.0")["schema:version"], "1.9.0");
        assert!(read_catalog(dir, "2026-01-01.0")
            .get("schema:version")
            .is_none());

        // Second pass finds nothing left to fill.
        let again = find_missing_schema_versions(&bucket, &ids, stub_resolver)
            .await
            .unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn null_schema_version_is_treated_as_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write_catalog(
            dir,
            "2026-02-01.0",
            json!({
                "id": "2026-02-01.0",
                "schema:version": null,
                "schema:tag": "https://github.com/OvertureMaps/schema/releases/tag/vNone",
            }),
        );
        let bucket = Bucket::from_url(&format!("file://{}/", dir.display())).unwrap();
        let ids = vec!["2026-02-01.0".to_string()];

        let missing = find_missing_schema_versions(&bucket, &ids, stub_resolver)
            .await
            .unwrap();
        apply_missing_schema_versions(&bucket, missing)
            .await
            .unwrap();

        let doc = read_catalog(dir, "2026-02-01.0");
        assert_eq!(doc["schema:version"], "2.0.0");
        assert_eq!(
            doc["schema:tag"],
            "https://github.com/OvertureMaps/schema/releases/tag/v2.0.0"
        );
    }
}
