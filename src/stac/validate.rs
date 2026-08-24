//! Validate a STAC catalog — local, over HTTP, or against an object-store bucket.
//!
//! Three axes:
//!   1. JSON-Schema conformance via [`stac_validate::Validator`] (fetches and
//!      caches STAC core + declared extension schemas).
//!   2. Link integrity — every href pointing inside the catalog's own base URL
//!      must resolve: locally it must exist on disk; in the crawl modes it
//!      must have been reachable. External hrefs (data-bucket asset URLs,
//!      extension schemas, etc.) are skipped by design in all modes.
//!   3. Overture-specific rules — `schema:version` null (bug #115), required
//!      `aws`/`azure` assets on items, license enum, PMTiles media type, valid
//!      bbox.
//!
//! Public entry points:
//!   - [`validate_catalog`] — walks a local directory. Used by
//!     `build --validate`, `reconcile --validate` (pre-upload), and
//!     `validate <DIR>`.
//!   - [`validate_url`] — crawls a live catalog over HTTP starting from a
//!     root URL. Tests what the CDN serves. Used by `validate --url` — the
//!     scheduled prod health check.
//!   - [`validate_catalog_uri`] — crawls a catalog over `object_store`
//!     (`s3://`, `gs://`, `az://`, `file://`). Tests the source of truth in
//!     the bucket, independent of any CDN cache. Used by `validate
//!     --catalog-uri` and by `reconcile --apply --validate` (post-apply) to
//!     cover the mutated root catalog.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use stac_validate::Validator;
use tokio::sync::Mutex;

use crate::storage::{get_json, Bucket};
use crate::{Error, Result, ResultExt};

const ALLOWED_LICENSES: &[&str] = &["CC0-1.0", "CC-BY-4.0", "ODbL-1.0", "other"];
const PMTILES_MEDIA_TYPE: &str = "application/vnd.pmtiles";
const REMOTE_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_MAX_CONCURRENCY: usize = 16;
const REMOTE_USER_AGENT: &str = concat!("overture-stac/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, Copy)]
pub struct ValidateOptions {
    /// Fetch STAC + extension JSON schemas and validate every doc against them.
    /// Requires network access on first use; schemas are cached after.
    pub check_schema: bool,
    /// In local mode: every in-base href must resolve to a file on disk.
    /// In url/catalog-uri modes: every in-base href must have been reachable
    /// via crawl.
    pub check_links: bool,
    /// Apply Overture-specific assertions (schema:version non-null, required
    /// assets, license enum, PMTiles media type, valid bbox).
    pub check_overture_rules: bool,
}

impl Default for ValidateOptions {
    fn default() -> Self {
        Self {
            check_schema: true,
            check_links: true,
            check_overture_rules: true,
        }
    }
}

#[derive(Debug)]
pub struct ValidationReport {
    /// Number of documents inspected. In local mode: JSON files walked.
    /// In url/catalog-uri modes: URLs successfully fetched (fetch failures
    /// count as failures in `failures`, not as inspected docs).
    pub files_checked: usize,
    pub failures: Vec<Failure>,
}

#[derive(Debug, Clone)]
pub struct Failure {
    /// File path (local mode) or URL (crawl modes) that failed.
    pub location: String,
    pub kind: FailureKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
pub enum FailureKind {
    Schema,
    Link,
    OvertureRule,
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailureKind::Schema => write!(f, "schema"),
            FailureKind::Link => write!(f, "link"),
            FailureKind::OvertureRule => write!(f, "overture-rule"),
        }
    }
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn print(&self, use_json: bool) {
        if use_json {
            let js = serde_json::json!({
                "files_checked": self.files_checked,
                "failures": self.failures.iter().map(|f| serde_json::json!({
                    "location": f.location,
                    "kind": f.kind.to_string(),
                    "message": f.message,
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&js).unwrap_or_default());
            return;
        }
        println!();
        if self.failures.is_empty() {
            println!("Validated {} document(s) — OK", self.files_checked);
            return;
        }

        // Group by location (BTreeMap → deterministic alphabetical order).
        let mut by_location: BTreeMap<&str, Vec<&Failure>> = BTreeMap::new();
        for f in &self.failures {
            by_location.entry(f.location.as_str()).or_default().push(f);
        }

        println!(
            "Validated {} document(s) — {} file(s) with failures ({} total)",
            self.files_checked,
            by_location.len(),
            self.failures.len()
        );

        // Pad the "[kind]" tag column to the longest tag in this report so
        // messages align across kinds.
        let tag_width = self
            .failures
            .iter()
            .map(|f| f.kind.to_string().len() + 2)
            .max()
            .unwrap_or(0);

        for (loc, fails) in &by_location {
            // Sort within a group so identical inputs always render the same
            // way — crawl completion order is otherwise non-deterministic.
            let mut fails = fails.clone();
            fails.sort_by(|a, b| {
                (a.kind.to_string(), &a.message).cmp(&(b.kind.to_string(), &b.message))
            });
            println!();
            println!("  {loc}");
            for f in fails {
                let tag = format!("[{}]", f.kind);
                println!("    {tag:<tag_width$} {}", f.message);
            }
        }
        // Trailing blank line separates the finding groups from whatever comes
        // next in the terminal (typically the "Error: validation failed …"
        // written to stderr by main).
        println!();
    }
}

// ─── LOCAL MODE ──────────────────────────────────────────────────────────────

pub async fn validate_catalog(
    dir: &Path,
    concurrency: usize,
    options: ValidateOptions,
) -> Result<ValidationReport> {
    let root_path = dir.join("catalog.json");
    let root_value: Value = serde_json::from_slice(
        &std::fs::read(&root_path).with_context(|| format!("reading {}", root_path.display()))?,
    )?;
    let base_url = derive_base_url_from_self_link(&root_value)
        .ok_or_else(|| Error::MissingSelfLink(root_path.clone()))?;

    let files = collect_json_files(dir)?;
    let files_checked = files.len();

    let validator = build_validator(options).await?;

    use futures::stream::{StreamExt, TryStreamExt};
    let dir_owned = dir.to_path_buf();
    let base_arc = Arc::new(base_url);

    let per_file: Vec<Vec<Failure>> = futures::stream::iter(files.into_iter().map(|file| {
        let validator = validator.clone();
        let dir = dir_owned.clone();
        let base = Arc::clone(&base_arc);
        async move { check_local_file(&file, validator.as_deref(), &dir, &base, options).await }
    }))
    .buffer_unordered(concurrency.max(1))
    .try_collect()
    .await?;

    let failures: Vec<Failure> = per_file.into_iter().flatten().collect();
    Ok(ValidationReport {
        files_checked,
        failures,
    })
}

async fn check_local_file(
    file: &Path,
    validator: Option<&Mutex<Validator>>,
    root_dir: &Path,
    base_url: &str,
    options: ValidateOptions,
) -> Result<Vec<Failure>> {
    let bytes = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let value: Value =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", file.display()))?;

    let location = file.display().to_string();
    let mut out = Vec::new();

    if let (true, Some(v)) = (options.check_schema, validator) {
        out.extend(run_schema_check(&location, &value, v).await);
    }
    if options.check_links {
        for href in all_hrefs(&value) {
            if let Some(rel) = href.strip_prefix(base_url) {
                let candidate = root_dir.join(rel);
                if !candidate.exists() {
                    out.push(Failure {
                        location: location.clone(),
                        kind: FailureKind::Link,
                        message: format!("href {href} → {} does not exist", candidate.display()),
                    });
                }
            }
        }
    }
    if options.check_overture_rules {
        out.extend(check_overture_rules(&location, &value));
    }

    Ok(out)
}

fn collect_json_files(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in
            std::fs::read_dir(&d).with_context(|| format!("reading dir {}", d.display()))?
        {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("json") {
                out.push(p);
            }
        }
    }
    Ok(out)
}

// ─── REMOTE MODE ─────────────────────────────────────────────────────────────

/// Fetch a live STAC catalog over HTTP and run the three checks against every
/// reachable Catalog / Collection / Item. Starts at `root_url` and follows
/// `rel=child` + `rel=item` links inside the same base URL. Concurrency is
/// capped at [`REMOTE_MAX_CONCURRENCY`] to be nice to CDNs.
///
/// Broken next/prev/child hrefs surface as [`FailureKind::Link`]: they point
/// inside the base URL but weren't in the successfully-fetched set.
pub async fn validate_url(
    root_url: &str,
    concurrency: usize,
    options: ValidateOptions,
) -> Result<ValidationReport> {
    let normalized = normalize_root_url(root_url)?;
    let client = reqwest::Client::builder()
        .user_agent(REMOTE_USER_AGENT)
        .timeout(REMOTE_TIMEOUT)
        .build()
        .context("building HTTP client for remote validate")?;

    let capped = concurrency.max(1).min(REMOTE_MAX_CONCURRENCY);
    let (fetched, fetch_failures) = crawl_http(&client, &normalized, capped).await?;
    finalize_report(fetched, fetch_failures, &normalized, capped, options).await
}

/// Accept the convenient forms users type — `https://host`, `https://host/`,
/// `https://host/catalog.json` — and normalise to a canonical entry URL.
/// Rejects non-http(s) schemes and un-parseable input up-front, before the
/// crawler would fail with an opaque "builder error".
fn normalize_root_url(input: &str) -> Result<String> {
    let mut parsed =
        url::Url::parse(input).map_err(|_| Error::InvalidValidateUrl(input.to_string()))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Error::InvalidValidateUrl(input.to_string()));
    }
    if !parsed.path().ends_with(".json") {
        let path = parsed.path().to_string();
        let new_path = if path.ends_with('/') {
            format!("{path}catalog.json")
        } else {
            format!("{path}/catalog.json")
        };
        parsed.set_path(&new_path);
    }
    Ok(parsed.to_string())
}

/// Fetch a live STAC catalog from an object-store URI (`s3://`, `gs://`, `az://`,
/// `file://`) and run the three checks against every reachable Catalog /
/// Collection / Item. Same crawl semantics as [`validate_url`] — the difference
/// is only how bytes are fetched. Used by:
///
///   - `validate --catalog-uri <URI>` — post-upload check against the actual
///     bucket, independent of any CDN caching.
///   - `reconcile --apply --validate` — closes the "mutated root never gets
///     checked" gap by validating after the apply completes.
pub async fn validate_catalog_uri(
    catalog_uri: &str,
    concurrency: usize,
    options: ValidateOptions,
) -> Result<ValidationReport> {
    let (bucket, prefix) = Bucket::from_url_with_prefix(catalog_uri)?;
    let root_key = format!("{prefix}catalog.json");
    let root_value = get_json(&bucket, &root_key)
        .await
        .with_context(|| format!("reading {root_key} from {catalog_uri}"))?;

    // The catalog's own self-link is the authoritative URL base — the hrefs we
    // discover in child/item links will use this base, not the s3:// URI.
    let url_base = derive_base_url_from_self_link(&root_value).ok_or_else(|| {
        Error::MissingSelfLink(std::path::PathBuf::from(format!(
            "{catalog_uri}catalog.json"
        )))
    })?;

    let root_url = format!("{url_base}catalog.json");

    let capped = concurrency.max(1).min(REMOTE_MAX_CONCURRENCY);
    let (fetched, fetch_failures) = crawl_bucket(
        &bucket,
        &prefix,
        &url_base,
        root_url.clone(),
        root_value,
        capped,
    )
    .await?;
    finalize_report(fetched, fetch_failures, &root_url, capped, options).await
}

/// Shared post-crawl work: derive base URL, build validator, run the three
/// checks concurrently against every fetched document, prepend fetch failures.
async fn finalize_report(
    fetched: HashMap<String, Value>,
    mut fetch_failures: Vec<Failure>,
    root_url: &str,
    concurrency: usize,
    options: ValidateOptions,
) -> Result<ValidationReport> {
    let base_url = fetched
        .get(root_url)
        .and_then(derive_base_url_from_self_link)
        .unwrap_or_else(|| strip_last_segment(root_url));

    let validator = build_validator(options).await?;

    use futures::stream::{StreamExt, TryStreamExt};
    let base_arc = Arc::new(base_url);
    let seen_arc: Arc<HashSet<String>> = Arc::new(fetched.keys().cloned().collect());

    let per_doc: Vec<Vec<Failure>> =
        futures::stream::iter(fetched.into_iter().map(|(url, value)| {
            let validator = validator.clone();
            let base = Arc::clone(&base_arc);
            let seen = Arc::clone(&seen_arc);
            async move {
                check_remote_doc(url, value, validator.as_deref(), &base, &seen, options).await
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;

    let files_checked = per_doc.len();
    let mut failures: Vec<Failure> = per_doc.into_iter().flatten().collect();
    failures.append(&mut fetch_failures);

    Ok(ValidationReport {
        files_checked,
        failures,
    })
}

async fn check_remote_doc(
    url: String,
    value: Value,
    validator: Option<&Mutex<Validator>>,
    base_url: &str,
    seen: &HashSet<String>,
    options: ValidateOptions,
) -> Result<Vec<Failure>> {
    let mut out = Vec::new();

    if let (true, Some(v)) = (options.check_schema, validator) {
        out.extend(run_schema_check(&url, &value, v).await);
    }
    if options.check_links {
        for href in all_hrefs(&value) {
            if href.starts_with(base_url) && !seen.contains(&href) {
                out.push(Failure {
                    location: url.clone(),
                    kind: FailureKind::Link,
                    message: format!("href {href} points inside catalog base but wasn't reachable"),
                });
            }
        }
    }
    if options.check_overture_rules {
        out.extend(check_overture_rules(&url, &value));
    }

    Ok(out)
}

async fn crawl_http(
    client: &reqwest::Client,
    root_url: &str,
    concurrency: usize,
) -> Result<(HashMap<String, Value>, Vec<Failure>)> {
    let base_url = strip_last_segment(root_url);
    let mut fetched: HashMap<String, Value> = HashMap::new();
    let mut failures: Vec<Failure> = Vec::new();
    let mut frontier: Vec<String> = vec![root_url.to_string()];

    while !frontier.is_empty() {
        // Dedupe within batch + against already-fetched.
        let batch: Vec<String> = {
            let mut s: HashSet<String> = frontier.drain(..).collect();
            s.retain(|u| !fetched.contains_key(u));
            s.into_iter().collect()
        };
        if batch.is_empty() {
            break;
        }

        use futures::stream::StreamExt;
        let results: Vec<(String, std::result::Result<bytes::Bytes, String>)> =
            futures::stream::iter(batch.into_iter().map(|u| {
                let client = client.clone();
                async move {
                    let r = fetch_bytes(&client, &u).await;
                    (u, r)
                }
            }))
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for (url, res) in results {
            match res {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(value) => {
                        for href in crawlable_hrefs(&value) {
                            if href.starts_with(&base_url) && !fetched.contains_key(&href) {
                                frontier.push(href);
                            }
                        }
                        fetched.insert(url, value);
                    }
                    Err(e) => failures.push(Failure {
                        location: url,
                        kind: FailureKind::Schema,
                        message: format!("body is not valid JSON: {e}"),
                    }),
                },
                Err(msg) => failures.push(Failure {
                    location: url,
                    kind: FailureKind::Link,
                    message: msg,
                }),
            }
        }
    }

    Ok((fetched, failures))
}

async fn fetch_bytes(
    client: &reqwest::Client,
    url: &str,
) -> std::result::Result<bytes::Bytes, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("fetch error: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    resp.bytes()
        .await
        .map_err(|e| format!("body read error: {e}"))
}

/// Bucket-mode BFS. Same shape as [`crawl_http`], but each URL is translated
/// to a bucket key by stripping the URL base and prepending the bucket prefix.
/// The root is fetched by the caller (needed to derive `url_base` in the first
/// place) and passed in.
async fn crawl_bucket(
    bucket: &Bucket,
    prefix: &str,
    url_base: &str,
    root_url: String,
    root_value: Value,
    concurrency: usize,
) -> Result<(HashMap<String, Value>, Vec<Failure>)> {
    let mut fetched: HashMap<String, Value> = HashMap::new();
    let mut failures: Vec<Failure> = Vec::new();
    let mut frontier: Vec<String> = crawlable_hrefs(&root_value)
        .into_iter()
        .filter(|h| h.starts_with(url_base))
        .collect();
    fetched.insert(root_url, root_value);

    while !frontier.is_empty() {
        let batch: Vec<String> = {
            let mut s: HashSet<String> = frontier.drain(..).collect();
            s.retain(|u| !fetched.contains_key(u));
            s.into_iter().collect()
        };
        if batch.is_empty() {
            break;
        }

        use futures::stream::StreamExt;
        let results: Vec<(String, std::result::Result<Value, String>)> =
            futures::stream::iter(batch.into_iter().map(|u| {
                let bucket = bucket.clone_ref();
                let prefix = prefix.to_string();
                let url_base = url_base.to_string();
                async move {
                    let Some(rel) = u.strip_prefix(&url_base) else {
                        return (
                            u,
                            Err("frontier url outside base — internal bug".to_string()),
                        );
                    };
                    let key = format!("{prefix}{rel}");
                    let r = get_json(&bucket, &key).await.map_err(|e| format!("{e}"));
                    (u, r)
                }
            }))
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for (url, res) in results {
            match res {
                Ok(value) => {
                    for href in crawlable_hrefs(&value) {
                        if href.starts_with(url_base) && !fetched.contains_key(&href) {
                            frontier.push(href);
                        }
                    }
                    fetched.insert(url, value);
                }
                Err(msg) => failures.push(Failure {
                    location: url,
                    kind: FailureKind::Link,
                    message: msg,
                }),
            }
        }
    }

    Ok((fetched, failures))
}

// ─── SHARED HELPERS ──────────────────────────────────────────────────────────

async fn build_validator(options: ValidateOptions) -> Result<Option<Arc<Mutex<Validator>>>> {
    if options.check_schema {
        Ok(Some(Arc::new(Mutex::new(
            Validator::new()
                .await
                .context("initialising STAC validator")?,
        ))))
    } else {
        Ok(None)
    }
}

async fn run_schema_check(
    location: &str,
    value: &Value,
    validator: &Mutex<Validator>,
) -> Vec<Failure> {
    let mut out = Vec::new();
    let mut v = validator.lock().await;
    if let Err(e) = v.validate(value).await {
        // Explode Validation(Vec<_>) into one Failure per sub-error; the
        // aggregate Display just prints "N validation error(s)".
        match e {
            stac_validate::Error::Validation(items) => {
                for item in items {
                    out.push(Failure {
                        location: location.to_string(),
                        kind: FailureKind::Schema,
                        message: strip_instance_dump(item.to_string()),
                    });
                }
            }
            other => out.push(Failure {
                location: location.to_string(),
                kind: FailureKind::Schema,
                message: format!("{other}"),
            }),
        }
    }
    out
}

fn derive_base_url_from_self_link(root: &Value) -> Option<String> {
    let self_href = root
        .get("links")
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|l| l.get("rel").and_then(|r| r.as_str()) == Some("self"))
        })
        .and_then(|l| l.get("href").and_then(|h| h.as_str()))?;
    Some(strip_last_segment(self_href))
}

fn strip_last_segment(url_or_path: &str) -> String {
    let base = url_or_path.rsplitn(2, '/').nth(1).unwrap_or("");
    format!("{base}/")
}

fn all_hrefs(value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(links) = value.get("links").and_then(|v| v.as_array()) {
        for link in links {
            if let Some(href) = link.get("href").and_then(|h| h.as_str()) {
                out.push(href.to_string());
            }
        }
    }
    if let Some(assets) = value.get("assets").and_then(|v| v.as_object()) {
        for asset in assets.values() {
            if let Some(href) = asset.get("href").and_then(|h| h.as_str()) {
                out.push(href.to_string());
            }
        }
    }
    out
}

/// Discover child + item link targets for the recursive crawl. `pub` so
/// integration tests can exercise the discovery rule directly.
pub fn crawlable_hrefs(value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(links) = value.get("links").and_then(|v| v.as_array()) {
        for link in links {
            let rel = link.get("rel").and_then(|r| r.as_str()).unwrap_or("");
            if rel == "child" || rel == "item" {
                if let Some(href) = link.get("href").and_then(|h| h.as_str()) {
                    out.push(href.to_string());
                }
            }
        }
    }
    out
}

// ─── OVERTURE RULES ──────────────────────────────────────────────────────────

fn check_overture_rules(location: &str, value: &Value) -> Vec<Failure> {
    let mut out = Vec::new();
    let ty = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match ty {
        "Catalog" => check_catalog(location, value, &mut out),
        "Collection" => check_collection(location, value, &mut out),
        "Feature" => check_item(location, value, &mut out),
        _ => {}
    }
    out
}

fn check_catalog(location: &str, value: &Value, out: &mut Vec<Failure>) {
    let is_release_catalog = value.get("release:version").is_some();
    if is_release_catalog {
        match value.get("schema:version") {
            None => {}
            Some(v) if v.is_null() => push_rule(
                out,
                location,
                "schema:version is null — bug #115 marker; omit the field instead",
            ),
            Some(v) if !v.is_string() => {
                push_rule(out, location, "schema:version present but not a string")
            }
            _ => {}
        }
        if let Some(tag) = value.get("schema:tag").and_then(|v| v.as_str()) {
            if tag.contains("/vNone") {
                push_rule(
                    out,
                    location,
                    "schema:tag contains 'vNone' — bug #115 marker",
                );
            }
        }
    }

    if let Some(links) = value.get("links").and_then(|v| v.as_array()) {
        for link in links {
            if link.get("rel").and_then(|v| v.as_str()) == Some("pmtiles") {
                let mt = link.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if mt != PMTILES_MEDIA_TYPE {
                    push_rule(
                        out,
                        location,
                        &format!(
                            "pmtiles link has media type '{mt}', expected '{PMTILES_MEDIA_TYPE}'"
                        ),
                    );
                }
            }
        }
    }
}

fn check_collection(location: &str, value: &Value, out: &mut Vec<Failure>) {
    match value.get("table:columns") {
        Some(Value::Array(cols)) if cols.is_empty() => {
            push_rule(out, location, "table:columns is empty")
        }
        Some(v) if !v.is_array() => {
            push_rule(out, location, "table:columns present but not an array")
        }
        _ => {}
    }
    if let Some(lic) = value.get("license").and_then(|v| v.as_str()) {
        if !ALLOWED_LICENSES.contains(&lic) {
            push_rule(
                out,
                location,
                &format!("license '{lic}' not in allowed set {:?}", ALLOWED_LICENSES),
            );
        }
        if lic == "other" {
            let has_license_link = value
                .get("links")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .any(|l| l.get("rel").and_then(|r| r.as_str()) == Some("license"))
                })
                .unwrap_or(false);
            if !has_license_link {
                push_rule(
                    out,
                    location,
                    "license='other' but no rel=license link on collection",
                );
            }
        }
    }
}

fn check_item(location: &str, value: &Value, out: &mut Vec<Failure>) {
    if let Some(bbox) = value.get("bbox").and_then(|v| v.as_array()) {
        if bbox.len() != 4 {
            push_rule(
                out,
                location,
                &format!("bbox has {} coords, expected 4", bbox.len()),
            );
        } else {
            let coords: Option<Vec<f64>> = bbox.iter().map(|v| v.as_f64()).collect();
            match coords {
                Some(c) => {
                    if c[0] > c[2] {
                        push_rule(out, location, "bbox xmin > xmax");
                    }
                    if c[1] > c[3] {
                        push_rule(out, location, "bbox ymin > ymax");
                    }
                }
                None => push_rule(out, location, "bbox contains non-numeric coordinate"),
            }
        }
    }

    let assets = value.get("assets").and_then(|v| v.as_object());
    let has = |k: &str| assets.map(|a| a.contains_key(k)).unwrap_or(false);
    if !has("aws") {
        push_rule(out, location, "item missing 'aws' asset");
    }
    if !has("azure") {
        push_rule(out, location, "item missing 'azure' asset");
    }
    if let Some(a) = assets {
        for key in ["aws", "azure"] {
            if let Some(asset) = a.get(key) {
                if asset.get("storage:refs").is_none() {
                    push_rule(
                        out,
                        location,
                        &format!("asset '{key}' missing storage:refs"),
                    );
                }
            }
        }
    }
}

fn push_rule(out: &mut Vec<Failure>, location: &str, msg: &str) {
    out.push(Failure {
        location: location.to_string(),
        kind: FailureKind::OvertureRule,
        message: msg.to_string(),
    });
}

/// stac-validate's per-error Display is:
///     `<type>[id=<id>]: <compact-json-instance> <schema-error-message>`
/// The middle JSON dump can be several KB. Strip it: keep the leading `type/id`
/// prefix (up to the first `": {"`) and the trailing schema message (after the
/// outermost `}`).
fn strip_instance_dump(full: String) -> String {
    let Some(brace) = full.rfind('}') else {
        return full;
    };
    let tail = full[brace + 1..].trim_start_matches([':', ' ']).to_string();
    match full.find(": {") {
        Some(colon) => format!("{}: {}", &full[..colon], tail),
        None => tail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn crawlable_hrefs_returns_child_and_item_only() {
        let v = json!({
            "links": [
                {"rel": "self", "href": "https://x/"},
                {"rel": "root", "href": "https://x/root"},
                {"rel": "parent", "href": "https://x/p"},
                {"rel": "child", "href": "https://x/a"},
                {"rel": "child", "href": "https://x/b"},
                {"rel": "item", "href": "https://x/c"},
                {"rel": "next", "href": "https://x/n"},
                {"rel": "prev", "href": "https://x/p2"},
                {"rel": "license", "href": "https://x/l"},
            ]
        });
        let got = crawlable_hrefs(&v);
        assert_eq!(got, vec!["https://x/a", "https://x/b", "https://x/c"]);
    }

    #[test]
    fn crawlable_hrefs_empty_when_no_links() {
        assert!(crawlable_hrefs(&json!({})).is_empty());
        assert!(crawlable_hrefs(&json!({"links": []})).is_empty());
    }

    #[test]
    fn strip_last_segment_url() {
        assert_eq!(
            strip_last_segment("https://stac.overturemaps.org/catalog.json"),
            "https://stac.overturemaps.org/"
        );
        assert_eq!(
            strip_last_segment("https://stac.overturemaps.org/2026-08-19.0/catalog.json"),
            "https://stac.overturemaps.org/2026-08-19.0/"
        );
    }

    #[test]
    fn derive_base_url_reads_self_link() {
        let v = json!({
            "links": [
                {"rel": "root", "href": "https://x/root"},
                {"rel": "self", "href": "https://x/catalog.json"},
            ]
        });
        assert_eq!(
            derive_base_url_from_self_link(&v).as_deref(),
            Some("https://x/")
        );
    }

    #[test]
    fn derive_base_url_none_without_self_link() {
        let v = json!({"links": [{"rel": "root", "href": "https://x/"}]});
        assert!(derive_base_url_from_self_link(&v).is_none());
    }

    #[test]
    fn strip_instance_dump_extracts_prefix_and_tail() {
        let full =
            r#"Catalog[id=X]: {"type":"Catalog","id":"X"} is not valid under any of the schemas"#;
        assert_eq!(
            strip_instance_dump(full.to_string()),
            "Catalog[id=X]: is not valid under any of the schemas"
        );
    }
}
