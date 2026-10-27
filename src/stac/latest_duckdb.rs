//! `latest.ddb` — a DuckDB database of views over the newest release's parquet files,
//! published next to the root catalog. The views follow the release's own
//! `theme=*/type=*` layout, so new feature types appear without a code change. The
//! database is written by the DuckDB CLI, which keeps the DuckDB engine out of the Rust build.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::json;
use serde_json::Value;

use crate::storage::{list_top_level, put_bytes, Bucket};
use crate::{Error, Result, ResultExt};

/// Key of the DuckDB file relative to the catalog root.
pub const LATEST_DUCKDB_KEY: &str = "latest.ddb";

/// Relative href of the root link to the DuckDB file. Also identifies that link on refresh.
pub const LATEST_DUCKDB_HREF: &str = "./latest.ddb";

const LATEST_DUCKDB_REL: &str = "alternate";
const LATEST_DUCKDB_MEDIA_TYPE: &str = "application/vnd.duckdb";

/// Stored on every view so readers see the requirement in `duckdb_views()`.
const VIEW_COMMENT: &str = "Requires the spatial extension: run LOAD spatial before querying.";

/// One view in `latest.ddb`, named after its feature type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct View {
    pub theme: String,
    pub r#type: String,
}

/// List every `theme=*/type=*` pair under `release/<latest>` in the data bucket.
pub async fn discover_views(data_bucket: &Bucket, latest: &str) -> Result<Vec<View>> {
    let release_prefix = format!("release/{latest}");
    let mut views = Vec::new();
    for theme_key in list_top_level(data_bucket, &release_prefix).await? {
        let Some(theme) = theme_key.strip_prefix("theme=") else {
            continue;
        };
        let theme_prefix = format!("{release_prefix}/{theme_key}");
        for type_key in list_top_level(data_bucket, &theme_prefix).await? {
            if let Some(ty) = type_key.strip_prefix("type=") {
                views.push(View {
                    theme: theme.to_string(),
                    r#type: ty.to_string(),
                });
            }
        }
    }
    views.sort();
    ensure_unique_types(&views)?;
    Ok(views)
}

/// Views are named after the type alone, so a type shared across themes would collide.
fn ensure_unique_types(views: &[View]) -> Result<()> {
    let mut seen = std::collections::HashMap::new();
    for v in views {
        if let Some(other) = seen.insert(v.r#type.as_str(), v.theme.as_str()) {
            return Err(Error::LatestDuckdb(format!(
                "type `{}` appears in themes `{other}` and `{}`",
                v.r#type, v.theme
            )));
        }
    }
    Ok(())
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// SQL that creates one view per feature type over `<data_uri>/release/<latest>/...`.
pub fn build_views_sql(latest: &str, data_uri: &str, views: &[View]) -> String {
    let data_uri = data_uri.trim_end_matches('/');
    // Spatial must be loaded at build time: views store the column types they were created
    // with, so a BLOB-typed view breaks as soon as a reader loads spatial. Readers need
    // `LOAD spatial` to open the file.
    // httpfs reads `s3://` and is fetched at build time; parquet is built into the CLI.
    let mut stmts = vec![
        "INSTALL httpfs;".to_string(),
        "LOAD httpfs;".to_string(),
        "INSTALL spatial;".to_string(),
        "LOAD spatial;".to_string(),
    ];
    for View { theme, r#type } in views {
        let path = quote_literal(&format!(
            "{data_uri}/release/{latest}/theme={theme}/type={type}/*.parquet"
        ));
        let view = quote_ident(r#type);
        stmts.push(format!(
            "CREATE OR REPLACE VIEW {view} AS (\n  SELECT * FROM read_parquet({path})\n);\n\
             COMMENT ON VIEW {view} IS {};",
            quote_literal(VIEW_COMMENT)
        ));
    }
    stmts.join("\n\n")
}

fn has_latest_duckdb_link(root: &Value) -> bool {
    root.get("links")
        .and_then(|v| v.as_array())
        .is_some_and(|l| l.iter().any(|l| l["href"] == LATEST_DUCKDB_HREF))
}

/// True when `latest.ddb` must be rebuilt: the `latest` release changed or the root had no link.
pub fn latest_duckdb_stale(before: &Value, after: &Value) -> bool {
    before.get("latest") != after.get("latest") || !has_latest_duckdb_link(before)
}

/// Bring `latest.ddb` and its root link in line with `root`, whose `latest` is already
/// refreshed. `before` is the root as read. Call before writing the root, so it never links
/// to a file that is not uploaded yet.
///
/// - `data` is `Some((data_bucket, data_uri))`: rebuild and upload when stale, then link it.
///   With no `latest`, drop the link.
/// - `None`: drop the link when `latest` changed, so it never points at the previous release.
pub async fn sync_latest_duckdb(
    catalog_bucket: &Bucket,
    data: Option<(&Bucket, &str)>,
    before: &Value,
    root: &mut Value,
) -> Result<()> {
    let latest = root
        .get("latest")
        .and_then(|v| v.as_str())
        .map(String::from);
    match (data, latest) {
        (Some((data_bucket, data_uri)), Some(latest)) => {
            if latest_duckdb_stale(before, root) {
                publish_latest_duckdb(catalog_bucket, data_bucket, data_uri, &latest).await?;
                println!("Published latest.ddb for {latest}.");
            }
            ensure_latest_duckdb_link(root);
        }
        (Some(_), None) => remove_latest_duckdb_link(root),
        (None, _) => {
            if before.get("latest") != root.get("latest") && has_latest_duckdb_link(root) {
                remove_latest_duckdb_link(root);
                println!(
                    "Removed latest.ddb link: `latest` changed and --generate-latest-ddb is off."
                );
            }
        }
    }
    Ok(())
}

/// Fail fast when the DuckDB CLI cannot run, before anything is published.
pub fn check_duckdb_cli() -> Result<()> {
    let bin = duckdb_bin();
    let out = Command::new(&bin)
        .arg("-version")
        .output()
        .map_err(|e| spawn_error(&bin, &e))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(Error::LatestDuckdb(format!(
            "`{bin} -version` exited with {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

fn duckdb_bin() -> String {
    std::env::var("DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string())
}

fn spawn_error(bin: &str, e: &std::io::Error) -> Error {
    Error::LatestDuckdb(format!(
        "could not run `{bin}` ({e}); install the DuckDB CLI or set DUCKDB_BIN"
    ))
}

/// Build `latest.ddb` over `latest` under `data_uri` (including any path prefix) and upload
/// it to the catalog root.
pub async fn publish_latest_duckdb(
    catalog_bucket: &Bucket,
    data_bucket: &Bucket,
    data_uri: &str,
    latest: &str,
) -> Result<()> {
    let views = discover_views(data_bucket, latest).await?;
    if views.is_empty() {
        return Err(Error::LatestDuckdb(format!(
            "no theme/type directories found for release {latest}"
        )));
    }
    let sql = build_views_sql(latest, data_uri, &views);
    let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let dir = tempfile::tempdir().context("creating temp dir for latest.ddb")?;
        let path = dir.path().join(LATEST_DUCKDB_KEY);
        run_duckdb_cli(&path, &sql)?;
        Ok(std::fs::read(&path)?)
    })
    .await
    .map_err(|e| Error::LatestDuckdb(e.to_string()))??;
    put_bytes(catalog_bucket, LATEST_DUCKDB_KEY, bytes).await
}

/// Run `sql` against the database at `path` with the DuckDB CLI (`$DUCKDB_BIN`, default
/// `duckdb`). The script is piped over stdin and `-bail` aborts on the first failing statement.
fn run_duckdb_cli(path: &Path, sql: &str) -> Result<()> {
    let bin = duckdb_bin();
    let mut child = Command::new(&bin)
        .arg("-bail")
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| spawn_error(&bin, &e))?;
    child
        .stdin
        .take()
        .ok_or_else(|| Error::LatestDuckdb("no stdin for duckdb".into()))?
        .write_all(sql.as_bytes())?;
    let out = child.wait_with_output()?;
    if out.status.success() {
        Ok(())
    } else {
        Err(Error::LatestDuckdb(format!(
            "`{bin}` exited with {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

/// Add the root link to `latest.ddb`, replacing any existing one. Only that link is touched.
pub fn ensure_latest_duckdb_link(root: &mut Value) {
    let Some(links) = root.get_mut("links").and_then(|v| v.as_array_mut()) else {
        return;
    };
    links.retain(|l| l.get("href").and_then(|v| v.as_str()) != Some(LATEST_DUCKDB_HREF));
    links.push(json!({
        "rel": LATEST_DUCKDB_REL,
        "href": LATEST_DUCKDB_HREF,
        "type": LATEST_DUCKDB_MEDIA_TYPE,
        "title": "DuckDB views over the latest release",
    }));
}

/// Drop the root link to `latest.ddb`. The uploaded file is left in place, unlinked.
pub fn remove_latest_duckdb_link(root: &mut Value) {
    if let Some(links) = root.get_mut("links").and_then(|v| v.as_array_mut()) {
        links.retain(|l| l.get("href").and_then(|v| v.as_str()) != Some(LATEST_DUCKDB_HREF));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_helpers_are_idempotent_and_leave_other_links() {
        let count = |root: &Value| {
            root["links"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|l| l["href"] == LATEST_DUCKDB_HREF)
                .count()
        };
        let mut root = json!({"links": [{"rel": "self", "href": "x"}]});
        ensure_latest_duckdb_link(&mut root);
        ensure_latest_duckdb_link(&mut root);
        assert_eq!(count(&root), 1);
        remove_latest_duckdb_link(&mut root);
        assert_eq!(count(&root), 0);
        assert_eq!(root["links"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn stale_when_latest_changes_or_link_missing() {
        let linked = json!({"latest": "r1", "links": [{"href": LATEST_DUCKDB_HREF}]});
        let unlinked = json!({"latest": "r1", "links": []});
        let moved = json!({"latest": "r2", "links": []});
        assert!(!latest_duckdb_stale(&linked, &linked));
        assert!(latest_duckdb_stale(&unlinked, &unlinked));
        assert!(latest_duckdb_stale(&linked, &moved));
    }

    #[test]
    fn rejects_duplicate_types_across_themes() {
        let theme = |t: &str, ty: &str| View {
            theme: t.into(),
            r#type: ty.into(),
        };
        let views = [
            theme("base", "address"),
            theme("base", "building"),
            theme("transportation", "address"),
            theme("transportation", "building"),
        ];
        let err = ensure_unique_types(&views).unwrap_err();
        assert!(err.to_string().contains("address"), "{err}");
        assert!(ensure_unique_types(&views[..2]).is_ok());
    }

    #[test]
    fn sql_has_one_view_per_type() {
        let views = vec![
            View {
                theme: "base".into(),
                r#type: "water".into(),
            },
            View {
                theme: "places".into(),
                r#type: "place".into(),
            },
        ];
        let sql = build_views_sql("2026-09-23.0", "s3://bucket/", &views);
        assert_eq!(sql.matches("CREATE OR REPLACE VIEW").count(), 2);
        assert_eq!(sql.matches("COMMENT ON VIEW").count(), 2);
        assert!(sql.contains(
            "read_parquet('s3://bucket/release/2026-09-23.0/theme=base/type=water/*.parquet')"
        ));
        assert!(sql.contains("VIEW \"water\" AS"));
    }

    #[test]
    fn sql_keeps_uri_prefix() {
        let views = vec![View {
            theme: "base".into(),
            r#type: "water".into(),
        }];
        let sql = build_views_sql("r1", "s3://bucket/mirror/", &views);
        assert!(sql.contains("'s3://bucket/mirror/release/r1/theme=base/type=water/*.parquet'"));
    }

    #[test]
    fn sql_quotes_hostile_names() {
        let views = vec![View {
            theme: "base".into(),
            r#type: "x\"; DROP VIEW y; --'".into(),
        }];
        let sql = build_views_sql("r1", "s3://b", &views);
        assert!(sql.contains("VIEW \"x\"\"; DROP VIEW y; --'\" AS"));
        assert!(sql.contains("type=x\"; DROP VIEW y; --''/*.parquet'"));
    }
}
