//! `latest.ddb` — a DuckDB database of views over the newest release's parquet files,
//! published next to the root catalog. The SQL is a build input only; the database file
//! is the artifact. The views follow the release's own `theme=*/type=*` layout, so new
//! feature types appear without a code change.

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
    // Views are named after the type alone, so a type shared across themes would collide.
    if let Some(pair) = views.windows(2).find(|w| w[0].r#type == w[1].r#type) {
        return Err(Error::LatestDuckdb(format!(
            "type `{}` appears in themes `{}` and `{}`",
            pair[0].r#type, pair[0].theme, pair[1].theme
        )));
    }
    Ok(views)
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
    // httpfs reads `s3://` and is fetched at build time; parquet is compiled in.
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

/// True when `latest.ddb` must be rebuilt: the `latest` release changed or the root had no link.
pub fn latest_duckdb_stale(before: &Value, after: &Value) -> bool {
    let has_link = |root: &Value| {
        root.get("links")
            .and_then(|v| v.as_array())
            .is_some_and(|l| l.iter().any(|l| l["href"] == LATEST_DUCKDB_HREF))
    };
    before.get("latest") != after.get("latest") || has_link(before) != has_link(after)
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
        {
            let conn = ::duckdb::Connection::open(&path)
                .map_err(|e| Error::LatestDuckdb(e.to_string()))?;
            conn.execute_batch(&sql)
                .map_err(|e| Error::LatestDuckdb(e.to_string()))?;
        }
        Ok(std::fs::read(&path)?)
    })
    .await
    .map_err(|e| Error::LatestDuckdb(e.to_string()))??;
    put_bytes(catalog_bucket, LATEST_DUCKDB_KEY, bytes).await
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

#[cfg(test)]
mod tests {
    use super::*;

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
