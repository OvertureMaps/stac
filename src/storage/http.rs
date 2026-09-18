//! HTTP fetches against a published STAC catalog.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

use crate::{Result, ResultExt};

// Regex: allows only tags of the form `v<semver>` (e.g., `v1.2.3`)
static SCHEMA_VERSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^v(\d+\.\d+\.\d+)$").expect("static regex"));

const USER_AGENT: &str = concat!("overture-stac/", env!("CARGO_PKG_VERSION"));

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("building HTTP client")
}

async fn fetch_json(url: &str) -> Result<serde_json::Value> {
    http_client()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("HTTP error from {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing JSON from {url}"))
}

/// Read `schema:version` from `<root_href>/<release_version>/catalog.json`.
/// Returns `None` when the field is absent or explicitly null.
pub async fn fetch_schema_version(
    root_href: &str,
    release_version: &str,
) -> Result<Option<String>> {
    let url = format!(
        "{}/{}/catalog.json",
        root_href.trim_end_matches('/'),
        release_version
    );
    let body = fetch_json(&url).await?;
    Ok(body
        .get("schema:version")
        .and_then(|v| v.as_str())
        .map(String::from))
}

/// Resolve `schema:version` by finding the `v<semver>` tag
/// that is currently coupled with the `data-<release_id>` tag on GitHub.
/// This allows to map a schema version to the corresponding release version
pub async fn resolve_schema_version_from_github(release_id: &str) -> Result<Option<String>> {
    let tags = fetch_all_schema_tags().await?;

    let data_tag = format!("data-{release_id}");
    let Some(data_sha) = tags.get(&data_tag) else {
        tracing::debug!(
            release_id,
            "no data-tag on OvertureMaps/schema; schema:version stays null"
        );
        return Ok(None);
    };

    let mut candidates: Vec<String> = tags
        .iter()
        .filter(|(_, sha)| sha == &data_sha)
        .filter_map(|(name, _)| SCHEMA_VERSION_RE.captures(name).map(|c| c[1].to_string()))
        .collect();

    match candidates.len() {
        0 => {
            tracing::warn!(
                release_id,
                %data_sha,
                "data-tag has no paired v<semver> tag; schema:version stays null"
            );
            Ok(None)
        }
        1 => Ok(candidates.pop()),
        _ => {
            candidates.sort_by(|a, b| compare_semver(b, a));
            tracing::warn!(
                release_id,
                ?candidates,
                "multiple v<semver> tags on data-tag commit; picking highest"
            );
            Ok(Some(candidates.remove(0)))
        }
    }
}

async fn fetch_all_schema_tags() -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for page in 1..=10 {
        let url = format!(
            "https://api.github.com/repos/OvertureMaps/schema/tags?per_page=100&page={page}"
        );
        let body = fetch_json(&url).await?;
        let Some(arr) = body.as_array() else { break };
        if arr.is_empty() {
            break;
        }
        for entry in arr {
            let (Some(name), Some(sha)) = (
                entry.get("name").and_then(|v| v.as_str()),
                entry
                    .get("commit")
                    .and_then(|c| c.get("sha"))
                    .and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            out.insert(name.to_string(), sha.to_string());
        }
        if arr.len() < 100 {
            break;
        }
    }
    Ok(out)
}

fn compare_semver(a: &str, b: &str) -> std::cmp::Ordering {
    fn parts(v: &str) -> (u64, u64, u64) {
        let mut it = v.split('.').map(|s| s.parse::<u64>().unwrap_or(0));
        (
            it.next().unwrap_or(0),
            it.next().unwrap_or(0),
            it.next().unwrap_or(0),
        )
    }
    parts(a).cmp(&parts(b))
}
