//! HTTP fetches against a published STAC catalog.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::{Result, ResultExt};

const USER_AGENT: &str = concat!("overture-stac/", env!("CARGO_PKG_VERSION"));

/// Overture release calendar. Shipped releases carry a `v<semver>` `schemaVersion`, upcoming ones are null.
pub const RELEASE_CALENDAR_URL: &str = "https://docs.overturemaps.org/release-calendar.json";

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

/// Look up `release_id` in the release calendar; returns its `schemaVersion` without the `v`, or `None` if absent/unshipped.
pub async fn resolve_schema_version_from_calendar(release_id: &str) -> Result<Option<String>> {
    static CALENDAR_CACHE: LazyLock<tokio::sync::OnceCell<Option<ReleaseCalendar>>> =
        LazyLock::new(tokio::sync::OnceCell::new);

    let cached = CALENDAR_CACHE
        .get_or_init(|| async {
            match fetch_json(RELEASE_CALENDAR_URL).await {
                Ok(body) => Some(parse_release_calendar(&body)),
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "failed to fetch release calendar; schema:version stays null for the rest of this run"
                    );
                    None
                }
            }
        })
        .await;

    let Some(calendar) = cached else {
        return Ok(None);
    };

    match calendar.get(release_id) {
        None => {
            tracing::debug!(
                release_id,
                "release not in release calendar; schema:version stays null"
            );
            Ok(None)
        }
        Some(None) => {
            tracing::debug!(
                release_id,
                "release calendar has no schemaVersion yet; schema:version stays null"
            );
            Ok(None)
        }
        Some(Some(v)) => Ok(Some(v.clone())),
    }
}

/// `dataVersion` -> `schemaVersion` without the `v` prefix (`None` when null).
type ReleaseCalendar = HashMap<String, Option<String>>;

fn parse_release_calendar(body: &serde_json::Value) -> ReleaseCalendar {
    body.get("releases")
        .and_then(|r| r.as_array())
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let data_version = entry.get("dataVersion")?.as_str()?;
            let schema_version = entry
                .get("schemaVersion")
                .and_then(|v| v.as_str())
                .map(|v| v.strip_prefix('v').unwrap_or(v).to_string());
            Some((data_version.to_string(), schema_version))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_calendar_entries() {
        let body = json!({
            "releases": [
                {"dataVersion": "2024-04-16-beta.0", "schemaVersion": "v0.10.0-beta"},
                {"dataVersion": "2026-08-19.0", "schemaVersion": "v1.18.0"},
                {"dataVersion": "2026-10-21.0", "schemaVersion": null},
                {"schemaVersion": "v9.9.9"},
            ]
        });
        let cal = parse_release_calendar(&body);
        assert_eq!(cal.len(), 3);
        assert_eq!(cal["2026-08-19.0"].as_deref(), Some("1.18.0"));
        assert_eq!(cal["2024-04-16-beta.0"].as_deref(), Some("0.10.0-beta"));
        assert_eq!(cal["2026-10-21.0"], None);
        assert!(!cal.contains_key("2026-12-16.0"));
    }

    #[test]
    fn tolerates_missing_releases_key() {
        assert!(parse_release_calendar(&json!({})).is_empty());
    }
}
