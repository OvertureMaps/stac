//! HTTP fetches against a published STAC catalog.
//!
//! - [`fetch_schema_version`] reads `schema:version` from
//!   `<root_href>/<release_version>/catalog.json`.
//! - [`fetch_catalog_children`] reads the release IDs from the root catalog's
//!   `rel: child` links.

use anyhow::{Context, Result};

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

/// Fetch the root catalog and return the release IDs from its `rel: child` links.
///
/// Each child href has the shape `<root_href>/<release_id>/catalog.json`; we take
/// the second-to-last path segment. Skips any link whose shape doesn't match.
pub async fn fetch_catalog_children(root_href: &str) -> Result<Vec<String>> {
    let url = format!("{}/catalog.json", root_href.trim_end_matches('/'));
    let body = fetch_json(&url).await?;
    let mut ids = Vec::new();
    let Some(links) = body.get("links").and_then(|v| v.as_array()) else {
        return Ok(ids);
    };
    for link in links {
        if link.get("rel").and_then(|v| v.as_str()) != Some("child") {
            continue;
        }
        let Some(href) = link.get("href").and_then(|v| v.as_str()) else {
            continue;
        };
        if let Some(id) = release_id_from_href(href) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn release_id_from_href(href: &str) -> Option<String> {
    // Strip a trailing "/catalog.json" if present, then take the last path segment.
    let trimmed = href.trim_end_matches("/catalog.json");
    trimmed
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::release_id_from_href;

    #[test]
    fn extracts_id_from_full_url() {
        assert_eq!(
            release_id_from_href("https://stac.overturemaps.org/2026-08-19.0/catalog.json"),
            Some("2026-08-19.0".to_string())
        );
    }

    #[test]
    fn extracts_id_from_relative_href() {
        assert_eq!(
            release_id_from_href("./2026-07-22.0/catalog.json"),
            Some("2026-07-22.0".to_string())
        );
    }

    #[test]
    fn returns_none_for_empty_href() {
        assert_eq!(release_id_from_href(""), None);
    }
}
