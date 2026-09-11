//! HTTP fetches against a published STAC catalog.

use crate::{Result, ResultExt};

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
