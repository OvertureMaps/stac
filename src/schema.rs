//! Schema-version discovery for a release.
//!
//! Reads `schema:version` from the published STAC catalog at
//! `<root_href>/<release_version>/catalog.json`. Returns `None` when the field is
//! absent or explicitly null — currently the upstream catalog always ships null
//! there (tracked separately), so callers should treat `None` as "no schema
//! version available; produce a catalog without it."

use anyhow::{Context, Result};

const USER_AGENT: &str = concat!("overture-stac/", env!("CARGO_PKG_VERSION"));

pub async fn fetch_schema_version(
    root_href: &str,
    release_version: &str,
) -> Result<Option<String>> {
    let url = format!(
        "{}/{}/catalog.json",
        root_href.trim_end_matches('/'),
        release_version
    );
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("building HTTP client")?;
    let body: serde_json::Value = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("HTTP error from {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing JSON from {url}"))?;
    Ok(body
        .get("schema:version")
        .and_then(|v| v.as_str())
        .map(String::from))
}
