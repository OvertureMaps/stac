//! Discover available PMTiles files for a release, mirroring OvertureRelease._get_available_pmtiles.

use anyhow::Result;
use std::collections::BTreeMap;

use crate::s3::{list_top_level, Bucket};

pub async fn discover(bucket_extras: &Bucket, release: &str) -> BTreeMap<String, String> {
    // The Python code catches all exceptions and warns; we do the same — a missing tiles
    // directory is not an error.
    let prefix = format!("tiles/{release}");
    match list_top_level(bucket_extras, &prefix).await {
        Ok(names) => names
            .into_iter()
            .filter(|n| n.ends_with(".pmtiles"))
            .map(|n| {
                let base = n.trim_end_matches(".pmtiles").to_string();
                let full = format!("overturemaps-extras-us-west-2/{prefix}/{n}");
                (base, full)
            })
            .collect(),
        Err(e) => {
            tracing::warn!("Couldn't find PMTiles for release {release}: {e}");
            BTreeMap::new()
        }
    }
}

pub fn discover_sync(_release: &str) -> Result<BTreeMap<String, String>> {
    // Not used; discover() is async. Placeholder for future sync callers.
    Ok(BTreeMap::new())
}
