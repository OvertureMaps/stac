//! Root catalog (`catalog.json`) parsing and mutation — the umbrella document that lists
//! every release as a `rel: child` link. Used by `reconcile` to compute drift and by the
//! apply path to insert/remove releases while keeping VCS provenance stamped on the root.

use serde_json::{json, Value};

use crate::storage::{get_json, Bucket};
use crate::{Error, Result};

const VCS_EXTENSION_URL: &str = "https://stac-extensions.github.io/vcs/v0.1.0/schema.json";

/// Read the root `catalog.json` from a catalog bucket and extract release IDs
/// from its `rel: child` links.
pub async fn read_catalog_children(
    catalog_bucket: &Bucket,
    catalog_prefix: &str,
) -> Result<Vec<String>> {
    let key = format!("{catalog_prefix}catalog.json");
    let root = get_json(catalog_bucket, &key).await?;
    Ok(children_from_root(&root))
}

pub fn children_from_root(root: &Value) -> Vec<String> {
    let Some(links) = root.get("links").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for link in links {
        if link.get("rel").and_then(|v| v.as_str()) != Some("child") {
            continue;
        }
        let Some(href) = link.get("href").and_then(|v| v.as_str()) else {
            continue;
        };
        if let Some(id) = release_id_from_href(href) {
            out.push(id);
        }
    }
    out
}

pub fn release_id_from_href(href: &str) -> Option<String> {
    href.trim_end_matches("/catalog.json")
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(String::from)
}

pub fn build_empty_root() -> Value {
    json!({
        "type": "Catalog",
        "id": "Overture Releases",
        "description": "All Overture Releases",
        "stac_version": "1.0.0",
        "links": [],
    })
}

pub fn remove_child_link(root: &mut Value, release: &str) {
    let Some(links) = root.get_mut("links").and_then(|v| v.as_array_mut()) else {
        return;
    };
    links.retain(|link| match link.get("href").and_then(|v| v.as_str()) {
        Some(href) => release_id_from_href(href).as_deref() != Some(release),
        None => true,
    });
}

pub fn add_child_link(root: &mut Value, release: &str, root_href: &str) -> Result<()> {
    let obj = root
        .as_object_mut()
        .ok_or_else(|| Error::MalformedCatalog("root is not a JSON object".into()))?;
    let links = obj
        .entry("links".to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| Error::MalformedCatalog("links is not an array".into()))?;
    // Idempotency: skip if a child link for this release already exists.
    let already_present = links.iter().any(|link| {
        link.get("rel").and_then(|v| v.as_str()) == Some("child")
            && link
                .get("href")
                .and_then(|v| v.as_str())
                .and_then(release_id_from_href)
                .as_deref()
                == Some(release)
    });
    if already_present {
        return Ok(());
    }
    links.push(json!({
        "rel": "child",
        "href": format!("{root_href}/{release}/catalog.json"),
        "type": "application/json",
        "title": format!("{release} Overture Release"),
    }));
    Ok(())
}

pub fn stamp_vcs(root: &mut Value) -> Result<()> {
    let obj = root
        .as_object_mut()
        .ok_or_else(|| Error::MalformedCatalog("root is not a JSON object".into()))?;
    let extensions = obj
        .entry("stac_extensions".to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| Error::MalformedCatalog("stac_extensions is not an array".into()))?;
    if !extensions
        .iter()
        .any(|v| v.as_str() == Some(VCS_EXTENSION_URL))
    {
        extensions.push(json!(VCS_EXTENSION_URL));
    }
    obj.insert("vcs:type".into(), json!("git"));
    obj.insert("vcs:branch".into(), json!(env!("GIT_BRANCH")));
    obj.insert("vcs:commit".into(), json!(env!("GIT_COMMIT")));
    Ok(())
}
