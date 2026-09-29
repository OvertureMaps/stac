//! Root catalog (`catalog.json`) parsing and mutation — the umbrella document that lists
//! every release as a `rel: child` link. Used by `reconcile` to compute drift and by the
//! apply path to insert/remove releases while keeping VCS provenance stamped on the root.

use serde_json::{json, Value};

use crate::storage::{get_json, Bucket};
use crate::{Error, Result};

const VCS_EXTENSION_URL: &str = "https://stac-extensions.github.io/vcs/v0.1.0/schema.json";

/// Title of the root catalog, used for `rel: root` / `rel: parent` link titles on sub-catalogs.
pub const ROOT_CATALOG_TITLE: &str = "Overture Releases";

/// Read the root `catalog.json` from a catalog bucket and extract release IDs
/// from its `rel: child` links. Assumes the bucket handle is rooted at the
/// catalog prefix (see [`Bucket::from_url`][crate::storage::Bucket::from_url]).
pub async fn read_catalog_children(catalog_bucket: &Bucket) -> Result<Vec<String>> {
    let root = get_json(catalog_bucket, "catalog.json").await?;
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

/// Recompute derived state on the root: top-level `"latest": "<id>"`, per-child-link
/// `"latest": true` on the newest release only, and normalized `"title"` on every child link.
pub fn refresh_latest(root: &mut Value) {
    let mut children = children_from_root(root);
    children.sort_by(|a, b| b.cmp(a));
    let latest = children.into_iter().next();

    if let Some(obj) = root.as_object_mut() {
        match &latest {
            Some(id) => {
                obj.insert("latest".into(), json!(id));
            }
            None => {
                obj.remove("latest");
            }
        }
    }

    let Some(links) = root.get_mut("links").and_then(|v| v.as_array_mut()) else {
        return;
    };
    for link in links {
        if link.get("rel").and_then(|v| v.as_str()) != Some("child") {
            continue;
        }
        let release = link
            .get("href")
            .and_then(|v| v.as_str())
            .and_then(release_id_from_href);
        let is_latest = release.as_deref() == latest.as_deref();
        let Some(obj) = link.as_object_mut() else {
            continue;
        };
        if is_latest {
            obj.insert("latest".into(), json!(true));
        } else {
            obj.remove("latest");
        }
        if let Some(id) = release {
            obj.insert("title".into(), json!(format!("{id} Overture Release")));
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn child_link(release: &str, latest: bool) -> Value {
        let mut link = json!({
            "rel": "child",
            "href": format!("https://example.com/{release}/catalog.json"),
            "type": "application/json",
        });
        if latest {
            link.as_object_mut()
                .unwrap()
                .insert("latest".into(), json!(true));
        }
        link
    }

    fn root_with(children: &[(&str, bool)], top_latest: Option<&str>) -> Value {
        let mut root = build_empty_root();
        let links = root
            .get_mut("links")
            .and_then(|v| v.as_array_mut())
            .unwrap();
        for (id, latest) in children {
            links.push(child_link(id, *latest));
        }
        if let Some(l) = top_latest {
            root.as_object_mut()
                .unwrap()
                .insert("latest".into(), json!(l));
        }
        root
    }

    fn child_latest_flags(root: &Value) -> Vec<(String, Option<bool>)> {
        root.get("links")
            .and_then(|v| v.as_array())
            .map(|links| {
                links
                    .iter()
                    .filter(|l| l.get("rel").and_then(|v| v.as_str()) == Some("child"))
                    .map(|l| {
                        let id = l
                            .get("href")
                            .and_then(|v| v.as_str())
                            .and_then(release_id_from_href)
                            .unwrap_or_default();
                        let flag = l.get("latest").and_then(|v| v.as_bool());
                        (id, flag)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn promotes_newest_and_clears_stale_flag() {
        let mut root = root_with(
            &[("2026-08-19.0", true), ("2026-09-23.0", false)],
            Some("2026-08-19.0"),
        );
        refresh_latest(&mut root);
        assert_eq!(
            root.get("latest").and_then(|v| v.as_str()),
            Some("2026-09-23.0")
        );
        assert_eq!(
            child_latest_flags(&root),
            vec![
                ("2026-08-19.0".to_string(), None),
                ("2026-09-23.0".to_string(), Some(true)),
            ]
        );
    }

    #[test]
    fn removing_latest_promotes_predecessor() {
        let mut root = root_with(&[("2026-08-19.0", false)], Some("2026-09-23.0"));
        refresh_latest(&mut root);
        assert_eq!(
            root.get("latest").and_then(|v| v.as_str()),
            Some("2026-08-19.0")
        );
        assert_eq!(
            child_latest_flags(&root),
            vec![("2026-08-19.0".to_string(), Some(true))]
        );
    }

    #[test]
    fn empty_children_removes_top_level_latest() {
        let mut root = root_with(&[], Some("2026-08-19.0"));
        refresh_latest(&mut root);
        assert!(root.get("latest").is_none());
    }

    #[test]
    fn is_idempotent() {
        let mut root = root_with(
            &[("2026-08-19.0", false), ("2026-09-23.0", true)],
            Some("2026-09-23.0"),
        );
        refresh_latest(&mut root);
        let after_first = root.clone();
        refresh_latest(&mut root);
        assert_eq!(root, after_first);
    }

    #[test]
    fn normalizes_child_link_titles_to_release_id() {
        // Seed with the stale "Latest Overture Release" title on the older link
        // and a plain release-ID title on the newer one. refresh_latest should
        // rewrite both to "<id> Overture Release".
        let mut root = build_empty_root();
        let links = root
            .get_mut("links")
            .and_then(|v| v.as_array_mut())
            .unwrap();
        links.push(json!({
            "rel": "child",
            "href": "https://example.com/2026-08-19.0/catalog.json",
            "title": "Latest Overture Release",
        }));
        links.push(json!({
            "rel": "child",
            "href": "https://example.com/2026-09-23.0/catalog.json",
            "title": "2026-09-23.0 Overture Release",
        }));

        refresh_latest(&mut root);

        let titles: Vec<(String, Option<String>)> = root
            .get("links")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter(|l| l.get("rel").and_then(|v| v.as_str()) == Some("child"))
            .map(|l| {
                let id = l
                    .get("href")
                    .and_then(|v| v.as_str())
                    .and_then(release_id_from_href)
                    .unwrap_or_default();
                let title = l.get("title").and_then(|v| v.as_str()).map(String::from);
                (id, title)
            })
            .collect();
        assert_eq!(
            titles,
            vec![
                (
                    "2026-08-19.0".to_string(),
                    Some("2026-08-19.0 Overture Release".to_string())
                ),
                (
                    "2026-09-23.0".to_string(),
                    Some("2026-09-23.0 Overture Release".to_string())
                ),
            ]
        );
    }

    #[test]
    fn non_child_links_are_untouched() {
        let mut root = root_with(&[("2026-08-19.0", false)], None);
        root.get_mut("links")
            .and_then(|v| v.as_array_mut())
            .unwrap()
            .push(json!({
                "rel": "self",
                "href": "https://example.com/catalog.json",
                "latest": "sentinel",
            }));
        refresh_latest(&mut root);
        let self_link = root
            .get("links")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .find(|l| l.get("rel").and_then(|v| v.as_str()) == Some("self"))
            .unwrap();
        assert_eq!(
            self_link.get("latest").and_then(|v| v.as_str()),
            Some("sentinel")
        );
    }
}
