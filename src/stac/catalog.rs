//! Assembly of the release catalog + the equivalent of pystac's
//! `normalize_hrefs` + `save(catalog_type=ABSOLUTE_PUBLISHED)`.
//!
//! We build typed `stac::Catalog`, `stac::Collection`, `stac::Item` values via `theme.rs`,
//! then walk the tree at save time — attaching root/parent/self/child/item links with
//! absolute hrefs — and serialize each node via `stac_io::write` (which uses the `stac`
//! crate's Serialize impls). `collections.parquet` is written via
//! `stac::geoparquet` through the `ItemCollection::into_geoparquet_path` helper.

use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use serde_json::{json, Value};
use stac::geoparquet::WriterOptions;
use stac::{Catalog, Collection, Item, ItemCollection, Link};
use stac_io::IntoGeoparquetPath;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::stac::theme::{process_theme, ThemeResult, ITEM_STAC_EXTENSIONS};
use crate::stac::{pmtiles, registry};
use crate::storage::{list_top_level, Bucket};
use crate::{Error, Result, ResultExt};

pub async fn list_release_ids(bucket: &Bucket, prefix: &str) -> Result<Vec<String>> {
    let mut ids = list_top_level(bucket, prefix).await?;
    ids.sort_by(|a, b| b.cmp(a));
    Ok(ids)
}

/// Handle to a fully-assembled release, ready to save. Carries typed stac values plus
/// the extras that only get materialized during `save_absolute_published`.
pub struct ReleaseCatalog {
    pub catalog: Catalog,
    pub bundles: Vec<ThemeResult>,
    /// Extra top-level links to attach after root (e.g. neighbor prev/next).
    pub neighbor_links: Vec<Link>,
    /// Optional list of child catalogs when this catalog is the multi-release root.
    pub sub_children: Vec<ReleaseCatalog>,
    /// Extra fields to render on this catalog's own `rel: child` link when it's nested
    /// under a parent (e.g. `latest: true` on the newest release).
    pub extra_child_fields: serde_json::Map<String, Value>,
}

pub async fn build_single_release(
    bucket: &Bucket,
    extras_bucket: Option<&Bucket>,
    release: &str,
    schema: &str,
    title: &str,
    debug: bool,
    concurrency: usize,
    output: &Path,
) -> Result<ReleaseCatalog> {
    let out_dir = output.join(release);
    std::fs::create_dir_all(&out_dir).with_context(|| format!("mkdir {}", out_dir.display()))?;

    let release_date = release
        .split('.')
        .next()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        .ok_or_else(|| Error::ParseReleaseDate(release.to_string()))?;
    let release_dt = Utc
        .with_ymd_and_hms(
            release_date.year(),
            release_date.month(),
            release_date.day(),
            0,
            0,
            0,
        )
        .single()
        .expect("valid datetime");

    let available_pmtiles = match extras_bucket {
        Some(eb) => pmtiles::discover(eb, release).await,
        None => BTreeMap::new(),
    };

    let release_prefix = format!("release/{release}");
    let mut theme_keys = list_top_level(bucket, &release_prefix).await?;
    theme_keys.sort();
    let theme_paths: Vec<String> = theme_keys
        .into_iter()
        .map(|k| format!("{release_prefix}/{k}"))
        .collect();

    let bucket_arc = Arc::new(bucket.clone_ref());
    let pmtiles_arc = Arc::new(available_pmtiles);
    let mut results = process_themes_parallel(
        theme_paths,
        bucket_arc,
        release.to_string(),
        debug,
        release_dt,
        pmtiles_arc,
        concurrency,
    )
    .await?;
    results.sort_by(|a, b| a.theme_name.cmp(&b.theme_name));

    // Write manifest.geojson + collections.parquet from the assembled items.
    let mut manifest_items = Vec::new();
    let mut all_items: Vec<Item> = Vec::new();
    for r in &results {
        manifest_items.extend(r.manifest_items.iter().cloned());
        for (_, _, items, _) in &r.type_collections {
            all_items.extend(items.iter().cloned());
        }
    }
    let manifest = json!({"type": "FeatureCollection", "features": manifest_items});
    std::fs::write(
        out_dir.join("manifest.geojson"),
        serde_json::to_vec(&manifest)?,
    )?;
    write_collections_parquet(&out_dir.join("collections.parquet"), all_items)?;

    // Release catalog metadata.
    let mut catalog = Catalog::new(
        release,
        format!("Geoparquet data released in the Overture {release} release"),
    );
    catalog.title = Some(title.to_string());
    catalog.extensions = ITEM_STAC_EXTENSIONS.iter().map(|s| s.to_string()).collect();
    catalog
        .additional_fields
        .insert("release:version".into(), json!(release));
    if !schema.is_empty() {
        catalog
            .additional_fields
            .insert("schema:version".into(), json!(schema));
        catalog.additional_fields.insert(
            "schema:tag".into(),
            json!(format!(
                "https://github.com/OvertureMaps/schema/releases/tag/v{schema}"
            )),
        );
    }

    Ok(ReleaseCatalog {
        catalog,
        bundles: results,
        neighbor_links: Vec::new(),
        sub_children: Vec::new(),
        extra_child_fields: serde_json::Map::new(),
    })
}

async fn process_themes_parallel(
    theme_paths: Vec<String>,
    bucket: Arc<Bucket>,
    release: String,
    debug: bool,
    release_dt: chrono::DateTime<Utc>,
    pmtiles: Arc<BTreeMap<String, String>>,
    concurrency: usize,
) -> Result<Vec<ThemeResult>> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut in_flight = FuturesUnordered::new();
    let mut paths_iter = theme_paths.into_iter();
    let mut results = Vec::new();
    for _ in 0..concurrency.max(1) {
        if let Some(p) = paths_iter.next() {
            in_flight.push(spawn_theme(
                bucket.clone(),
                p,
                release.clone(),
                debug,
                release_dt,
                pmtiles.clone(),
            ));
        }
    }
    while let Some(r) = in_flight.next().await {
        results.push(r?);
        if let Some(p) = paths_iter.next() {
            in_flight.push(spawn_theme(
                bucket.clone(),
                p,
                release.clone(),
                debug,
                release_dt,
                pmtiles.clone(),
            ));
        }
    }
    Ok(results)
}

fn spawn_theme(
    bucket: Arc<Bucket>,
    theme_path: String,
    release: String,
    debug: bool,
    release_dt: chrono::DateTime<Utc>,
    pmtiles: Arc<BTreeMap<String, String>>,
) -> impl std::future::Future<Output = Result<ThemeResult>> + Send + 'static {
    async move { process_theme(&bucket, &theme_path, &release, debug, release_dt, &pmtiles).await }
}

pub fn link_neighbor_releases(catalog: &mut ReleaseCatalog, all_ids: &[String], root_href: &str) {
    let id = catalog.catalog.id.clone();
    let Some(idx) = all_ids.iter().position(|x| *x == id) else {
        return;
    };
    let root = root_href.trim_end_matches('/').to_string();
    if idx > 0 {
        let newer = &all_ids[idx - 1];
        let mut l = Link::new(format!("{root}/{newer}/catalog.json"), "next");
        l.r#type = Some("application/json".into());
        l.title = Some(format!("{newer} Overture Release"));
        catalog.neighbor_links.push(l);
    }
    if idx < all_ids.len() - 1 {
        let older = &all_ids[idx + 1];
        let mut l = Link::new(format!("{root}/{older}/catalog.json"), "prev");
        l.r#type = Some("application/json".into());
        l.title = Some(format!("{older} Overture Release"));
        catalog.neighbor_links.push(l);
    }
}

/// Save a ReleaseCatalog (or top-level Overture Releases catalog) under `dest`, using
/// absolute self-hrefs rooted at `base_url`.
pub fn save_absolute_published(
    release: &ReleaseCatalog,
    base_url: &str,
    dest: &Path,
) -> Result<()> {
    let base_url = base_url.trim_end_matches('/').to_string();
    std::fs::create_dir_all(dest)?;
    let self_href = format!("{base_url}/catalog.json");
    let root_title = release
        .catalog
        .title
        .clone()
        .unwrap_or_else(|| release.catalog.id.clone());
    write_release(
        release,
        &base_url,
        &self_href,
        &self_href,
        &root_title,
        dest,
        None,
    )
}

fn write_release(
    release: &ReleaseCatalog,
    base_url: &str,
    self_href: &str,
    root_href: &str,
    root_title: &str,
    dest: &Path,
    parent: Option<(String, String)>,
) -> Result<()> {
    let mut catalog = release.catalog.clone();
    catalog.links.clear();

    // root
    catalog.links.push(mk_link(
        "root",
        root_href,
        Some("application/json"),
        Some(root_title),
    ));

    // children (themes for a release catalog; nested release catalogs for the top-level catalog)
    for r in &release.bundles {
        let href = format!("{}/{}/catalog.json", strip_last(self_href), r.theme_name);
        let title = r
            .theme_catalog
            .title
            .clone()
            .unwrap_or_else(|| r.theme_name.clone());
        catalog.links.push(mk_link(
            "child",
            &href,
            Some("application/json"),
            Some(&title),
        ));
    }
    for c in &release.sub_children {
        let href = format!("{}/{}/catalog.json", strip_last(self_href), c.catalog.id);
        let title = c
            .catalog
            .title
            .clone()
            .unwrap_or_else(|| c.catalog.id.clone());
        let mut link = mk_link("child", &href, Some("application/json"), Some(&title));
        for (k, v) in &c.extra_child_fields {
            link.additional_fields.insert(k.clone(), v.clone());
        }
        catalog.links.push(link);
    }

    // neighbor prev/next
    for l in &release.neighbor_links {
        catalog.links.push(l.clone());
    }

    // parent
    if let Some((h, t)) = &parent {
        catalog
            .links
            .push(mk_link("parent", h, Some("application/json"), Some(t)));
    }
    // self
    catalog
        .links
        .push(mk_link("self", self_href, Some("application/json"), None));

    stac_io::write(dest.join("catalog.json"), catalog.clone())
        .with_context(|| format!("write {}/catalog.json", dest.display()))?;

    // Recurse: theme children
    let self_title = release
        .catalog
        .title
        .clone()
        .unwrap_or_else(|| release.catalog.id.clone());
    for r in &release.bundles {
        let child_dir = dest.join(&r.theme_name);
        std::fs::create_dir_all(&child_dir)?;
        let child_self = format!("{}/{}/catalog.json", strip_last(self_href), r.theme_name);
        write_theme(
            r,
            &child_self,
            root_href,
            root_title,
            &child_dir,
            (self_href.to_string(), self_title.clone()),
        )?;
    }
    // Recurse: nested release catalogs
    for c in &release.sub_children {
        let child_dir = dest.join(&c.catalog.id);
        std::fs::create_dir_all(&child_dir)?;
        let child_self = format!("{}/{}/catalog.json", strip_last(self_href), c.catalog.id);
        write_release(
            c,
            base_url,
            &child_self,
            root_href,
            root_title,
            &child_dir,
            Some((self_href.to_string(), self_title.clone())),
        )?;
    }
    Ok(())
}

fn write_theme(
    result: &ThemeResult,
    self_href: &str,
    root_href: &str,
    root_title: &str,
    dest: &Path,
    parent: (String, String),
) -> Result<()> {
    let mut catalog = result.theme_catalog.clone();
    catalog.links.clear();
    catalog.links.push(mk_link(
        "root",
        root_href,
        Some("application/json"),
        Some(root_title),
    ));
    for l in &result.theme_extra_links {
        catalog.links.push(l.clone());
    }
    for (type_name, _, _, _) in &result.type_collections {
        let href = format!("{}/{}/collection.json", strip_last(self_href), type_name);
        catalog.links.push(mk_link(
            "child",
            &href,
            Some("application/json"),
            Some(type_name),
        ));
    }
    catalog.links.push(mk_link(
        "parent",
        &parent.0,
        Some("application/json"),
        Some(&parent.1),
    ));
    catalog
        .links
        .push(mk_link("self", self_href, Some("application/json"), None));

    stac_io::write(dest.join("catalog.json"), catalog)?;

    let theme_title = result
        .theme_catalog
        .title
        .clone()
        .unwrap_or_else(|| result.theme_name.clone());
    for (type_name, collection, items, extra_links) in &result.type_collections {
        let coll_dir = dest.join(type_name);
        std::fs::create_dir_all(&coll_dir)?;
        let coll_href = format!("{}/{}/collection.json", strip_last(self_href), type_name);
        write_collection(
            collection,
            items,
            extra_links,
            &coll_href,
            root_href,
            root_title,
            &coll_dir,
            (self_href.to_string(), theme_title.clone()),
        )?;
    }
    Ok(())
}

fn write_collection(
    collection: &Collection,
    items: &[Item],
    extra_links: &[Link],
    self_href: &str,
    root_href: &str,
    root_title: &str,
    dest: &Path,
    parent: (String, String),
) -> Result<()> {
    let mut coll = collection.clone();
    coll.links.clear();
    coll.links.push(mk_link(
        "root",
        root_href,
        Some("application/json"),
        Some(root_title),
    ));
    for l in extra_links {
        coll.links.push(l.clone());
    }
    for it in items {
        let href = format!("{}/{}/{}.json", strip_last(self_href), it.id, it.id);
        coll.links
            .push(mk_link("item", &href, Some("application/geo+json"), None));
    }
    coll.links.push(mk_link(
        "parent",
        &parent.0,
        Some("application/json"),
        Some(&parent.1),
    ));
    coll.links
        .push(mk_link("self", self_href, Some("application/json"), None));

    stac_io::write(dest.join("collection.json"), coll)?;

    let type_name = collection.id.clone();
    for it in items {
        let item_dir = dest.join(&it.id);
        std::fs::create_dir_all(&item_dir)?;
        let item_self = format!("{}/{}/{}.json", strip_last(self_href), it.id, it.id);
        write_item(
            it, &item_self, root_href, root_title, &item_dir, self_href, &type_name,
        )?;
    }
    Ok(())
}

fn write_item(
    item: &Item,
    self_href: &str,
    root_href: &str,
    root_title: &str,
    dest: &Path,
    collection_href: &str,
    collection_id: &str,
) -> Result<()> {
    let mut it = item.clone();
    it.links.clear();
    it.links.push(mk_link(
        "root",
        root_href,
        Some("application/json"),
        Some(root_title),
    ));
    it.links.push(mk_link(
        "collection",
        collection_href,
        Some("application/json"),
        Some(collection_id),
    ));
    it.links.push(mk_link(
        "parent",
        collection_href,
        Some("application/json"),
        Some(collection_id),
    ));
    it.links
        .push(mk_link("self", self_href, Some("application/json"), None));
    it.collection = Some(collection_id.to_string());

    stac_io::write(dest.join(format!("{}.json", item.id)), it)?;
    Ok(())
}

fn mk_link(rel: &str, href: &str, media_type: Option<&str>, title: Option<&str>) -> Link {
    let mut l = Link::new(href, rel);
    l.r#type = media_type.map(str::to_string);
    l.title = title.map(str::to_string);
    l
}

fn strip_last(href: &str) -> String {
    if let Some(idx) = href.rfind('/') {
        href[..idx].to_string()
    } else {
        href.to_string()
    }
}

/// Build the top-level `Overture Releases` catalog (multi-release path).
pub async fn build_top_catalog(
    bucket: &Bucket,
    extras_bucket: Option<&Bucket>,
    ids: &[String],
    _root_href: &str,
    debug: bool,
    concurrency: usize,
    output: &Path,
) -> Result<ReleaseCatalog> {
    let mut children: Vec<ReleaseCatalog> = Vec::new();
    for (idx, release) in ids.iter().enumerate() {
        let title = if idx == 0 {
            "Latest Overture Release".to_string()
        } else {
            format!("{release} Overture Release")
        };
        let mut child = build_single_release(
            bucket,
            extras_bucket,
            release,
            "",
            &title,
            debug,
            concurrency,
            output,
        )
        .await?;
        if idx == 0 {
            child
                .catalog
                .additional_fields
                .insert("latest".into(), json!(true));
            child
                .extra_child_fields
                .insert("latest".into(), json!(true));
        }
        children.push(child);
    }

    let mut top = Catalog::new("Overture Releases", "All Overture Releases");
    top.title = Some("Overture Releases".into());
    if let Some(latest) = ids.first() {
        top.additional_fields.insert("latest".into(), json!(latest));
    }
    let manifest = registry::create_manifest(bucket).await.unwrap_or(json!([]));
    top.additional_fields.insert(
        "registry".into(),
        json!({"path": "s3://overturemaps-us-west-2/registry", "manifest": manifest}),
    );

    Ok(ReleaseCatalog {
        catalog: top,
        bundles: Vec::new(),
        neighbor_links: Vec::new(),
        sub_children: children,
        extra_child_fields: serde_json::Map::new(),
    })
}

fn write_collections_parquet(path: &Path, items: Vec<Item>) -> Result<()> {
    if items.is_empty() {
        // Match pystac behavior: emit an empty parquet placeholder so directory shape holds.
        std::fs::write(path, b"")?;
        return Ok(());
    }
    let coll: ItemCollection = items.into();
    coll.into_geoparquet_path(path, WriterOptions::default())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
