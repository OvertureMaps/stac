//! Per-theme processing — port of `process_theme_worker` in `overture_stac.py`.
//!
//! Returns typed `stac` values (Catalog per theme, Collection per type, Item per fragment)
//! plus the manifest features destined for `manifest.geojson`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map as JsonMap, Value};
use stac::{Bbox, Catalog, Collection, Extent, Item, Link, SpatialExtent, TemporalExtent};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::{list_top_level, read_fragment, Bucket, FragmentInfo};
use crate::{Error, Result};

pub const ITEM_STAC_EXTENSIONS: &[&str] = &[
    "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
];

pub const TABLE_EXTENSION: &str = "https://stac-extensions.github.io/table/v1.2.0/schema.json";

pub fn type_license(type_name: &str) -> Option<&'static str> {
    match type_name {
        "bathymetry" => Some("CC0-1.0"),
        "land_cover" => Some("CC-BY-4.0"),
        "infrastructure" | "land" | "land_use" | "water" | "building" | "building_part"
        | "division" | "division_area" | "division_boundary" | "segment" | "connector" => {
            Some("ODbL-1.0")
        }
        "place" | "address" => Some("other"),
        _ => None,
    }
}

pub struct ThemeResult {
    pub theme_name: String,
    pub theme_catalog: Catalog,
    /// Extra links (e.g. pmtiles) that the save walk attaches after `root` and before children.
    pub theme_extra_links: Vec<Link>,
    pub manifest_items: Vec<Value>,
    /// Ordered per-type: (type_name, collection, items, extra_links).
    pub type_collections: Vec<(String, Collection, Vec<Item>, Vec<Link>)>,
}

pub async fn process_theme(
    bucket: &Bucket,
    theme_key: &str,
    release: &str,
    debug: bool,
    release_datetime: DateTime<Utc>,
    available_pmtiles: &BTreeMap<String, String>,
) -> Result<ThemeResult> {
    let theme_name = theme_key
        .rsplit_once('=')
        .map(|(_, n)| n)
        .unwrap_or(theme_key)
        .to_string();
    tracing::info!("Processing Theme: {theme_name}");

    let mut theme_catalog = Catalog::new(&theme_name, format!("Overture's {theme_name} theme"));
    theme_catalog.title = Some(theme_name.clone());

    let mut theme_extra_links = Vec::new();
    if available_pmtiles.contains_key(&theme_name) {
        tracing::info!("Adding PMTiles link for theme {theme_name}");
        let mut link = Link::new(
            format!("https://tiles.overturemaps.org/{release}/{theme_name}.pmtiles"),
            "pmtiles",
        );
        link.r#type = Some("application/vnd.pmtiles".into());
        link.title = Some("PMTiles".into());
        theme_extra_links.push(link);
    }

    // List types under this theme prefix and fan out concurrently (bounded).
    use futures::stream::{StreamExt, TryStreamExt};
    const TYPES_PER_THEME: usize = 4;
    let type_keys = list_top_level(bucket, theme_key).await?;
    let theme_key_owned = theme_key.to_string();
    let bucket_owned = Arc::new(bucket.clone_ref());
    let type_futs = type_keys.into_iter().map(|type_key| {
        let bucket = Arc::clone(&bucket_owned);
        let theme_key = theme_key_owned.clone();
        async move { process_type(&bucket, &theme_key, &type_key, debug, release_datetime).await }
    });
    let per_type: Vec<PerType> = futures::stream::iter(type_futs)
        .buffer_unordered(TYPES_PER_THEME)
        .try_collect()
        .await?;

    let mut manifest_items = Vec::new();
    let mut type_collections = Vec::new();
    for pt in per_type {
        manifest_items.extend(pt.manifest_items);
        type_collections.push((pt.type_name, pt.collection, pt.items, pt.extra_links));
    }

    Ok(ThemeResult {
        theme_name,
        theme_catalog,
        theme_extra_links,
        manifest_items,
        type_collections,
    })
}

struct PerType {
    type_name: String,
    collection: Collection,
    items: Vec<Item>,
    manifest_items: Vec<Value>,
    extra_links: Vec<Link>,
}

async fn process_type(
    bucket: &Bucket,
    theme_key: &str,
    type_key: &str,
    debug: bool,
    release_datetime: DateTime<Utc>,
) -> Result<PerType> {
    let type_full = format!("{theme_key}/{type_key}");
    let type_name = type_key
        .rsplit_once('=')
        .map(|(_, n)| n)
        .unwrap_or(type_key)
        .to_string();
    tracing::info!("Opening Type: {type_name}");

    let mut fragments_keys = list_top_level(bucket, &type_full).await?;
    fragments_keys.retain(|f| f.ends_with(".parquet"));
    fragments_keys.sort();
    if debug {
        fragments_keys.truncate(3);
    }
    let total_fragments = fragments_keys.len();
    let full_keys: Vec<String> = fragments_keys
        .iter()
        .map(|f| format!("{type_full}/{f}"))
        .collect();

    // Concurrent fragment metadata reads, bounded to keep S3 fanout in check.
    use futures::stream::{StreamExt, TryStreamExt};
    const FRAGMENT_CONCURRENCY: usize = 32;
    let store_arc = bucket.store.clone();
    let fragment_futs: Vec<_> = full_keys
        .clone()
        .into_iter()
        .enumerate()
        .map(|(idx, key)| {
            let store = Arc::clone(&store_arc);
            async move {
                let info = read_fragment(store, &key).await?;
                Ok::<(usize, FragmentInfo), Error>((idx, info))
            }
        })
        .collect();
    let mut fragments: Vec<(usize, FragmentInfo)> = futures::stream::iter(fragment_futs)
        .buffer_unordered(FRAGMENT_CONCURRENCY)
        .try_collect()
        .await?;
    fragments.sort_by_key(|(i, _)| *i);

    let mut items: Vec<Item> = Vec::with_capacity(fragments.len());
    let mut manifest_items: Vec<Value> = Vec::with_capacity(fragments.len());
    let mut columns_hint: Option<Vec<String>> = None;
    let mut geoparquet_version: Option<String> = None;
    let mut total_row_count: i64 = 0;

    for (idx, fragment) in &fragments {
        if idx % 10 == 0 || *idx == total_fragments - 1 {
            let dir_last = full_keys[*idx].split('/').rev().nth(1).unwrap_or("?");
            tracing::info!(" [ {dir_last} : {}/{total_fragments} fragments ]", idx + 1);
        }
        if columns_hint.is_none() {
            columns_hint = Some(fragment.column_names.clone());
        }
        if geoparquet_version.is_none() {
            geoparquet_version = fragment.geoparquet_version.clone();
        }
        total_row_count += fragment.num_rows;

        let [xmin, ymin, xmax, ymax] = fragment.bbox;
        let filename = fragment.path.rsplit('/').next().unwrap_or("");
        let rel_path = fragment.path.clone();
        let item_id = filename.split('-').nth(1).unwrap_or(filename).to_string();

        let geojson_bbox = json!({
            "type": "Polygon",
            "coordinates": [[
                [xmin, ymin],
                [xmax, ymin],
                [xmax, ymax],
                [xmin, ymax],
                [xmin, ymin],
            ]]
        });

        let mut item = Item::new(&item_id);
        item.extensions = ITEM_STAC_EXTENSIONS.iter().map(|s| s.to_string()).collect();
        item.geometry = Some(serde_json::from_value(geojson_bbox.clone())?);
        item.bbox = Some(Bbox::new(xmin, ymin, xmax, ymax));

        // Properties: num_rows, num_row_groups, storage:schemes, datetime.
        let props = &mut item.properties.additional_fields;
        props.insert("num_rows".into(), json!(fragment.num_rows));
        props.insert("num_row_groups".into(), json!(fragment.num_row_groups));
        props.insert(
            "storage:schemes".into(),
            json!({
                "aws": {
                    "type": "aws-s3",
                    "platform": "https://{bucket}.s3.{region}.amazonaws.com",
                    "bucket": "overturemaps-us-west-2",
                    "region": "us-west-2",
                    "requester_pays": false,
                },
                "azure": {
                    "type": "ms-azure",
                    "platform": "https://{account}.blob.core.windows.net",
                    "account": "overturemapswestus2",
                    "requester_pays": false,
                },
            }),
        );
        let iso = release_datetime.to_rfc3339_opts(SecondsFormat::Secs, true);
        item.properties.datetime = Some(iso.parse().ok().unwrap_or_default());

        // Assets
        let mut aws_asset = stac::Asset::new(format!(
            "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/{rel_path}"
        ));
        aws_asset.r#type = Some("application/vnd.apache.parquet".into());
        aws_asset.title = Some("GeoParquet on AWS S3".into());
        aws_asset.description = Some(
            "Zstd-compressed GeoParquet in the overturemaps-us-west-2 bucket, served over HTTPS."
                .into(),
        );
        aws_asset.roles = vec!["data".into()];
        aws_asset
            .additional_fields
            .insert("storage:refs".into(), json!(["aws"]));
        aws_asset
            .additional_fields
            .insert("alternate:name".into(), json!("HTTPS"));
        aws_asset.additional_fields.insert(
            "alternate".into(),
            json!({
                "s3": {
                    "href": format!("s3://overturemaps-us-west-2/{}", fragment.path),
                    "alternate:name": "S3",
                    "description": "Access the files via regular Amazon AWS S3 tooling.",
                    "roles": ["data"],
                }
            }),
        );
        item.assets.insert("aws".into(), aws_asset);

        let mut azure_asset = stac::Asset::new(format!(
            "https://overturemapswestus2.blob.core.windows.net/{rel_path}"
        ));
        azure_asset.r#type = Some("application/vnd.apache.parquet".into());
        azure_asset.title = Some("GeoParquet on Azure Blob Storage".into());
        azure_asset.description = Some(
            "Zstd-compressed GeoParquet in the overturemapswestus2 storage account (West US 2), served over HTTPS.".into(),
        );
        azure_asset.roles = vec!["data".into()];
        azure_asset
            .additional_fields
            .insert("storage:refs".into(), json!(["azure"]));
        item.assets.insert("azure".into(), azure_asset);

        manifest_items.push(json!({
            "type": "Feature",
            "properties": {
                "ovt_type": type_name,
                "rel_path": rel_path,
            },
            "geometry": geojson_bbox,
            "bbox": [xmin, ymin, xmax, ymax],
        }));

        items.push(item);
    }

    // Build Collection with extent from item bboxes (matches pystac's SpatialExtent(bboxes=[...])).
    let extent = Extent {
        spatial: SpatialExtent {
            bbox: items
                .iter()
                .filter_map(|it| it.bbox.as_ref().cloned())
                .collect(),
        },
        temporal: TemporalExtent {
            interval: vec![[None, None]],
        },
        additional_fields: JsonMap::new(),
    };

    let mut collection = Collection::new(&type_name, format!("Overture's {type_name} collection"));
    collection.title = Some(type_name.clone());
    collection.extent = extent;
    collection.extensions = vec![TABLE_EXTENSION.into()];

    let extras = &mut collection.additional_fields;
    let columns_json: Vec<Value> = columns_hint
        .as_ref()
        .map(|cols| cols.iter().map(|n| json!({"name": n})).collect())
        .unwrap_or_default();
    extras.insert("table:columns".into(), Value::Array(columns_json));
    extras.insert("table:primary_geometry".into(), json!("geometry"));
    extras.insert("table:row_count".into(), json!(total_row_count));
    extras.insert(
        "geoparquet:version".into(),
        geoparquet_version
            .as_ref()
            .map(|v| json!(v))
            .unwrap_or(Value::Null),
    );
    if !debug {
        extras.insert("features".into(), json!(total_row_count));
    }

    if let Some(lic) = type_license(&type_name) {
        collection.license = lic.into();
    }

    if !items.is_empty() {
        let rows: Vec<i64> = items
            .iter()
            .filter_map(|i| i.properties.additional_fields.get("num_rows"))
            .filter_map(|v| v.as_i64())
            .collect();
        let groups: Vec<i64> = items
            .iter()
            .filter_map(|i| i.properties.additional_fields.get("num_row_groups"))
            .filter_map(|v| v.as_i64())
            .collect();
        let summaries = json!({
            "num_rows": {"minimum": rows.iter().min().unwrap_or(&0), "maximum": rows.iter().max().unwrap_or(&0)},
            "num_row_groups": {"minimum": groups.iter().min().unwrap_or(&0), "maximum": groups.iter().max().unwrap_or(&0)},
        });
        let mut summaries_map = JsonMap::new();
        if let Value::Object(m) = summaries {
            summaries_map = m;
        }
        collection.summaries =
            serde_json::from_value(Value::Object(summaries_map)).unwrap_or_default();
    }

    // License link (only for "other"-licensed types).
    let mut extra_links = Vec::new();
    if type_license(&type_name) == Some("other") {
        let mut l = Link::new("https://docs.overturemaps.org/attribution/", "license");
        l.title = Some("Overture Maps Attribution and Licensing".into());
        extra_links.push(l);
    }

    Ok(PerType {
        type_name,
        collection,
        items,
        manifest_items,
        extra_links,
    })
}
