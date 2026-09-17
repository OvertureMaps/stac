//! Integration tests for `overture-stac validate`.
//!
//! Fixture-based; runs offline by disabling schema checks (which would fetch
//! JSON schemas over HTTPS). Link integrity and Overture rules are exercised
//! directly against hand-crafted broken/clean catalogs.

use std::fs;
use std::path::Path;

use overture_stac::stac::{validate_catalog, FailureKind, ValidateOptions};
use serde_json::{json, Value};

const BASE_URL: &str = "https://test.example/";

fn no_schema() -> ValidateOptions {
    ValidateOptions {
        check_schema: false,
        check_links: true,
        check_overture_rules: true,
    }
}

fn write_json(path: &Path, value: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn write_root(dir: &Path, extra_links: Vec<Value>) {
    let mut links = vec![json!({
        "rel": "self",
        "href": format!("{BASE_URL}catalog.json"),
        "type": "application/json",
    })];
    links.extend(extra_links);
    write_json(
        &dir.join("catalog.json"),
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "root",
            "stac_version": "1.1.0",
            "links": links,
        }),
    );
}

#[tokio::test]
async fn clean_minimal_catalog_passes() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(tmp.path(), vec![]);
    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    assert!(
        report.is_ok(),
        "clean fixture should pass, got: {:?}",
        report.failures
    );
    assert_eq!(report.files_checked, 1);
}

#[tokio::test]
async fn broken_child_link_is_caught() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(
        tmp.path(),
        vec![json!({
            "rel": "child",
            "href": format!("{BASE_URL}missing/catalog.json"),
            "type": "application/json",
        })],
    );
    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    let link_failures: Vec<_> = report
        .failures
        .iter()
        .filter(|f| matches!(f.kind, FailureKind::Link))
        .collect();
    assert_eq!(
        link_failures.len(),
        1,
        "expected exactly one link failure: {:?}",
        report.failures
    );
    assert!(link_failures[0].message.contains("missing/catalog.json"));
}

#[tokio::test]
async fn schema_version_null_is_caught_on_release_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(
        tmp.path(),
        vec![json!({
            "rel": "child",
            "href": format!("{BASE_URL}2026-08-19.0/catalog.json"),
            "type": "application/json",
        })],
    );
    write_json(
        &tmp.path().join("2026-08-19.0").join("catalog.json"),
        &json!({
            "type": "Catalog",
            "id": "2026-08-19.0",
            "description": "release",
            "stac_version": "1.1.0",
            "release:version": "2026-08-19.0",
            "schema:version": Value::Null,
            "schema:tag": "https://github.com/OvertureMaps/schema/releases/tag/vNone",
            "links": [
                {"rel": "self", "href": format!("{BASE_URL}2026-08-19.0/catalog.json"), "type": "application/json"},
            ],
        }),
    );
    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    let overture: Vec<_> = report
        .failures
        .iter()
        .filter(|f| matches!(f.kind, FailureKind::OvertureRule))
        .collect();
    assert!(
        overture
            .iter()
            .any(|f| f.message.contains("schema:version is null")),
        "missing schema:version-null failure: {:?}",
        report.failures
    );
    assert!(
        overture.iter().any(|f| f.message.contains("vNone")),
        "missing vNone tag failure: {:?}",
        report.failures
    );
}

#[tokio::test]
async fn item_missing_aws_asset_is_caught() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(tmp.path(), vec![]);
    write_json(
        &tmp.path().join("item.json"),
        &json!({
            "type": "Feature",
            "id": "bad-item",
            "stac_version": "1.1.0",
            "bbox": [0.0, 0.0, 1.0, 1.0],
            "geometry": {"type": "Point", "coordinates": [0.5, 0.5]},
            "properties": {"datetime": "2026-08-19T00:00:00Z"},
            "assets": {
                "azure": {
                    "href": "https://azure.example/x.parquet",
                    "storage:refs": ["azure"],
                },
            },
            "links": [{"rel": "self", "href": format!("{BASE_URL}item.json"), "type": "application/geo+json"}],
        }),
    );
    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    assert!(
        report
            .failures
            .iter()
            .any(|f| matches!(f.kind, FailureKind::OvertureRule)
                && f.message.contains("missing 'aws' asset")),
        "expected missing-aws-asset failure: {:?}",
        report.failures
    );
}

#[tokio::test]
async fn bad_bbox_and_bad_license_are_caught() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(tmp.path(), vec![]);

    // Item with xmin > xmax.
    write_json(
        &tmp.path().join("item.json"),
        &json!({
            "type": "Feature",
            "id": "bad-bbox",
            "stac_version": "1.1.0",
            "bbox": [5.0, 0.0, 1.0, 1.0],
            "geometry": {"type": "Point", "coordinates": [3.0, 0.5]},
            "properties": {"datetime": "2026-08-19T00:00:00Z"},
            "assets": {
                "aws": {"href": "https://x", "storage:refs": ["aws"]},
                "azure": {"href": "https://y", "storage:refs": ["azure"]},
            },
            "links": [{"rel": "self", "href": format!("{BASE_URL}item.json"), "type": "application/geo+json"}],
        }),
    );

    // Collection with license 'MIT' (not in allowed set).
    write_json(
        &tmp.path().join("collection.json"),
        &json!({
            "type": "Collection",
            "id": "bad-license",
            "description": "bad",
            "stac_version": "1.1.0",
            "license": "MIT",
            "extent": {"spatial": {"bbox": [[-180,-90,180,90]]}, "temporal": {"interval": [[null,null]]}},
            "links": [{"rel": "self", "href": format!("{BASE_URL}collection.json"), "type": "application/json"}],
        }),
    );

    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    let overture: Vec<&str> = report
        .failures
        .iter()
        .filter(|f| matches!(f.kind, FailureKind::OvertureRule))
        .map(|f| f.message.as_str())
        .collect();
    assert!(
        overture.iter().any(|m| m.contains("bbox xmin > xmax")),
        "{overture:?}"
    );
    assert!(
        overture.iter().any(|m| m.contains("license 'MIT'")),
        "{overture:?}"
    );
}

/// End-to-end against real Overture prod (CDN). Ignored by default —
/// network-dependent, hits the CDN. Run explicitly with `cargo test -- --ignored`.
#[tokio::test]
#[ignore]
async fn remote_validate_against_overture_prod_completes() {
    use overture_stac::stac::validate_url;
    let opts = ValidateOptions {
        check_schema: false,
        check_links: true,
        check_overture_rules: true,
    };
    let report = validate_url("https://stac.overturemaps.org/catalog.json", 8, opts)
        .await
        .expect("crawl should not error");
    assert!(
        report.files_checked > 0,
        "expected at least the root document to be checked"
    );
}

/// End-to-end against real Overture prod (bucket source of truth via
/// object_store anonymous S3). Ignored by default. Requires unset AWS creds
/// (or set AWS_ACCESS_KEY_ID='' to force the anonymous branch of Bucket::from_url).
#[tokio::test]
#[ignore]
async fn bucket_validate_against_overture_prod_completes() {
    use overture_stac::stac::validate_catalog_uri;
    let opts = ValidateOptions {
        check_schema: false,
        check_links: true,
        check_overture_rules: true,
    };
    let report = validate_catalog_uri("s3://overturemaps-extras-us-west-2/stac/", 8, opts)
        .await
        .expect("crawl should not error");
    assert!(
        report.files_checked > 0,
        "expected at least the root document to be checked"
    );
}

#[tokio::test]
async fn pmtiles_link_wrong_media_type_is_caught() {
    let tmp = tempfile::tempdir().unwrap();
    write_root(tmp.path(), vec![]);
    write_json(
        &tmp.path().join("theme.json"),
        &json!({
            "type": "Catalog",
            "id": "buildings",
            "description": "theme",
            "stac_version": "1.1.0",
            "links": [
                {"rel": "self", "href": format!("{BASE_URL}theme.json"), "type": "application/json"},
                {"rel": "pmtiles", "href": "https://tiles.example/x.pmtiles", "type": "application/json"},
            ],
        }),
    );
    let report = validate_catalog(tmp.path(), 2, no_schema()).await.unwrap();
    assert!(
        report
            .failures
            .iter()
            .any(|f| matches!(f.kind, FailureKind::OvertureRule)
                && f.message.contains("pmtiles link has media type")),
        "expected pmtiles-media-type failure: {:?}",
        report.failures
    );
}
