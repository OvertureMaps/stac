//! End-to-end fixture test for `overture-stac reconcile --apply`.
//!
//! Uses `file://` URIs so nothing hits the network. Only exercises the REMOVE
//! path (needs no real Overture-shaped parquet data to be present); ADD is
//! covered separately by the ignored integration test that hits real S3.

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{json, Value};

fn write_json(path: &Path, value: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[test]
fn apply_removes_stale_release() {
    // Layout:
    //   <tmp>/catalog/catalog.json      — root catalog with a stale child link
    //   <tmp>/catalog/2026-01-01.0/…    — files for the stale release
    //   <tmp>/data/release/             — empty (no releases in the bucket)
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");

    let root_path = catalog_dir.join("catalog.json");
    let stale_release_dir = catalog_dir.join("2026-01-01.0");
    let stale_file = stale_release_dir.join("catalog.json");
    let stale_manifest = stale_release_dir.join("manifest.geojson");

    write_json(
        &root_path,
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "latest": "2026-01-01.0",
            "links": [
                {
                    "rel": "root",
                    "href": "https://stac.overturemaps.org/catalog.json",
                    "type": "application/json",
                },
                {
                    "rel": "child",
                    "href": "https://stac.overturemaps.org/2026-01-01.0/catalog.json",
                    "type": "application/json",
                    "title": "2026-01-01.0 Overture Release",
                },
            ],
        }),
    );
    write_json(
        &stale_file,
        &json!({"type": "Catalog", "id": "2026-01-01.0"}),
    );
    fs::write(
        &stale_manifest,
        r#"{"type":"FeatureCollection","features":[]}"#,
    )
    .unwrap();

    // Empty data bucket — no `release/` subdir at all.
    fs::create_dir_all(&data_dir).unwrap();

    let catalog_uri = format!("file://{}/", catalog_dir.display());
    let data_uri = format!("file://{}", data_dir.display());

    // Run reconcile --apply.
    Command::cargo_bin("overture-stac")
        .unwrap()
        .args([
            "reconcile",
            "--apply",
            "--catalog-uri",
            &catalog_uri,
            "--data-uri",
            &data_uri,
            "--extras-uri",
            "",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("removing 2026-01-01.0"));

    // Assertions on final state.
    let root_after = read_json(&root_path);
    let children: Vec<&str> = root_after
        .get("links")
        .and_then(|v| v.as_array())
        .map(|links| {
            links
                .iter()
                .filter(|l| l.get("rel").and_then(|v| v.as_str()) == Some("child"))
                .filter_map(|l| l.get("href").and_then(|v| v.as_str()))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        children.is_empty(),
        "expected no child links after apply, got: {children:?}"
    );

    assert!(
        !stale_file.exists(),
        "stale release's catalog.json should have been deleted"
    );
    assert!(
        !stale_manifest.exists(),
        "stale release's manifest should have been deleted"
    );
}

#[test]
fn apply_is_noop_when_in_sync() {
    // Both sides empty. Apply should succeed and change nothing.
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");
    let root_path = catalog_dir.join("catalog.json");

    write_json(
        &root_path,
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "links": [],
        }),
    );
    fs::create_dir_all(&data_dir).unwrap();

    let catalog_uri = format!("file://{}/", catalog_dir.display());
    let data_uri = format!("file://{}", data_dir.display());

    Command::cargo_bin("overture-stac")
        .unwrap()
        .args([
            "reconcile",
            "--apply",
            "--catalog-uri",
            &catalog_uri,
            "--data-uri",
            &data_uri,
            "--extras-uri",
            "",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Nothing to apply"));
}
