//! End-to-end fixture tests for `overture-stac reconcile --apply` and
//! `overture-stac refresh-root`.
//!
//! Uses `file://` URIs so nothing hits the network. Only exercises the REMOVE
//! path for reconcile (needs no real Overture-shaped parquet data to be
//! present); ADD is covered separately by the ignored integration test that
//! hits real S3.

use std::fs;
use std::path::Path;

mod common;

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
fn apply_republishes_latest_duckdb_when_latest_changes() {
    // Removing the newest release promotes the older one, so latest.ddb must follow.
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");
    let root_href = "https://stac.example.com";
    let child = |id: &str| {
        json!({
            "rel": "child",
            "href": format!("{root_href}/{id}/catalog.json"),
            "type": "application/json",
        })
    };

    write_json(
        &catalog_dir.join("catalog.json"),
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "latest": "2026-02-01.0",
            "links": [child("2026-01-01.0"), child("2026-02-01.0")],
        }),
    );
    for id in ["2026-01-01.0", "2026-02-01.0"] {
        write_json(
            &catalog_dir.join(id).join("catalog.json"),
            &json!({"type": "Catalog", "id": id}),
        );
    }
    common::write_release_fixture(&data_dir, "2026-01-01.0", &[("base", "water")]);

    Command::cargo_bin("overture-stac")
        .unwrap()
        .args([
            "reconcile",
            "--apply",
            "--generate-latest-ddb",
            "--catalog-uri",
            &format!("file://{}/", catalog_dir.display()),
            "--data-uri",
            &format!("file://{}", data_dir.display()),
            "--extras-uri",
            "",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Published latest.ddb for 2026-01-01.0",
        ));

    assert!(catalog_dir.join("latest.ddb").is_file());
    let root = read_json(&catalog_dir.join("catalog.json"));
    assert_eq!(root["latest"], "2026-01-01.0");
    assert!(root["links"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["href"] == "./latest.ddb"));
}

#[test]
fn apply_drops_latest_duckdb_link_when_flag_off() {
    // Without --generate-latest-ddb a changed `latest` must not leave a link to the old release.
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");
    let root_href = "https://stac.example.com";
    let child = |id: &str| {
        json!({
            "rel": "child",
            "href": format!("{root_href}/{id}/catalog.json"),
            "type": "application/json",
        })
    };

    write_json(
        &catalog_dir.join("catalog.json"),
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "latest": "2026-02-01.0",
            "links": [
                child("2026-01-01.0"),
                child("2026-02-01.0"),
                {"rel": "alternate", "href": "./latest.ddb", "type": "application/vnd.duckdb"},
            ],
        }),
    );
    for id in ["2026-01-01.0", "2026-02-01.0"] {
        write_json(
            &catalog_dir.join(id).join("catalog.json"),
            &json!({"type": "Catalog", "id": id}),
        );
    }
    common::write_release_fixture(&data_dir, "2026-01-01.0", &[("base", "water")]);

    Command::cargo_bin("overture-stac")
        .unwrap()
        .env("DUCKDB_BIN", "/nonexistent/duckdb")
        .args([
            "reconcile",
            "--apply",
            "--catalog-uri",
            &format!("file://{}/", catalog_dir.display()),
            "--data-uri",
            &format!("file://{}", data_dir.display()),
            "--extras-uri",
            "",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed latest.ddb link"));

    let root = read_json(&catalog_dir.join("catalog.json"));
    assert_eq!(root["latest"], "2026-01-01.0");
    assert!(!root["links"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["href"] == "./latest.ddb"));
    assert!(!catalog_dir.join("latest.ddb").exists());
}

#[test]
fn generate_flag_fails_before_writing_when_duckdb_is_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");
    let root_path = catalog_dir.join("catalog.json");
    let id = "2026-01-01.0";

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
    common::write_release_fixture(&data_dir, id, &[("base", "water")]);
    let before = std::fs::read(&root_path).unwrap();
    let catalog_uri = format!("file://{}/", catalog_dir.display());
    let data_uri = format!("file://{}", data_dir.display());

    Command::cargo_bin("overture-stac")
        .unwrap()
        .env("DUCKDB_BIN", "/nonexistent/duckdb")
        .args([
            "reconcile",
            "--apply",
            "--generate-latest-ddb",
            "--catalog-uri",
            &catalog_uri,
            "--data-uri",
            &data_uri,
            "--extras-uri",
            "",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("could not run"));

    Command::cargo_bin("overture-stac")
        .unwrap()
        .env("DUCKDB_BIN", "/nonexistent/duckdb")
        .args([
            "refresh-root",
            "--generate-latest-ddb",
            "--catalog-uri",
            &catalog_uri,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("could not run"));

    assert_eq!(std::fs::read(&root_path).unwrap(), before);
    assert!(!catalog_dir.join(id).exists());
    assert!(!catalog_dir.join("latest.ddb").exists());
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

#[test]
fn apply_repairs_missing_latest_duckdb_when_in_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let data_dir = tmp.path().join("data");
    let id = "2026-01-01.0";

    write_json(
        &catalog_dir.join("catalog.json"),
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "latest": id,
            "links": [{
                "rel": "child",
                "href": format!("https://stac.example.com/{id}/catalog.json"),
                "type": "application/json",
                "latest": true,
            }],
        }),
    );
    write_json(
        &catalog_dir.join(id).join("catalog.json"),
        &json!({"type": "Catalog", "id": id}),
    );
    common::write_release_fixture(&data_dir, id, &[("base", "water")]);

    Command::cargo_bin("overture-stac")
        .unwrap()
        .args([
            "reconcile",
            "--apply",
            "--generate-latest-ddb",
            "--catalog-uri",
            &format!("file://{}/", catalog_dir.display()),
            "--data-uri",
            &format!("file://{}", data_dir.display()),
            "--extras-uri",
            "",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Published latest.ddb for 2026-01-01.0",
        ));

    assert!(catalog_dir.join("latest.ddb").is_file());
}

#[test]
fn refresh_root_heals_stale_latest_and_is_idempotent() {
    // Stale flag on the older child, none on the newer. First run heals, second is a no-op.
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    let root_path = catalog_dir.join("catalog.json");

    let root_href = "https://stac.example.com";
    write_json(
        &root_path,
        &json!({
            "type": "Catalog",
            "id": "Overture Releases",
            "description": "All Overture Releases",
            "stac_version": "1.0.0",
            "latest": "2026-02-01.0",
            "links": [
                {
                    "rel": "child",
                    "href": format!("{root_href}/2026-01-01.0/catalog.json"),
                    "type": "application/json",
                    "title": "2026-01-01.0 Overture Release",
                    "latest": true,
                },
                {
                    "rel": "child",
                    "href": format!("{root_href}/2026-02-01.0/catalog.json"),
                    "type": "application/json",
                    "title": "2026-02-01.0 Overture Release",
                },
            ],
        }),
    );

    let catalog_uri = format!("file://{}/", catalog_dir.display());
    let data_dir = tmp.path().join("data");
    common::write_release_fixture(&data_dir, "2026-02-01.0", &[("base", "water")]);
    let data_uri = format!("file://{}", data_dir.display());
    let refresh = || {
        let mut cmd = Command::cargo_bin("overture-stac").unwrap();
        cmd.args([
            "refresh-root",
            "--generate-latest-ddb",
            "--catalog-uri",
            &catalog_uri,
            "--data-uri",
            &data_uri,
        ]);
        cmd
    };

    // First run: writes the fixed root and publishes latest.ddb.
    refresh()
        .assert()
        .success()
        .stdout(predicate::str::contains("Refreshed root"));
    assert!(catalog_dir.join("latest.ddb").is_file());

    let root_after = read_json(&root_path);
    let flags: Vec<(String, Option<bool>)> = root_after
        .get("links")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .filter(|l| l.get("rel").and_then(|v| v.as_str()) == Some("child"))
        .map(|l| {
            let id = l
                .get("href")
                .and_then(|v| v.as_str())
                .and_then(|h| h.strip_suffix("/catalog.json"))
                .and_then(|h| h.rsplit('/').next())
                .unwrap_or("")
                .to_string();
            (id, l.get("latest").and_then(|v| v.as_bool()))
        })
        .collect();
    assert_eq!(
        flags,
        vec![
            ("2026-01-01.0".to_string(), None),
            ("2026-02-01.0".to_string(), Some(true)),
        ],
        "stale flag should move from the older child to the newer one",
    );

    // Second run: no-op.
    refresh()
        .assert()
        .success()
        .stdout(predicate::str::contains("already consistent"));
}

#[test]
fn refresh_root_reports_missing_catalog_json() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog_dir = tmp.path().join("catalog");
    std::fs::create_dir_all(&catalog_dir).unwrap();
    let catalog_uri = format!("file://{}/", catalog_dir.display());

    Command::cargo_bin("overture-stac")
        .unwrap()
        .args(["refresh-root", "--catalog-uri", &catalog_uri])
        .assert()
        .success()
        .stdout(predicate::str::contains("Nothing to refresh"));
}
