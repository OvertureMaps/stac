//! `latest.ddb` end to end against a local `file://` data bucket. Needs the DuckDB CLI on
//! PATH and network once: the build installs DuckDB's `httpfs` and `spatial` extensions.

mod common;

use overture_stac::stac::publish_latest_duckdb;
use overture_stac::storage::Bucket;

fn dir_uri(path: &std::path::Path) -> String {
    url::Url::from_directory_path(path).unwrap().to_string()
}

#[tokio::test]
async fn publishes_views_for_every_discovered_type() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let catalog_dir = tmp.path().join("catalog");
    std::fs::create_dir_all(&catalog_dir).unwrap();
    common::write_release_fixture(
        &data_dir,
        "2026-09-23.0",
        &[("base", "water"), ("places", "place")],
    );

    let data_uri = dir_uri(&data_dir);
    let data = Bucket::from_url(&data_uri).unwrap();
    let catalog = Bucket::from_url(&dir_uri(&catalog_dir)).unwrap();
    publish_latest_duckdb(&catalog, &data, &data_uri, "2026-09-23.0")
        .await
        .unwrap();

    let db = catalog_dir.join("latest.ddb");
    let names = common::duckdb_query(
        &db,
        "SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1",
    );
    assert_eq!(names.lines().collect::<Vec<_>>(), ["place", "water"]);

    let comments = common::duckdb_query(
        &db,
        "SELECT count(*) FROM duckdb_views() WHERE NOT internal AND comment LIKE '%LOAD spatial%'",
    );
    assert_eq!(comments.trim(), "2");

    let rows = common::duckdb_query(&db, "LOAD spatial; SELECT count(*) FROM water");
    assert_eq!(rows.trim(), "1");
}

#[tokio::test]
async fn fails_when_release_has_no_types() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let catalog_dir = tmp.path().join("catalog");
    std::fs::create_dir_all(data_dir.join("release/2026-09-23.0")).unwrap();
    std::fs::create_dir_all(&catalog_dir).unwrap();

    let data_uri = dir_uri(&data_dir);
    let data = Bucket::from_url(&data_uri).unwrap();
    let catalog = Bucket::from_url(&dir_uri(&catalog_dir)).unwrap();
    let err = publish_latest_duckdb(&catalog, &data, &data_uri, "2026-09-23.0")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no theme/type"), "{err}");
    assert!(!catalog_dir.join("latest.ddb").exists());
}
