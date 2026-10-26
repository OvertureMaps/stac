//! `latest.ddb` end to end against a local `file://` data bucket. Needs network once:
//! the build installs DuckDB's `httpfs` and `spatial` extensions.

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

    let conn = duckdb::Connection::open_with_flags(
        catalog_dir.join("latest.ddb"),
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .unwrap(),
    )
    .unwrap();
    conn.execute_batch("LOAD spatial;").unwrap();

    let mut views: Vec<(String, String)> = conn
        .prepare("SELECT view_name, comment FROM duckdb_views() WHERE NOT internal")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    views.sort();
    let names: Vec<&str> = views.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["place", "water"]);
    assert!(views.iter().all(|(_, c)| c.contains("LOAD spatial")));

    let rows: i64 = conn
        .query_row("SELECT count(*) FROM water", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 1);
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
