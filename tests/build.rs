//! End-to-end integration test for `build_single_release`. Hits the public Overture
//! bucket in debug mode. Marked `#[ignore]` because it's network-dependent and slow —
//! run explicitly with `cargo test -- --ignored`.

use std::path::PathBuf;

use overture_stac::catalog::build_single_release;
use overture_stac::s3::Bucket;

const TEST_RELEASE: &str = "2026-07-22.0";
const TEST_SCHEMA: &str = "1.18.0";

#[tokio::test]
#[ignore]
async fn build_single_release_debug_mode() {
    let output = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("build_single_release");
    let _ = std::fs::remove_dir_all(&output);

    let bucket = Bucket::from_url("s3://overturemaps-us-west-2").expect("data bucket");
    let extras = Bucket::from_url("s3://overturemaps-extras-us-west-2").expect("extras bucket");

    let catalog = build_single_release(
        &bucket,
        Some(&extras),
        TEST_RELEASE,
        TEST_SCHEMA,
        "Test Release",
        true, // debug — 1 item per collection
        4,
        &output,
    )
    .await
    .expect("build_single_release");

    // Structural assertions on the returned handle.
    assert_eq!(catalog.catalog.id, TEST_RELEASE);
    assert!(!catalog.bundles.is_empty(), "at least one theme processed");

    // Filesystem assertions on the output tree.
    let release_dir = output.join(TEST_RELEASE);
    assert!(
        release_dir.join("manifest.geojson").exists(),
        "manifest.geojson written"
    );
    assert!(
        release_dir.join("collections.parquet").exists(),
        "collections.parquet written"
    );
}
