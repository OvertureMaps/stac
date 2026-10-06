//! Tests that write helpers set an explicit `Content-Type` on every object.
//!
//! Uses an in-memory store: `LocalFileSystem` rejects object attributes.

use std::fs;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::{path::Path, Attribute, ObjectStoreExt};
use overture_stac::storage::{put_json, upload_directory, Bucket};
use serde_json::json;

fn memory_bucket() -> Bucket {
    Bucket {
        store: Arc::new(InMemory::new()),
        anonymous_fallback: None,
        scheme: "memory".into(),
        name: "memory".into(),
        region: String::new(),
    }
}

async fn content_type(bucket: &Bucket, key: &str) -> String {
    let got = bucket
        .store
        .get(&Path::from(key))
        .await
        .expect("get object");
    got.attributes
        .get(&Attribute::ContentType)
        .unwrap_or_else(|| panic!("{key} has no Content-Type"))
        .to_string()
}

#[tokio::test]
async fn put_json_sets_application_json() {
    let bucket = memory_bucket();
    put_json(&bucket, "catalog.json", &json!({"type": "Catalog"}))
        .await
        .expect("put_json");
    assert_eq!(
        content_type(&bucket, "catalog.json").await,
        "application/json"
    );
}

#[tokio::test]
async fn upload_directory_derives_type_from_extension() {
    let bucket = memory_bucket();
    let dir = tempfile::tempdir().expect("tempdir");
    for name in ["a.json", "b.geojson", "c.parquet"] {
        fs::write(dir.path().join(name), b"{}").expect("write fixture");
    }
    let uploaded = upload_directory(&bucket, "rel/", dir.path())
        .await
        .expect("upload_directory");
    assert_eq!(uploaded, 3);
    assert_eq!(
        content_type(&bucket, "rel/a.json").await,
        "application/json"
    );
    assert_eq!(
        content_type(&bucket, "rel/b.geojson").await,
        "application/geo+json"
    );
    assert_eq!(
        content_type(&bucket, "rel/c.parquet").await,
        "application/octet-stream"
    );
}
