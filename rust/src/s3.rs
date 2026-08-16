//! Anonymous S3 access to the public Overture bucket. Mirrors `pyarrow.fs.S3FileSystem(anonymous=True)`.

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path};
use std::sync::Arc;

/// Handle to a single anonymous public S3 bucket.
pub struct Bucket {
    pub store: Arc<dyn ObjectStore>,
    pub name: String,
}

pub fn new_public_bucket(bucket: &str, region: &str) -> Result<Bucket> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_skip_signature(true)
        .build()
        .with_context(|| format!("building anonymous S3 client for {bucket}"))?;
    Ok(Bucket {
        store: Arc::new(store),
        name: bucket.to_string(),
    })
}

/// List "top-level" entries under `prefix`, returning the last path segment for each — the
/// same shape pyarrow's `FileSelector(prefix)` returns: directories and files immediately under it.
pub async fn list_top_level(bucket: &Bucket, prefix: &str) -> Result<Vec<String>> {
    let p = Path::from(prefix);
    let result = bucket.store.list_with_delimiter(Some(&p)).await
        .with_context(|| format!("listing {prefix} in {}", bucket.name))?;
    let mut out = Vec::new();
    for pref in result.common_prefixes {
        if let Some(name) = pref.parts().last() {
            out.push(name.as_ref().to_string());
        }
    }
    for obj in result.objects {
        if let Some(name) = obj.location.parts().last() {
            out.push(name.as_ref().to_string());
        }
    }
    Ok(out)
}

/// Recursively list all object keys under `prefix`.
pub async fn list_all(bucket: &Bucket, prefix: &str) -> Result<Vec<String>> {
    use futures::stream::StreamExt;
    let p = Path::from(prefix);
    let mut stream = bucket.store.list(Some(&p));
    let mut out = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.with_context(|| format!("listing {prefix}"))?;
        out.push(meta.location.to_string());
    }
    Ok(out)
}
