//! Cloud-agnostic object store handle via `object_store::parse_url`.

use anyhow::{bail, Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::{parse_url, parse_url_opts, path::Path, ObjectStore};
use std::sync::Arc;
use url::Url;

/// Handle to a single object-store-backed bucket.
///
/// Constructed via [`Bucket::from_url`]; supports any scheme `object_store` recognises
/// (`s3://`, `gs://`, `az://`, `http(s)://`, `file://`). For `s3://`, anonymous access
/// is used by default — matches the public Overture bucket's access model.
pub struct Bucket {
    pub store: Arc<dyn ObjectStore>,
    pub name: String,
}

impl Bucket {
    /// Build a bucket handle from an object-store URI. The URI must point at the bucket root
    /// (no path segment); internal code uses fixed prefixes on top.
    pub fn from_url(uri: &str) -> Result<Bucket> {
        let url = Url::parse(uri).with_context(|| format!("parsing URI: {uri}"))?;
        let (store, path) = if url.scheme() == "s3" {
            parse_url_opts(&url, [("skip_signature", "true")])
                .with_context(|| format!("initialising object store for {uri}"))?
        } else {
            parse_url(&url).with_context(|| format!("initialising object store for {uri}"))?
        };
        if !path.as_ref().is_empty() {
            bail!("URI must point at bucket root (no path segment): {uri}");
        }
        let name = url.host_str().unwrap_or(uri).to_string();
        Ok(Bucket {
            store: Arc::from(store),
            name,
        })
    }

    pub fn clone_ref(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            name: self.name.clone(),
        }
    }
}

/// Kept for backwards-compat with any direct callers; prefer [`Bucket::from_url`].
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
    let result = bucket
        .store
        .list_with_delimiter(Some(&p))
        .await
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
