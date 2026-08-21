//! Cloud-agnostic object store handle via `object_store::parse_url`.

use anyhow::{bail, Context, Result};
use object_store::{parse_url, parse_url_opts, path::Path, ObjectStore, ObjectStoreExt};
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
    /// (no path segment); internal code uses fixed prefixes on top. See
    /// [`Bucket::from_url_with_prefix`] for URIs that include a path.
    ///
    /// For `s3://` URIs, region is read from `AWS_REGION` (defaults to `us-west-2` — matches
    /// where the public Overture buckets live). Access is anonymous by default.
    pub fn from_url(uri: &str) -> Result<Bucket> {
        let (bucket, path) = Self::from_url_with_prefix(uri)?;
        if !path.is_empty() {
            bail!("URI must point at bucket root (no path segment): {uri}");
        }
        Ok(bucket)
    }

    /// Same as [`Bucket::from_url`] but also returns the URI's path portion (with a trailing
    /// `/` normalised in), so callers can prepend it to their own keys. Used by commands
    /// (e.g. `reconcile`) that target a specific sub-tree of a bucket.
    pub fn from_url_with_prefix(uri: &str) -> Result<(Bucket, String)> {
        let url = Url::parse(uri).with_context(|| format!("parsing URI: {uri}"))?;
        let (store, path) = if url.scheme() == "s3" {
            let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
            let mut opts: Vec<(String, String)> = vec![("region".into(), region)];
            let access = std::env::var("AWS_ACCESS_KEY_ID").ok();
            let secret = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
            if let (Some(a), Some(s)) = (access.as_ref(), secret.as_ref()) {
                // Static credentials in env — pass them explicitly so object_store
                // doesn't fall through its chain to IMDS.
                opts.push(("access_key_id".into(), a.clone()));
                opts.push(("secret_access_key".into(), s.clone()));
                if let Ok(t) = std::env::var("AWS_SESSION_TOKEN") {
                    opts.push(("token".into(), t));
                }
            } else {
                // No usable credentials in env — assume the bucket is public
                // (matches the Overture prod buckets, which are readable anon).
                opts.push(("skip_signature".into(), "true".into()));
            }
            parse_url_opts(&url, opts)
                .with_context(|| format!("initialising object store for {uri}"))?
        } else {
            parse_url(&url).with_context(|| format!("initialising object store for {uri}"))?
        };
        let name = url.host_str().unwrap_or(uri).to_string();
        let mut prefix = path.as_ref().to_string();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Ok((
            Bucket {
                store: Arc::from(store),
                name,
            },
            prefix,
        ))
    }

    pub fn clone_ref(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            name: self.name.clone(),
        }
    }
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

/// Fetch and parse a JSON object.
pub async fn get_json(bucket: &Bucket, key: &str) -> Result<serde_json::Value> {
    let p = Path::from(key);
    let bytes = bucket
        .store
        .get(&p)
        .await
        .with_context(|| format!("getting {key} from {}", bucket.name))?
        .bytes()
        .await
        .with_context(|| format!("reading body of {key} from {}", bucket.name))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing JSON from {key}"))
}

/// Serialize `value` to JSON and PUT it.
pub async fn put_json(bucket: &Bucket, key: &str, value: &serde_json::Value) -> Result<()> {
    let p = Path::from(key);
    let body = serde_json::to_vec(value).with_context(|| format!("serializing JSON for {key}"))?;
    bucket
        .store
        .put(&p, body.into())
        .await
        .with_context(|| format!("putting {key} to {}", bucket.name))?;
    Ok(())
}

/// PUT raw bytes at `key`.
pub async fn put_bytes(bucket: &Bucket, key: &str, bytes: Vec<u8>) -> Result<()> {
    let p = Path::from(key);
    bucket
        .store
        .put(&p, bytes.into())
        .await
        .with_context(|| format!("putting {key} to {}", bucket.name))?;
    Ok(())
}

/// Recursively delete every object under `prefix`.
pub async fn delete_prefix(bucket: &Bucket, prefix: &str) -> Result<usize> {
    use futures::stream::StreamExt;
    let p = Path::from(prefix);
    let mut stream = bucket.store.list(Some(&p));
    let mut deleted = 0;
    while let Some(meta) = stream.next().await {
        let meta = meta.with_context(|| format!("listing {prefix} in {}", bucket.name))?;
        bucket
            .store
            .delete(&meta.location)
            .await
            .with_context(|| format!("deleting {} from {}", meta.location, bucket.name))?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Walk a local directory and upload every file under it, mapping paths to `key_prefix`.
pub async fn upload_directory(
    bucket: &Bucket,
    key_prefix: &str,
    local_dir: &std::path::Path,
) -> Result<usize> {
    let mut uploaded = 0;
    let mut stack = vec![local_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading dir {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let rel = path
                .strip_prefix(local_dir)
                .expect("descendant of local_dir")
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            let key = format!("{key_prefix}{rel}");
            let bytes =
                std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            put_bytes(bucket, &key, bytes).await?;
            uploaded += 1;
        }
    }
    Ok(uploaded)
}
