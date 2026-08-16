//! Read GeoParquet fragment metadata from S3 — bbox from the `geo` metadata blob, plus
//! `num_rows`, `num_row_groups`, and the column names for `table:columns`.
//!
//! Fast path: `ParquetMetaDataReader::load_via_suffix_and_finish` issues one ranged suffix GET
//! (`Range: bytes=-N`), and only issues a second GET if the encoded metadata is larger than the
//! prefetch. Avoids a separate HEAD and the full stream-builder setup.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path as ObjPath};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::arrow::async_reader::{MetadataFetch, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result as ParquetResult};
use std::ops::Range;
use parquet::file::metadata::ParquetMetaDataReader;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

/// Cap total concurrent parquet metadata GETs across the whole process. Combined with
/// theme-level and type-level fan-out we can otherwise open thousands of concurrent S3
/// connections and hit object_store's retry-timeout wall.
static GLOBAL_PARQUET_LIMIT: OnceLock<Semaphore> = OnceLock::new();

fn parquet_limit() -> &'static Semaphore {
    GLOBAL_PARQUET_LIMIT.get_or_init(|| Semaphore::new(64))
}

pub struct FragmentInfo {
    pub path: String,
    pub num_rows: i64,
    pub num_row_groups: i64,
    pub bbox: [f64; 4],
    pub column_names: Vec<String>,
    pub geoparquet_version: Option<String>,
}

pub async fn read_fragment(store: Arc<dyn ObjectStore>, key: &str) -> Result<FragmentInfo> {
    let _permit = parquet_limit().acquire().await.expect("global parquet semaphore poisoned");
    let path = ObjPath::from(key);
    let mut fetch = SuffixFetch { store: &store, path };
    let meta = ParquetMetaDataReader::new()
        .load_via_suffix_and_finish(&mut fetch)
        .await
        .with_context(|| format!("load parquet metadata for {key}"))?;
    let file_meta = meta.file_metadata();

    let arrow_schema = parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
        .with_context(|| format!("parquet -> arrow schema for {key}"))?;
    let column_names: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let kv = file_meta.key_value_metadata();
    let geo_str = kv
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == "geo"))
        .and_then(|kv| kv.value.clone())
        .ok_or_else(|| anyhow::anyhow!("no `geo` metadata in {key}"))?;
    let geo: Value = serde_json::from_str(&geo_str)
        .with_context(|| format!("parse geo metadata in {key}"))?;

    let geometry = geo
        .get("columns")
        .and_then(|c| c.get("geometry"))
        .ok_or_else(|| anyhow::anyhow!("no columns.geometry in geo metadata"))?;
    let bbox_arr = geometry
        .get("bbox")
        .and_then(|b| b.as_array())
        .ok_or_else(|| anyhow::anyhow!("no bbox in geometry metadata"))?;
    if bbox_arr.len() != 4 {
        bail!("bbox length != 4 in {key}");
    }
    let mut bbox = [0f64; 4];
    for (i, v) in bbox_arr.iter().enumerate() {
        bbox[i] = v
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("bbox coord {i} not a number"))?;
    }

    let geoparquet_version = geo.get("version").and_then(|v| v.as_str()).map(str::to_owned);

    Ok(FragmentInfo {
        path: key.to_string(),
        num_rows: file_meta.num_rows(),
        num_row_groups: meta.num_row_groups() as i64,
        bbox,
        column_names,
        geoparquet_version,
    })
}

struct SuffixFetch<'a> {
    store: &'a Arc<dyn ObjectStore>,
    path: ObjPath,
}

impl<'a> MetadataFetch for &mut SuffixFetch<'a> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = self.store;
        let path = self.path.clone();
        async move {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range)),
                ..GetOptions::default()
            };
            let resp = store
                .get_opts(&path, opts)
                .await
                .map_err(|e| ParquetError::General(format!("object_store bounded GET: {e}")))?;
            resp.bytes()
                .await
                .map_err(|e| ParquetError::General(format!("object_store body: {e}")))
        }
        .boxed()
    }
}

impl<'a> MetadataSuffixFetch for &mut SuffixFetch<'a> {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = self.store;
        let path = self.path.clone();
        async move {
            let opts = GetOptions {
                range: Some(GetRange::Suffix(suffix as u64)),
                ..GetOptions::default()
            };
            let resp = store
                .get_opts(&path, opts)
                .await
                .map_err(|e| ParquetError::General(format!("object_store suffix GET: {e}")))?;
            resp.bytes()
                .await
                .map_err(|e| ParquetError::General(format!("object_store body: {e}")))
        }
        .boxed()
    }
}
