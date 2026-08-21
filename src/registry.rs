//! Port of `RegistryManifest.create_manifest`.
//!
//! Reads every parquet under `overturemaps-us-west-2/registry/`, pulls the max value of the
//! `id` column from the LAST row group's statistics of the first fragment, and returns a
//! `[[filename, max_id], ...]` list sorted by max_id.

use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
#[allow(deprecated)]
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::statistics::Statistics;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::s3::{list_all, Bucket};
use crate::{Result, ResultExt};

pub async fn create_manifest(bucket: &Bucket) -> Result<Value> {
    let prefix = "registry";
    tracing::info!("Scanning registry path: {}/{}", bucket.name, prefix);

    let files = list_all(bucket, prefix).await?;
    let parquet_files: Vec<String> = files
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    tracing::info!("Found {} parquet files in registry", parquet_files.len());

    let mut entries: Vec<(String, String)> = Vec::new();

    for key in parquet_files {
        match read_max_id(&bucket.store, &key).await {
            Ok(Some(max_id)) => {
                let filename = key.trim_start_matches("registry/").to_string();
                let last = filename.rsplit('/').next().unwrap_or(&filename).to_string();
                tracing::info!("{last}: Max ID: {max_id}");
                entries.push((filename, max_id));
            }
            Ok(None) => tracing::warn!("No 'id' column or stats in {key}"),
            Err(e) => tracing::error!("Error processing {key}: {e}"),
        }
    }

    entries.sort_by(|a, b| a.1.cmp(&b.1));
    tracing::info!("Total files processed: {}", entries.len());

    let arr: Vec<Value> = entries.into_iter().map(|(f, id)| json!([f, id])).collect();
    Ok(Value::Array(arr))
}

#[allow(deprecated)]
async fn read_max_id(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Option<String>> {
    let path = ObjPath::from(key);
    let meta = store
        .head(&path)
        .await
        .with_context(|| format!("HEAD {key}"))?;
    let reader = ParquetObjectReader::new(Arc::clone(store), path).with_file_size(meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("open parquet {key}"))?;
    let pq_meta = builder.metadata();
    let file_meta = pq_meta.file_metadata();
    let schema = file_meta.schema_descr();

    let id_col_idx = (0..schema.num_columns()).find(|i| schema.column(*i).name() == "id");
    let Some(idx) = id_col_idx else {
        return Ok(None);
    };

    if pq_meta.num_row_groups() == 0 {
        return Ok(None);
    }
    let last_rg = pq_meta.row_group(pq_meta.num_row_groups() - 1);
    let col = last_rg.column(idx);
    let Some(stats) = col.statistics() else {
        return Ok(None);
    };
    Ok(extract_max(stats))
}

fn extract_max(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::ByteArray(s) => s
            .max_opt()
            .map(|b| String::from_utf8_lossy(b.data()).into_owned()),
        Statistics::FixedLenByteArray(s) => s
            .max_opt()
            .map(|b| String::from_utf8_lossy(b.data()).into_owned()),
        Statistics::Int32(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int96(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Boolean(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Float(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Double(s) => s.max_opt().map(|v| v.to_string()),
    }
}
