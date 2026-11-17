//! Read GeoParquet fragment metadata from S3 — bbox from the `geo` metadata blob, plus
//! `num_rows`, `num_row_groups`, and the column names for `table:columns`.
//!
//! Fast path: `ParquetMetaDataReader::load_via_suffix_and_finish` issues one ranged suffix GET
//! (`Range: bytes=-N`), and only issues a second GET if the encoded metadata is larger than the
//! prefetch. Avoids a separate HEAD and the full stream-builder setup.

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use object_store::{path::Path as ObjPath, GetOptions, GetRange};
use parquet::arrow::async_reader::{MetadataFetch, MetadataSuffixFetch};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{KeyValue, ParquetMetaData, ParquetMetaDataReader};
use serde_json::Value;
use std::ops::Range;
use std::sync::OnceLock;
use tokio::sync::Semaphore;

use crate::stac::is_schema_version;
use crate::storage::{list_top_level, list_top_level_files, Bucket};
use crate::{Error, Result, ResultExt};

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

pub async fn read_fragment(bucket: &Bucket, key: &str) -> Result<FragmentInfo> {
    let meta = read_metadata(bucket, key).await?;
    let file_meta = meta.file_metadata();

    let arrow_schema =
        parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
            .with_context(|| format!("parquet → arrow schema for {key}"))?;
    let column_names: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let kv = file_meta.key_value_metadata();
    let geo_str = kv
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == "geo"))
        .and_then(|kv| kv.value.clone())
        .ok_or_else(|| Error::MissingGeoMetadata(key.to_string()))?;
    let geo: Value =
        serde_json::from_str(&geo_str).with_context(|| format!("parse geo metadata in {key}"))?;

    let geometry = geo
        .get("columns")
        .and_then(|c| c.get("geometry"))
        .ok_or_else(|| Error::MissingGeometryColumn(key.to_string()))?;
    let bbox_arr = geometry
        .get("bbox")
        .and_then(|b| b.as_array())
        .ok_or_else(|| Error::MissingBbox(key.to_string()))?;
    if bbox_arr.len() != 4 {
        return Err(Error::InvalidBboxCoord {
            key: key.to_string(),
            index: bbox_arr.len(),
        });
    }
    let mut bbox = [0f64; 4];
    for (i, v) in bbox_arr.iter().enumerate() {
        bbox[i] = v.as_f64().ok_or_else(|| Error::InvalidBboxCoord {
            key: key.to_string(),
            index: i,
        })?;
    }

    let geoparquet_version = geo
        .get("version")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    Ok(FragmentInfo {
        path: key.to_string(),
        num_rows: file_meta.num_rows(),
        num_row_groups: meta.num_row_groups() as i64,
        bbox,
        column_names,
        geoparquet_version,
    })
}

/// Read the public schema version stamped in a release's parquet metadata.
///
/// Each promoted type writes the same `schema_versions` stamp to its parquet
/// fragments, so inspect one fragment per type. Use the first public version
/// and warn if other types have a different version. Older releases return None.
pub async fn resolve_schema_version_from_parquet(
    bucket: &Bucket,
    release: &str,
) -> Result<Option<String>> {
    let release_prefix = format!("release/{release}");
    let mut themes = list_top_level(bucket, &release_prefix).await?;
    themes.sort();
    let mut resolved = None;
    for theme in themes {
        let theme_prefix = format!("{release_prefix}/{theme}");
        let mut types = list_top_level(bucket, &theme_prefix).await?;
        types.sort();
        for type_name in types {
            let type_prefix = format!("{theme_prefix}/{type_name}");
            let mut files = list_top_level_files(bucket, &type_prefix).await?;
            files.retain(|file| file.name.ends_with(".parquet"));
            files.sort_by(|a, b| a.name.cmp(&b.name));
            if let Some(file) = files.first() {
                let key = format!("{type_prefix}/{}", file.name);
                let metadata = read_metadata(bucket, &key).await?;
                if let Some(version) = schema_version_from_metadata(
                    metadata.file_metadata().key_value_metadata(),
                    &key,
                ) {
                    match &resolved {
                        None => resolved = Some(version),
                        Some(first) if first != &version => {
                            tracing::warn!(
                                release,
                                key,
                                version,
                                selected = first,
                                "different schema versions found in release parquet metadata"
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(resolved)
}

fn schema_version_from_metadata(
    key_value_metadata: Option<&Vec<KeyValue>>,
    key: &str,
) -> Option<String> {
    let entry = key_value_metadata
        .and_then(|metadata| metadata.iter().find(|entry| entry.key == "schema_versions"))?;
    let Some(value) = entry.value.as_deref() else {
        tracing::warn!(key, "schema_versions parquet metadata has no value");
        return None;
    };
    let versions: Value = match serde_json::from_str(value) {
        Ok(versions) => versions,
        Err(error) => {
            tracing::warn!(key, %error, "invalid schema_versions parquet metadata");
            return None;
        }
    };
    if !versions.is_object() {
        tracing::warn!(key, "schema_versions parquet metadata is not an object");
        return None;
    }
    let version = versions.get("overture-schema")?;
    let Some(version) = version.as_str() else {
        tracing::warn!(key, "overture-schema parquet metadata is not a string");
        return None;
    };
    if !is_schema_version(version) {
        tracing::warn!(
            key,
            version,
            "overture-schema parquet metadata is not a plain X.Y.Z version"
        );
        return None;
    }
    Some(version.to_string())
}

async fn read_metadata(bucket: &Bucket, key: &str) -> Result<ParquetMetaData> {
    let _permit = parquet_limit()
        .acquire()
        .await
        .expect("global parquet semaphore poisoned");
    let path = ObjPath::from(key);
    let mut fetch = SuffixFetch { bucket, path };
    ParquetMetaDataReader::new()
        .load_via_suffix_and_finish(&mut fetch)
        .await
        .with_context(|| format!("load parquet metadata for {key}"))
}

/// Ranged/suffix GETs route through [`Bucket::get_opts`] so they inherit the
/// same anonymous-auth retry as everything else in `storage::bucket` — stale
/// SSO creds against the public Overture bucket fall back cleanly instead of
/// aborting the build mid-fragment.
struct SuffixFetch<'a> {
    bucket: &'a Bucket,
    path: ObjPath,
}

impl<'a> MetadataFetch for &mut SuffixFetch<'a> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bucket = self.bucket;
        let path = self.path.clone();
        async move {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range)),
                ..GetOptions::default()
            };
            let resp = bucket
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
        let bucket = self.bucket;
        let path = self.path.clone();
        async move {
            let opts = GetOptions {
                range: Some(GetRange::Suffix(suffix as u64)),
                ..GetOptions::default()
            };
            let resp = bucket
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;

    fn write_parquet(path: &std::path::Path, schema_versions: Option<&str>) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let mut metadata = vec![KeyValue::new(
            "geo".into(),
            Some(r#"{"version":"1.0.0","columns":{"geometry":{"bbox":[0,0,1,1]}}}"#.into()),
        )];
        if let Some(versions) = schema_versions {
            metadata.push(KeyValue::new(
                "schema_versions".into(),
                Some(versions.to_string()),
            ));
        }
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(metadata))
            .build();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn release_schema_version_reads_overture_schema_stamp_from_parquet_footer() {
        let tmp = tempfile::tempdir().unwrap();
        let release = "2026-11-18.0";
        let base = tmp
            .path()
            .join("release")
            .join(release)
            .join("theme=addresses");
        write_parquet(
            &base.join("type=address/part-0.parquet"),
            Some(r#"{"overture-schema-pyspark":"2.0.0"}"#),
        );
        write_parquet(
            &base.join("type=address_alt/part-0.parquet"),
            Some(r#"{"overture-schema":"2.1.0","overture-schema-system":"2.1.0"}"#),
        );

        let bucket = Bucket::from_url(&format!("file://{}", tmp.path().display())).unwrap();
        assert_eq!(
            resolve_schema_version_from_parquet(&bucket, release)
                .await
                .unwrap()
                .as_deref(),
            Some("2.1.0")
        );
    }

    #[tokio::test]
    async fn release_schema_version_is_unresolved_when_no_parquet_has_public_stamp() {
        let tmp = tempfile::tempdir().unwrap();
        let release = "2026-10-21.0";
        write_parquet(
            &tmp.path().join(format!(
                "release/{release}/theme=addresses/type=address/part-0.parquet"
            )),
            Some(r#"{"overture-schema-pyspark":"2.0.0"}"#),
        );
        let bucket = Bucket::from_url(&format!("file://{}", tmp.path().display())).unwrap();

        assert_eq!(
            resolve_schema_version_from_parquet(&bucket, release)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn release_schema_version_skips_unusable_stamps() {
        let tmp = tempfile::tempdir().unwrap();
        let release = "2026-11-18.0";
        let base = tmp
            .path()
            .join(format!("release/{release}/theme=addresses"));
        write_parquet(
            &base.join("type=a/part.parquet"),
            Some(r#"{"overture-schema":"2.0.0.post1"}"#),
        );
        let bucket = Bucket::from_url(&format!("file://{}", tmp.path().display())).unwrap();
        assert_eq!(
            resolve_schema_version_from_parquet(&bucket, release)
                .await
                .unwrap(),
            None
        );
        assert!(read_fragment(
            &bucket,
            &format!("release/{release}/theme=addresses/type=a/part.parquet")
        )
        .await
        .is_ok());
        write_parquet(
            &base.join("type=b/part.parquet"),
            Some(r#"{"overture-schema":"2.1.0"}"#),
        );
        assert_eq!(
            resolve_schema_version_from_parquet(&bucket, release)
                .await
                .unwrap()
                .as_deref(),
            Some("2.1.0")
        );
    }

    #[test]
    fn malformed_schema_metadata_is_unresolved() {
        for value in ["{", "[]", r#"{"overture-schema":3}"#] {
            let metadata = vec![KeyValue::new("schema_versions".into(), Some(value.into()))];
            assert_eq!(
                schema_version_from_metadata(Some(&metadata), "fragment"),
                None
            );
        }
    }

    #[tokio::test]
    async fn mixed_schema_versions_emit_warning() {
        use std::io::Write;
        use std::sync::Mutex;

        #[derive(Clone)]
        struct Log(Arc<Mutex<Vec<u8>>>);
        impl Write for Log {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let release = "2026-11-18.0";
        let base = tmp
            .path()
            .join(format!("release/{release}/theme=addresses"));
        write_parquet(
            &base.join("type=a/part.parquet"),
            Some(r#"{"overture-schema":"2.0.0"}"#),
        );
        write_parquet(
            &base.join("type=b/part.parquet"),
            Some(r#"{"overture-schema":"2.1.0"}"#),
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = Log(output.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let bucket = Bucket::from_url(&format!("file://{}", tmp.path().display())).unwrap();
        assert_eq!(
            resolve_schema_version_from_parquet(&bucket, release)
                .await
                .unwrap()
                .as_deref(),
            Some("2.0.0")
        );
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(output.contains("different schema versions"), "{output}");
        assert!(
            output.contains("2.0.0") && output.contains("2.1.0"),
            "{output}"
        );
    }
}
