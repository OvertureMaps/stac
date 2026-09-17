//! Write a minimal Overture-shaped fixture tree for the offline stac-validate CI
//! job. Produces two parquet files:
//!
//! - `release/2026-01-01.0/theme=addresses/type=address/part-00000-fixture.parquet`
//!   — one row, one `count` column, KV metadata `geo` with a valid bbox. Enough
//!   for `read_fragment` to extract bbox + column names + row-group count.
//! - `registry/part-00000-fixture.parquet` — one row, one `id` column
//!   (byte-array), row-group statistics enabled. Enough for `create_manifest`
//!   to read a max-id.
//!
//! Usage: `cargo run --example gen_fixture -- <output-dir>`

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const GEO_KV: &str = r#"{"columns":{"geometry":{"encoding":"WKB","geometry_types":[],"bbox":[0.0,0.0,1.0,1.0]}},"version":"1.0.0","primary_column":"geometry"}"#;

fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    batch: RecordBatch,
    kv: Vec<KeyValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(path.parent().unwrap())?;
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_key_value_metadata(Some(kv))
        .build();
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: gen_fixture <output-dir>"),
    );

    // Data parquet — item-shaped, with `geo` KV so read_fragment picks up a bbox.
    let data_schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::Int64,
        false,
    )]));
    let data_batch = RecordBatch::try_new(
        data_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )?;
    write_parquet(
        &output
            .join("release/2026-01-01.0/theme=addresses/type=address/part-00000-fixture.parquet"),
        data_schema,
        data_batch,
        vec![KeyValue::new("geo".to_string(), Some(GEO_KV.to_string()))],
    )?;

    // Registry parquet — `id` column with per-row-group stats so create_manifest
    // can read a max id.
    let reg_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let reg_batch = RecordBatch::try_new(
        reg_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["aaaa-1111"]))],
    )?;
    write_parquet(
        &output.join("registry/part-00000-fixture.parquet"),
        reg_schema,
        reg_batch,
        vec![],
    )?;

    println!("Wrote fixture to {}", output.display());
    Ok(())
}
