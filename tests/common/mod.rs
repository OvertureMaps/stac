//! Shared test fixtures.

#![allow(dead_code)]

use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;

/// Write one tiny parquet file per `(theme, type)` under `<data_dir>/release/<release>`,
/// matching the Overture bucket layout that `latest.ddb` views read.
pub fn write_release_fixture(data_dir: &Path, release: &str, types: &[(&str, &str)]) {
    let ids: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    for (theme, ty) in types {
        let dir = data_dir
            .join("release")
            .join(release)
            .join(format!("theme={theme}"))
            .join(format!("type={ty}"));
        fs::create_dir_all(&dir).unwrap();
        let file = File::create(dir.join("part-0.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}

/// Run `sql` against `db` with the DuckDB CLI (`$DUCKDB_BIN`, default `duckdb`) and return
/// stdout as headerless CSV. CI installs the CLI via `.github/actions/setup-duckdb`.
pub fn duckdb_query(db: &Path, sql: &str) -> String {
    let bin = std::env::var("DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string());
    let out = Command::new(&bin)
        .args(["-readonly", "-csv", "-noheader"])
        .arg(db)
        .arg("-c")
        .arg(sql)
        .output()
        .unwrap_or_else(|e| panic!("could not run `{bin}`: {e}; install the DuckDB CLI"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
