//! Shared test fixtures.

use std::fs;
use std::path::Path;

/// Write one tiny parquet file per `(theme, type)` under `<data_dir>/release/<release>`,
/// matching the Overture bucket layout that `latest.ddb` views read.
pub fn write_release_fixture(data_dir: &Path, release: &str, types: &[(&str, &str)]) {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    for (theme, ty) in types {
        let dir = data_dir
            .join("release")
            .join(release)
            .join(format!("theme={theme}"))
            .join(format!("type={ty}"));
        fs::create_dir_all(&dir).unwrap();
        let file = dir
            .join("part-0.parquet")
            .to_string_lossy()
            .replace('\\', "/");
        conn.execute_batch(&format!(
            "COPY (SELECT 'a' AS id, 'POINT(0 0)' AS wkt) TO '{file}' (FORMAT parquet);"
        ))
        .unwrap();
    }
}
