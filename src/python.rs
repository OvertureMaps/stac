//! Python bindings for the Overture STAC catalog builder.
//!
//! Single-layer PyO3 module: the pymodule IS the whole Python surface. Users:
//! ```python
//! import asyncio
//! import overture_stac
//!
//! asyncio.run(overture_stac.build_catalog("2026-07-22.0", "1.18.0"))
//! ```
//!
//! Errors from Rust surface as `overture_stac.OvertureStacError` (subclass of `RuntimeError`).

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::catalog::{
    build_single_release, link_neighbor_releases, list_release_ids, save_absolute_published,
};
use crate::s3::Bucket;

const DEFAULT_DATA_URI: &str = "s3://overturemaps-us-west-2";
const DEFAULT_EXTRAS_URI: &str = "s3://overturemaps-extras-us-west-2";
const DEFAULT_ROOT_HREF: &str = "https://stac.overturemaps.org";
const DEFAULT_OUTPUT: &str = "./public_releases/";

create_exception!(overture_stac, OvertureStacError, PyRuntimeError);

fn map_err(e: anyhow::Error) -> PyErr {
    OvertureStacError::new_err(format!("{e:#}"))
}

/// Build a STAC catalog for a single Overture release.
///
/// Writes catalog.json / collections.parquet / manifest.geojson under `<output>/<release_version>/`.
/// Returns None; the interesting output is on the filesystem.
///
/// Args:
///     release_version: Release identifier, e.g. "2026-07-22.0".
///     schema_version: Overture schema version, e.g. "1.18.0".
///     output: Local output directory. Defaults to "./public_releases/".
///     data_uri: Object-store URI to the data bucket. Any URI supported by
///         `object_store` (s3://, gs://, az://, file:// ...). Defaults to
///         "s3://overturemaps-us-west-2".
///     root_href: Public URL prefix baked into absolute self links. Defaults
///         to "https://stac.overturemaps.org".
///     extras_uri: Object-store URI to the extras bucket (PMTiles). Pass None
///         to skip PMTiles discovery. Defaults to
///         "s3://overturemaps-extras-us-west-2".
///     concurrency: Number of theme-processing futures to run concurrently.
///         None = autodetect (`num_cpus / 2`, minimum 1).
///     debug: When True, samples 1 item per collection for fast iteration.
///
/// Raises:
///     OvertureStacError: on any error from the Rust core.
#[pyfunction]
#[pyo3(signature = (
    release_version,
    schema_version,
    *,
    output = DEFAULT_OUTPUT.to_string(),
    data_uri = DEFAULT_DATA_URI.to_string(),
    root_href = DEFAULT_ROOT_HREF.to_string(),
    extras_uri = Some(DEFAULT_EXTRAS_URI.to_string()),
    concurrency = None,
    debug = false,
))]
#[allow(clippy::too_many_arguments)]
fn build_catalog<'py>(
    py: Python<'py>,
    release_version: String,
    schema_version: String,
    output: String,
    data_uri: String,
    root_href: String,
    extras_uri: Option<String>,
    concurrency: Option<usize>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    tracing::info!(
        "build_catalog: release={release_version} schema={schema_version} data_uri={data_uri}"
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let root_href = root_href.trim_end_matches('/').to_string();
        let output_path = PathBuf::from(&output);
        std::fs::create_dir_all(&output_path).map_err(|e| {
            OvertureStacError::new_err(format!("creating output dir {output}: {e}"))
        })?;

        let bucket = Bucket::from_url(&data_uri).map_err(map_err)?;
        let extras_bucket = match extras_uri {
            Some(uri) => Some(Bucket::from_url(&uri).map_err(map_err)?),
            None => None,
        };

        let title = format!("{release_version} Overture Release");
        let mut catalog = build_single_release(
            &bucket,
            extras_bucket.as_ref(),
            &release_version,
            &schema_version,
            &title,
            debug,
            concurrency.unwrap_or_else(|| (num_cpus::get() / 2).max(1)),
            &output_path,
        )
        .await
        .map_err(map_err)?;

        let ids = list_release_ids(&bucket, "release")
            .await
            .map_err(map_err)?;
        link_neighbor_releases(&mut catalog, &ids, &root_href);

        let dest = output_path.join(&release_version);
        save_absolute_published(&catalog, &format!("{root_href}/{release_version}"), &dest)
            .map_err(map_err)?;

        Ok(())
    })
}

#[pymodule]
fn overture_stac(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Forward Rust `tracing`/`log` events to Python's `logging` module. Callers control
    // filtering the usual Python way: `logging.getLogger("overture_stac").setLevel(...)`.
    // TODO(#pyo3-log): install() succeeds but Rust `tracing`/`log` events aren't
    // currently reaching Python's `logging`. Skeleton kept in place so it starts
    // flowing once the missing wiring is identified.
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(build_catalog, m)?)?;
    m.add("OvertureStacError", py.get_type::<OvertureStacError>())?;
    Ok(())
}
