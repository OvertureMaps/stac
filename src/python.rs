//! Python bindings for the Overture STAC catalog builder.
//!
//! Single-layer PyO3 module: the pymodule IS the whole Python surface. Users:
//! ```python
//! import asyncio
//! import overture_stac
//!
//! asyncio.run(overture_stac.build_catalog("2026-07-22.0"))
//! ```
//!
//! Errors from Rust surface as `overture_stac.OvertureStacError` (subclass of `RuntimeError`).

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::stac::{
    build_single_release, link_neighbor_releases, list_release_ids, save_absolute_published,
    validate_catalog as rust_validate_catalog, validate_catalog_uri as rust_validate_catalog_uri,
    validate_url as rust_validate_url, Failure, ValidateOptions, ValidationReport,
};
use crate::storage::{fetch_schema_version, Bucket};

const DEFAULT_DATA_URI: &str = "s3://overturemaps-us-west-2";
const DEFAULT_EXTRAS_URI: &str = "s3://overturemaps-extras-us-west-2";
const DEFAULT_ROOT_HREF: &str = "https://stac.overturemaps.org";
const DEFAULT_OUTPUT: &str = "./public_releases/";

/// Sentinel string for `schema_version` — distinguishes "user omitted the arg"
/// (auto-infer) from "user explicitly passed None" (no schema).
const AUTO_SENTINEL: &str = "auto";

create_exception!(overture_stac, OvertureStacError, PyRuntimeError);

fn map_err(e: crate::Error) -> PyErr {
    OvertureStacError::new_err(format!("{e}"))
}

/// Resolve the three-state `schema_version` arg:
///   - `None` (explicit)                    → `Some("")`   — build with no schema
///   - `Some("auto")` (default / omitted)   → auto-infer from the STAC catalog
///   - `Some("1.18.0")` (explicit string)   → `Some("1.18.0")`
async fn resolve_schema_version(
    schema_version: Option<String>,
    root_href: &str,
    release_version: &str,
) -> crate::Result<String> {
    match schema_version {
        None => Ok(String::new()),
        Some(s) if s == AUTO_SENTINEL => Ok(fetch_schema_version(root_href, release_version)
            .await?
            .unwrap_or_default()),
        Some(s) => Ok(s),
    }
}

/// Extract the schema_version arg into `Option<String>` where:
///   - Python `None` → `Ok(None)`
///   - Python `str`  → `Ok(Some(str))`
///   - anything else → TypeError via extract failure
fn extract_schema_arg(obj: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if obj.is_none() {
        Ok(None)
    } else {
        Ok(Some(obj.cast::<PyString>()?.to_string()))
    }
}

/// Build a STAC catalog for a single Overture release.
///
/// Writes catalog.json / collections.parquet / manifest.geojson under `<output>/<release_version>/`.
/// Returns None; the interesting output is on the filesystem.
///
/// Args:
///     release_version: Release identifier, e.g. "2026-07-22.0".
///     schema_version: One of:
///         - Omitted (default "auto"): auto-infer from
///           `<root_href>/<release_version>/catalog.json`.
///         - None: build the catalog without any schema:version field.
///         - str like "1.18.0": use as-is.
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
    schema_version = None,
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
    schema_version: Option<Bound<'py, PyAny>>,
    output: String,
    data_uri: String,
    root_href: String,
    extras_uri: Option<String>,
    concurrency: Option<usize>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let schema_input = match schema_version {
        None => Some(AUTO_SENTINEL.to_string()),
        Some(obj) => extract_schema_arg(&obj)?,
    };
    tracing::info!(
        "build_catalog: release={release_version} schema={schema_input:?} data_uri={data_uri}"
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let root_href = root_href.trim_end_matches('/').to_string();
        let output_path = PathBuf::from(&output);
        std::fs::create_dir_all(&output_path).map_err(|e| {
            OvertureStacError::new_err(format!("creating output dir {output}: {e}"))
        })?;

        let resolved_schema = resolve_schema_version(schema_input, &root_href, &release_version)
            .await
            .map_err(map_err)?;

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
            &resolved_schema,
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

// ─── validate ────────────────────────────────────────────────────────────────

/// A single validation finding — location, kind, and human-readable message.
#[pyclass(
    name = "Failure",
    frozen,
    skip_from_py_object,
    module = "overture_stac"
)]
#[derive(Clone)]
struct PyFailure {
    #[pyo3(get)]
    location: String,
    /// One of `"schema"`, `"link"`, `"overture-rule"`.
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl PyFailure {
    fn __repr__(&self) -> String {
        format!(
            "Failure(kind={:?}, location={:?}, message={:?})",
            self.kind, self.location, self.message
        )
    }
}

impl From<Failure> for PyFailure {
    fn from(f: Failure) -> Self {
        Self {
            location: f.location,
            kind: f.kind.to_string(),
            message: f.message,
        }
    }
}

/// Result of a validation run.
#[pyclass(name = "ValidationReport", frozen, module = "overture_stac")]
struct PyValidationReport {
    #[pyo3(get)]
    ok: bool,
    #[pyo3(get)]
    files_checked: usize,
    #[pyo3(get)]
    failures: Vec<PyFailure>,
}

#[pymethods]
impl PyValidationReport {
    fn __repr__(&self) -> String {
        format!(
            "ValidationReport(ok={}, files_checked={}, failures=[{} item(s)])",
            self.ok,
            self.files_checked,
            self.failures.len()
        )
    }
}

impl From<ValidationReport> for PyValidationReport {
    fn from(r: ValidationReport) -> Self {
        let ok = r.is_ok();
        Self {
            ok,
            files_checked: r.files_checked,
            failures: r.failures.into_iter().map(PyFailure::from).collect(),
        }
    }
}

fn opts(check_schema: bool, check_links: bool, check_overture_rules: bool) -> ValidateOptions {
    ValidateOptions {
        check_schema,
        check_links,
        check_overture_rules,
    }
}

fn default_concurrency(user: Option<usize>) -> usize {
    user.unwrap_or_else(|| (num_cpus::get() / 2).max(1))
}

/// Validate a built STAC catalog on disk. Returns a [`ValidationReport`].
///
/// Args:
///     dir: Directory containing the built catalog (must have catalog.json at the root).
///     concurrency: Concurrent file-check futures. None = num_cpus / 2 (min 1).
///     check_schema: JSON-Schema validation via stac-validate (network on first use).
///     check_links: Every in-base href must resolve to a file on disk.
///     check_overture_rules: Overture-specific assertions.
///
/// Raises:
///     OvertureStacError: on any I/O or validator initialization error.
#[pyfunction]
#[pyo3(signature = (
    dir,
    *,
    concurrency = None,
    check_schema = true,
    check_links = true,
    check_overture_rules = true,
))]
fn validate_catalog<'py>(
    py: Python<'py>,
    dir: PathBuf,
    concurrency: Option<usize>,
    check_schema: bool,
    check_links: bool,
    check_overture_rules: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let c = default_concurrency(concurrency);
    let o = opts(check_schema, check_links, check_overture_rules);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = rust_validate_catalog(&dir, c, o).await.map_err(map_err)?;
        Ok(PyValidationReport::from(report))
    })
}

/// Validate a live catalog over HTTP. Fetches the root URL and crawls
/// rel=child + rel=item links inside the same base URL. Returns a
/// [`ValidationReport`]. Concurrency is capped at 16 for CDN politeness.
///
/// Args:
///     url: Root catalog URL (e.g. "https://stac.overturemaps.org/catalog.json").
///     concurrency: Concurrent HTTP GETs. None = num_cpus / 2 (min 1, cap 16).
///     check_schema: JSON-Schema validation.
///     check_links: In-base hrefs must have been reachable via crawl.
///     check_overture_rules: Overture-specific assertions.
#[pyfunction]
#[pyo3(signature = (
    url,
    *,
    concurrency = None,
    check_schema = true,
    check_links = true,
    check_overture_rules = true,
))]
fn validate_url<'py>(
    py: Python<'py>,
    url: String,
    concurrency: Option<usize>,
    check_schema: bool,
    check_links: bool,
    check_overture_rules: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let c = default_concurrency(concurrency);
    let o = opts(check_schema, check_links, check_overture_rules);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = rust_validate_url(&url, c, o).await.map_err(map_err)?;
        Ok(PyValidationReport::from(report))
    })
}

/// Validate a catalog over `object_store` (`s3://`, `gs://`, `az://`,
/// `file://`). Same crawl semantics as [`validate_url`] but reads bytes
/// from the bucket instead of the CDN — proves the source of truth,
/// independent of any HTTP cache.
///
/// Args:
///     catalog_uri: Object-store URI (e.g. "s3://overturemaps-extras-us-west-2/stac/").
///     concurrency: Concurrent object-store GETs. None = num_cpus / 2 (min 1, cap 16).
///     check_schema / check_links / check_overture_rules: same as validate_url.
#[pyfunction]
#[pyo3(signature = (
    catalog_uri,
    *,
    concurrency = None,
    check_schema = true,
    check_links = true,
    check_overture_rules = true,
))]
fn validate_catalog_uri<'py>(
    py: Python<'py>,
    catalog_uri: String,
    concurrency: Option<usize>,
    check_schema: bool,
    check_links: bool,
    check_overture_rules: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let c = default_concurrency(concurrency);
    let o = opts(check_schema, check_links, check_overture_rules);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = rust_validate_catalog_uri(&catalog_uri, c, o)
            .await
            .map_err(map_err)?;
        Ok(PyValidationReport::from(report))
    })
}

// ─── list_releases ───────────────────────────────────────────────────────────

/// List release IDs in the data bucket, newest first.
///
/// Args:
///     data_uri: Object-store URI to the data bucket root. Defaults to
///         "s3://overturemaps-us-west-2".
#[pyfunction]
#[pyo3(signature = (data_uri = DEFAULT_DATA_URI.to_string()))]
fn list_releases(py: Python<'_>, data_uri: String) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let bucket = Bucket::from_url(&data_uri).map_err(map_err)?;
        list_release_ids(&bucket, "release").await.map_err(map_err)
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
    m.add_function(wrap_pyfunction!(validate_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(validate_url, m)?)?;
    m.add_function(wrap_pyfunction!(validate_catalog_uri, m)?)?;
    m.add_function(wrap_pyfunction!(list_releases, m)?)?;
    m.add_class::<PyValidationReport>()?;
    m.add_class::<PyFailure>()?;
    m.add("OvertureStacError", py.get_type::<OvertureStacError>())?;
    Ok(())
}
