class OvertureStacError(RuntimeError):
    """Raised when the Rust core reports an error."""

class Failure:
    """A single validation finding."""

    location: str
    """File path (local mode) or URL (crawl modes) that failed."""
    kind: str
    """One of ``"schema"``, ``"link"``, ``"overture-rule"``."""
    message: str

class ValidationReport:
    """Result of a validation run."""

    ok: bool
    """``True`` iff ``failures`` is empty."""
    files_checked: int
    """Documents inspected (fetch failures counted in ``failures``, not here)."""
    failures: list[Failure]

async def build_catalog(
    release_version: str,
    schema_version: str | None = ...,
    *,
    output: str = ...,
    data_uri: str = ...,
    root_href: str = ...,
    extras_uri: str | None = ...,
    concurrency: int | None = ...,
    debug: bool = ...,
) -> None:
    """Build a STAC catalog for a single Overture release.

    Writes ``catalog.json`` / ``collections.parquet`` / ``manifest.geojson`` under
    ``<output>/<release_version>/``. Returns ``None``; the interesting output is on
    the filesystem.

    Args:
        release_version: Release identifier, e.g. ``"2026-07-22.0"``.
        schema_version: One of:

            - **Omitted** (default is the sentinel string ``"auto"``): auto-infer
              from ``<root_href>/<release_version>/catalog.json`` — reads the
              ``schema:version`` field.
            - ``None``: build the catalog without any ``schema:version`` field.
            - ``"1.18.0"`` (or any other string): use as-is.
        output: Local output directory. Defaults to ``"./public_releases/"``.
        data_uri: Object-store URI to the data bucket. Any URI supported by
            ``object_store`` (``s3://``, ``gs://``, ``az://``, ``file://`` ...).
            Defaults to ``"s3://overturemaps-us-west-2"``.
        root_href: Public URL prefix baked into absolute self links. Defaults
            to ``"https://stac.overturemaps.org"``.
        extras_uri: Object-store URI to the extras bucket (PMTiles). Pass
            ``None`` to skip PMTiles discovery. Defaults to
            ``"s3://overturemaps-extras-us-west-2"``.
        concurrency: Number of theme-processing futures to run concurrently.
            ``None`` = autodetect (``num_cpus / 2``, minimum 1).
        debug: When ``True``, samples 1 item per collection for fast iteration.

    Raises:
        OvertureStacError: On any error from the Rust core.
    """
    ...

async def validate_catalog(
    dir: str,
    *,
    concurrency: int | None = ...,
    check_schema: bool = ...,
    check_links: bool = ...,
    check_overture_rules: bool = ...,
) -> ValidationReport:
    """Validate a built STAC catalog on disk.

    Args:
        dir: Directory containing the built catalog (must have ``catalog.json``
            at the root).
        concurrency: Concurrent file-check futures. ``None`` = ``num_cpus / 2``.
        check_schema: JSON-Schema validation via ``stac-validate`` (fetches
            schemas on first use — needs network access).
        check_links: Every in-base href must resolve to a file on disk.
        check_overture_rules: Overture-specific assertions (``schema:version``
            non-null, required assets, license enum, etc.).

    Raises:
        OvertureStacError: On any I/O or validator initialization error.
    """
    ...

async def validate_url(
    url: str,
    *,
    concurrency: int | None = ...,
    check_schema: bool = ...,
    check_links: bool = ...,
    check_overture_rules: bool = ...,
) -> ValidationReport:
    """Validate a live catalog over HTTP. Tests what the CDN serves to users.

    Fetches the root URL, crawls ``rel=child`` + ``rel=item`` links inside the
    same base URL, runs the three checks against every reachable document.
    Concurrency is capped at 16 for CDN politeness.

    Accepts convenient forms: ``https://host``, ``https://host/``, or
    ``https://host/catalog.json``. Bare hostnames without ``http(s)://``
    are rejected with a clear error.

    Args:
        url: Root catalog URL, e.g. ``"https://stac.overturemaps.org/catalog.json"``.
        concurrency: Concurrent HTTP GETs. ``None`` = ``num_cpus / 2`` (cap 16).
        check_schema / check_links / check_overture_rules: same as ``validate_catalog``.
    """
    ...

async def validate_catalog_uri(
    catalog_uri: str,
    *,
    concurrency: int | None = ...,
    check_schema: bool = ...,
    check_links: bool = ...,
    check_overture_rules: bool = ...,
) -> ValidationReport:
    """Validate a catalog over ``object_store`` — tests the bucket source of truth.

    Same crawl semantics as :func:`validate_url` but reads bytes from the
    bucket, independent of any CDN cache. Anonymous S3 is used when no AWS
    credentials are set in the environment.

    Args:
        catalog_uri: Object-store URI to the catalog root, e.g.
            ``"s3://overturemaps-extras-us-west-2/stac/"``. Supports ``s3://``,
            ``gs://``, ``az://``, ``file://``.
        concurrency / check_*: same as :func:`validate_url`.
    """
    ...

async def list_releases(data_uri: str = ...) -> list[str]:
    """Return release IDs in the data bucket, newest first.

    Args:
        data_uri: Object-store URI to the data bucket root. Defaults to
            ``"s3://overturemaps-us-west-2"``.
    """
    ...
