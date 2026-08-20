class OvertureStacError(RuntimeError):
    """Raised when the Rust core reports an error."""

async def build_catalog(
    release_version: str,
    schema_version: str,
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
        schema_version: Overture schema version, e.g. ``"1.18.0"``.
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
