# overture-stac-rs

Rust port of `overture_stac` (`../src/overture_stac/`). Semantic parity with the Python `gen-stac` CLI — same STAC output, faster and more resilient to transient S3 errors.

## Build

```sh
cd rust
cargo build --release
```

## Run

Same flags as the Python CLI:

```sh
cargo run --release -- \
  --release 2026-07-22.0 \
  --schema-version 1.18.0 \
  --output ../rust_output \
  --workers 6
```

Or from the repo root: `just rs-run 1.18.0 2026-07-22.0` (debug) / `just rs-run 1.18.0 2026-07-22.0 CARGS=''` (full).

## Parity strategy

Semantic, not byte-identical. Field order and whitespace follow the `stac` crate's Serialize impls, which differ from `pystac`'s output. Content matches: same catalog/collection/item structure, same items, same asset hrefs, same extension fields.

- Catalog/Collection/Item modeled via the `stac` crate (`Catalog`, `Collection`, `Item`, `Link`, `Asset`, `Bbox`, `Extent`).
- OMF-specific extension fields (`storage:schemes`, `table:columns`, `release:version`, etc.) live in `additional_fields`.
- `collections.parquet` written via `stac`'s `geoparquet` feature (`ItemCollection::into_geoparquet_path`).
- Parquet fragment metadata read via `object_store` + `parquet::ParquetMetaDataReader::load_via_suffix_and_finish` — one ranged suffix GET per fragment, no HEAD.

## Verify against Python

From the repo root:

```sh
just rs-compare 1.18.0 2026-07-22.0            # debug: 3 fragments per type
just rs-compare 1.18.0 2026-07-22.0 CARGS=''   # full
```

Prints any files that diverge. Note that Python's theme child ordering is non-deterministic (`ProcessPoolExecutor.as_completed`), so the release-level `catalog.json` and `manifest.geojson` will have permuted arrays — set content matches.
