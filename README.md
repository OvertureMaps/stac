# Overture STAC

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Publish Release](https://github.com/OvertureMaps/stac/actions/workflows/publish-release.yaml/badge.svg)](https://github.com/OvertureMaps/stac/actions/workflows/publish-release.yaml)

`overture-stac` generates, validates, and reconciles the STAC catalog for public [Overture Maps](https://overturemaps.org) releases. It ships as a Rust CLI.

- [`docs/architecture.md`](./docs/architecture.md) — how the production catalog gets built and published.

**[Browse the catalog](https://radiantearth.github.io/stac-browser/#/external/stac.overturemaps.org/catalog.json?.language=en)**

## Build

```bash
cargo build --release
```

## Usage

Four subcommands (`overture-stac --help` for full listings):

- `build` — walk the data bucket and write a full STAC catalog to disk. Defaults to all current releases.
- `list-releases` — print release IDs the data bucket currently exposes, newest first.
- `reconcile` — compare the live catalog against the data bucket and report drift. `--apply` writes the fix.
- `validate` — run JSON-schema + link-integrity + Overture-specific checks against a built catalog (local dir, remote URL, or object-store URI).

Typical single-release build:

```bash
cargo run --release -- build \
  --release-version 2026-07-22.0 \
  --schema-version 1.18.0 \
  --output ./public_releases \
  --concurrency 6
```

Pass `--debug` for a fast run (a few fragments per type). The `build` subcommand reads from the Overture public bucket over `object_store`, which supports `s3://`, `gs://`, `az://`, and `http(s)://` URIs. The CLI defaults to Overture's S3 location, but the underlying core is cloud-agnostic.

### Item index

Each non-empty release catalog advertises its existing `collections.parquet` through a `rel: "index"` link:

```json
{
  "rel": "index",
  "href": "https://stac.overturemaps.org/2026-10-21.0/collections.parquet",
  "type": "application/vnd.apache.parquet",
  "title": "STAC Item index (GeoParquet)"
}
```

The index contains one row per STAC Item, not per Overture feature. Clients can discover it by its relation and media type, then query fragment bounds and asset URLs without fetching every Item JSON. Existing child and Item links remain unchanged. Empty releases do not advertise an index; debug builds index only their sampled Items.

For example, find candidate building files around San Francisco with DuckDB. Replace the URL with the index `href` from the chosen release:

```sql
SELECT assets.aws.href AS data_href
FROM read_parquet(
  'https://stac.overturemaps.org/2026-10-21.0/collections.parquet'
)
WHERE collection = 'building'
  AND bbox.xmin <= -122.35 AND bbox.xmax >= -122.55
  AND bbox.ymin <= 37.85 AND bbox.ymax >= 37.70;
```

Read the returned data files and apply feature-level filters to obtain the actual buildings. The index only identifies files whose bounds overlap the requested area.

## Migrating from [1.4.0](https://pypi.org/project/overture-stac/1.4.0/)

`overture-stac` 2.x replaced the pure-Python [1.4.0](https://pypi.org/project/overture-stac/1.4.0/) release with a Rust core plus thin Python bindings; those bindings have since been retired in favor of the Rust CLI/crate only. The [1.4.0](https://pypi.org/project/overture-stac/1.4.0/) API is not preserved. The old entry points map as follows:

- `gen-stac` is replaced by the `overture-stac` CLI, distributed through the Rust crate: `cargo install overture-stac`.
- The `OvertureRelease` class is replaced by the CLI's `build`, `validate`, and `list-releases` subcommands.

## Development

A [`justfile`](./justfile) collects the common commands. Install [just](https://github.com/casey/just) with `brew install just` and run `just` to see recipes. `just check` runs `cargo fmt --check`, `cargo clippy`, and `cargo test`, the same checks CI would run.
