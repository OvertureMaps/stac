# Overture STAC (Rust)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Rust port of `overture-stac`, the CLI that generates STAC catalogs for public Overture releases. This branch is a working prototype — the production catalog at `stac.overturemaps.org` is still published by the Python implementation on `main`. See [`docs/architecture.md`](./docs/architecture.md) for how the production catalog gets built and published.

**[Browse the catalog](https://radiantearth.github.io/stac-browser/#/external/stac.overturemaps.org/catalog.json?.language=en)**

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run --release -- \
  --release 2026-07-22.0 \
  --schema-version 1.18.0 \
  --output ./public_releases \
  --workers 6
```

Same flags as the Python CLI. Pass `--debug` for a fast run (a few fragments per type).

## Development

A [`justfile`](./justfile) collects the common commands. Install [just](https://github.com/casey/just) with `brew install just` and run `just` to see recipes. `just check` runs `cargo fmt --check`, `cargo clippy`, and `cargo test` — the same checks CI would run.

## Parity strategy

Semantic parity with the Python `gen-stac` CLI, not byte-identical. Field order and whitespace follow the `stac` crate's Serialize impls, which differ from `pystac`'s output. Content matches: same catalog/collection/item structure, same items, same asset hrefs, same extension fields.

- Catalog/Collection/Item modeled via the `stac` crate (`Catalog`, `Collection`, `Item`, `Link`, `Asset`, `Bbox`, `Extent`).
- OMF-specific extension fields (`storage:schemes`, `table:columns`, `release:version`, etc.) live in `additional_fields`.
- `collections.parquet` written via `stac`'s `geoparquet` feature (`ItemCollection::into_geoparquet_path`).
- Parquet fragment metadata read via `object_store` + `parquet::ParquetMetaDataReader::load_via_suffix_and_finish` — one ranged suffix GET per fragment, no HEAD.

## Verify against the Python implementation

Check out the Python implementation from `main` in a sibling directory to compare outputs — see the PR that introduced this branch ([#101](https://github.com/OvertureMaps/stac/pull/101)) for the compare harness and results (4.2× faster, 996/996 semantic parity on `2026-07-22.0`).
