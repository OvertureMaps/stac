# Overture STAC (Rust)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Experimental branch.** Rust port of `overture-stac` — CLI + Python bindings.
> The production catalog at `stac.overturemaps.org` is still published by the
> Python implementation on `main`. See the [TODO](#todo) section for what's
> pending before this branch can replace `main`.

Rust port of `overture-stac`, the CLI that generates STAC catalogs for public Overture releases. See [`docs/architecture.md`](./docs/architecture.md) for how the production catalog gets built and published.

**[Browse the catalog](https://radiantearth.github.io/stac-browser/#/external/stac.overturemaps.org/catalog.json?.language=en)**

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run --release -- build \
  --release-version 2026-07-22.0 \
  --schema-version 1.18.0 \
  --output ./public_releases \
  --concurrency 6
```

Pass `--debug` for a fast run (a few fragments per type). The `build` subcommand reads from the Overture public bucket over `object_store`, which supports `s3://`, `gs://`, `az://`, and `http(s)://` URIs — the CLI defaults to Overture's S3 location, but the underlying core is cloud-agnostic.

## Python bindings

Same core, imported from Python:

```python
import asyncio
import overture_stac

asyncio.run(overture_stac.build_catalog(
    release_version="2026-07-22.0",
    schema_version="1.18.0",
))
```

Build & install into the current venv with `maturin`:

```bash
uv run maturin develop --release --features python,extension-module
# or: just py-develop
```

The function is async (returns a coroutine) and matches the CLI defaults — `data_uri`, `extras_uri`, `root_href`, `concurrency`, `debug` are all keyword arguments with production defaults. Errors surface as `overture_stac.OvertureStacError`.

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

## TODO

Tracking here so we don't lose track of pending work while this branch is experimental.

- **CI on `rust`** — no workflow currently builds Rust or runs tests on this branch. `main`'s CI targets the Python code; nothing verifies changes here. Needed before this can replace `main`.
- **Wheel distribution** — building locally via `just py-develop` works. Not published anywhere. Once a service wants to `pip install overture-stac`, we need a `publish-pypi.yml` restored for maturin (or an internal index).
- **Streaming upload** — `output` currently expects a local path. `object_store::multipart` would let `output=s3://…` write the catalog directly to the destination bucket. Would eliminate the intermediate on-disk step, but breaks the current "validate locally, then sync" production pattern. Design first.
- **`pyo3-log` end-to-end** — the bridge is installed (`pyo3_log::init()`) but Rust `tracing`/`log` events aren't reaching Python's `logging` in practice. TODO comment in `src/python.rs`. Non-blocking (CLI logs work fine); annoying for scripts.
- **Production migration** — `publish-catalog.yaml` on `main` invokes the Python CLI. To retire the Python impl, that workflow needs to install and invoke `overture-stac build` instead.
