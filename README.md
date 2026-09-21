# Overture STAC

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Publish Release](https://github.com/OvertureMaps/stac/actions/workflows/publish-release.yaml/badge.svg)](https://github.com/OvertureMaps/stac/actions/workflows/publish-release.yaml)

`overture-stac` generates, validates, and reconciles the STAC catalog for public [Overture Maps](https://overturemaps.org) releases. It ships as a Rust CLI, with Python bindings for calling the same core from Python.

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

The function is async (returns a coroutine) and matches the CLI defaults: `data_uri`, `extras_uri`, `root_href`, `concurrency`, `debug` are all keyword arguments with production defaults. Errors surface as `overture_stac.OvertureStacError`.

The same module also exposes the CLI's `validate` and `list-releases` operations, so scripts and services can consume the STAC catalog without shelling out:

```python
import asyncio
import overture_stac

async def main():
    ids = await overture_stac.list_releases()
    print("current releases:", ids)

    # Also: validate_catalog(dir=...) for a local build,
    #       validate_catalog_uri(catalog_uri="s3://...") for the bucket.
    report = await overture_stac.validate_url("https://stac.overturemaps.org")
    if not report.ok:
        for f in report.failures:
            print(f"[{f.kind}] {f.location}: {f.message}")

asyncio.run(main())
```

`validate_url` accepts `https://host`, `https://host/`, or a full `.json` URL. Concurrency is capped at 16 for CDN politeness. `check_schema`, `check_links`, `check_overture_rules` are keyword flags (all `True` by default).

### Logging

Rust `tracing`/`log` events forward into Python's `logging` module via `pyo3-log` (installed automatically on module import). Every event lands on the logger whose name matches the Rust module path (`overture_stac.stac.theme`, `object_store.aws.builder`, `reqwest.connect`, etc.). Python's root logger defaults to `WARNING`, so **INFO events are silently filtered** unless you configure `logging`:

```python
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

import overture_stac  # events now visible during any call
```

Route a specific target with the usual `logging.getLogger("overture_stac.stac.theme").setLevel(...)`.

## Development

A [`justfile`](./justfile) collects the common commands. Install [just](https://github.com/casey/just) with `brew install just` and run `just` to see recipes. `just check` runs `cargo fmt --check`, `cargo clippy`, and `cargo test`, the same checks CI would run.
