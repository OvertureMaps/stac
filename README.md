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

## Migrating from [1.4.0](https://pypi.org/project/overture-stac/1.4.0/)

`overture-stac` 2.x replaced the pure-Python [1.4.0](https://pypi.org/project/overture-stac/1.4.0/) release with a Rust core plus thin Python bindings; those bindings have since been retired in favor of the Rust CLI/crate only. The [1.4.0](https://pypi.org/project/overture-stac/1.4.0/) API is not preserved. The old entry points map as follows:

- `gen-stac` is replaced by the `overture-stac` CLI, distributed through the Rust crate: `cargo install overture-stac`.
- The `OvertureRelease` class is replaced by the CLI's `build`, `validate`, and `list-releases` subcommands.

## Development

A [`justfile`](./justfile) collects the common commands. Install [just](https://github.com/casey/just) with `brew install just` and run `just` to see recipes. `just check` runs `cargo fmt --check`, `cargo clippy`, and `cargo test`, the same checks CI would run.
