# Overture STAC

[![CI](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml/badge.svg)](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generate STAC catalogs for all public Overture Maps releases.

**[Browse the catalog](https://radiantearth.github.io/stac-browser/#/external/labs.overturemaps.org/stac/catalog.json?.language=en)**

## Setup

```bash
uv sync
```

## Usage

```bash
gen-stac --output ./releases

# Debug mode (2 items per collection)
gen-stac --output ./releases --debug

# Custom worker count (default: 4)
gen-stac --output ./releases --workers 8
```

## Development

```bash
uv run ruff format . && uv run ruff check . && uv run pytest
```
