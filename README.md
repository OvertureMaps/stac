# Overture STAC

Generate STAC (SpatioTemporal Asset Catalog) catalogs for all public Overture Maps releases.

See it in action here:
<https://radiantearth.github.io/stac-browser/#/external/labs.overturemaps.org/stac/catalog.json?.language=en>

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management. If you don't have it installed:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install the package

```bash
# Install in development mode with all dev dependencies
uv pip install -e ".[dev]"

# Or install just the package
uv pip install -e .
```

The structure looks like this:

```
- catalog.json
- <RELEASE>/
  | - catalog.json
  | - <THEME>/
      | - catalog.json
      | - <TYPE>/
          | - collection.json
          | - 00001/
              | - 00001.json
          | - 00002/
              | - 00002.json
```

The top-level `catalog.json` intends to be a catalog of all publicly available Overture releases. Briefly, it looks like this:

```json
{
  "type": "Catalog",
  "id": "Overture Releases",
  "stac_version": "1.1.0",
  "description": "All Overture Releases",
  "links": [
    {
      "rel": "child",
      "href": "./2025-07-23.0/catalog.json",
      "type": "application/json",
      "title": "Latest Overture Release",
      "latest": true
    },
    {
      "rel": "child",
      "href": "./2025-06-25.0/catalog.json",
      "type": "application/json",
      "title": "2025-06-25.0 Overture Release"
    }
  ],
  "latest": "2025-07-23.0"
}
```

The top level catalog points to the `latest` Overture release, and this release also has the tag `latest:true`.

## Additional Files

At the root of each release, there are two additional files: `manifest.geojson` and `collections.parquet`

```
- <RELEASE>/
  | - manifest.geojson
  | - collections.parquet
```

#### `manifest.geojson`: Basic GeoJSON Manifest

To support the download functionality of `explore.overturemaps.org`, a basic `manifest.geojson` summary of the distribution is available at the root of a release.

#### `collections.parquet`: STAC GeoParquet

An Overture release is composed of nearly 500 individual parquet files, and therefore the STAC index is composed of nearly 500 individual `json` files. This single `collections.parquet` is created by the [`stac-geoparquet`](https://github.com/stac-utils/stac-geoparquet) utility.
To support the download functionality of `explore.overturemaps.org`, a basic `manifest.geojson` summary of the distribution is available at the root of a release.

#### `collections.parquet`: STAC GeoParquet

An Overture release is composed of nearly 500 individual parquet files, and therefore the STAC index is composed of nearly 500 individual `json` files. This single `collections.parquet` is created by the [`stac-geoparquet`](https://github.com/stac-utils/stac-geoparquet) utility.

## Usage

### Command Line

After installation, you can use the `gen-stac` command:

```bash
# Generate STAC catalogs for all releases
gen-stac --output ./public_releases

# Run in debug mode (generates only 1 item per collection)
gen-stac --output ./public_releases --debug
```

### Python API

You can also use the package programmatically:

```python
from pathlib import Path
from overture_stac import OvertureRelease, RegistryManifest

# Generate STAC catalog for a specific release
release = OvertureRelease(
    release="2025-07-23.0",
    schema="1.11.0",
    output=Path("./output"),
)
release.build_release_catalog(title="My Release")

# Create registry manifest
registry = RegistryManifest()
manifest_data = registry.create_manifest()
print(f"Found {len(manifest_data)} files in registry")
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=overture_stac --cov-report=html

# Run specific test file
pytest tests/test_registry_manifest.py
```

### Code Quality

```bash
# Format code with ruff
ruff format .

# Lint code
ruff check .

# Fix linting issues automatically
ruff check --fix .
```

## Project Structure

```
stac/
├── src/overture_stac/       # Main package
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # Command-line interface
│   ├── overture_stac.py     # Main STAC generation logic
│   └── registry_manifest.py # Registry manifest generation
├── tests/                   # Test suite
│   ├── test_overture_stac.py
│   └── test_registry_manifest.py
├── pyproject.toml           # Project configuration
└── README.md
```
