# Overture STAC

[![CI](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml/badge.svg)](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generate STAC (SpatioTemporal Asset Catalog) catalogs for all public Overture Maps releases.

See it in action here:
<https://radiantearth.github.io/stac-browser/#/external/labs.overturemaps.org/stac/catalog.json?.language=en>

### Installing/Updating Dependencies

```bash
# Install package in editable mode with dev dependencies
uv pip install -e ".[dev]"

# Install just the package (no dev dependencies)
uv pip install -e .

# Update dependencies
uv pip install --upgrade -e ".[dev]"

# Add a new dependency (manually edit pyproject.toml, then):
uv pip install -e ".[dev]"
```

### Running the Application

```bash
# Run the STAC generator (parallel mode with 4 workers by default)
gen-stac --output ./public_releases

# Run in debug mode (generates only 1 item per collection)
gen-stac --output ./public_releases --debug

# Control parallelization
gen-stac --output ./public_releases --workers 8  # Use 8 parallel workers
gen-stac --output ./public_releases --no-parallel  # Disable parallelization

# Recommended for production (balance speed and resource usage)
gen-stac --output ./public_releases --workers 4
```

### Before Committing

```bash
# Run the full CI check locally
ruff format . && ruff check . && pytest
```
