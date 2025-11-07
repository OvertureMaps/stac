# Overture STAC

[![CI](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml/badge.svg)](https://github.com/OvertureMaps/stac/actions/workflows/ci.yaml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generate STAC (SpatioTemporal Asset Catalog) catalogs for all public Overture Maps releases.

See it in action here:
<https://radiantearth.github.io/stac-browser/#/external/labs.overturemaps.org/stac/catalog.json?.language=en>

## Quick Start for Development

### 1. Install UV (if you don't have it)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clone and Navigate to the Project

```bash
cd /Users/jenningsa/Overture/stac
```

### 3. Create a Virtual Environment and Install

```bash
# Create a virtual environment (uv will manage it)
uv venv

# Activate it
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows

# Install the package in editable mode with dev dependencies
uv pip install -e ".[dev]"
```

### 4. Verify Installation

```bash
# Test that the CLI works
gen-stac --help

# Test imports
python -c "from overture_stac import OvertureRelease, RegistryManifest; print('✓ Package installed successfully')"
```

## Common Development Commands

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
# Run the STAC generator
gen-stac --output ./public_releases

# Run in debug mode (generates only 1 item per collection)
gen-stac --output ./public_releases --debug
```

### Testing

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_registry_manifest.py

# Run integration tests (connects to real S3 - may be slow)
pytest -v -m integration

# Run ONLY the integration test
pytest -v -s tests/test_registry_manifest.py::test_create_registry_manifest_integration

# Skip integration/slow tests
pytest -v -m "not integration"
```

### Code Quality

```bash
# Format all code
ruff format .

# Check formatting (without changing files)
ruff format --check .

# Lint code
ruff check .

# Auto-fix linting issues
ruff check --fix .

# Run both format check and lint
ruff format --check . && ruff check .
```

### Before Committing

```bash
# Run the full CI check locally
ruff format . && ruff check . && pytest
```
