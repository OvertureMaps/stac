#!/usr/bin/env just --justfile

set shell := ['bash', '-c']

# How to call the current just executable. Note that just_executable() may have `\` in Windows paths, so we need to quote it.
just := quote(just_executable())

# Default output path for generated catalogs (matches the CLI's --output default).
OUTPUT := 'public_releases'

# Local fixture catalog used by the e2e test (matches CI).
FIXTURE_DIR := 'tests/data'

@_default:
    {{just}} --list

# Install project and dev-group dependencies into the local .venv (mirrors CI's `uv sync --all-groups`).
sync:
    uv sync --all-groups

# Run the unit test suite.
test:
    uv run pytest

# Run only the end-to-end catalog test (needs a fixture + running server — see `serve`).
test-e2e:
    uv run pytest tests/test_e2e_stac_catalog.py

# Format all Python sources with ruff.
fmt:
    uv run ruff format .

# Check formatting without rewriting files (same command CI runs).
fmt-check:
    uv run ruff format --check .

# Run ruff lints.
lint:
    uv run ruff check .

# Auto-fix everything ruff can fix, then reformat.
fix:
    uv run ruff check --fix .
    uv run ruff format .

# Run every check CI runs (format, lint, unit tests). Stops on the first failure.
check: fmt-check lint test

# Generate a STAC catalog locally. Defaults to --debug mode (1 item per collection); pass mode=full for the full catalog. Builds the latest release by default (discovered from stac.overturemaps.org/catalog.json); pass release=YYYY-MM-DD.N to override.
run schema release='' mode='debug': sync
    #!/usr/bin/env bash
    set -euo pipefail
    release='{{release}}'
    if [[ -z "$release" ]]; then
      release=$(curl -sf https://stac.overturemaps.org/catalog.json | jq -r .latest)
      echo "Latest release: $release"
    fi
    debug_flag="--debug"
    if [[ "{{mode}}" == "full" ]]; then
      debug_flag=""
    fi
    uv run gen-stac $debug_flag --output {{OUTPUT}} --workers 6 --release "$release" --schema-version {{schema}}

# Build the small fixture catalog the e2e test consumes (writes to tests/data).
fixture:
    uv run python tests/setup_test_catalog.py --output {{FIXTURE_DIR}} --workers 2

# Serve the fixture catalog on http://127.0.0.1:8888 (Ctrl+C to stop).
serve:
    uv run python tests/setup_test_catalog.py --serve-only --output {{FIXTURE_DIR}}

# Remove all generated catalog outputs.
clean:
    rm -rf {{OUTPUT}} {{FIXTURE_DIR}}
