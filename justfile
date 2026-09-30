#!/usr/bin/env just --justfile

set shell := ['bash', '-c']

just := quote(just_executable())

# Default output path for generated catalogs (matches the CLI's --output default).
OUTPUT := 'public_releases'

BIN := './target/release/overture-stac'

@_default:
    {{just}} --list

# Build the CLI in release mode.
build:
    cargo build --release

# Install the CLI into ~/.cargo/bin (available on PATH globally).
install:
    cargo install --path . --force

# Run the unit test suite.
test:
    cargo test

# Format all Rust sources.
fmt:
    cargo fmt

# Check formatting without rewriting files (same command CI runs).
fmt-check:
    cargo fmt --check

# Run clippy lints (informational — not gated).
lint:
    cargo clippy --all-targets

# Run the OMF security audit on workflow files (same persona as CI).
zizmor:
    zizmor --persona pedantic .github/workflows/

# Run format check and tests. Stops on the first failure.
check: fmt-check test

# Generate a STAC catalog locally. Defaults to --debug mode (a few fragments per type);
# pass mode=full for the full catalog. Builds the latest release by default (discovered
# from stac.overturemaps.org/catalog.json); pass release=YYYY-MM-DD.N to override.
run schema release='' mode='debug': build
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
    {{BIN}} build $debug_flag --output {{OUTPUT}} --concurrency 6 --release-version "$release" --schema-version {{schema}}

# Remove all generated catalog outputs.
clean:
    rm -rf {{OUTPUT}} target
