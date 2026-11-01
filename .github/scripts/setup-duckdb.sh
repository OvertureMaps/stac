#!/usr/bin/env bash
# Install a pinned DuckDB CLI (linux-amd64) to /usr/local/bin.
# Shared by .github/actions/setup-duckdb and the rust-workflows build-test
# lane (setup_script), which can't run local composite actions.
set -euo pipefail

DUCKDB_VERSION="${DUCKDB_VERSION:-v1.4.5}"
DUCKDB_SHA256="${DUCKDB_SHA256:-ff4ef9ec59fe3e1a1f3dd1004c6218d1fd59c0533c185c968c4403fd0240d02b}"

dest="${RUNNER_TEMP:-$(mktemp -d)}/duckdb"
mkdir -p "$dest"
curl -fsSL --retry 3 -o "$dest/duckdb.zip" \
  "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip"
echo "${DUCKDB_SHA256}  $dest/duckdb.zip" | sha256sum -c -
unzip -q -o "$dest/duckdb.zip" -d "$dest"
sudo install -m 0755 "$dest/duckdb" /usr/local/bin/duckdb
