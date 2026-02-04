#!/usr/bin/env python3
"""
Setup script for building test STAC catalogs.

This script builds a STAC catalog in debug mode using the same discovery
mechanism as the CLI. Run this before running integration tests.

Usage:
    python tests/setup_test_catalog.py
    python tests/setup_test_catalog.py --output tests/data
    python tests/setup_test_catalog.py --release 2025-01-22.0
    python tests/setup_test_catalog.py --serve  # Build and serve on localhost
"""

import argparse
import logging
import os
import sys
import threading
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pyarrow.fs as fs
import pystac

from overture_stac.overture_stac import OvertureRelease

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema version mapping (same as CLI)
SCHEMA_VERSION_MAPPING: dict[str, str] = {
    "2026-03-18.0": "TBD",
    "2026-02-18.0": "1.15.0",
    "2026-01-21.0": "1.15.0",
    "2025-12-17.0": "1.15.0",
}

DEFAULT_PORT = 8888


def discover_releases() -> list[str]:
    """
    Discover available releases from S3 using the same mechanism as the CLI.

    Returns:
        List of release names sorted by date (newest first)
    """
    logger.info("Discovering available releases from S3...")
    filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")
    available_releases = filesystem.get_file_info(
        fs.FileSelector("overturemaps-us-west-2/release")
    )

    releases = [info.path.split("/")[-1] for info in available_releases]
    releases.sort(reverse=True)

    logger.info(f"Found {len(releases)} releases: {releases[:5]}...")
    return releases


def get_latest_release() -> str:
    """Get the latest available release."""
    releases = discover_releases()
    if not releases:
        raise RuntimeError("No releases found in S3 bucket")
    return releases[0]


def build_test_catalog(
    output_dir: Path,
    release: str | None = None,
    workers: int = 2,
) -> Path:
    """
    Build a STAC catalog in debug mode for testing.

    Args:
        output_dir: Directory to output the catalog
        release: Specific release to build (None = latest)
        workers: Number of parallel workers

    Returns:
        Path to the built catalog directory
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Discover or use specified release
    if release is None:
        release = get_latest_release()
        logger.info(f"Using latest release: {release}")
    else:
        logger.info(f"Using specified release: {release}")

    schema = SCHEMA_VERSION_MAPPING.get(release, "unknown")
    logger.info(f"Schema version: {schema}")

    # Build the release catalog
    logger.info(f"Building catalog for {release} in debug mode...")

    overture_release = OvertureRelease(
        release=release,
        schema=schema,
        output=output_dir,
        debug=True,  # Only process first 2 fragments per type
    )

    title = f"Test Release {release}"
    overture_release.build_release_catalog(title=title, max_workers=workers)

    # Create root catalog to match CLI structure
    root_catalog = pystac.Catalog(
        id="Overture Releases",
        description="All Overture Releases (Test)",
    )

    child = root_catalog.add_child(
        child=overture_release.release_catalog,
        title=title,
    )
    child.extra_fields = {"latest": True}
    overture_release.release_catalog.extra_fields["latest"] = True
    root_catalog.extra_fields = {"latest": release}

    # # Add registry manifest
    # try:
    #     registry_manifest = RegistryManifest()
    #     root_catalog.extra_fields["registry"] = {
    #         "path": "s3://overturemaps-us-west-2/registry",
    #         "manifest": registry_manifest.create_manifest(),
    #     }
    # except Exception as e:
    #     logger.warning(f"Could not create registry manifest: {e}")

    # Normalize and save
    logger.info(f"Saving catalog to {output_dir}...")
    root_catalog.normalize_and_save(
        root_href=str(output_dir),
        catalog_type=pystac.CatalogType.SELF_CONTAINED,
    )

    catalog_path = output_dir / release
    logger.info(f"Catalog built successfully at {catalog_path}")

    return catalog_path


class CORSRequestHandler(SimpleHTTPRequestHandler):
    """HTTP request handler with CORS support for STAC browser compatibility."""

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        logger.debug(f"HTTP: {args[0]}")


def serve_catalog(directory: Path, port: int = DEFAULT_PORT) -> HTTPServer:
    """
    Start an HTTP server to serve the catalog directory.

    Args:
        directory: Directory to serve
        port: Port to serve on

    Returns:
        HTTPServer instance
    """
    os.chdir(directory)
    handler = partial(CORSRequestHandler, directory=directory)
    server = HTTPServer(("localhost", port), handler)
    return server


def run_server_blocking(directory: Path, port: int = DEFAULT_PORT):
    """Run the HTTP server in blocking mode (for CLI use)."""
    server = serve_catalog(directory, port)
    print(f"Serving catalog at http://localhost:{port}")
    print(f"Root catalog: http://localhost:{port}/catalog.json")
    print("Press Ctrl+C to stop...")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server.shutdown()
        server.server_close()
        print("Server stopped.")


def start_server_background(directory: Path, port: int = DEFAULT_PORT) -> HTTPServer:
    """
    Start the HTTP server in a background thread.

    Args:
        directory: Directory to serve
        port: Port to serve on

    Returns:
        HTTPServer instance (call shutdown() to stop)
    """
    server = serve_catalog(directory, port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Server started at http://localhost:{port}")
    return server


def main():
    """Main entry point for the setup script."""
    parser = argparse.ArgumentParser(
        description="Build a test STAC catalog in debug mode"
    )

    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).parent / "data"),
        help="Output directory for the test catalog (default: tests/data)",
    )

    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="Specific release to build (default: latest)",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2)",
    )

    parser.add_argument(
        "--list-releases",
        action="store_true",
        help="List available releases and exit",
    )

    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve the catalog on localhost after building",
    )

    parser.add_argument(
        "--serve-only",
        action="store_true",
        help="Only serve an existing catalog (don't build)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port for HTTP server (default: {DEFAULT_PORT})",
    )

    args = parser.parse_args()

    if args.list_releases:
        releases = discover_releases()
        print("Available releases:")
        for release in releases:
            schema = SCHEMA_VERSION_MAPPING.get(release, "unknown")
            print(f"  {release} (schema: {schema})")
        return

    output_dir = Path(args.output)

    if args.serve_only:
        if not output_dir.exists():
            print(f"Error: Output directory {output_dir} does not exist.")
            print("Run without --serve-only to build the catalog first.")
            sys.exit(1)
        run_server_blocking(output_dir, args.port)
        return

    # Build the catalog
    catalog_path = build_test_catalog(
        output_dir=output_dir,
        release=args.release,
        workers=args.workers,
    )

    print("\n✓ Test catalog built successfully!")
    print(f"  Location: {catalog_path}")

    if args.serve:
        print("\nStarting HTTP server...")
        run_server_blocking(output_dir, args.port)
    else:
        print("\nTo serve the catalog:")
        print("  python tests/setup_test_catalog.py --serve-only")
        print("\nRun integration tests with:")
        print("  pytest tests/test_e2e_stac_catalog.py -m integration")


if __name__ == "__main__":
    main()
