"""Command-line interface for generating STAC catalogs."""

import argparse
from pathlib import Path

import pyarrow.fs as fs
import pystac

from overture_stac.overture_stac import OvertureRelease
from overture_stac.registry_manifest import RegistryManifest


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Generate a STAC Index for Overture Maps Data from the public release bucket."
    )

    parser.add_argument(
        "--output",
        type=str,
        default="public_releases",
        help="Output path for Catalog",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Debug flag to only generate 1 item per collection",
    )

    parser.add_argument(
        "--no-parallel",
        dest="parallel",
        action="store_false",
        help="Disable parallel processing (default: parallel enabled)",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )

    args = parser.parse_args()

    filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")
    public_releases = filesystem.get_file_info(
        fs.FileSelector("overturemaps-us-west-2/release")
    )

    # TODO: These should be stored elsewhere, but for now we'll hardcode them here
    schema_version_mapping = {
        "2026-01-21.0": "1.15.0",
        "2025-12-17.0": "1.15.0",
        "2025-11-19.0": "1.14.0",
    }

    overture_releases_catalog = pystac.Catalog(
        id="Overture Releases",
        description="All Overture Releases",
    )

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    for idx, release_info in enumerate(
        sorted(public_releases, key=lambda p: p.path, reverse=True)
    ):
        release = release_info.path.split("/")[-1]

        title = f"{release} Overture Release" if idx > 0 else "Latest Overture Release"

        this_release = OvertureRelease(
            release=release,
            schema=schema_version_mapping.get(release),
            output=output,
            debug=args.debug,
        )

        this_release.build_release_catalog(
            title=title, parallel=args.parallel, max_workers=args.workers
        )

        child = overture_releases_catalog.add_child(
            child=this_release.release_catalog, title=title
        )

        if idx == 0:
            child.extra_fields = {"latest": True}
            this_release.release_catalog.extra_fields["latest"] = True
            overture_releases_catalog.extra_fields = {"latest": release}

    registry_manifest = RegistryManifest()
    overture_releases_catalog.extra_fields["registry"] = {
        "path": "s3://overturemaps-us-west-2/registry",
        "manifest": registry_manifest.create_manifest(),
    }

    overture_releases_catalog.normalize_and_save(
        root_href=str(output), catalog_type=pystac.CatalogType.SELF_CONTAINED
    )


if __name__ == "__main__":
    main()
