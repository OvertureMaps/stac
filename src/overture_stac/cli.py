"""Command-line interface for generating STAC catalogs."""

import argparse
import re
from pathlib import Path

import pyarrow.fs as fs
import pystac

from overture_stac.overture_stac import OvertureRelease
from overture_stac.registry_manifest import RegistryManifest

PROD_ROOT_HREF = "https://stac.overturemaps.org"


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
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )

    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="Release version to generate STAC for (e.g. 2026-05-20.0). When omitted, all releases are processed.",
    )

    parser.add_argument(
        "--schema-version",
        type=str,
        default=None,
        help="Schema version for the release (e.g. 1.17.0). Required when --release is provided.",
    )

    parser.add_argument(
        "--root-href",
        type=str,
        default=PROD_ROOT_HREF,
        help=(
            "Public root URL the catalog will be hosted at, used to build absolute "
            f"'self' links (default: {PROD_ROOT_HREF}). Override for staging/testing, "
            "e.g. https://staging.overturemaps.org/stac/pr/123."
        ),
    )

    args = parser.parse_args()

    # Normalize to no trailing slash so downstream f"{root_href}/..." hrefs
    # always join with exactly one slash, regardless of user input.
    root_href = args.root_href.rstrip("/")

    if args.release and not args.schema_version:
        parser.error("--schema-version is required when --release is provided")

    if args.release and not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d+", args.release):
        parser.error("--release must be in format YYYY-MM-DD.N (e.g. 2026-05-20.0)")

    if args.schema_version and not re.fullmatch(r"\d+\.\d+\.\d+", args.schema_version):
        parser.error("--schema-version must be in format X.Y.Z (e.g. 1.17.0)")

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    if args.release:
        this_release = OvertureRelease(
            release=args.release,
            schema=args.schema_version,
            output=output,
            debug=args.debug,
        )
        title = f"{args.release} Overture Release"
        this_release.build_release_catalog(title=title, max_workers=args.workers)
        this_release.release_catalog.normalize_hrefs(f"{root_href}/{args.release}/")
        this_release.release_catalog.save(
            catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
            dest_href=str(output / args.release),
        )
        return

    filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")
    available_releases = filesystem.get_file_info(
        fs.FileSelector("overturemaps-us-west-2/release")
    )

    overture_releases_catalog = pystac.Catalog(
        id="Overture Releases",
        title="Overture Releases",
        description="All Overture Releases",
    )

    for idx, release_info in enumerate(
        sorted(available_releases, key=lambda p: p.path, reverse=True)
    ):
        release = release_info.path.split("/")[-1]

        title: str = (
            f"{release} Overture Release" if idx > 0 else "Latest Overture Release"
        )

        this_release = OvertureRelease(
            release=release,
            schema=None,
            output=output,
            debug=args.debug,
        )

        this_release.build_release_catalog(title=title, max_workers=args.workers)

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

    overture_releases_catalog.normalize_hrefs(f"{root_href}/")
    overture_releases_catalog.save(
        catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED, dest_href=str(output)
    )


if __name__ == "__main__":
    main()
