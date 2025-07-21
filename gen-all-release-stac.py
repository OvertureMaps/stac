import argparse
from pathlib import Path

import pyarrow.fs as fs
import pystac
import yaml

from util.overture_stac import OvertureRelease

if __name__ == "__main__":

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
        "--schema-versions",
        type=str,
        default="overture_releases.yaml",
        help="Path to the Schema Version <> Release mapping yaml file.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Debug flag to only generate 1 item per collection",
    )

    args = parser.parse_args()

    filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")
    public_releases = filesystem.get_file_info(
        fs.FileSelector("overturemaps-us-west-2/release")
    )

    schema_version_mapping = dict()

    # From https://labs.overturemaps.org/data/overture_releases.yaml — TODO: Where should this live / is it a CDP variable?
    for _ in yaml.safe_load(
        """
- schema: "1.10.0"
  release: "2025-06-25.0"
- schema: "1.9.0"
  release: "2025-05-21.0"
- schema: "1.8.0"
  release: "2025-04-23.0"
- schema: "1.7.0"
  release: "2025-03-19.1"
- schema: "1.7.0"
  release: "2025-03-19.0"
- schema: "1.6.0"
  release: "2025-02-19.0"
- schema: "1.5.0"
  release: "2025-01-22.0" 
"""
    ):
        schema_version_mapping[_.get("release")] = _.get("schema")

    overture_releases_catalog = pystac.Catalog(
        id="Overture Releases",
        description=f"All Overture Releases",
    )

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    # Just GA releases
    public_releases = [
        r for r in public_releases if "alpha" not in r.path and "beta" not in r.path
    ]

    # In the future, the bucket will be pruned of old releases and we'll iterate over the entire bucket contents
    # How many releases to go back?
    limit = 3 if args.debug else 2

    for idx, release_info in enumerate(
        list(reversed(sorted(public_releases, key=lambda p: p.path)))[:limit]
    ):
        release = release_info.path.split("/")[-1]
        print(release)

        title = f"{release} Overture Release" if idx > 0 else "Latest Overture Release"

        this_release = OvertureRelease(
            release=release,
            schema=schema_version_mapping.get(release),
            output=output,
            debug=args.debug,
        )

        this_release.build_release_catalog(title=title)

        child = overture_releases_catalog.add_child(
            child=this_release.release_catalog, title=title
        )

        if idx == 0:
            child.extra_fields = {"latest": True}
            this_release.release_catalog.extra_fields["latest"] = True
            overture_releases_catalog.extra_fields = {"latest": release}

    overture_releases_catalog.normalize_and_save(
        root_href=str(output), catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
