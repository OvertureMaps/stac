import argparse
import pyarrow.fs as fs
import yaml
import pystac
from pathlib import Path

from util.overture_stac import OvertureRelease

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate a STAC Catalog for Overture Maps Data from a Public S3 Bucket."
    )

    parser.add_argument(
        "--releases_path",
        type=str,
        default="s3://overturemaps-us-west-2/release",
        help="The release version of the data.",
    )

    parser.add_argument(
        "--s3_region",
        type=str,
        default="us-west-2",
        help="The release version of the data.",
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
        help="Path to the Schema Version <> Release mapping yaml file."
    )
    
    args = parser.parse_args()

    filesystem = fs.S3FileSystem(anonymous=True, region=args.s3_region)
    public_releases_selector = fs.FileSelector(args.releases_path.replace("s3://", ""))
    public_releases = filesystem.get_file_info(public_releases_selector)

    schema_version_mapping = dict()

    for _ in yaml.safe_load(open(args.schema_versions, 'r')):
        schema_version_mapping[_.get('release')] = _.get('schema')

    overture_releases_catalog = pystac.Catalog(
        id="Overture Releases",
        description=f"All Overture Releases",
    )

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    # Just GA releases
    public_releases = [r for r in public_releases if 'alpha' not in r.path and 'beta' not in r.path]

    # Only use the latest 5 releases
    for idx, release_info in enumerate(list(reversed(sorted(public_releases, key=lambda p: p.path)))):
        release = release_info.path.split("/")[-1]
        print(release)

        this_release = OvertureRelease(
            release=release,
            schema=schema_version_mapping.get(release),
            output=output,
        )

        this_release.build_release_catalog()

        child = overture_releases_catalog.add_child(
            child=this_release.release_catalog,
            title='release'
        )

        if idx==0:
            child.extra_fields = {'latest':True}

    overture_releases_catalog.normalize_and_save(
        root_href=str(output),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
