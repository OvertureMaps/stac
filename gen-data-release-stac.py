import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pystac
import stac_geoparquet


S3_RELEASE_PATH = "s3://overturemaps-us-west-2/release"
S3_BUCKET = "overturemaps-us-west-2"
S3_REGION = "us-west-2"

TYPE_LICENSE_MAP = {
    "bathymetry": "CC0-1.0",
    "land_cover": "	CC-BY-4.0",
    "infrastructure": "ODbL-1.0",
    "land": "ODbL-1.0",
    "land_use": "ODbL-1.0",
    "water": "ODbL-1.0",
    "building": "ODbL-1.0",
    "division": "ODbL-1.0",
    "division_area": "ODbL-1.0",
    "division_bopundary": "ODbL-1.0",
    "segment": "ODbL-1.0",
    "connector": "ODbL-1.0",
    "place": "CDLA-Permissive-2.0",
    "address": "Multiple Open Licenses",
}


class OvertureRelease:
    logging.basicConfig()
    logger = logging.getLogger("pystac")
    logger.setLevel(logging.DEBUG)

    def __init__(
        self,
        release: str,
        schema: str,
        parquet_output: Optional[str],
        manifest: Optional[str],
    ):
        self.release = release
        self.schema = schema
        self.release_path = (f"{S3_RELEASE_PATH}/{self.release}",)
        self.parquet_output = parquet_output
        self.filesystem = fs.S3FileSystem(anonymous=True, region=S3_REGION)
        self.manifest = f"{manifest}.geojson"

        self.release_datetime = datetime.strptime(release.split(".")[0], "%Y-%m-%d")

    def make_release_catalog(self):
        self.release_catalog = pystac.Catalog(
            id="release",
            href="./build",
            description=f"This catalog is for the geoparquet data released in version {self.release}",
            stac_extensions=[
                "https://stac-extensions.github.io/storage/v2.0.0/schema.json"
            ],
        )
        self.release_catalog.extra_fields = {
            "release:version": self.release,
            "schema:version": self.schema,
            "schema:tag": f"https://github.com/OvertureMaps/schema/releases/tag/v{self.schema}",
            "storage:schemes": {
                "aws": {
                    "type": "aws-s3",
                    "platform": "https://{bucket}.s3.{region}.amazonaws.com/release/{release_version}",
                    "release_version": self.release,
                    "bucket": "overturemaps-us-west-2",
                    "region": "us-west-2",
                    "requester_pays": "false",
                },
                "azure": {
                    "type": "ms-azure",
                    "platform": "https://{account}.blob.core.windows.net/release/{release_version}",
                    "account": "overturemapswestus2",
                    "requester_pays": "false",
                },
            },
        }

    def get_release_themes(self):
        release_path_selector = fs.FileSelector(f"{S3_BUCKET}/release/{self.release}")
        self.themes = self.filesystem.get_file_info(release_path_selector)

        if self.manifest is not None:
            self.manifest_items = []

    def create_stac_item_from_fragment(self, fragment, schema=None, type_name=None):

        if schema is None:
            schema = fragment.metadata.schema.to_arrow_schema()

        filename = fragment.path.split("/")[-1]
        rel_path = ("/").join(fragment.path.split("/")[1:])

        self.logger.info(f"Creating STAC item from: {filename}")

        # Build bbox from metadata:
        geo = json.loads(schema.metadata[b"geo"].decode("utf-8"))

        xmin, ymin, xmax, ymax = geo.get("columns").get("geometry").get("bbox")

        geojson_bbox_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [xmin, ymin],
                    [xmax, ymin],
                    [xmax, ymax],
                    [xmin, ymax],
                    [xmin, ymin],
                ]
            ],
        }

        filename = fragment.path.split("/")[-1]
        rel_path = ("/").join(fragment.path.split("/")[1:])

        stac_item = pystac.Item(
            id=filename.split("-")[1],
            geometry=geojson_bbox_geometry,
            bbox=[xmin, ymin, xmax, ymax],
            properties={
                "num_rows": fragment.count_rows(),
                "num_row_groups": fragment.num_row_groups,
            },
            datetime=self.release_datetime,
        )

        if self.manifest is not None:
            self.manifest_items.append(
                {
                    "type": "Feature",
                    "properties": {
                        "ovt_type": type_name,
                        "rel_path": rel_path,
                        # "num_rows": fragment.count_rows(),
                        # "num_row_groups": fragment.num_row_groups,
                    },
                    "geometry": geojson_bbox_geometry,
                }
            )

        # Add GeoParquet from s3
        stac_item.add_asset(
            key="aws-s3",
            asset=pystac.Asset(
                href=f"s3://{fragment.path}",
                media_type="application/vnd.apache.parquet",  # application/x-parquet ?
            ),
        )

        # Add s3 http link
        stac_item.add_asset(
            key="aws-https",
            asset=pystac.Asset(
                href=f"https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/{rel_path}",
                media_type="application/vnd.apache.parquet",  # application/x-parquet ?
            ),
        )

        # Add Azure https link
        stac_item.add_asset(
            key="azure-https",
            asset=pystac.Asset(
                href=f"https://overturemapswestus2.blob.core.windows.net/{rel_path}",
                media_type="application/vnd.apache.parquet",  # application/x-parquet ?
            ),
        )

        return stac_item

    def process_type(self, theme_type: fs.FileInfo):
        type_name = theme_type.path.split("=")[-1]
        self.logger.info(f"Opening Type: {type_name}")

        type_dataset = ds.dataset(
            theme_type.path, filesystem=self.filesystem, format="parquet"
        )

        items = []
        schema = None
        for fragment in type_dataset.get_fragments():

            schema = fragment.metadata.schema.to_arrow_schema()

            item = self.create_stac_item_from_fragment(
                fragment, schema, type_name=type_name
            )

            items.append(item)

        type_collection = pystac.Collection(
            id=type_name,
            description=f"Overture's {type_name} collection",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[i.bbox for i in items]),
                temporal=pystac.TemporalExtent(intervals=[None, None]),
            ),
            license=TYPE_LICENSE_MAP.get(type_name),
        )

        type_collection.add_items(items)

        if self.parquet_output is not None:
            output_path = Path(self.parquet_output)
            output_path.mkdir(parents=True, exist_ok=True)

            stac_geoparquet.arrow.to_parquet(
                table=stac_geoparquet.arrow.parse_stac_items_to_arrow(items),
                output_path=f"{output_path}/{type_name}.parquet",
            )

        type_collection.summaries = pystac.Summaries(
            {
                "schema": (
                    json.loads(schema.metadata[b"geo"]).get("version")
                    if schema is not None
                    else None
                ),
                "columns": schema.names if schema is not None else None,
            }
        )

        type_collection.extra_fields = {"features": type_dataset.count_rows()}

        return type_collection

    def add_theme_to_catalog(self, theme: fs.FileSelector):
        theme_name = theme.path.split("=")[-1]
        self.logger.info(f"Processing Theme: {theme_name}")
        theme_path_selector = fs.FileSelector(theme.path)
        theme_types = self.filesystem.get_file_info(theme_path_selector)

        theme_catalog = pystac.Catalog(
            id=theme_name, description=f"Overture's {theme_name} theme"
        )

        for theme_type in theme_types:
            type_name = theme_type.path.split("=")[-1]
            self.logger.info(f"Found Type: {type_name}")

            theme_catalog.add_child(self.process_type(theme_type))

        self.release_catalog.add_child(theme_catalog)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate a STAC Catalog for the latest Overture Maps Data."
    )

    parser.add_argument(
        "--release",
        type=str,
        required=True,
        help="The release version of the data.",
    )

    parser.add_argument(
        "--schema", type=str, required=True, help="The schema version to use."
    )

    parser.add_argument(
        "--output",
        type=str,
        required=False,
        default="stac",
        help="The schema version to use.",
    )

    parser.add_argument(
        "--parquet",
        type=str,
        required=False,
        help="The schema version to use.",
    )

    parser.add_argument(
        "--manifest",
        type=str,
        required=False,
        help="Name of GeoJSON manifest file for this release",
    )

    args = parser.parse_args()

    release = OvertureRelease(
        release=args.release,
        schema=args.schema,
        parquet_output=args.parquet,
        manifest=args.manifest,
    )

    release.make_release_catalog()

    release.get_release_themes()

    for theme in release.themes:
        release.add_theme_to_catalog(theme)

    release.release_catalog.normalize_and_save(
        root_href=args.output, catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    if release.manifest is not None:
        with open(f"{release.manifest}", "w") as f:
            json.dump(
                {"type": "FeatureCollection", "features": release.manifest_items}, f
            )
