import json
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pyarrow.dataset as ds
import pyarrow.fs as fs
import pystac
import stac_geoparquet

TYPE_LICENSE_MAP: dict[str, str] = {
    "bathymetry": "CC0-1.0",
    "land_cover": "	CC-BY-4.0",
    "infrastructure": "ODbL-1.0",
    "land": "ODbL-1.0",
    "land_use": "ODbL-1.0",
    "water": "ODbL-1.0",
    "building": "ODbL-1.0",
    "division": "ODbL-1.0",
    "division_area": "ODbL-1.0",
    "division_boundary": "ODbL-1.0",
    "segment": "ODbL-1.0",
    "connector": "ODbL-1.0",
    "place": "CDLA-Permissive-2.0, Apache 2.0, CC0 1.0.",
    "address": "Multiple Open Licenses",
}


def process_theme_worker(
    theme_path: str,
    release_path: str,
    s3_region: str,
    debug: bool,
    release_datetime: datetime,
    release: str,
    available_pmtiles: dict[str, str],
) -> tuple[pystac.Catalog, list[dict], dict[str, list[pystac.Item]], str]:
    """
    Worker function to process a single theme independently.

    This function runs in a separate process and returns all results
    instead of mutating shared state.

    Args:
        theme_path: Path to the theme directory
        release_path: S3 path to the release
        s3_region: AWS region
        debug: Debug mode flag
        release_datetime: Release datetime
        release: Release version string
        available_pmtiles: Dict of available PMTiles files for this release

    Returns:
        tuple: (theme_catalog, manifest_items, type_collections, theme_name)
    """
    logger = logging.getLogger("pystac")

    # Create a new filesystem connection for this process
    filesystem = fs.S3FileSystem(anonymous=True, region=s3_region)

    theme_name = theme_path.split("=")[-1]
    logger.info(f"Processing Theme: {theme_name}")

    theme_catalog = pystac.Catalog(
        id=theme_name, description=f"Overture's {theme_name} theme"
    )

    # Add PMTiles link if available for this theme
    if theme_name in available_pmtiles:
        logger.info(f"Adding PMTiles link for theme {theme_name}")
        theme_catalog.add_link(
            pystac.Link(
                rel="pmtiles",
                target=f"https://tiles.overturemaps.org/{release}/{theme_name}.pmtiles",
                media_type="application/vnd.pmtiles",
                title=f"PMTiles",
            )
        )

    # Get theme types
    theme_path_selector = fs.FileSelector(theme_path)
    theme_types = filesystem.get_file_info(theme_path_selector)

    # Local state for this theme
    local_manifest_items = []
    local_type_collections = {}

    for theme_type in theme_types:
        type_name = theme_type.path.split("=")[-1]
        logger.info(f"Opening Type: {type_name}")

        type_dataset = ds.dataset(
            theme_type.path, filesystem=filesystem, format="parquet"
        )

        local_type_collections[type_name] = []
        schema = None

        for fragment in (
            list(type_dataset.get_fragments())[:1]
            if debug
            else type_dataset.get_fragments()
        ):
            schema = fragment.metadata.schema.to_arrow_schema()

            # Create STAC item from fragment
            filename = fragment.path.split("/")[-1]
            rel_path = ("/").join(fragment.path.split("/")[1:])

            logger.info(
                f" [ {fragment.path.split('/')[-2]} : {'.' * len(local_manifest_items)} ]"
            )

            # Build bbox from metadata
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

            stac_item = pystac.Item(
                id=filename.split("-")[1],
                geometry=geojson_bbox_geometry,
                bbox=[xmin, ymin, xmax, ymax],
                properties={
                    "num_rows": fragment.count_rows(),
                    "num_row_groups": fragment.num_row_groups,
                    "storage:schemes": {
                        "aws": {
                            "type": "aws-s3",
                            "platform": "https://{bucket}.s3.{region}.amazonaws.com",
                            "bucket": "overturemaps-us-west-2",
                            "region": "us-west-2",
                            "requester_pays": "false",
                        },
                        "azure": {
                            "type": "ms-azure",
                            "platform": "https://{account}.blob.core.windows.net/",
                            "account": "overturemapswestus2",
                            "requester_pays": "false",
                        },
                    },
                },
                datetime=release_datetime,
            )

            local_manifest_items.append(
                {
                    "type": "Feature",
                    "properties": {
                        "ovt_type": type_name,
                        "rel_path": rel_path,
                    },
                    "geometry": geojson_bbox_geometry,
                    "bbox": [xmin, ymin, xmax, ymax],
                }
            )

            # Add assets
            stac_item.add_asset(
                key="aws",
                asset=pystac.Asset(
                    href=f"https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/{rel_path}",
                    media_type="application/vnd.apache.parquet",
                    extra_fields={
                        "storage:refs": ["aws"],
                        "alternate": {
                            "href": f"s3://{fragment.path}",
                            "storage:refs": ["aws"],
                            "name": "S3",
                        },
                    },
                ),
            )
            stac_item.add_asset(
                key="azure",
                asset=pystac.Asset(
                    href=f"https://overturemapswestus2.blob.core.windows.net/{rel_path}",
                    media_type="application/vnd.apache.parquet",
                    extra_fields={"storage:refs": ["azure"]},
                ),
            )

            local_type_collections[type_name].append(stac_item)

        # Create type collection
        type_collection = pystac.Collection(
            id=type_name,
            description=f"Overture's {type_name} collection",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(
                    bboxes=[i.bbox for i in local_type_collections[type_name]]
                ),
                temporal=pystac.TemporalExtent(intervals=[None, None]),
            ),
            license=TYPE_LICENSE_MAP.get(type_name),
        )

        type_collection.add_items(local_type_collections[type_name])

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

        if not debug:
            type_collection.extra_fields = {"features": type_dataset.count_rows()}

        theme_catalog.add_child(type_collection)

    return (theme_catalog, local_manifest_items, local_type_collections, theme_name)


class OvertureRelease:
    logging.basicConfig()
    logger = logging.getLogger("pystac")
    logger.setLevel(logging.INFO)

    def __init__(
        self,
        release: str,
        schema: str,
        output: Path,
        s3_release_path: str = "s3://overturemaps-us-west-2/release",
        s3_region: str = "us-west-2",
        debug: bool = False,
    ):
        self.debug = debug
        if self.debug:
            self.logger.setLevel(logging.DEBUG)

        self.release = release
        self.schema = schema
        self.release_path = f"{s3_release_path}/{self.release}"
        self.filesystem = fs.S3FileSystem(anonymous=True, region=s3_region)

        self.manifest_items = []
        self.type_collections = {}

        self.output = Path(output, self.release)
        self.output.mkdir(parents=True, exist_ok=True)

        self.release_datetime = datetime.strptime(release.split(".")[0], "%Y-%m-%d")

        # Discover available PMTiles for this release
        self.available_pmtiles = self._get_available_pmtiles()

    def _get_available_pmtiles(self) -> dict[str, str]:
        """
        Discover available PMTiles files for this release.

        Returns:
            dict: Mapping of base names (without .pmtiles) to full S3 paths
        """
        pmtiles_path: str = f"overturemaps-extras-us-west-2/tiles/{self.release}"
        available_pmtiles: dict[str, str] = {}

        try:
            pmtiles_selector = fs.FileSelector(pmtiles_path)
            pmtiles_files = self.filesystem.get_file_info(pmtiles_selector)

            for file_info in pmtiles_files:
                if file_info.path.endswith(".pmtiles"):
                    filename = file_info.path.split("/")[-1]
                    base_name = filename.replace(".pmtiles", "")
                    available_pmtiles[base_name] = file_info.path
                    self.logger.debug(f"Found PMTiles: {filename}")

            self.logger.info(
                f"Discovered {len(available_pmtiles)} PMTiles files for release {self.release}"
            )
        except Exception as e:
            self.logger.warning(
                f"Couldn't find PMTiles for release: {self.release}: {e}"
            )

        return available_pmtiles

    def make_release_catalog(self, title: Optional[str]) -> None:
        self.logger.info(
            f"Creating Release Catalog for {self.release} with schema {self.schema}"
        )

        self.release_catalog = pystac.Catalog(
            id=self.release,
            title=title if title is not None else self.release,
            description=f"Geoparquet data released in the Overture {self.release} release",
            stac_extensions=[
                "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
                "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
            ],
        )
        self.release_catalog.extra_fields = {
            "release:version": self.release,
            "schema:version": self.schema,
            "schema:tag": f"https://github.com/OvertureMaps/schema/releases/tag/v{self.schema}",
        }

    def get_release_themes(self) -> None:
        release_path_selector = fs.FileSelector(self.release_path.replace("s3://", ""))
        self.themes = self.filesystem.get_file_info(release_path_selector)

    def build_release_catalog(self, title: str, max_workers: int = 4) -> None:
        """
        Build release catalog using parallel processing.

        Args:
            title: Title for the release catalog
            max_workers: Number of parallel workers (default: 4)
        """
        self.make_release_catalog(title=title)
        self.get_release_themes()

        self.logger.info(f"Building catalog in parallel with {max_workers} workers...")

        # Prepare arguments for worker processes
        theme_paths = [theme.path for theme in self.themes]
        s3_region = "us-west-2"

        # Process themes in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_theme = {
                executor.submit(
                    process_theme_worker,
                    theme_path,
                    self.release_path,
                    s3_region,
                    self.debug,
                    self.release_datetime,
                    self.release,
                    self.available_pmtiles,
                ): theme_path
                for theme_path in theme_paths
            }

            for future in as_completed(future_to_theme):
                theme_path = future_to_theme[future]
                try:
                    (
                        theme_catalog,
                        manifest_items,
                        type_collections,
                        theme_name,
                    ) = future.result()

                    self.logger.info(f"Merging results for theme: {theme_name}")
                    self.release_catalog.add_child(
                        child=theme_catalog, title=theme_name
                    )
                    self.manifest_items.extend(manifest_items)
                    self.type_collections.update(type_collections)

                    theme_path_dir = Path(self.output, theme_name)
                    theme_path_dir.mkdir(parents=True, exist_ok=True)

                except Exception as exc:
                    self.logger.error(
                        f"Theme {theme_path} generated an exception: {exc}"
                    )
                    raise

        # Write outputs
        with open(f"{self.output}/manifest.geojson", "w") as f:
            json.dump({"type": "FeatureCollection", "features": self.manifest_items}, f)

        # Write GeoParquet Collections
        all_items = []
        for _ovt_type, items in self.type_collections.items():
            all_items += items

        stac_geoparquet.arrow.to_parquet(
            table=stac_geoparquet.arrow.parse_stac_items_to_arrow(all_items),
            output_path=f"{self.output}/collections.parquet",
        )
