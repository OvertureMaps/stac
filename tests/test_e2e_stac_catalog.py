"""End-to-end integration tests for STAC catalog validation.

These tests validate a pre-built STAC catalog served via HTTP using pystac.
Run the setup script first to build the test catalog:

    python tests/setup_test_catalog.py

Then run the tests (server will start automatically):

    pytest tests/test_e2e_stac_catalog.py -m integration
"""

import json
import socket
import time
from pathlib import Path

import pystac
import pytest

from tests.setup_test_catalog import start_server_background

DEFAULT_PORT = 8888


def get_test_data_dir() -> Path:
    """Get the test data directory."""
    return Path(__file__).parent / "data"


def find_release_name() -> str | None:
    """
    Find a release directory name in the test data directory.

    Returns:
        Release name (e.g., '2025-01-22.0'), or None if not found
    """
    data_dir = get_test_data_dir()
    if not data_dir.exists():
        return None

    for item in data_dir.iterdir():
        if item.is_dir() and item.name[0].isdigit():
            catalog_path = item / "catalog.json"
            if catalog_path.exists():
                return item.name

    return None


def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def get_all_items_from_catalog(catalog: pystac.Catalog) -> list[pystac.Item]:
    """
    Recursively collect all items from a catalog.

    This replaces the deprecated get_all_items() method.
    """
    items = []
    for child in catalog.get_children():
        if isinstance(child, pystac.Collection):
            items.extend(child.get_items())
        elif isinstance(child, pystac.Catalog):
            items.extend(get_all_items_from_catalog(child))
    return items


def get_all_collections_from_catalog(
    catalog: pystac.Catalog,
) -> list[pystac.Collection]:
    """
    Recursively collect all collections from a catalog.

    This replaces the deprecated get_all_collections() method.
    """
    collections = []
    for child in catalog.get_children():
        if isinstance(child, pystac.Collection):
            collections.append(child)
        elif isinstance(child, pystac.Catalog):
            collections.extend(get_all_collections_from_catalog(child))
    return collections


@pytest.fixture(scope="module")
def catalog_server():
    """
    Start an HTTP server to serve the test catalog.

    Yields the base URL for the catalog.
    """
    data_dir = get_test_data_dir()

    if not data_dir.exists() or not (data_dir / "catalog.json").exists():
        pytest.skip(
            "Test catalog not found. Run 'python tests/setup_test_catalog.py' first."
        )

    port = DEFAULT_PORT

    # Check if server is already running
    if is_port_in_use(port):
        # Assume it's our server already running
        yield f"http://localhost:{port}"
        return

    # Start the server
    server = start_server_background(data_dir, port)
    time.sleep(0.5)  # Give server time to start

    try:
        yield f"http://localhost:{port}"
    finally:
        # Properly shutdown and close the server
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="module")
def root_catalog_url(catalog_server) -> str:
    """Get the URL for the root catalog."""
    return f"{catalog_server}/catalog.json"


@pytest.fixture(scope="module")
def release_catalog_url(catalog_server) -> str:
    """Get the URL for a release catalog."""
    release_name = find_release_name()
    if release_name is None:
        pytest.skip("No release catalog found in test data.")
    return f"{catalog_server}/{release_name}/catalog.json"


@pytest.fixture(scope="module")
def release_name() -> str:
    """Get the release name from the test data directory."""
    name = find_release_name()
    if name is None:
        pytest.skip("No release found in test data.")
    return name


@pytest.fixture(scope="module")
def root_catalog(root_catalog_url) -> pystac.Catalog:
    """Load the root catalog from the HTTP server."""
    return pystac.Catalog.from_file(root_catalog_url)


@pytest.fixture(scope="module")
def release_catalog(release_catalog_url) -> pystac.Catalog:
    """Load a release catalog from the HTTP server."""
    return pystac.Catalog.from_file(release_catalog_url)


class TestServerSetup:
    """Tests that verify the test server is running correctly."""

    @pytest.mark.integration
    def test_server_is_running(self, catalog_server):
        """Test that the HTTP server is running."""
        import urllib.request

        url = f"{catalog_server}/catalog.json"
        response = urllib.request.urlopen(url)
        assert response.status == 200

    @pytest.mark.integration
    def test_catalog_json_is_valid(self, catalog_server):
        """Test that catalog.json is valid JSON."""
        import urllib.request

        url = f"{catalog_server}/catalog.json"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode())
        assert "type" in data
        assert data["type"] == "Catalog"


class TestRootCatalogValidation:
    """Tests for validating the root catalog structure via HTTP."""

    @pytest.mark.integration
    def test_root_catalog_readable(self, root_catalog):
        """Test that pystac can read the root catalog from HTTP."""
        assert root_catalog is not None
        assert root_catalog.id is not None
        assert root_catalog.description is not None

    @pytest.mark.integration
    def test_root_catalog_has_children(self, root_catalog):
        """Test that the root catalog has child releases."""
        children = list(root_catalog.get_children())
        assert len(children) > 0, "Root catalog should have release children"

    @pytest.mark.integration
    def test_root_catalog_has_latest_field(self, root_catalog):
        """Test that the root catalog has a 'latest' field."""
        assert "latest" in root_catalog.extra_fields


class TestReleaseCatalogValidation:
    """Tests for validating release catalog with pystac via HTTP."""

    @pytest.mark.integration
    def test_release_catalog_readable(self, release_catalog):
        """Test that pystac can read the release catalog from HTTP."""
        assert release_catalog is not None
        assert release_catalog.id is not None
        assert release_catalog.description is not None

    @pytest.mark.integration
    def test_release_catalog_has_themes(self, release_catalog):
        """Test that the release catalog has theme children."""
        children = list(release_catalog.get_children())
        assert len(children) > 0, "Release catalog should have theme children"

    @pytest.mark.integration
    def test_release_catalog_has_collections(self, release_catalog):
        """Test that the release catalog contains collections."""
        collections = get_all_collections_from_catalog(release_catalog)
        assert len(collections) > 0, "Release catalog should have collections"

    @pytest.mark.integration
    def test_release_catalog_has_items(self, release_catalog):
        """Test that the release catalog contains items."""
        items = get_all_items_from_catalog(release_catalog)
        assert len(items) > 0, "Release catalog should have items"

    @pytest.mark.integration
    def test_release_catalog_extra_fields(self, release_catalog):
        """Test that the release catalog has expected extra fields."""
        assert "release:version" in release_catalog.extra_fields
        assert "schema:version" in release_catalog.extra_fields
        assert "schema:tag" in release_catalog.extra_fields

    @pytest.mark.integration
    def test_release_catalog_stac_extensions(self, release_catalog):
        """Test that the release catalog declares STAC extensions."""
        assert release_catalog.stac_extensions is not None
        assert len(release_catalog.stac_extensions) > 0

        extension_urls = release_catalog.stac_extensions
        assert any("storage" in ext for ext in extension_urls)
        assert any("alternate" in ext for ext in extension_urls)


class TestCollectionValidation:
    """Tests for validating STAC collections via HTTP."""

    @pytest.mark.integration
    def test_collections_have_valid_extents(self, release_catalog):
        """Test that collections have valid spatial and temporal extents."""
        for collection in get_all_collections_from_catalog(release_catalog):
            assert collection.extent is not None, (
                f"Collection {collection.id} should have extent"
            )
            assert collection.extent.spatial is not None
            assert collection.extent.temporal is not None

            # Spatial extent should have bboxes
            assert len(collection.extent.spatial.bboxes) > 0, (
                f"Collection {collection.id} should have spatial bboxes"
            )

            # Temporal extent should have intervals
            assert len(collection.extent.temporal.intervals) > 0, (
                f"Collection {collection.id} should have temporal intervals"
            )

    @pytest.mark.integration
    def test_collections_have_licenses(self, release_catalog):
        """Test that collections have licenses where expected."""
        for collection in get_all_collections_from_catalog(release_catalog):
            if collection.license is not None:
                assert isinstance(collection.license, str)

    @pytest.mark.integration
    def test_collections_have_summaries(self, release_catalog):
        """Test that collections have summaries."""
        for collection in get_all_collections_from_catalog(release_catalog):
            assert collection.summaries is not None, (
                f"Collection {collection.id} should have summaries"
            )


class TestItemValidation:
    """Tests for validating STAC items via HTTP."""

    @pytest.mark.integration
    def test_items_have_valid_geometry(self, release_catalog):
        """Test that items have valid geometry and bbox."""
        for item in get_all_items_from_catalog(release_catalog):
            assert item.geometry is not None, f"Item {item.id} should have geometry"
            assert item.geometry["type"] == "Polygon"
            assert "coordinates" in item.geometry

            assert item.bbox is not None, f"Item {item.id} should have bbox"
            assert len(item.bbox) == 4

    @pytest.mark.integration
    def test_items_have_datetime(self, release_catalog):
        """Test that items have datetime."""
        for item in get_all_items_from_catalog(release_catalog):
            assert item.datetime is not None, f"Item {item.id} should have datetime"

    @pytest.mark.integration
    def test_items_have_assets(self, release_catalog):
        """Test that items have assets."""
        for item in get_all_items_from_catalog(release_catalog):
            assert len(item.assets) > 0, f"Item {item.id} should have assets"
            assert "aws" in item.assets or "azure" in item.assets, (
                f"Item {item.id} should have aws or azure asset"
            )

    @pytest.mark.integration
    def test_items_have_required_properties(self, release_catalog):
        """Test that items have required custom properties."""
        for item in get_all_items_from_catalog(release_catalog):
            assert "num_rows" in item.properties, (
                f"Item {item.id} should have num_rows property"
            )
            assert "num_row_groups" in item.properties, (
                f"Item {item.id} should have num_row_groups property"
            )

    @pytest.mark.integration
    def test_items_have_storage_schemes(self, release_catalog):
        """Test that items have storage:schemes property."""
        for item in get_all_items_from_catalog(release_catalog):
            assert "storage:schemes" in item.properties, (
                f"Item {item.id} should have storage:schemes property"
            )
            schemes = item.properties["storage:schemes"]
            assert "aws" in schemes or "azure" in schemes


class TestCatalogWalk:
    """Tests for walking through the catalog structure via HTTP."""

    @pytest.mark.integration
    def test_walk_catalog_hierarchy(self, release_catalog):
        """Test walking through the entire catalog hierarchy."""
        theme_count = 0
        collection_count = 0
        item_count = 0

        for theme_catalog in release_catalog.get_children():
            theme_count += 1

            for collection in theme_catalog.get_children():
                if isinstance(collection, pystac.Collection):
                    collection_count += 1

                    for _item in collection.get_items():
                        item_count += 1

        assert theme_count > 0, "Should have at least one theme"
        assert collection_count > 0, "Should have at least one collection"
        assert item_count > 0, "Should have at least one item"

        print(
            f"\nCatalog structure: {theme_count} themes, "
            f"{collection_count} collections, {item_count} items"
        )

    @pytest.mark.integration
    def test_all_links_resolve(self, release_catalog):
        """Test that all catalog links can be resolved via HTTP."""
        for child in release_catalog.get_children():
            assert child is not None
            for grandchild in child.get_children():
                assert grandchild is not None


class TestAssetValidation:
    """Tests for validating STAC assets."""

    @pytest.mark.integration
    def test_assets_have_valid_hrefs(self, release_catalog):
        """Test that assets have valid href URLs."""
        for item in get_all_items_from_catalog(release_catalog):
            for asset_key, asset in item.assets.items():
                assert asset.href is not None, (
                    f"Asset {asset_key} in item {item.id} should have href"
                )
                assert asset.href.startswith("http"), (
                    f"Asset {asset_key} href should be a URL"
                )

    @pytest.mark.integration
    def test_assets_have_media_type(self, release_catalog):
        """Test that assets have media type."""
        for item in get_all_items_from_catalog(release_catalog):
            for asset_key, asset in item.assets.items():
                assert asset.media_type is not None, (
                    f"Asset {asset_key} in item {item.id} should have media_type"
                )
                assert "parquet" in asset.media_type, "Asset should be parquet type"


class TestManifestValidation:
    """Tests for validating the manifest.geojson file."""

    @pytest.mark.integration
    def test_manifest_accessible(self, catalog_server, release_name):
        """Test that manifest.geojson is accessible via HTTP."""
        import urllib.request

        url = f"{catalog_server}/{release_name}/manifest.geojson"
        response = urllib.request.urlopen(url)
        assert response.status == 200

    @pytest.mark.integration
    def test_manifest_is_valid_geojson(self, catalog_server, release_name):
        """Test that manifest.geojson is valid GeoJSON."""
        import urllib.request

        url = f"{catalog_server}/{release_name}/manifest.geojson"
        response = urllib.request.urlopen(url)
        manifest = json.loads(response.read().decode())

        assert manifest["type"] == "FeatureCollection"
        assert "features" in manifest
        assert len(manifest["features"]) > 0

    @pytest.mark.integration
    def test_manifest_features_have_properties(self, catalog_server, release_name):
        """Test that manifest features have expected properties."""
        import urllib.request

        url = f"{catalog_server}/{release_name}/manifest.geojson"
        response = urllib.request.urlopen(url)
        manifest = json.loads(response.read().decode())

        for feature in manifest["features"]:
            assert "properties" in feature
            assert "ovt_type" in feature["properties"]
            assert "rel_path" in feature["properties"]
            assert "geometry" in feature
            assert "bbox" in feature
