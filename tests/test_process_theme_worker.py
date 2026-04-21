"""Unit tests for process_theme_worker and build_release_catalog.

These tests mock the S3/PyArrow layer to verify:
- Row counts are read from parquet metadata (not fragment.count_rows())
- Total row counts are accumulated from fragments (not type_dataset.count_rows())
- workers<=1 runs in-process without ProcessPoolExecutor
"""

import json
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pystac
import pytest

from overture_stac.overture_stac import OvertureRelease, process_theme_worker


def make_mock_fragment(path: str, num_rows: int = 100, num_row_groups: int = 2):
    """Create a mock parquet fragment with metadata."""
    geo_metadata = json.dumps(
        {
            "version": "1.0.0",
            "columns": {
                "geometry": {"bbox": [-180.0, -90.0, 180.0, 90.0]},
            },
        }
    ).encode("utf-8")

    schema = MagicMock()
    schema.metadata = {b"geo": geo_metadata}
    schema.names = ["id", "geometry", "name"]

    metadata = MagicMock()
    metadata.num_rows = num_rows
    metadata.schema.to_arrow_schema.return_value = schema

    fragment = MagicMock()
    fragment.path = path
    fragment.metadata = metadata
    fragment.num_row_groups = num_row_groups
    # count_rows should NOT be called — if it is, fail loudly
    fragment.count_rows = MagicMock(
        side_effect=AssertionError("count_rows() should not be called on fragments")
    )

    return fragment


def make_mock_theme_type(path: str, fragments: list):
    """Create a mock theme type (FileInfo + dataset)."""
    file_info = MagicMock()
    file_info.path = path

    dataset = MagicMock()
    dataset.get_fragments.return_value = iter(fragments)
    # count_rows should NOT be called — if it is, fail loudly
    dataset.count_rows = MagicMock(
        side_effect=AssertionError(
            "count_rows() should not be called on the full dataset"
        )
    )

    return file_info, dataset


class TestProcessThemeWorker:
    """Tests for the process_theme_worker function."""

    @patch("overture_stac.overture_stac.ds")
    @patch("overture_stac.overture_stac.fs")
    def test_uses_metadata_num_rows(self, mock_fs, mock_ds):
        """Verify row counts come from fragment.metadata.num_rows, not count_rows()."""
        fragments = [
            make_mock_fragment(
                "bucket/release/theme=test/type=widget/part-00000-abc.parquet",
                num_rows=500,
            ),
            make_mock_fragment(
                "bucket/release/theme=test/type=widget/part-00001-def.parquet",
                num_rows=300,
            ),
        ]

        file_info, dataset = make_mock_theme_type(
            "bucket/release/theme=test/type=widget", fragments
        )

        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = [file_info]
        mock_fs.S3FileSystem.return_value = mock_filesystem
        mock_ds.dataset.return_value = dataset

        theme_catalog, manifest_items, type_collections, theme_name = (
            process_theme_worker(
                theme_path="bucket/release/theme=test",
                release_path="s3://bucket/release",
                s3_region="us-west-2",
                debug=False,
                release_datetime=datetime(2026, 4, 15),
                release="2026-04-15.0",
                available_pmtiles={},
            )
        )

        assert theme_name == "test"
        assert len(manifest_items) == 2

        # Verify num_rows came from metadata
        items = type_collections["widget"]
        assert items[0].properties["num_rows"] == 500
        assert items[1].properties["num_rows"] == 300

    @patch("overture_stac.overture_stac.ds")
    @patch("overture_stac.overture_stac.fs")
    def test_total_row_count_accumulated(self, mock_fs, mock_ds):
        """Verify total row count is summed from fragments, not dataset.count_rows()."""
        fragments = [
            make_mock_fragment(
                "bucket/release/theme=things/type=gadget/part-00000-abc.parquet",
                num_rows=1000,
            ),
            make_mock_fragment(
                "bucket/release/theme=things/type=gadget/part-00001-def.parquet",
                num_rows=2000,
            ),
            make_mock_fragment(
                "bucket/release/theme=things/type=gadget/part-00002-ghi.parquet",
                num_rows=3000,
            ),
        ]

        file_info, dataset = make_mock_theme_type(
            "bucket/release/theme=things/type=gadget", fragments
        )

        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = [file_info]
        mock_fs.S3FileSystem.return_value = mock_filesystem
        mock_ds.dataset.return_value = dataset

        theme_catalog, manifest_items, type_collections, theme_name = (
            process_theme_worker(
                theme_path="bucket/release/theme=things",
                release_path="s3://bucket/release",
                s3_region="us-west-2",
                debug=False,
                release_datetime=datetime(2026, 4, 15),
                release="2026-04-15.0",
                available_pmtiles={},
            )
        )

        # Find the collection and check its features (total row count)
        collections = list(theme_catalog.get_children())
        assert len(collections) == 1
        assert collections[0].extra_fields["features"] == 6000

    @patch("overture_stac.overture_stac.ds")
    @patch("overture_stac.overture_stac.fs")
    def test_debug_mode_skips_total_count(self, mock_fs, mock_ds):
        """Verify debug mode does not set the features extra field."""
        fragments = [
            make_mock_fragment(
                "bucket/release/theme=dbg/type=item/part-00000-abc.parquet",
                num_rows=10,
            ),
        ]

        file_info, dataset = make_mock_theme_type(
            "bucket/release/theme=dbg/type=item", fragments
        )

        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = [file_info]
        mock_fs.S3FileSystem.return_value = mock_filesystem
        mock_ds.dataset.return_value = dataset

        theme_catalog, _, _, _ = process_theme_worker(
            theme_path="bucket/release/theme=dbg",
            release_path="s3://bucket/release",
            s3_region="us-west-2",
            debug=True,
            release_datetime=datetime(2026, 4, 15),
            release="2026-04-15.0",
            available_pmtiles={},
        )

        collections = list(theme_catalog.get_children())
        assert "features" not in collections[0].extra_fields

    @patch("overture_stac.overture_stac.ds")
    @patch("overture_stac.overture_stac.fs")
    def test_pmtiles_link_added(self, mock_fs, mock_ds):
        """Verify PMTiles link is added when theme is in available_pmtiles."""
        fragments = [
            make_mock_fragment(
                "bucket/release/theme=buildings/type=building/part-00000-abc.parquet",
            ),
        ]

        file_info, dataset = make_mock_theme_type(
            "bucket/release/theme=buildings/type=building", fragments
        )

        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = [file_info]
        mock_fs.S3FileSystem.return_value = mock_filesystem
        mock_ds.dataset.return_value = dataset

        theme_catalog, _, _, _ = process_theme_worker(
            theme_path="bucket/release/theme=buildings",
            release_path="s3://bucket/release",
            s3_region="us-west-2",
            debug=False,
            release_datetime=datetime(2026, 4, 15),
            release="2026-04-15.0",
            available_pmtiles={"buildings": "some/path.pmtiles"},
        )

        pmtiles_links = [l for l in theme_catalog.links if l.rel == "pmtiles"]
        assert len(pmtiles_links) == 1


class TestBuildReleaseCatalog:
    """Tests for the build_release_catalog method."""

    @patch("overture_stac.overture_stac.stac_geoparquet")
    @patch("overture_stac.overture_stac.fs")
    @patch("overture_stac.overture_stac.process_theme_worker")
    def test_workers_1_runs_in_process(
        self, mock_worker, mock_fs, mock_stac_geoparquet
    ):
        """Verify workers<=1 calls process_theme_worker directly (no subprocess)."""
        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = []
        mock_fs.S3FileSystem.return_value = mock_filesystem

        mock_table = MagicMock()
        mock_stac_geoparquet.arrow.parse_stac_items_to_arrow.return_value = mock_table

        mock_catalog = pystac.Catalog(id="test", description="test theme")
        mock_worker.return_value = (mock_catalog, [], {}, "test")

        release = OvertureRelease(
            release="2026-04-15.0",
            schema="1.0",
            output="test_output",
        )

        # Patch get_release_themes to set themes directly
        mock_theme = MagicMock()
        mock_theme.path = "bucket/release/theme=test"
        release.get_release_themes = lambda: setattr(release, "themes", [mock_theme])

        with patch(
            "overture_stac.overture_stac.ProcessPoolExecutor"
        ) as mock_pool_cls:
            release.build_release_catalog(title="Test", max_workers=1)
            # ProcessPoolExecutor should NOT have been instantiated
            mock_pool_cls.assert_not_called()

        # But process_theme_worker should have been called directly
        mock_worker.assert_called_once()

    @patch("overture_stac.overture_stac.stac_geoparquet")
    @patch("overture_stac.overture_stac.fs")
    @patch("overture_stac.overture_stac.process_theme_worker")
    def test_workers_gt1_uses_process_pool(
        self, mock_worker, mock_fs, mock_stac_geoparquet
    ):
        """Verify workers>1 uses ProcessPoolExecutor."""
        mock_filesystem = MagicMock()
        mock_filesystem.get_file_info.return_value = []
        mock_fs.S3FileSystem.return_value = mock_filesystem

        mock_table = MagicMock()
        mock_stac_geoparquet.arrow.parse_stac_items_to_arrow.return_value = mock_table

        mock_catalog = pystac.Catalog(id="test", description="test theme")

        release = OvertureRelease(
            release="2026-04-15.0",
            schema="1.0",
            output="test_output",
        )

        mock_theme = MagicMock()
        mock_theme.path = "bucket/release/theme=test"
        release.get_release_themes = lambda: setattr(release, "themes", [mock_theme])

        with patch(
            "overture_stac.overture_stac.ProcessPoolExecutor"
        ) as mock_pool_cls:
            mock_future = MagicMock()
            mock_future.result.return_value = (mock_catalog, [], {}, "test")

            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=False)
            mock_executor.submit.return_value = mock_future

            mock_pool_cls.return_value = mock_executor

            # Patch as_completed to yield the same future object that submit returned
            with patch(
                "overture_stac.overture_stac.as_completed",
                return_value=iter([mock_future]),
            ):
                release.build_release_catalog(title="Test", max_workers=4)

            mock_pool_cls.assert_called_once_with(max_workers=4)
