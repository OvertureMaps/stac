"""Tests for overture_stac module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from overture_stac.overture_stac import OvertureRelease, TYPE_LICENSE_MAP


class TestOvertureRelease:
    """Test OvertureRelease class."""

    @patch("overture_stac.overture_stac.fs.S3FileSystem")
    def test_init(self, mock_s3, tmp_path):
        """Test OvertureRelease initialization."""
        release = OvertureRelease(
            release="2025-07-23.0",
            schema="1.11.0",
            output=tmp_path,
        )

        assert release.release == "2025-07-23.0"
        assert release.schema == "1.11.0"
        assert release.output == tmp_path / "2025-07-23.0"
        assert release.release_datetime == datetime(2025, 7, 23)

    @patch("overture_stac.overture_stac.fs.S3FileSystem")
    def test_init_with_debug(self, mock_s3, tmp_path):
        """Test OvertureRelease initialization with debug mode."""
        release = OvertureRelease(
            release="2025-07-23.0",
            schema="1.11.0",
            output=tmp_path,
            debug=True,
        )

        assert release.debug is True

    @patch("overture_stac.overture_stac.fs.S3FileSystem")
    @patch("overture_stac.overture_stac.pystac")
    def test_make_release_catalog(self, mock_pystac, mock_s3, tmp_path):
        """Test making a release catalog."""
        mock_catalog = MagicMock()
        mock_pystac.Catalog.return_value = mock_catalog

        release = OvertureRelease(
            release="2025-07-23.0",
            schema="1.11.0",
            output=tmp_path,
        )

        release.make_release_catalog(title="Test Release")

        mock_pystac.Catalog.assert_called_once()
        assert release.release_catalog is not None

    @patch("overture_stac.overture_stac.fs.S3FileSystem")
    def test_release_path_construction(self, mock_s3, tmp_path):
        """Test that release path is constructed correctly."""
        release = OvertureRelease(
            release="2025-07-23.0",
            schema="1.11.0",
            output=tmp_path,
            s3_release_path="s3://custom-bucket/release",
        )

        assert release.release_path == "s3://custom-bucket/release/2025-07-23.0"

    @patch("overture_stac.overture_stac.fs.S3FileSystem")
    def test_output_directory_created(self, mock_s3, tmp_path):
        """Test that output directory is created."""
        release_name = "2025-07-23.0"

        release = OvertureRelease(
            release=release_name,
            schema="1.11.0",
            output=tmp_path,
        )

        expected_path = tmp_path / release_name
        assert expected_path.exists()
        assert expected_path.is_dir()


class TestTypeLicenseMap:
    """Test TYPE_LICENSE_MAP constant."""

    def test_license_map_exists(self):
        """Test that TYPE_LICENSE_MAP is defined."""
        assert TYPE_LICENSE_MAP is not None
        assert isinstance(TYPE_LICENSE_MAP, dict)

    def test_common_types_have_licenses(self):
        """Test that common types have license mappings."""
        common_types = ["building", "place", "address", "water", "land"]

        for type_name in common_types:
            assert type_name in TYPE_LICENSE_MAP

    def test_license_values_are_strings(self):
        """Test that all license values are strings."""
        for license_value in TYPE_LICENSE_MAP.values():
            assert isinstance(license_value, str)
            assert len(license_value) > 0
