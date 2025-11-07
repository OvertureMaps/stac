"""Tests for overture_stac module."""

from overture_stac.overture_stac import TYPE_LICENSE_MAP


def test_license_map_exists():
    """Test that TYPE_LICENSE_MAP is defined."""
    assert TYPE_LICENSE_MAP is not None
    assert isinstance(TYPE_LICENSE_MAP, dict)


def test_common_types_have_licenses():
    """Test that common types have license mappings."""
    common_types = ["building", "place", "address", "water", "land"]

    for type_name in common_types:
        assert type_name in TYPE_LICENSE_MAP


def test_license_values_are_strings():
    """Test that all license values are strings."""
    for license_value in TYPE_LICENSE_MAP.values():
        assert isinstance(license_value, str)
        assert len(license_value) > 0
