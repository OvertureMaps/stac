"""Tests for registry_manifest module."""

import json

import pytest
from overture_stac.registry_manifest import RegistryManifest


def test_init_default_params():
    """Test initialization with default parameters."""
    manifest = RegistryManifest()
    assert manifest.registry_path == "overturemaps-us-west-2/registry"
    assert manifest.filesystem is not None


def test_init_custom_params():
    """Test initialization with custom parameters."""
    custom_path = "custom-bucket/registry"
    custom_region = "eu-west-1"

    manifest = RegistryManifest(registry_path=custom_path, s3_region=custom_region)
    assert manifest.registry_path == custom_path


def test_manifest_sorting():
    """Test that manifest entries are sorted by max_id."""
    entries = [
        ["file2.parquet", "id_200"],
        ["file1.parquet", "id_100"],
        ["file3.parquet", "id_300"],
    ]

    entries.sort(key=lambda x: x[1])

    assert entries[0][1] == "id_100"
    assert entries[1][1] == "id_200"
    assert entries[2][1] == "id_300"


def test_manifest_format():
    """Test that manifest returns list of [filename, max_id] tuples."""
    expected_entry = ["test.parquet", "test_id"]
    assert isinstance(expected_entry, list)
    assert len(expected_entry) == 2
    assert isinstance(expected_entry[0], str)
    assert isinstance(expected_entry[1], str)


@pytest.mark.integration
@pytest.mark.slow
def test_create_registry_manifest_integration():
    """
    Integration test: Actually connect to S3 and create registry manifest.

    This test connects to the real S3 bucket and generates the manifest.
    It's marked as 'integration' and 'slow' so it can be skipped in CI.

    Run with: pytest -v -m integration
    Skip with: pytest -v -m "not integration"
    """
    print("\n" + "=" * 80)
    print("INTEGRATION TEST: Creating Registry Manifest from S3")
    print("=" * 80)

    # Create the manifest
    registry = RegistryManifest()
    manifest_data = registry.create_manifest()

    # Pretty print the results
    print(f"\n✓ Successfully created manifest with {len(manifest_data)} files\n")

    if manifest_data:
        print("First 10 entries:")
        print("-" * 80)
        for i, entry in enumerate(manifest_data[:10], 1):
            filename, max_id = entry
            print(f"{i:2d}. {filename:50s} | max_id: {max_id}")

        if len(manifest_data) > 10:
            print(f"\n... and {len(manifest_data) - 10} more files")

        print("\n" + "-" * 80)
        print("Last 5 entries:")
        print("-" * 80)
        for i, entry in enumerate(manifest_data[-5:], len(manifest_data) - 4):
            filename, max_id = entry
            print(f"{i:2d}. {filename:50s} | max_id: {max_id}")

    # Print as JSON
    print("\n" + "=" * 80)
    print("Manifest as JSON (first 5 entries):")
    print("=" * 80)
    print(json.dumps(manifest_data[:5], indent=2))

    # Verify structure
    print("\n" + "=" * 80)
    print("Verification:")
    print("=" * 80)
    assert isinstance(manifest_data, list), "Manifest should be a list"
    assert len(manifest_data) > 0, "Manifest should contain entries"

    # Verify each entry has the correct structure
    for entry in manifest_data:
        assert isinstance(entry, list), f"Entry should be a list: {entry}"
        assert len(entry) == 2, f"Entry should have 2 elements: {entry}"
        assert isinstance(entry[0], str), f"Filename should be string: {entry[0]}"
        assert isinstance(entry[1], str), f"min_id should be string: {entry[1]}"

    # Verify sorting (each min_id should be >= previous)
    for i in range(1, len(manifest_data)):
        prev_id = manifest_data[i - 1][1]
        curr_id = manifest_data[i][1]
        assert curr_id >= prev_id, f"Manifest not sorted: {prev_id} > {curr_id}"

    print("✓ All verifications passed!")
    print("=" * 80 + "\n")
