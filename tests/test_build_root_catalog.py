"""Unit tests for build_root_catalog."""

import json
from pathlib import Path

from overture_stac.overture_stac import build_root_catalog

ROOT = "https://stac.overturemaps.org"


def _read_root(output: Path) -> dict:
    with open(output / "catalog.json") as f:
        return json.load(f)


def _children(doc: dict) -> list[dict]:
    return [link for link in doc["links"] if link["rel"] == "child"]


class TestBuildRootCatalog:
    def test_writes_catalog_json_at_output_root(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0", "2026-07-22.0"])
        assert (tmp_path / "catalog.json").exists()

    def test_root_has_expected_identity(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0"])
        doc = _read_root(tmp_path)
        assert doc["type"] == "Catalog"
        assert doc["id"] == "Overture Releases"
        assert doc["title"] == "Overture Releases"

    def test_child_link_per_release_in_order(self, tmp_path):
        ids = ["2026-08-05.0", "2026-07-22.0", "2026-06-17.0"]
        build_root_catalog(tmp_path, ROOT, ids)
        children = _children(_read_root(tmp_path))
        assert [c["href"] for c in children] == [
            f"{ROOT}/{i}/catalog.json" for i in ids
        ]

    def test_input_order_is_normalised_to_newest_first(self, tmp_path):
        """Caller may pass any order; function sorts newest-first internally."""
        build_root_catalog(
            tmp_path, ROOT, ["2026-06-17.0", "2026-08-05.0", "2026-07-22.0"]
        )
        doc = _read_root(tmp_path)
        assert [c["href"] for c in _children(doc)] == [
            f"{ROOT}/2026-08-05.0/catalog.json",
            f"{ROOT}/2026-07-22.0/catalog.json",
            f"{ROOT}/2026-06-17.0/catalog.json",
        ]
        assert doc["latest"] == "2026-08-05.0"

    def test_newest_child_link_titled_latest(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0", "2026-07-22.0"])
        children = _children(_read_root(tmp_path))
        assert children[0]["title"] == "Latest Overture Release"
        assert children[0]["latest"] is True
        assert children[1]["title"] == "2026-07-22.0 Overture Release"
        assert "latest" not in children[1]

    def test_root_extra_fields_latest_matches_newest_id(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0", "2026-07-22.0"])
        assert _read_root(tmp_path)["latest"] == "2026-08-05.0"

    def test_registry_embedded_when_provided(self, tmp_path):
        registry = {"path": "s3://bucket/registry", "manifest": [["file.parquet", "z"]]}
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0"], registry=registry)
        assert _read_root(tmp_path)["registry"] == registry

    def test_registry_omitted_when_none(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0"])
        assert "registry" not in _read_root(tmp_path)

    def test_empty_release_list_still_writes_root(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, [])
        doc = _read_root(tmp_path)
        assert _children(doc) == []
        assert "latest" not in doc

    def test_trailing_slash_on_root_href_is_normalised(self, tmp_path):
        build_root_catalog(tmp_path, ROOT + "/", ["2026-08-05.0"])
        children = _children(_read_root(tmp_path))
        assert children[0]["href"] == f"{ROOT}/2026-08-05.0/catalog.json"

    def test_self_link_uses_absolute_root_href(self, tmp_path):
        build_root_catalog(tmp_path, ROOT, ["2026-08-05.0"])
        doc = _read_root(tmp_path)
        self_links = [link for link in doc["links"] if link["rel"] == "self"]
        assert len(self_links) == 1
        assert self_links[0]["href"] == f"{ROOT}/catalog.json"

    def test_output_dir_is_created_if_missing(self, tmp_path):
        target = tmp_path / "does" / "not" / "exist"
        build_root_catalog(target, ROOT, ["2026-08-05.0"])
        assert (target / "catalog.json").exists()
