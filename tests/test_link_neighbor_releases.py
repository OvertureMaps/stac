"""Unit tests for link_neighbor_releases, including its self-healing contract."""

import pystac

from overture_stac.overture_stac import link_neighbor_releases

ROOT = "https://stac.overturemaps.org"


def _mk(release_id: str) -> pystac.Catalog:
    return pystac.Catalog(id=release_id, description=f"release {release_id}")


def _neighbours(cat: pystac.Catalog) -> dict[str, str | None]:
    result: dict[str, str | None] = {"prev": None, "next": None}
    for link in cat.links:
        if link.rel in ("prev", "next"):
            result[link.rel] = link.target.split("/")[-2]
    return result


class TestLinkNeighborReleases:
    def test_middle_release_gets_prev_and_next(self):
        ids = ["2026-08-05", "2026-07-22", "2026-06-17"]
        middle = _mk("2026-07-22")
        link_neighbor_releases(middle, ids, ROOT)
        assert _neighbours(middle) == {"prev": "2026-06-17", "next": "2026-08-05"}

    def test_newest_has_prev_only(self):
        ids = ["2026-08-05", "2026-07-22"]
        newest = _mk("2026-08-05")
        link_neighbor_releases(newest, ids, ROOT)
        assert _neighbours(newest) == {"prev": "2026-07-22", "next": None}

    def test_oldest_has_next_only(self):
        ids = ["2026-08-05", "2026-07-22"]
        oldest = _mk("2026-07-22")
        link_neighbor_releases(oldest, ids, ROOT)
        assert _neighbours(oldest) == {"prev": None, "next": "2026-08-05"}

    def test_single_release_has_neither(self):
        lone = _mk("2026-08-05")
        link_neighbor_releases(lone, ["2026-08-05"], ROOT)
        assert _neighbours(lone) == {"prev": None, "next": None}

    def test_catalog_not_in_list_is_noop(self):
        orphan = _mk("2026-01-01")
        link_neighbor_releases(orphan, ["2026-08-05", "2026-07-22"], ROOT)
        assert _neighbours(orphan) == {"prev": None, "next": None}

    def test_self_heals_when_middle_release_disappears(self):
        """Yesterday [C, B, A]; today B is gone. C.prev must skip to A."""
        ids = ["2026-08-05", "2026-06-17"]  # B (2026-07-22) is gone
        c, a = _mk("2026-08-05"), _mk("2026-06-17")
        link_neighbor_releases(c, ids, ROOT)
        link_neighbor_releases(a, ids, ROOT)
        assert _neighbours(c) == {"prev": "2026-06-17", "next": None}
        assert _neighbours(a) == {"prev": None, "next": "2026-08-05"}

    def test_single_release_mode_still_gets_neighbors(self):
        """On release day only the new release is built, but it must still
        link back to older siblings still living in the bucket."""
        new_release = _mk("2026-08-05")
        bucket_ids = ["2026-08-05", "2026-07-22", "2026-06-17"]
        link_neighbor_releases(new_release, bucket_ids, ROOT)
        assert _neighbours(new_release) == {"prev": "2026-07-22", "next": None}

    def test_link_target_is_absolute_url(self):
        newest = _mk("2026-08-05")
        link_neighbor_releases(newest, ["2026-08-05", "2026-07-22"], ROOT)
        prev_link = next(link for link in newest.links if link.rel == "prev")
        assert prev_link.target == f"{ROOT}/2026-07-22/catalog.json"
        assert prev_link.media_type == "application/json"
        assert prev_link.title == "2026-07-22 Overture Release"

    def test_trailing_slash_on_root_href_is_normalised(self):
        newest = _mk("2026-08-05")
        link_neighbor_releases(newest, ["2026-08-05", "2026-07-22"], ROOT + "/")
        prev_link = next(link for link in newest.links if link.rel == "prev")
        assert prev_link.target == f"{ROOT}/2026-07-22/catalog.json"
