#!/usr/bin/env python3
"""Build a Portolan-conformant demo catalog from the mirrored Overture STAC.

Scope: places/place, buildings/building, buildings/building_part.
Input:  ./mirror/2026-08-19.0/   (fetched from stac.overturemaps.org)
Output: ./demo/

This is the "repair in place" path from the portolan-migrate skill: the source
data is untouched, only the metadata layer is rebuilt.
"""

import json
import os
import shutil
from pathlib import Path

import columns as COLS

SRC = Path("mirror/2026-08-19.0")
OUT = Path("demo")
RELEASE = "2026-08-19.0"
PORTOLAN_SCHEMA = "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json"
TABLE_EXT = "https://stac-extensions.github.io/table/v1.2.0/schema.json"

# Overture Maps Foundation, from https://overturemaps.org/ and
# https://docs.overturemaps.org/attribution/ (tier: attested).
PROVIDERS = [
    {
        "name": "Overture Maps Foundation",
        "description": (
            "Overture Maps Foundation is a collaborative effort of its members to build "
            "open, interoperable map data. It assembles, conflates, and quality-checks the "
            "source datasets behind this collection and publishes them on a monthly cadence."
        ),
        "roles": ["producer", "licensor", "processor", "host"],
        "url": "https://overturemaps.org/",
    }
]

THEMES = {
    "places": {
        "title": "Places",
        "description": (
            "Points of interest — businesses, services, landmarks, and other named places. "
            "The places theme carries a single collection, [Places](place/collection.json), "
            "with 73.6 million points worldwide.\n\n"
            "Read the [Overture places schema reference](https://docs.overturemaps.org/schema/reference/places/place/) "
            "for the authoritative field definitions."
        ),
        "types": ["place"],
    },
    "buildings": {
        "title": "Buildings",
        "description": (
            "Building footprints and their constituent parts, conflated from OpenStreetMap, "
            "Esri Community Maps, Google Open Buildings, Microsoft ML Building Footprints, "
            "and other sources. Two collections: "
            "[Buildings](building/collection.json) holds the footprints, and "
            "[Building Parts](building_part/collection.json) holds sub-features that refine "
            "a footprint's shape or height.\n\n"
            "Read the [Overture buildings schema reference](https://docs.overturemaps.org/schema/reference/buildings/building/) "
            "for the authoritative field definitions."
        ),
        "types": ["building", "building_part"],
    },
}

TITLES = {
    "place": "Places",
    "building": "Buildings",
    "building_part": "Building Parts",
}

KEYWORDS = {
    "place": ["places", "points of interest", "poi", "business", "gers", "overture"],
    "building": ["buildings", "building footprints", "3d", "gers", "overture"],
    "building_part": ["buildings", "building parts", "3d", "gers", "overture"],
}


def read(p):
    return json.loads(Path(p).read_text())


def write(p, obj):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n")


def write_text(p, s):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s)


def doc_links(depth_to_self="."):
    """rel:describedby -> README.md, rel:agents -> AGENTS.md (PTL-FIL-002/003)."""
    return [
        {
            "rel": "describedby",
            "href": f"{depth_to_self}/README.md",
            "type": "text/markdown",
            "title": "README",
        },
        {
            "rel": "agents",
            "href": f"{depth_to_self}/AGENTS.md",
            "type": "text/markdown",
            "title": "Agent guide",
        },
    ]


def build_collection(theme, tname):
    src = read(SRC / theme / tname / "collection.json")
    dst_dir = OUT / theme / tname

    coldefs = COLS.BY_COLLECTION[tname]
    src_cols = [c["name"] for c in src.get("table:columns", [])]
    missing = [c for c in src_cols if c not in coldefs]
    if missing:
        raise SystemExit(f"{tname}: no description authored for {missing}")

    table_columns = []
    for name in src_cols:
        ctype, desc = coldefs[name]
        table_columns.append({"name": name, "type": ctype, "description": desc})

    # Structural links, all relative. Item links gain titles (PTL-TTL-003).
    links = [
        {"rel": "root", "href": "../../catalog.json", "type": "application/json",
         "title": "Overture Places and Buildings"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json",
         "title": THEMES[theme]["title"]},
        {"rel": "self", "href": f"https://example.invalid/{theme}/{tname}/collection.json",
         "type": "application/json"},
    ]
    links += doc_links()
    for l in src["links"]:
        if l["rel"] == "license":
            links.append(dict(l))

    n_items = 0
    for l in src["links"]:
        if l["rel"] != "item":
            continue
        part = l["href"].rstrip("/").split("/")[-1].replace(".json", "")
        links.append({
            "rel": "item",
            "href": f"./{part}/{part}.json",
            "type": "application/geo+json",
            "title": f"{TITLES[tname]} partition {part}",
        })
        n_items += 1

    out = {
        "type": "Collection",
        "id": src["id"],
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT],
        "title": TITLES[tname],
        "description": DESCRIPTIONS[tname],
        "license": src.get("license"),
        "keywords": KEYWORDS[tname],
        "providers": PROVIDERS,
        "extent": fixed_extent(src),
        "links": links,
        "assets": {},
        "summaries": src.get("summaries", {}),
        "table:primary_geometry": "geometry",
        "table:row_count": src.get("table:row_count"),
        "table:columns": table_columns,
        "geoparquet:version": src.get("geoparquet:version"),
    }
    write(dst_dir / "collection.json", out)

    # Copy item files across, rewriting their structural links to relative.
    for l in src["links"]:
        if l["rel"] != "item":
            continue
        part = l["href"].rstrip("/").split("/")[-1].replace(".json", "")
        item = read(SRC / theme / tname / part / f"{part}.json")
        item["links"] = [
            {"rel": "root", "href": "../../../catalog.json", "type": "application/json",
             "title": "Overture Places and Buildings"},
            {"rel": "parent", "href": "../collection.json", "type": "application/json",
             "title": TITLES[tname]},
            {"rel": "collection", "href": "../collection.json", "type": "application/json",
             "title": TITLES[tname]},
            {"rel": "self",
             "href": f"https://example.invalid/{theme}/{tname}/{part}/{part}.json",
             "type": "application/geo+json"},
        ]
        item["collection"] = src["id"]
        write(dst_dir / part / f"{part}.json", item)

    return n_items, len(table_columns)


def fixed_extent(src):
    """Prepend the overall bbox, which the generator omits (issue #121)."""
    bb = src["extent"]["spatial"]["bbox"]
    if len(bb) > 1:
        union = [
            min(b[0] for b in bb), min(b[1] for b in bb),
            max(b[2] for b in bb), max(b[3] for b in bb),
        ]
        # Clamp to valid WGS84; land_cover is the only theme that overshoots,
        # but guard anyway so the overall bbox is always spec-valid.
        union = [max(union[0], -180.0), max(union[1], -90.0),
                 min(union[2], 180.0), min(union[3], 90.0)]
        bb = [union] + bb
    return {
        "spatial": {"bbox": bb},
        "temporal": src["extent"]["temporal"],
    }


DESCRIPTIONS = {}  # filled in by descriptions.py


def build_theme(theme):
    meta = THEMES[theme]
    links = [
        {"rel": "root", "href": "../catalog.json", "type": "application/json",
         "title": "Overture Places and Buildings"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json",
         "title": "Overture Places and Buildings"},
        {"rel": "self", "href": f"https://example.invalid/{theme}/catalog.json",
         "type": "application/json"},
    ]
    links += doc_links()
    for t in meta["types"]:
        links.append({
            "rel": "child",
            "href": f"./{t}/collection.json",
            "type": "application/json",
            "title": TITLES[t],
        })
    write(OUT / theme / "catalog.json", {
        "type": "Catalog",
        "id": theme,
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "title": meta["title"],
        "description": meta["description"],
        "links": links,
    })


def build_root():
    links = [
        {"rel": "root", "href": "./catalog.json", "type": "application/json",
         "title": "Overture Places and Buildings"},
        {"rel": "self", "href": "https://example.invalid/catalog.json",
         "type": "application/json"},
    ]
    links += doc_links()
    for theme in THEMES:
        links.append({
            "rel": "child",
            "href": f"./{theme}/catalog.json",
            "type": "application/json",
            "title": THEMES[theme]["title"],
        })
    write(OUT / "catalog.json", {
        "type": "Catalog",
        "id": "overture-places-buildings-demo",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "title": "Overture Places and Buildings",
        "description": ROOT_DESCRIPTION,
        "links": links,
    })


ROOT_DESCRIPTION = ""  # filled in by descriptions.py


if __name__ == "__main__":
    import descriptions
    DESCRIPTIONS.update(descriptions.COLLECTION_DESCRIPTIONS)
    ROOT_DESCRIPTION = descriptions.ROOT_DESCRIPTION

    if OUT.exists():
        shutil.rmtree(OUT)
    build_root()
    total_items = 0
    for theme, meta in THEMES.items():
        build_theme(theme)
        for t in meta["types"]:
            n, ncols = build_collection(theme, t)
            total_items += n
            print(f"  {theme}/{t:16} {n:>4} items, {ncols:>2} columns documented")
    print(f"total items: {total_items}")
