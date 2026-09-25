#!/usr/bin/env python3
"""Write MapLibre style JSON for each collection and register them as assets.

Every `match` / `step` branch below corresponds to a value measured in the
published data (see dist.txt / bsample_out.txt); no branch is invented.

Legend note: the Portolan browser derives a legend only from a `fill` layer
whose `fill-color` is a `match` or `step` expression. Places are points, so
their styles produce no legend — that is a browser limitation, not a defect in
these styles. Recorded in known_issues.
"""

import hashlib
import json
from pathlib import Path

OUT = Path("demo")
SRC_TPL = "https://tiles.overturemaps.org/2026-08-19.0/{theme}.pmtiles"


def style(name, layers, source_layer_theme):
    return {
        "version": 8,
        "name": name,
        "sources": {
            "overture": {
                "type": "vector",
                "url": f"pmtiles://{SRC_TPL.format(theme=source_layer_theme)}",
            }
        },
        "layers": layers,
    }


# ---------------------------------------------------------------- places
# basic_category values and counts from partition 00000 (4,599,286 rows).
PLACE_CATS = [
    ("restaurant", "#e6550d"),
    ("personal_or_beauty_service", "#fd8d3c"),
    ("fashion_and_apparel_store", "#756bb1"),
    ("food_and_beverage_store", "#31a354"),
    ("automotive_service", "#636363"),
    ("professional_service", "#3182bd"),
    ("hardware_home_and_garden_store", "#8c6d31"),
    ("home_service", "#9ecae1"),
    ("real_estate_service", "#bcbddc"),
    ("casual_eatery", "#fdae6b"),
]

PLACES_STYLES = {
    "default": style("Places", [{
        "id": "places",
        "type": "circle",
        "source": "overture",
        "source-layer": "place",
        "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 1.2, 12, 4],
            "circle-color": "#e6550d",
            "circle-opacity": 0.75,
            "circle-stroke-width": 0,
        },
    }], "places"),

    "by-confidence": style("Places by confidence", [{
        "id": "places-confidence",
        "type": "circle",
        "source": "overture",
        "source-layer": "place",
        "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 1.2, 12, 4],
            "circle-color": [
                "step", ["get", "confidence"],
                "#d73027",
                0.4, "#fc8d59",
                0.6, "#fee08b",
                0.8, "#91cf60",
                0.9, "#1a9850",
            ],
            "circle-opacity": 0.8,
        },
    }], "places"),

    "by-category": style("Places by category", [{
        "id": "places-category",
        "type": "circle",
        "source": "overture",
        "source-layer": "place",
        "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 1.2, 12, 4],
            "circle-color": (
                ["match", ["get", "basic_category"]]
                + [v for cat, col in PLACE_CATS for v in (cat, col)]
                + ["#cccccc"]
            ),
            "circle-opacity": 0.8,
        },
    }], "places"),
}

# ------------------------------------------------------------- buildings
# subtype values from 8 of 512 partitions (39,680,766 rows); all 13 present.
BUILDING_SUBTYPES = [
    ("residential", "#8c6bb1"),
    ("commercial", "#3182bd"),
    ("education", "#31a354"),
    ("outbuilding", "#bdbdbd"),
    ("religious", "#756bb1"),
    ("industrial", "#636363"),
    ("agricultural", "#a1d99b"),
    ("civic", "#2b8cbe"),
    ("medical", "#e6550d"),
    ("service", "#969696"),
    ("transportation", "#fdae6b"),
    ("entertainment", "#e7298a"),
    ("military", "#7f2704"),
]

BUILDING_STYLES = {
    # Default is deliberately one colour: 98.4% of footprints carry no subtype,
    # so an attribute-driven default would render as a grey map with a legend.
    "default": style("Buildings", [{
        "id": "buildings",
        "type": "fill",
        "source": "overture",
        "source-layer": "building",
        "paint": {
            "fill-color": "#8d6e63",
            "fill-opacity": 0.85,
            "fill-outline-color": "#5d4037",
        },
    }], "buildings"),

    "by-subtype": style("Buildings by subtype", [{
        "id": "buildings-subtype",
        "type": "fill",
        "source": "overture",
        "source-layer": "building",
        "paint": {
            "fill-color": (
                ["match", ["get", "subtype"]]
                + [v for st, col in BUILDING_SUBTYPES for v in (st, col)]
                # Fallback: the 98.4% with no subtype. Shown, not hidden.
                + ["#d9d9d9"]
            ),
            "fill-opacity": 0.85,
        },
    }], "buildings"),

    # height is populated on 0.11% of rows; this style is for inspecting the
    # subset that has it, not for a global view.
    "by-height": style("Buildings by height (where known)", [{
        "id": "buildings-height",
        "type": "fill",
        "source": "overture",
        "source-layer": "building",
        "filter": ["has", "height"],
        "paint": {
            "fill-color": [
                "step", ["get", "height"],
                "#fee5d9",
                6, "#fcae91",
                12, "#fb6a4a",
                20, "#de2d26",
                50, "#a50f15",
            ],
            "fill-opacity": 0.9,
        },
    }], "buildings"),
}

# roof_shape values measured in partition 00000 of buildings; parts share the
# vocabulary. Ordered by observed frequency.
PART_ROOFS = [
    ("gabled", "#3182bd"),
    ("flat", "#969696"),
    ("hipped", "#31a354"),
    ("skillion", "#fdae6b"),
    ("pyramidal", "#756bb1"),
    ("half_hipped", "#9ecae1"),
    ("round", "#e6550d"),
    ("dome", "#e7298a"),
    ("saltbox", "#a1d99b"),
    ("onion", "#8c6d31"),
    ("mansard", "#636363"),
]

PART_STYLES = {
    "default": style("Building Parts", [{
        "id": "building-parts",
        "type": "fill",
        "source": "overture",
        "source-layer": "building_part",
        "paint": {
            "fill-color": "#5c6bc0",
            "fill-opacity": 0.85,
            "fill-outline-color": "#303f9f",
        },
    }], "buildings"),

    "by-roof-shape": style("Building Parts by roof shape", [{
        "id": "parts-roof",
        "type": "fill",
        "source": "overture",
        "source-layer": "building_part",
        "paint": {
            "fill-color": (
                ["match", ["get", "roof_shape"]]
                + [v for rs, col in PART_ROOFS for v in (rs, col)]
                + ["#d9d9d9"]
            ),
            "fill-opacity": 0.85,
        },
    }], "buildings"),
}

TITLES = {
    "default": "Default style",
    "by-confidence": "Coloured by confidence score",
    "by-category": "Coloured by basic category",
    "by-subtype": "Coloured by building subtype",
    "by-height": "Coloured by height, where known",
    "by-roof-shape": "Coloured by roof shape",
}

BY_COLLECTION = {
    ("places", "place"): PLACES_STYLES,
    ("buildings", "building"): BUILDING_STYLES,
    ("buildings", "building_part"): PART_STYLES,
}


def main():
    for (theme, tname), styles in BY_COLLECTION.items():
        cdir = OUT / theme / tname
        cpath = cdir / "collection.json"
        coll = json.loads(cpath.read_text())
        assets = coll.setdefault("assets", {})

        for key, doc in styles.items():
            rel = f"styles/{key}.json"
            p = cdir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            body = json.dumps(doc, indent=2) + "\n"
            p.write_text(body)
            raw = body.encode()
            roles = ["style", "default"] if key == "default" else ["style"]
            assets[f"style-{key}" if key != "default" else "style"] = {
                "href": f"./{rel}",
                "type": "application/json",
                "title": TITLES[key],
                "roles": roles,
                "file:size": len(raw),
                "file:checksum": "1220" + hashlib.sha256(raw).hexdigest(),
            }

        cpath.write_text(json.dumps(coll, indent=2) + "\n")
        print(f"  {theme}/{tname:16} {len(styles)} styles")


if __name__ == "__main__":
    main()
