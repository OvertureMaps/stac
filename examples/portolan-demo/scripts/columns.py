"""Column metadata for the Overture places+buildings Portolan demo catalog.

Every `description` is quoted or condensed from the Overture schema reference at
https://docs.overturemaps.org/schema/reference/ (tier: attested).
Every `type` is read from the published Parquet footer with DuckDB `DESCRIBE`
(tier: derived; see AGENTS.md for the query).

SOURCES:
  places/place            https://docs.overturemaps.org/schema/reference/places/place/
  buildings/building      https://docs.overturemaps.org/schema/reference/buildings/building/
  buildings/building_part https://docs.overturemaps.org/schema/reference/buildings/building_part/
"""

# Shared across all Overture feature types.
_COMMON = {
    "id": (
        "string",
        "Stable feature identifier. Where the feature is part of the Global Entity "
        "Reference System (GERS), this is its GERS ID and can be used to join Overture "
        "releases to each other and to external datasets.",
    ),
    "geometry": (
        "binary",
        "Feature geometry as WKB, in EPSG:4326 (longitude, latitude). This is the "
        "GeoParquet primary geometry column.",
    ),
    "bbox": (
        "struct<xmin: double, xmax: double, ymin: double, ymax: double>",
        "Bounding box of the feature geometry, stored as a struct so that readers can "
        "filter on it with Parquet row-group statistics before decoding any geometry.",
    ),
    "version": (
        "int32",
        "Version of this feature. Incremented by Overture whenever the feature changes "
        "between releases.",
    ),
    "sources": (
        "list<struct<property: string, dataset: string, license: string, record_id: string, "
        "update_time: string, confidence: double, between: list<double>, provider: string, "
        "resource: string, version: string>>",
        "Provenance for the feature: which upstream dataset each property came from, the "
        "record id in that dataset, its license, and when it was last updated. `property` "
        "is a JSON pointer to the field the entry explains; an empty pointer covers the "
        "whole feature.",
    ),
    "names": (
        "struct<primary: string, common: map<string, string>, "
        "rules: list<struct<variant: string, language: string, "
        "perspectives: struct<mode: string, countries: list<string>>, value: string, "
        "between: list<double>, side: string>>>",
        "All known names for the feature. `primary` is the main name; `common` maps "
        "language codes to translations; `rules` carries variants that apply only in a "
        "given language, along a linear range, or from a given geopolitical perspective.",
    ),
    "level": (
        "int32",
        "Z-order of the feature, where 0 is ground level. Used to resolve draw order "
        "where features overlap, such as a bridge over a road.",
    ),
}

PLACES = {
    **{k: _COMMON[k] for k in ("id", "geometry", "bbox", "version", "sources", "names")},
    "categories": (
        "struct<primary: string, alternate: list<string>>",
        "Overture category classification for the place. `primary` is the single best-fit "
        "category; `alternate` holds additional categories that also apply.",
    ),
    "basic_category": (
        "string",
        "Intuitive, general-level category for the place — a coarser grouping than "
        "`categories.primary`, intended for readers who want a small, stable vocabulary.",
    ),
    "taxonomy": (
        "struct<primary: string, hierarchy: list<string>, alternates: list<string>>",
        "Structured representation of the place's category within the Overture taxonomy. "
        "`hierarchy` is the full path from the broadest ancestor down to `primary`, which "
        "makes it possible to aggregate at any level without a lookup table.",
    ),
    "confidence": (
        "double",
        "Score between 0 and 1 indicating how confident Overture is that the place exists. "
        "Filter on this before treating places as ground truth; low-confidence rows are "
        "retained rather than dropped.",
    ),
    "operating_status": (
        "string",
        "Whether the place is operating, temporarily closed, or permanently closed.",
    ),
    "websites": ("list<string>", "Web addresses for the place. Unique, at least one entry when present."),
    "socials": ("list<string>", "Social media URLs for the place. Unique, at least one entry when present."),
    "emails": ("list<string>", "Email addresses for the place. Unique, at least one entry when present."),
    "phones": ("list<string>", "Telephone numbers for the place. Unique, at least one entry when present."),
    "brand": (
        "struct<wikidata: string, names: struct<primary: string, common: map<string, string>, "
        "rules: list<struct<variant: string, language: string, "
        "perspectives: struct<mode: string, countries: list<string>>, value: string, "
        "between: list<double>, side: string>>>>",
        "Brand associated with the place, including its Wikidata QID where known. The "
        "Wikidata id is the reliable key for grouping chain locations; brand names vary.",
    ),
    "addresses": (
        "list<struct<freeform: string, locality: string, postcode: string, region: string, "
        "country: string>>",
        "One or more postal addresses for the place. `freeform` carries the street line as "
        "published; `country` is an ISO 3166-1 alpha-2 code.",
    ),
}

_BUILDING_SHARED = {
    "height": (
        "double",
        "Height of the building or part in metres, measured from its lowest point to its "
        "highest point.",
    ),
    "min_height": (
        "double",
        "Altitude above ground in metres where the bottom of the building or part starts. "
        "Non-zero for features such as a skywalk or an overhanging upper storey.",
    ),
    "roof_height": (
        "double",
        "Height of the roof in metres, from the base of the roof to its highest point.",
    ),
    "is_underground": (
        "boolean",
        "Whether the entire building or part sits completely below ground.",
    ),
    "num_floors": ("int32", "Number of above-ground floors of the building or part."),
    "num_floors_underground": ("int32", "Number of below-ground floors of the building or part."),
    "min_floor": ("int32", "Start floor of this building or part."),
    "facade_color": ("string", "Facade colour in `#rgb` or `#rrggbb` hex notation."),
    "facade_material": ("string", "Outer surface material of the facade."),
    "roof_material": ("string", "Outer surface material of the roof."),
    "roof_shape": ("string", "Shape of the roof, such as `flat`, `gabled`, or `hipped`."),
    "roof_direction": ("double", "Bearing of the roof ridge line in degrees clockwise from north."),
    "roof_orientation": (
        "string",
        "Orientation of the roof shape relative to the footprint — `along` or `across`.",
    ),
    "roof_color": ("string", "Roof colour in `#rgb` or `#rrggbb` hex notation."),
}

BUILDING = {
    **{k: _COMMON[k] for k in ("id", "geometry", "bbox", "version", "sources", "names", "level")},
    **_BUILDING_SHARED,
    "subtype": (
        "string",
        "Broad classification of the building's current use and purpose, such as "
        "`residential`, `commercial`, or `education`.",
    ),
    "class": (
        "string",
        "More specific classification of the building's current use and purpose, nested "
        "under `subtype` — for example `house` or `apartments` within `residential`.",
    ),
    "has_parts": (
        "boolean",
        "Whether this building has associated `building_part` features. Join on "
        "`building_part.building_id` to retrieve them.",
    ),
}

BUILDING_PART = {
    **{k: _COMMON[k] for k in ("id", "geometry", "bbox", "version", "sources", "names", "level")},
    **_BUILDING_SHARED,
    "building_id": (
        "string",
        "The `building.id` this part belongs to. Every building part is associated with a "
        "parent building feature through this field; use it to reassemble a building and "
        "its parts.",
    ),
}

BY_COLLECTION = {
    "place": PLACES,
    "building": BUILDING,
    "building_part": BUILDING_PART,
}
