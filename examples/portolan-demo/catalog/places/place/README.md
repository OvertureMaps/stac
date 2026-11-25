# Places

73.6 million points of interest worldwide — businesses, services, landmarks, and other named places — conflated by Overture from multiple open and member-contributed sources.

Each place carries a category (`categories.primary`, with a coarser `basic_category` and a full `taxonomy.hierarchy`), a `confidence` score between 0 and 1, and, where known, `brand`, `addresses`, `websites`, `phones`, and `socials`. Places are point geometries, not footprints; join to [Buildings](../../buildings/building/collection.json) on location if you need a footprint.

**Read `confidence` before you trust a row.** Overture retains low-confidence candidates rather than dropping them, so an unfiltered extract includes places that may not exist. Measured over partition `00000` (4.6 million rows), confidence is spread across the full range, with the largest single band at 0.9. The [agent guide](AGENTS.md) carries the query and a suggested filtering threshold.

Category coverage is good: `basic_category` is populated on roughly 96% of rows in that same partition, led by `restaurant`, `personal_or_beauty_service`, and `fashion_and_apparel_store`.

Field definitions come from the [Overture places schema reference](https://docs.overturemaps.org/schema/reference/places/place/).


## At a glance

| | |
|---|---|
| Rows | 73,631,092 |
| Partitions | 16 |
| Format | GeoParquet 1.1.0 (zstd) |
| CRS | EPSG:4326 (longitude, latitude) |
| Licence | `other` |
| Release | Overture `2026-08-19.0` |

## Access

The data lives in Overture's own buckets. This catalog does not re-host it.

```bash
# One partition, over HTTPS
duckdb -c "INSTALL httpfs; LOAD httpfs;
  SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet') LIMIT 10;"
```

Every partition is also reachable on Azure and by `s3://` — see the `alternate`
block on any item's `aws` asset.

## Columns

| Column | Type | Description |
|---|---|---|
| `id` | `string` | Stable feature identifier. Where the feature is part of the Global Entity Reference System (GERS), this is its GERS ID and can be used to join Overture releases to each other and to external datasets. |
| `geometry` | `binary` | Feature geometry as WKB, in EPSG:4326 (longitude, latitude). This is the GeoParquet primary geometry column. |
| `categories` | `struct` | Overture category classification for the place. `primary` is the single best-fit category; `alternate` holds additional categories that also apply. |
| `confidence` | `double` | Score between 0 and 1 indicating how confident Overture is that the place exists. Filter on this before treating places as ground truth; low-confidence rows are retained rather than dropped. |
| `websites` | `list` | Web addresses for the place. Unique, at least one entry when present. |
| `emails` | `list` | Email addresses for the place. Unique, at least one entry when present. |
| `socials` | `list` | Social media URLs for the place. Unique, at least one entry when present. |
| `phones` | `list` | Telephone numbers for the place. Unique, at least one entry when present. |
| `brand` | `struct` | Brand associated with the place, including its Wikidata QID where known. The Wikidata id is the reliable key for grouping chain locations; brand names vary. |
| `addresses` | `list` | One or more postal addresses for the place. `freeform` carries the street line as published; `country` is an ISO 3166-1 alpha-2 code. |
| `names` | `struct` | All known names for the feature. `primary` is the main name; `common` maps language codes to translations; `rules` carries variants that apply only in a given language, along a linear range, or from a given geopolitical perspective. |
| `sources` | `list` | Provenance for the feature: which upstream dataset each property came from, the record id in that dataset, its license, and when it was last updated. `property` is a JSON pointer to the field the entry explains; an empty pointer covers the whole feature. |
| `operating_status` | `string` | Whether the place is operating, temporarily closed, or permanently closed. |
| `basic_category` | `string` | Intuitive, general-level category for the place — a coarser grouping than `categories.primary`, intended for readers who want a small, stable vocabulary. |
| `taxonomy` | `struct` | Structured representation of the place's category within the Overture taxonomy. `hierarchy` is the full path from the broadest ancestor down to `primary`, which makes it possible to aggregate at any level without a lookup table. |
| `version` | `int32` | Version of this feature. Incremented by Overture whenever the feature changes between releases. |
| `bbox` | `struct` | Bounding box of the feature geometry, stored as a struct so that readers can filter on it with Parquet row-group statistics before decoding any geometry. |

Column descriptions come from the [Overture schema reference](https://docs.overturemaps.org/schema/reference/);
types are read from the published Parquet footers.

## More

- [Agent guide](AGENTS.md) — query recipes, joins, and the quirks worth knowing
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
- [Overture releases](https://docs.overturemaps.org/release/)
