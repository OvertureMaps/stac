# Buildings

2.53 billion building footprints worldwide, conflated by Overture from OpenStreetMap, Esri Community Maps, Google Open Buildings, Microsoft ML Building Footprints, and other sources.

**The geometry is the product; the attributes are sparse.** This is the single most important thing to know before querying or styling this collection. Measured across 8 of 512 partitions (39,680,766 rows — see the [agent guide](AGENTS.md) for the query):

| Column | Populated |
|---|---|
| `subtype` | 1.62% |
| `class` | 1.56% |
| `names` | 0.26% |
| `height` | 0.11% |
| `roof_shape` | 0.027% |

A choropleth on `subtype` therefore paints about 98% of the world in a single "unclassified" colour. The styles shipped here say so rather than hiding it: the default renders footprints as one colour, and the attribute styles carry an explicit unclassified class in the legend. Where `subtype` *is* present it is dominated by `residential`.

Building parts that refine a footprint's shape or height live in [Building Parts](../building_part/collection.json), joined on `building_id`.

Field definitions come from the [Overture buildings schema reference](https://docs.overturemaps.org/schema/reference/buildings/building/).


## At a glance

| | |
|---|---|
| Rows | 2,529,582,613 |
| Partitions | 512 |
| Format | GeoParquet 1.1.0 (zstd) |
| CRS | EPSG:4326 (longitude, latitude) |
| Licence | `ODbL-1.0` |
| Release | Overture `2026-08-19.0` |

## Access

The data lives in Overture's own buckets. This catalog does not re-host it.

```bash
# One partition, over HTTPS
duckdb -c "INSTALL httpfs; LOAD httpfs;
  SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet') LIMIT 10;"
```

Every partition is also reachable on Azure and by `s3://` — see the `alternate`
block on any item's `aws` asset.

## Columns

| Column | Type | Description |
|---|---|---|
| `id` | `string` | Stable feature identifier. Where the feature is part of the Global Entity Reference System (GERS), this is its GERS ID and can be used to join Overture releases to each other and to external datasets. |
| `names` | `struct` | All known names for the feature. `primary` is the main name; `common` maps language codes to translations; `rules` carries variants that apply only in a given language, along a linear range, or from a given geopolitical perspective. |
| `sources` | `list` | Provenance for the feature: which upstream dataset each property came from, the record id in that dataset, its license, and when it was last updated. `property` is a JSON pointer to the field the entry explains; an empty pointer covers the whole feature. |
| `level` | `int32` | Z-order of the feature, where 0 is ground level. Used to resolve draw order where features overlap, such as a bridge over a road. |
| `height` | `double` | Height of the building or part in metres, measured from its lowest point to its highest point. |
| `min_height` | `double` | Altitude above ground in metres where the bottom of the building or part starts. Non-zero for features such as a skywalk or an overhanging upper storey. |
| `is_underground` | `boolean` | Whether the entire building or part sits completely below ground. |
| `num_floors` | `int32` | Number of above-ground floors of the building or part. |
| `num_floors_underground` | `int32` | Number of below-ground floors of the building or part. |
| `min_floor` | `int32` | Start floor of this building or part. |
| `subtype` | `string` | Broad classification of the building's current use and purpose, such as `residential`, `commercial`, or `education`. |
| `class` | `string` | More specific classification of the building's current use and purpose, nested under `subtype` — for example `house` or `apartments` within `residential`. |
| `facade_color` | `string` | Facade colour in `#rgb` or `#rrggbb` hex notation. |
| `facade_material` | `string` | Outer surface material of the facade. |
| `roof_material` | `string` | Outer surface material of the roof. |
| `roof_shape` | `string` | Shape of the roof, such as `flat`, `gabled`, or `hipped`. |
| `roof_direction` | `double` | Bearing of the roof ridge line in degrees clockwise from north. |
| `roof_orientation` | `string` | Orientation of the roof shape relative to the footprint — `along` or `across`. |
| `roof_color` | `string` | Roof colour in `#rgb` or `#rrggbb` hex notation. |
| `roof_height` | `double` | Height of the roof in metres, from the base of the roof to its highest point. |
| `geometry` | `binary` | Feature geometry as WKB, in EPSG:4326 (longitude, latitude). This is the GeoParquet primary geometry column. |
| `has_parts` | `boolean` | Whether this building has associated `building_part` features. Join on `building_part.building_id` to retrieve them. |
| `version` | `int32` | Version of this feature. Incremented by Overture whenever the feature changes between releases. |
| `bbox` | `struct` | Bounding box of the feature geometry, stored as a struct so that readers can filter on it with Parquet row-group statistics before decoding any geometry. |

Column descriptions come from the [Overture schema reference](https://docs.overturemaps.org/schema/reference/);
types are read from the published Parquet footers.

## More

- [Agent guide](AGENTS.md) — query recipes, joins, and the quirks worth knowing
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
- [Overture releases](https://docs.overturemaps.org/release/)
