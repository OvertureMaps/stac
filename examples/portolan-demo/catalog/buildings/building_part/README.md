# Building Parts

4.3 million building parts — sub-features that refine a parent building's shape, height, or roof where a single footprint is not enough to describe the structure. A tower on a podium, a wing at a different height, or a distinct roof section each become a part.

Every part references its parent through `building_id`, which joins to `id` in [Buildings](../building/collection.json). The [agent guide](AGENTS.md) carries that join as a runnable query.

This collection is small relative to Buildings — 4.3 million parts against 2.53 billion footprints — because parts exist only where a source described the structure in that much detail, which in practice means predominantly OpenStreetMap-mapped areas.

Field definitions come from the [Overture building_part schema reference](https://docs.overturemaps.org/schema/reference/buildings/building_part/).


## At a glance

| | |
|---|---|
| Rows | 4,339,150 |
| Partitions | 1 |
| Format | GeoParquet 1.1.0 (zstd) |
| CRS | EPSG:4326 (longitude, latitude) |
| Licence | `ODbL-1.0` |
| Release | Overture `2026-08-19.0` |

## Access

The data lives in Overture's own buckets. This catalog does not re-host it.

```bash
# One partition, over HTTPS
duckdb -c "INSTALL httpfs; LOAD httpfs;
  SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building_part/part-00000-18b25a16-1800-5503-8e41-c3f077aa7e2d-c000.zstd.parquet') LIMIT 10;"
```

Every partition is also reachable on Azure and by `s3://` — see the `alternate`
block on any item's `aws` asset.

## Columns

| Column | Type | Description |
|---|---|---|
| `id` | `string` | Stable feature identifier. Where the feature is part of the Global Entity Reference System (GERS), this is its GERS ID and can be used to join Overture releases to each other and to external datasets. |
| `names` | `struct` | All known names for the feature. `primary` is the main name; `common` maps language codes to translations; `rules` carries variants that apply only in a given language, along a linear range, or from a given geopolitical perspective. |
| `height` | `double` | Height of the building or part in metres, measured from its lowest point to its highest point. |
| `min_height` | `double` | Altitude above ground in metres where the bottom of the building or part starts. Non-zero for features such as a skywalk or an overhanging upper storey. |
| `is_underground` | `boolean` | Whether the entire building or part sits completely below ground. |
| `num_floors` | `int32` | Number of above-ground floors of the building or part. |
| `num_floors_underground` | `int32` | Number of below-ground floors of the building or part. |
| `min_floor` | `int32` | Start floor of this building or part. |
| `facade_color` | `string` | Facade colour in `#rgb` or `#rrggbb` hex notation. |
| `facade_material` | `string` | Outer surface material of the facade. |
| `roof_material` | `string` | Outer surface material of the roof. |
| `roof_shape` | `string` | Shape of the roof, such as `flat`, `gabled`, or `hipped`. |
| `roof_direction` | `double` | Bearing of the roof ridge line in degrees clockwise from north. |
| `roof_orientation` | `string` | Orientation of the roof shape relative to the footprint — `along` or `across`. |
| `roof_color` | `string` | Roof colour in `#rgb` or `#rrggbb` hex notation. |
| `roof_height` | `double` | Height of the roof in metres, from the base of the roof to its highest point. |
| `level` | `int32` | Z-order of the feature, where 0 is ground level. Used to resolve draw order where features overlap, such as a bridge over a road. |
| `sources` | `list` | Provenance for the feature: which upstream dataset each property came from, the record id in that dataset, its license, and when it was last updated. `property` is a JSON pointer to the field the entry explains; an empty pointer covers the whole feature. |
| `geometry` | `binary` | Feature geometry as WKB, in EPSG:4326 (longitude, latitude). This is the GeoParquet primary geometry column. |
| `building_id` | `string` | The `building.id` this part belongs to. Every building part is associated with a parent building feature through this field; use it to reassemble a building and its parts. |
| `version` | `int32` | Version of this feature. Incremented by Overture whenever the feature changes between releases. |
| `bbox` | `struct` | Bounding box of the feature geometry, stored as a struct so that readers can filter on it with Parquet row-group statistics before decoding any geometry. |

Column descriptions come from the [Overture schema reference](https://docs.overturemaps.org/schema/reference/);
types are read from the published Parquet footers.

## More

- [Agent guide](AGENTS.md) — query recipes, joins, and the quirks worth knowing
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
- [Overture releases](https://docs.overturemaps.org/release/)
