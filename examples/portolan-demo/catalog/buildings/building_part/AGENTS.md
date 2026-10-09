# Agent guide — Building Parts

## Overview

4,339,150 building parts in a single GeoParquet partition, EPSG:4326. A part
refines a parent building's shape, height, or roof where one footprint is not
enough to describe the structure.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building_part/part-00000-18b25a16-1800-5503-8e41-c3f077aa7e2d-c000.zstd.parquet') LIMIT 10;
```

The whole collection is one file, so unlike Buildings you can read it directly.

## Schema & field notes

`building_id` references `building.id` in
[Buildings](../building/collection.json). Every part has one.

Parts share the building attribute vocabulary — `height`, `min_height`,
`roof_shape`, `facade_material` — but describe only their own portion of the
structure. A tower on a podium is a part with a high `min_height`.

## Data quality & usage notes

4.3 million parts against 2.53 billion footprints, so fewer than one footprint
in 500 has any. Parts exist where a source described the structure in detail,
which in practice means predominantly OpenStreetMap-mapped areas. Absence of a
part means nobody mapped one, not that the building is simple.

Attribute coverage is far better here than in Buildings, because parts are
almost all human-mapped — see the coverage query below.

## Example queries

Attribute coverage across the whole collection:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT count(*) AS total,
       round(100.0 * count(height)     / count(*), 2) AS height_pct,
       round(100.0 * count(roof_shape) / count(*), 2) AS roof_shape_pct,
       round(100.0 * count(names)      / count(*), 2) AS names_pct
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building_part/part-00000-18b25a16-1800-5503-8e41-c3f077aa7e2d-c000.zstd.parquet');
```

**The join to Buildings.** Parts carry `building_id`; buildings carry `id`. This
reads one building partition and the whole parts file, so scope the building
side to the partition you care about:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT b.id           AS building_id,
       b.height       AS building_height,
       count(*)       AS n_parts,
       max(p.height)  AS tallest_part
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet') b
JOIN read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building_part/part-00000-18b25a16-1800-5503-8e41-c3f077aa7e2d-c000.zstd.parquet') p
  ON p.building_id = b.id
GROUP BY 1, 2
ORDER BY n_parts DESC
LIMIT 20;
```

Roof shapes actually present:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT roof_shape, count(*) AS n
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building_part/part-00000-18b25a16-1800-5503-8e41-c3f077aa7e2d-c000.zstd.parquet')
WHERE roof_shape IS NOT NULL
GROUP BY 1 ORDER BY n DESC;
```

## Related collections

- [Buildings](../building/collection.json) — the parent footprints
