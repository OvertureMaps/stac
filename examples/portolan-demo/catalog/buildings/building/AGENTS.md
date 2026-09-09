# Agent guide — Buildings

## Overview

2,529,582,613 footprints in 512 GeoParquet partitions, EPSG:4326. This is one of
the largest collections Overture publishes; read one partition at a time.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet') LIMIT 10;
```

Globbing all 512 partitions over HTTPS does not work — there is no listing.
Take partition hrefs from the item records, or use `s3://` with AWS credentials
where you want a glob.

## Schema & field notes

`geometry` is the footprint, or the roofprint where it was traced from aerial or
satellite imagery. Those are not the same thing, and Overture does not flag
which you have.

`has_parts` tells you whether [Building Parts](../building_part/collection.json)
exist for this footprint; join on `building_part.building_id = building.id`.

`height` is metres from lowest to highest point. `min_height` is non-zero for
features that start above ground, such as an overhanging storey.

## Data quality & usage notes

**The geometry is the product; the attributes are sparse.** Measured over
39,680,766 rows spanning 8 of 512 partitions (0, 64, 128, 192, 256, 320, 384,
448):

| Column | Populated |
|---|---|
| `subtype` | 1.62% |
| `class` | 1.56% |
| `names` | 0.26% |
| `height` | 0.11% |
| `roof_shape` | 0.027% |

Reproduce it with the coverage query below. Two consequences:

1. A choropleth on `subtype` paints ~98% of the world one colour. The styles in
   this collection carry an explicit unclassified class rather than hiding it.
2. Any analysis conditioned on `height` or `class` is working with a small,
   geographically biased subset — attribute coverage tracks OpenStreetMap
   density, not building density.

Where `subtype` is present it is dominated by `residential`.

## Example queries

Attribute coverage (the query behind the table above, on one partition):

```sql
INSTALL httpfs; LOAD httpfs;
SELECT count(*) AS total,
       round(100.0 * count(subtype)    / count(*), 2) AS subtype_pct,
       round(100.0 * count(class)      / count(*), 2) AS class_pct,
       round(100.0 * count(height)     / count(*), 2) AS height_pct,
       round(100.0 * count(roof_shape) / count(*), 3) AS roof_shape_pct,
       round(100.0 * count(names)      / count(*), 2) AS names_pct
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet');
```

Subtype distribution, excluding the unclassified majority:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT subtype, count(*) AS n
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet')
WHERE subtype IS NOT NULL
GROUP BY 1 ORDER BY n DESC;
```

Buildings with a known height, tallest first:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, height, num_floors, subtype
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet')
WHERE height IS NOT NULL
ORDER BY height DESC LIMIT 20;
```

## Related collections

- [Building Parts](../building_part/collection.json) — joined on `building_id`
- [Places](../../places/place/collection.json)
