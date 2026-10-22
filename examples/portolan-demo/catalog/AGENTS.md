# Agent guide — Overture Places and Buildings (demo)

## Overview

A Portolan-conformant **demonstration** view of two themes from the Overture
`2026-08-19.0` release. The metadata is new; the data is Overture's own and is
read directly from their buckets. Nothing here is re-hosted.

- **Places** — 73,631,092 points, 16 partitions
- **Buildings** — 2,529,582,613 footprints, 512 partitions
- **Building Parts** — 4,339,150 parts, 1 partition

## Accessing the data

All assets are zstd-compressed GeoParquet in EPSG:4326. Use DuckDB with
`httpfs`; read a single partition rather than globbing unless you mean it.

```sql
INSTALL httpfs; LOAD httpfs;
```

Each item carries `aws` and `azure` assets over HTTPS, plus an `s3://`
alternate under `aws.alternate.s3`.

## Schema & field notes

`geometry` is WKB. `bbox` is a struct of `xmin/xmax/ymin/ymax`, which is what
you want for cheap spatial filtering: it lets Parquet skip row groups before
decoding any geometry.

`id` is the GERS identifier where the feature participates in the Global Entity
Reference System, which is what makes joins across Overture releases stable.

## Data quality & usage notes

**Buildings attributes are sparse.** Measured over 39,680,766 rows spanning 8 of
512 partitions: `subtype` 1.62%, `class` 1.56%, `names` 0.26%, `height` 0.11%,
`roof_shape` 0.027%. Treat buildings as a geometry product. See the
[Buildings agent guide](buildings/building/AGENTS.md).

**Places carry a confidence score.** Overture retains low-confidence candidates,
so filter on `confidence` before treating places as ground truth. See the
[Places agent guide](places/place/AGENTS.md).

## Example queries

Count places above a confidence threshold in one partition:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT count(*) AS confident_places
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet')
WHERE confidence >= 0.8;
```

## Related collections

- [Places](places/place/collection.json)
- [Buildings](buildings/building/collection.json)
- [Building Parts](buildings/building_part/collection.json)
