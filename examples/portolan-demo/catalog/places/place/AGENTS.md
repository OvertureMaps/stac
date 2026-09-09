# Agent guide — Places

## Overview

73,631,092 points of interest in 16 GeoParquet partitions, EPSG:4326. Points,
not footprints.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet') LIMIT 10;
```

## Schema & field notes

Three category fields, increasing in specificity:

- `basic_category` — a coarse, stable vocabulary. Start here.
- `categories.primary` — the single best-fit Overture category, with
  `categories.alternate` for others that also apply.
- `taxonomy.hierarchy` — the full ancestor path, so you can roll up to any level
  without a lookup table.

`brand.wikidata` is the reliable key for grouping chain locations; brand names
vary in spelling and case across sources.

`addresses` is a **list** of structs, not a single struct. Unnest it before
reading `freeform` or `postcode`.

## Data quality & usage notes

**Filter on `confidence`.** Overture keeps low-confidence candidates rather than
dropping them. Measured over partition `00000` (4,599,286 rows), confidence
spreads across the whole range, with the largest band at 0.9. There is no single
correct threshold — 0.8 is a reasonable default for "probably real".

`basic_category` is populated on 96.3% of rows in that partition; the remaining
3.7% are NULL rather than an "unknown" sentinel, so use `IS NOT NULL`.

## Example queries

Confidence distribution:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT round(confidence, 1) AS bucket, count(*) AS n
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet')
GROUP BY 1 ORDER BY bucket;
```

Most common categories among high-confidence places:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT basic_category, count(*) AS n
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet')
WHERE confidence >= 0.8 AND basic_category IS NOT NULL
GROUP BY 1 ORDER BY n DESC LIMIT 20;
```

Places within a bounding box, using the bbox struct so Parquet can skip row
groups (this example is central Mexico City; partition `00000` covers the
Americas, so a London bbox would return nothing):

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, names.primary AS name, basic_category
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet')
WHERE bbox.xmin BETWEEN -99.20 AND -99.10
  AND bbox.ymin BETWEEN 19.40 AND 19.45
LIMIT 20;
```

Unnest addresses:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, names.primary AS name, a.freeform, a.locality, a.country
FROM read_parquet('https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-08-19.0/theme=places/type=place/part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet'), UNNEST(addresses) AS t(a)
WHERE a.country = 'MX'
LIMIT 20;
```

## Related collections

- [Buildings](../../buildings/building/collection.json) — footprints, if you
  need a polygon rather than a point
