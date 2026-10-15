# Agent guide — Places

## Overview

A single collection of 73,631,092 points of interest worldwide.

## Accessing the data

Every collection below is zstd-compressed GeoParquet in EPSG:4326, read directly
from Overture's buckets with DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
```

## Schema & field notes

Per-collection field notes live in each collection's own agent guide, linked
below. Fields shared across all Overture themes — `id`, `geometry`, `bbox`,
`version`, `sources`, `names` — are documented in `table:columns` on every
collection.

## Data quality & usage notes

Places carry a `confidence` score between 0 and 1. Overture retains low-confidence candidates, so filter before treating places as ground truth.

## Example queries

See the collection agent guides; each carries recipes that have been run against
the published data.

## Related collections

- [Places](place/collection.json)
