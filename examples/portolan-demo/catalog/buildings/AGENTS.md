# Agent guide — Buildings

## Overview

2,529,582,613 footprints and 4,339,150 building parts.

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

Building attributes are sparse — `subtype` is populated on roughly 1.6% of rows. Treat buildings as a geometry product and read the Buildings agent guide before styling or analysing by attribute.

## Example queries

See the collection agent guides; each carries recipes that have been run against
the published data.

## Related collections

- [Buildings](building/collection.json)
- [Building Parts](building_part/collection.json)
