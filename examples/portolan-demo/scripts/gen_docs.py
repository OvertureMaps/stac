#!/usr/bin/env python3
"""Generate README.md and AGENTS.md for every catalog and collection in demo/.

READMEs are generated from the STAC metadata (Portolan treats them as build
output). AGENTS.md carries hand-written guidance; every query in it is executed
by verify_queries.py before publication.
"""

import json
from pathlib import Path

OUT = Path("demo")

AWS = ("https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/"
       "2026-08-19.0/theme={theme}/type={type}/")

# One real partition href per collection, used in the runnable examples.
SAMPLE = {
    "place": AWS.format(theme="places", type="place")
    + "part-00000-c7e47654-8483-5b8f-b183-7ba73334f7a5-c000.zstd.parquet",
}


def load(p):
    return json.loads(Path(p).read_text())


def sample_href(theme, tname):
    item = load(OUT / theme / tname / "00000" / "00000.json")
    return item["assets"]["aws"]["href"]


def readme_for_collection(theme, tname):
    c = load(OUT / theme / tname / "collection.json")
    href = sample_href(theme, tname)
    rows = c["table:row_count"]
    n_items = sum(1 for l in c["links"] if l["rel"] == "item")

    cols = "\n".join(
        f"| `{col['name']}` | `{col['type'].split('<')[0]}` | {col['description']} |"
        for col in c["table:columns"]
    )

    return f"""# {c['title']}

{c['description']}

## At a glance

| | |
|---|---|
| Rows | {rows:,} |
| Partitions | {n_items} |
| Format | GeoParquet {c.get('geoparquet:version')} (zstd) |
| CRS | EPSG:4326 (longitude, latitude) |
| Licence | `{c['license']}` |
| Release | Overture `2026-08-19.0` |

## Access

The data lives in Overture's own buckets. This catalog does not re-host it.

```bash
# One partition, over HTTPS
duckdb -c "INSTALL httpfs; LOAD httpfs;
  SELECT * FROM read_parquet('{href}') LIMIT 10;"
```

Every partition is also reachable on Azure and by `s3://` — see the `alternate`
block on any item's `aws` asset.

## Columns

| Column | Type | Description |
|---|---|---|
{cols}

Column descriptions come from the [Overture schema reference](https://docs.overturemaps.org/schema/reference/);
types are read from the published Parquet footers.

## More

- [Agent guide](AGENTS.md) — query recipes, joins, and the quirks worth knowing
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
- [Overture releases](https://docs.overturemaps.org/release/)
"""


def readme_for_catalog(path, title, description, children):
    kids = "\n".join(f"- [{t}]({h})" for t, h in children)
    return f"""# {title}

{description}

## Contents

{kids}

## More

- [Agent guide](AGENTS.md)
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
"""


AGENTS_ROOT = """# Agent guide — Overture Places and Buildings (demo)

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
FROM read_parquet('{place_href}')
WHERE confidence >= 0.8;
```

## Related collections

- [Places](places/place/collection.json)
- [Buildings](buildings/building/collection.json)
- [Building Parts](buildings/building_part/collection.json)
"""


AGENTS_PLACE = """# Agent guide — Places

## Overview

73,631,092 points of interest in 16 GeoParquet partitions, EPSG:4326. Points,
not footprints.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('{href}') LIMIT 10;
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
FROM read_parquet('{href}')
GROUP BY 1 ORDER BY bucket;
```

Most common categories among high-confidence places:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT basic_category, count(*) AS n
FROM read_parquet('{href}')
WHERE confidence >= 0.8 AND basic_category IS NOT NULL
GROUP BY 1 ORDER BY n DESC LIMIT 20;
```

Places within a bounding box, using the bbox struct so Parquet can skip row
groups (this example is central Mexico City; partition `00000` covers the
Americas, so a London bbox would return nothing):

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, names.primary AS name, basic_category
FROM read_parquet('{href}')
WHERE bbox.xmin BETWEEN -99.20 AND -99.10
  AND bbox.ymin BETWEEN 19.40 AND 19.45
LIMIT 20;
```

Unnest addresses:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, names.primary AS name, a.freeform, a.locality, a.country
FROM read_parquet('{href}'), UNNEST(addresses) AS t(a)
WHERE a.country = 'MX'
LIMIT 20;
```

## Related collections

- [Buildings](../../buildings/building/collection.json) — footprints, if you
  need a polygon rather than a point
"""


AGENTS_BUILDING = """# Agent guide — Buildings

## Overview

2,529,582,613 footprints in 512 GeoParquet partitions, EPSG:4326. This is one of
the largest collections Overture publishes; read one partition at a time.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('{href}') LIMIT 10;
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
FROM read_parquet('{href}');
```

Subtype distribution, excluding the unclassified majority:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT subtype, count(*) AS n
FROM read_parquet('{href}')
WHERE subtype IS NOT NULL
GROUP BY 1 ORDER BY n DESC;
```

Buildings with a known height, tallest first:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT id, height, num_floors, subtype
FROM read_parquet('{href}')
WHERE height IS NOT NULL
ORDER BY height DESC LIMIT 20;
```

## Related collections

- [Building Parts](../building_part/collection.json) — joined on `building_id`
- [Places](../../places/place/collection.json)
"""


AGENTS_BUILDING_PART = """# Agent guide — Building Parts

## Overview

4,339,150 building parts in a single GeoParquet partition, EPSG:4326. A part
refines a parent building's shape, height, or roof where one footprint is not
enough to describe the structure.

## Accessing the data

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('{href}') LIMIT 10;
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
FROM read_parquet('{href}');
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
FROM read_parquet('{building_href}') b
JOIN read_parquet('{href}') p
  ON p.building_id = b.id
GROUP BY 1, 2
ORDER BY n_parts DESC
LIMIT 20;
```

Roof shapes actually present:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT roof_shape, count(*) AS n
FROM read_parquet('{href}')
WHERE roof_shape IS NOT NULL
GROUP BY 1 ORDER BY n DESC;
```

## Related collections

- [Buildings](../building/collection.json) — the parent footprints
"""


AGENTS_THEME = """# Agent guide — {title}

## Overview

{description}

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

{quality}

## Example queries

See the collection agent guides; each carries recipes that have been run against
the published data.

## Related collections

{related}
"""


def main():
    place_href = sample_href("places", "place")
    building_href = sample_href("buildings", "building")
    part_href = sample_href("buildings", "building_part")

    # Root
    (OUT / "README.md").write_text(readme_for_catalog(
        OUT, "Overture Places and Buildings",
        load(OUT / "catalog.json")["description"],
        [("Places", "places/catalog.json"), ("Buildings", "buildings/catalog.json")],
    ))
    (OUT / "AGENTS.md").write_text(AGENTS_ROOT.format(place_href=place_href))

    # Themes
    (OUT / "places" / "README.md").write_text(readme_for_catalog(
        OUT / "places", "Places", load(OUT / "places" / "catalog.json")["description"],
        [("Places", "place/collection.json")],
    ))
    (OUT / "places" / "AGENTS.md").write_text(AGENTS_THEME.format(
        title="Places",
        description="A single collection of 73,631,092 points of interest worldwide.",
        quality="Places carry a `confidence` score between 0 and 1. Overture retains "
                "low-confidence candidates, so filter before treating places as ground truth.",
        related="- [Places](place/collection.json)",
    ))

    (OUT / "buildings" / "README.md").write_text(readme_for_catalog(
        OUT / "buildings", "Buildings", load(OUT / "buildings" / "catalog.json")["description"],
        [("Buildings", "building/collection.json"),
         ("Building Parts", "building_part/collection.json")],
    ))
    (OUT / "buildings" / "AGENTS.md").write_text(AGENTS_THEME.format(
        title="Buildings",
        description="2,529,582,613 footprints and 4,339,150 building parts.",
        quality="Building attributes are sparse — `subtype` is populated on roughly 1.6% of "
                "rows. Treat buildings as a geometry product and read the Buildings agent "
                "guide before styling or analysing by attribute.",
        related="- [Buildings](building/collection.json)\n"
                "- [Building Parts](building_part/collection.json)",
    ))

    # Collections
    specs = [
        ("places", "place", AGENTS_PLACE.format(href=place_href)),
        ("buildings", "building", AGENTS_BUILDING.format(href=building_href)),
        ("buildings", "building_part",
         AGENTS_BUILDING_PART.format(href=part_href, building_href=building_href)),
    ]
    for theme, tname, agents in specs:
        d = OUT / theme / tname
        (d / "README.md").write_text(readme_for_collection(theme, tname))
        (d / "AGENTS.md").write_text(agents)

    n = len(list(OUT.rglob("README.md"))) + len(list(OUT.rglob("AGENTS.md")))
    print(f"wrote {n} markdown files")


if __name__ == "__main__":
    main()
