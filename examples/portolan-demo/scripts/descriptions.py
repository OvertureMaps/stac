"""Prose for the demo catalog.

Provenance for every claim:
  attested  — Overture schema reference and attribution pages, linked inline.
  derived   — measured with DuckDB against the published Parquet; the query is
              in the relevant AGENTS.md and the sample size is stated inline.
Nothing here is asserted without one of those two backings.
"""

ROOT_DESCRIPTION = """\
A Portolan-conformant view of two themes from the [Overture Maps Foundation](https://overturemaps.org/) \
`2026-08-19.0` release: **Places** (73.6 million points of interest) and **Buildings** \
(2.53 billion footprints, plus 4.3 million building parts).

This is a **demonstration catalog**. It re-describes data that Overture already publishes at \
[stac.overturemaps.org](https://stac.overturemaps.org/) — it does not copy, re-host, or modify \
any of it. Every asset href points at Overture's own buckets on AWS and Azure. The purpose is to \
show what the Overture STAC catalog looks like once it carries documented columns, providers, \
styles, and agent guides, so the Overture community can judge whether adopting the \
[Portolan profile](https://github.com/portolan-sdi/portolan-spec) is worth doing for the full catalog.

Start at the [agent guide](AGENTS.md) if you are querying this programmatically; it carries the \
DuckDB recipes and the join between buildings and building parts.

**Data currency.** Metadata describes the Overture `2026-08-19.0` release. Overture publishes \
monthly, so a newer release probably exists; see [Overture releases](https://docs.overturemaps.org/release/).

**Licensing.** Licensing differs per theme and is stated on each collection. See \
[Overture attribution and licensing](https://docs.overturemaps.org/attribution/) for the terms \
that govern reuse.
"""

COLLECTION_DESCRIPTIONS = {
    "place": """\
73.6 million points of interest worldwide — businesses, services, landmarks, and other named \
places — conflated by Overture from multiple open and member-contributed sources.

Each place carries a category (`categories.primary`, with a coarser `basic_category` and a full \
`taxonomy.hierarchy`), a `confidence` score between 0 and 1, and, where known, `brand`, \
`addresses`, `websites`, `phones`, and `socials`. Places are point geometries, not footprints; \
join to [Buildings](../../buildings/building/collection.json) on location if you need a footprint.

**Read `confidence` before you trust a row.** Overture retains low-confidence candidates rather \
than dropping them, so an unfiltered extract includes places that may not exist. Measured over \
partition `00000` (4.6 million rows), confidence is spread across the full range, with the \
largest single band at 0.9. The [agent guide](AGENTS.md) carries the query and a suggested \
filtering threshold.

Category coverage is good: `basic_category` is populated on roughly 96% of rows in that same \
partition, led by `restaurant`, `personal_or_beauty_service`, and `fashion_and_apparel_store`.

Field definitions come from the [Overture places schema reference](https://docs.overturemaps.org/schema/reference/places/place/).
""",

    "building": """\
2.53 billion building footprints worldwide, conflated by Overture from OpenStreetMap, Esri \
Community Maps, Google Open Buildings, Microsoft ML Building Footprints, and other sources.

**The geometry is the product; the attributes are sparse.** This is the single most important \
thing to know before querying or styling this collection. Measured across 8 of 512 partitions \
(39,680,766 rows — see the [agent guide](AGENTS.md) for the query):

| Column | Populated |
|---|---|
| `subtype` | 1.62% |
| `class` | 1.56% |
| `names` | 0.26% |
| `height` | 0.11% |
| `roof_shape` | 0.027% |

A choropleth on `subtype` therefore paints about 98% of the world in a single "unclassified" \
colour. The styles shipped here say so rather than hiding it: the default renders footprints as \
one colour, and the attribute styles carry an explicit unclassified class in the legend. Where \
`subtype` *is* present it is dominated by `residential`.

Building parts that refine a footprint's shape or height live in \
[Building Parts](../building_part/collection.json), joined on `building_id`.

Field definitions come from the [Overture buildings schema reference](https://docs.overturemaps.org/schema/reference/buildings/building/).
""",

    "building_part": """\
4.3 million building parts — sub-features that refine a parent building's shape, height, or roof \
where a single footprint is not enough to describe the structure. A tower on a podium, a wing at \
a different height, or a distinct roof section each become a part.

Every part references its parent through `building_id`, which joins to `id` in \
[Buildings](../building/collection.json). The [agent guide](AGENTS.md) carries that join as a \
runnable query.

This collection is small relative to Buildings — 4.3 million parts against 2.53 billion \
footprints — because parts exist only where a source described the structure in that much \
detail, which in practice means predominantly OpenStreetMap-mapped areas.

Field definitions come from the [Overture building_part schema reference](https://docs.overturemaps.org/schema/reference/buildings/building_part/).
""",
}
