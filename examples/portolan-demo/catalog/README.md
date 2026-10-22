# Overture Places and Buildings

A Portolan-conformant view of two themes from the [Overture Maps Foundation](https://overturemaps.org/) `2026-08-19.0` release: **Places** (73.6 million points of interest) and **Buildings** (2.53 billion footprints, plus 4.3 million building parts).

This is a **demonstration catalog**. It re-describes data that Overture already publishes at [stac.overturemaps.org](https://stac.overturemaps.org/) — it does not copy, re-host, or modify any of it. Every asset href points at Overture's own buckets on AWS and Azure. The purpose is to show what the Overture STAC catalog looks like once it carries documented columns, providers, styles, and agent guides, so the Overture community can judge whether adopting the [Portolan profile](https://github.com/portolan-sdi/portolan-spec) is worth doing for the full catalog.

Start at the [agent guide](AGENTS.md) if you are querying this programmatically; it carries the DuckDB recipes and the join between buildings and building parts.

**Data currency.** Metadata describes the Overture `2026-08-19.0` release. Overture publishes monthly, so a newer release probably exists; see [Overture releases](https://docs.overturemaps.org/release/).

**Licensing.** Licensing differs per theme and is stated on each collection. See [Overture attribution and licensing](https://docs.overturemaps.org/attribution/) for the terms that govern reuse.


## Contents

- [Places](places/catalog.json)
- [Buildings](buildings/catalog.json)

## More

- [Agent guide](AGENTS.md)
- [Overture attribution and licensing](https://docs.overturemaps.org/attribution/)
