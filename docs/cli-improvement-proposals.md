# CLI improvement proposals

> Deep research pass on how to level up `overture-stac` (Rust branch), grounded in the current STAC ecosystem (2026), how Overture ships data, and gaps in the existing Rust output. Uncommitted working doc — not authoritative until reviewed.

## The strategic frame

Overture "fully embraced STAC" in Feb 2026 (their words). The catalog is now load-bearing infrastructure: Python client, Explorer, internal QA, data pipelines all read it. That changes what the CLI should optimize for. Today's CLI ships **a browse tree**. The direction that would compound value across every downstream is **turn the catalog into a queryable execution surface**: something you point DuckDB at, something whose JSON is directly runnable, something whose CLI can subset the data it advertises.

Three flagship features fall out of this frame. Each one has independent evidence, each one closes a real hole we can point to.

---

## Flagship 1 — Catalog-wide `catalog.parquet` (stac-geoparquet index)

**What.** Emit one geoparquet at the catalog root containing every STAC Item across every release, plus columns `release_version` / `theme` / `type`. Reference it from the root catalog with `assets["catalog-mirror"]` (role `collection-mirror`, media type `application/vnd.apache.parquet` — the spec's convention).

**Why it works.** Planetary Computer already does this per-collection for bulk queries against millions of items. Overture is 3–4 orders of magnitude smaller: ~996 items per release × ~30 kept releases → ~30k rows → ~30–60 MB compressed. That is squarely in DuckDB's happy path. Dev Seed's "Right-Sizing STAC" benchmark shows stac-fastapi-geoparquet outperforms pgstac at ≤100k items — Overture is well under.

**What it unlocks.** Today, "which Overture parquet fragments intersect this bbox in release X" requires walking JSON, or shelling out to overturemaps-py, or hand-writing DuckDB against the data buckets. With one catalog.parquet it becomes:

```sql
SELECT array_agg(assets['aws'].href)
FROM read_parquet('https://stac.overturemaps.org/catalog.parquet')
WHERE release_version = '2026-08-19.0'
  AND theme = 'buildings'
  AND bbox_intersects(bbox, [-0.1, 51.5, -0.05, 51.55]);
```

**Cost.** Low. The `stac` crate already writes geoparquet via `ItemCollection::into_geoparquet_path`. The pieces are already assembled in `build_top_catalog` — every child release has typed items reachable in memory. This is a fan-in + one geoparquet write.

**Not-a-castle check.** The existing per-release `collections.parquet` at `src/stac/catalog.rs:577` proves the write path works; the stac-geoparquet spec explicitly recommends this pattern for "querying the whole dataset instead of reading every specific GeoJSON file." Adversarial concern: file grows unbounded. Answer: bound to N releases matching the catalog's own retention (currently N≈30). Also: cheap to rebuild — computed at reconcile time.

---

## Flagship 2 — Executable DuckDB macros baked into the catalog

**What.** Add a `duckdb` extension field to every catalog level with runnable SQL. On the root: view-per-type over `latest`. On a release catalog: view-per-type pinned to that release. On a collection: a canonical bbox-filtered example.

```jsonc
// stac.overturemaps.org/catalog.json
{
  "duckdb": {
    "install": "INSTALL spatial; INSTALL httpfs; LOAD spatial; LOAD httpfs;",
    "macros": "CREATE OR REPLACE VIEW building AS SELECT * FROM read_parquet('s3://overturemaps-us-west-2/release/2026-08-19.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1); …"
  }
}
```

**Why it works.** This is literally issue [#67 on this repo](https://github.com/OvertureMaps/stac/issues/67). The proposal there is:

```bash
curl https://stac.overturemaps.org/catalog.json | jq -r .duckdb | duckdb overture.db -c -
# then: SELECT * FROM place;
```

It replaces the `labs.overturemaps.org/data/latest.ddb` sidecar artifact that has to be regenerated out-of-band. Since we're rebuilding the catalog anyway, this is free maintenance.

**Cost.** Trivial. String templating over the same theme/type list `build_top_catalog` already walks. `latest` is already tracked on the root — just formats the paths differently for `latest` vs pinned releases.

**Not-a-castle check.** The `catalog.json` `duckdb` field is a non-standard extension. Two options: (a) namespace it under `x:duckdb` and skip declaring an extension; (b) publish `OvertureMaps/stac-duckdb-extension` as a proper STAC extension. The user-facing behavior is identical; (b) is the "do it right" version and can follow (a).

---

## Flagship 3 — `overture-stac query` — one-shot bbox subset over the catalog

**What.** New subcommand:

```bash
overture-stac query --bbox '-0.1,51.5,-0.05,51.55' \
  --theme buildings --type building \
  --release latest \
  --output london_buildings.parquet
```

Uses the STAC catalog's item-level bboxes to filter which fragments to fetch, streams the row-group-level bbox filter down to `parquet::arrow`, and writes out. Same infrastructure as `build`, just consuming instead of producing.

**Why it works.** The most common Overture user workflow *today* is "give me X in this bbox" — every tutorial (Pukar Bhandari, PyPI's overturemapsdownloader, Overture docs' quickstart) is a variation of "install DuckDB, load spatial, load httpfs, write bbox SQL, wait, export." Bundling that into the STAC CLI collapses three tools' worth of setup into one binary. It also gives the STAC catalog a first-party consumer — proving the metadata is enough to drive real workloads, which is the strongest possible advertisement for the catalog itself.

**Why it's a natural fit here.** The current `build_single_release` already reads every fragment's bbox from parquet metadata (`src/storage/parquet.rs:read_fragment`). Query is that read path in reverse: read STAC bbox → filter fragments → read matching row groups. The concurrency scaffolding (`process_themes_parallel`), the object-store glue, and the bounded fragment reader are all in place.

**Cost.** Medium. Naive first cut (download matching fragments whole) is small. Row-group push-down and multi-format output (parquet + GeoJSONSeq + FlatGeobuf) are follow-ups. Ship the naive cut first.

**Not-a-castle check.** Adversarial concern: reimplementing DuckDB spatial in Rust is a losing battle. Answer: we don't. We're doing coarse fragment-level bbox filter (which STAC already indexed) + `parquet::arrow` row-group pruning (which the parquet crate already does). Anything more granular, the user runs DuckDB. Second concern: output format sprawl. Answer: ship parquet only in v1; formats behind `--format` flag as demand shows.

---

## Second-tier — high value, cheap, ship alongside

### Bugs / gaps to fix while you're in there

- **`collections.parquet` isn't spec-conformant and has an empty-file bug.** Today `src/stac/catalog.rs:577` writes one giant per-release parquet with items from all themes/types mixed, and writes a *zero-byte file* when empty (`b""`). The stac-geoparquet spec says one parquet **per collection** with homogeneous items and warns explicitly that mixing collections wastes space. Rewrite as one parquet per (theme, type), placed at `<release>/<theme>/<type>/items.parquet`, referenced from the Collection via `assets["collection-mirror"]`. Empty case: write a real 0-row parquet with the correct schema, not a zero-byte file. **Reason to do it now:** flagship 1's catalog-wide parquet reuses the same encoder; harmonizing shapes means one code path.
- **Fix walk-all `schema:version` (#115).** The Python bug — writing `"schema:version": null` and `"schema:tag": ".../vNone"` — is *partially* fixed in Rust (walk-all omits the field via `src/stac/catalog.rs:127`'s `!schema.is_empty()` guard). But the field is silently missing on every release built via the CLI walk-all path. The `src/python.rs` bindings already auto-infer from the live catalog via `fetch_schema_version`. Call the same path from the CLI. Bootstrap concern (old releases with `null`): one-time backfill by scraping Overture's own release-notes blog posts (`docs.overturemaps.org/blog/YYYY/MM/DD/release-notes/`) which include schema version in every post.
- **Wire `reconcile` into `publish-catalog.yaml` to close #109.** The reconcile diff *is* the cheap pre-check that issue asks for. Prod today does a full rebuild every 6 hours. `reconcile` (no `--apply`) exits 0 in-sync / 1 drift — perfect gate. `reconcile --apply` is the actual publish. Old walk-all becomes a break-glass rebuild.
- **`--extras-uri ""` sentinel is fragile.** `src/main.rs:180` uses empty-string-means-none. That's the kind of API that gets people. Prefer `--no-extras` boolean or `Option<String>` with clap.

### Provenance and validation

- **STAC `processing:*` extension on release catalogs.** `processing:software = "overture-stac"`, `processing:version = <cargo pkg version>`, `processing:datetime = <build time>`. The `vcs:*` extension you already stamp is complementary — one is *where the code lives*, the other is *what actually ran*. Cheap; audit gold.
- **STAC `file:*` extension on assets.** `file:size` + `file:checksum:sha256` on each parquet asset. The fragment reader already knows the size (from `head()`); checksums require an extra pass we might defer. Sizes alone unlock "how much am I about to download?" in every consumer.
- **`stac-check` inline validation.** Add `overture-stac validate <dir>` and run it after `build` behind `--validate`. Today validation happens in `stac-check-action` in a separate CI step. Inlining catches errors before they land in the artifact and lets library callers verify without a separate tool. No native Rust equivalent exists, so a first cut can shell out; a native reimplementation of the rules we care about (link integrity, media types, required fields) is a couple hundred lines.

### Overture-specific value

- **Cross-release changelog surface.** Overture already publishes a data-changelog as parquet (partitioned by theme/type/change_type, with `columns_changed`) — landed 2026-03-18. Nobody currently discovers it. Add `assets["changelog"]` to each release catalog linking to Overture's own changelog parquet for that release. Zero content to compute — just link out. Downstream: "which places changed between 2026-07-22.0 and 2026-08-19.0" becomes a DuckDB query the catalog itself directed you to.
- **Schema-change fingerprint on Collections.** Hash the `table:columns` per (theme,type,release) and surface `schema:fingerprint`. Consumers can then diff two release manifests without walking columns. Would have caught the `columns_changed` moves cleanly.

### DX / operational

- **Progress bars for `build`.** With ~1000 items and 30s+ builds, silent runs feel broken. `indicatif` crate integrates with `futures::stream::buffer_unordered`.
- **`--json` summary at end of `build` / `reconcile`.** Machine-readable exit — item counts, byte counts, timings. `publish-catalog.yaml` can then emit metrics.
- **Broaden Python surface.** `src/python.rs` only exposes `build_catalog`. Add `list_releases`, `read_fragment_bbox`, `diff`. Ties into the "STAC as backbone" story — internal Overture services should be able to reach into these primitives without shelling out. That's the intent of [#99](https://github.com/OvertureMaps/stac/issues/99).

---

## Things looked at and would NOT recommend (or defer)

- **STAC API server (stac-fastapi-geoparquet).** Would rehost the same data behind an HTTP API. Real value at planet-scale multi-catalog federation; overkill for one catalog with 30 releases. If someone wants search, the catalog.parquet + DuckDB (flagship 1) covers 95% of the ask at 1% of the ops.
- **Streaming upload (README's TODO).** Nice but the current "build locally then sync" pattern is intentional — it lets stac-check validate before publish. Don't rip that seam without a validator that can run over remote catalogs.
- **HTML sidecars (#79).** Stac-browser handles it hosted; making the catalog carry its own HTML doubles the artifact size for a UX layer that already exists.
- **Portolan registration (#92).** External registration, not a CLI feature.

---

## Risks and caveats

- **stac-geoparquet spec is pre-1.0.** The `collection-mirror` role and the parquet metadata layout could shift. Track [radiantearth/stac-geoparquet-spec](https://github.com/radiantearth/stac-geoparquet-spec) and pin the crate. Mitigation: the spec has been stable enough for Planetary Computer to run production on it; small risk.
- **The `duckdb` field is bespoke.** Ship as `x:duckdb` (informal extension) until formalized. Downstream tools ignoring unknown fields is standard STAC behavior — no breakage.
- **Query subcommand overlaps overturemaps-py.** Own it explicitly: this is the Rust-native path with zero-install (single binary). Different distribution story. Not competing on features.
- **`table:columns` fingerprint changes need care with STAC 1.1 vs 1.0 field ordering.** Don't hash raw JSON — hash a normalized (sorted, typed) representation.

---

## Suggested first slice

If picking one week's work that lands the biggest step-change and sets up the flagships:

1. **Fix collections.parquet** — one per collection, no zero-byte files, `collection-mirror` asset. (Precursor for everything geoparquet-shaped.)
2. **Emit `catalog.parquet`** at the top level, hook it as `assets["catalog-mirror"]` on the root. (Flagship 1 landed.)
3. **Add the `duckdb` field on root + per release + per collection.** (Flagship 2 landed, closes #67.)
4. **Wire `reconcile --apply` as the publish step in `publish-catalog.yaml`.** (Closes #109.)
5. **Fix walk-all `schema:version` in the CLI** by calling the same `fetch_schema_version` the Python bindings already use. (Closes #115.)

That's ~2-3 net-new files in the Rust codebase, mostly touching `src/stac/catalog.rs` and adding one `src/stac/duckdb.rs`. Everything above builds on primitives already in place.

`overture-stac query` (flagship 3) is the next milestone after that lands — it's the biggest new user surface and deserves its own iteration.

---

## Sources

- [Right-sizing STAC — Development Seed](https://developmentseed.org/blog/2025-05-07-stac-geoparquet/)
- [stac-geoparquet specification](https://radiantearth.github.io/stac-geoparquet-spec/latest/)
- [Bulk STAC item queries with GeoParquet — Planetary Computer](https://planetarycomputer.microsoft.com/docs/quickstarts/stac-geoparquet/)
- [Overture Has Fully Embraced STAC — Overture blog, Feb 2026](https://docs.overturemaps.org/blog/2026/02/11/stac/)
- [Overture data changelog with `columns_changed`](https://docs.overturemaps.org/gers/changelog/)
- [STAC Processing extension](https://github.com/stac-extensions/processing)
- [stac-check](https://stac-utils.github.io/stac-check/)
- [Issue #67 — Embed DuckDB SQL macros](https://github.com/OvertureMaps/stac/issues/67)
- [Issue #109 — Skip publish rebuild when unchanged](https://github.com/OvertureMaps/stac/issues/109)
- [Issue #115 — schema:version null bug](https://github.com/OvertureMaps/stac/issues/115)
- [Issue #99 — Callable as library](https://github.com/OvertureMaps/stac/issues/99)
