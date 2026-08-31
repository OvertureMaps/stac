# Portolan demo catalog

**This is a worked example for discussion, not something to merge.** It exists so the
Overture community can look at a concrete artifact while deciding whether the
[Portolan profile](https://github.com/portolan-sdi/portolan-spec) is worth adopting for
`stac.overturemaps.org`. See #92, where the question was first raised.

Portolan is a profile layered on top of STAC — the same JSON, with stricter rules about
what a catalog has to document before it counts as usable. `rashid` is its validator.
Neither is a STAC validator; our catalog already passes STAC validation today.

## What this shows

Three collections — `places/place`, `buildings/building`, `buildings/building_part` — from
release `2026-08-19.0`, re-described to meet the profile.

It **re-describes** Overture's data. It does not copy, re-host, or modify any of it. Every
asset href points at Overture's own AWS and Azure buckets.

```
$ rashid check --no-data --summary catalog/catalog.json
3 error(s), 28 warning(s), 0 info(s) across 13 files
```

The same three collections as published today report **565 errors**. The 3 remaining are
`PTL-VIZ-001`: each collection wants a rendered PNG thumbnail, which is not done here.

Requires `rashid>=0.1.8,<0.2.0` — older releases silently under-report, and 0.1.5 through
0.1.7 wrongly reject a conforming v0.2.0 catalog.

```bash
uv venv && uv pip install 'rashid>=0.1.8,<0.2.0'
```

## What is actually in it

| | |
|---|---|
| `table:columns` | 63 columns with a type and a description, sourced from [the Overture schema reference](https://docs.overturemaps.org/schema/reference/) |
| `README.md` / `AGENTS.md` | 12 files — one pair per catalog and collection |
| Query recipes | 14 DuckDB examples, every one executed against the published Parquet |
| Styles | 8 MapLibre styles, every `match` branch checked against measured values |
| Metadata | providers, keywords, human titles, Portolan schema URI, `rel:describedby` and `rel:agents` |

**Trimmed to 3 items per collection.** The full build carries all 529; the rest are
mechanical copies that demonstrate nothing extra and would go stale with the next release.
`scripts/` rebuilds the complete version.

## Two findings worth reading even if we don't adopt Portolan

**Building attributes are sparse.** Measured across 39,680,766 rows spanning 8 of 512
partitions: `subtype` 1.62%, `class` 1.56%, `names` 0.26%, `height` 0.11%, `roof_shape`
0.027%. Colouring buildings by subtype paints about 98% of the world a single "unknown"
colour. That belongs in the collection description regardless of what we do here.

**Places tiles exist only at zoom 14.** `places.pmtiles` advertises zoom 0–14 but holds no
tiles below z14 — its lowest tile id is 94,968,953, and z14 begins at 89,478,485. So
Overture places render as an empty map at world view in any browser. Buildings starts
around z4.

Both are reproducible; the queries are in [NOTES.md](NOTES.md).

## Known gaps

- **No thumbnails.** chiitiler installs but never binds a port on macOS arm64; parked.
- **Pinned to one release.** Everything here describes `2026-08-19.0`. How a Portolan
  catalog should express monthly releases is unresolved, and is the open question do-me
  raised in #92. This example sidesteps it rather than answering it.
- **Points get no legend.** The Portolan browser derives legends only from a `fill` layer
  with a `match` or `step` `fill-color`, so the places styles render but produce no legend.
  A browser limitation worth raising upstream.
- **`places` licence is `other`.** Left as published. Licence is a field to confirm with
  the publisher rather than infer.

## Contents

```
catalog/    the demo catalog itself
scripts/    build scripts — throwaway quality, kept as a reproduction record
NOTES.md    the full working record: audit, findings, measurements
```

## Reproducing

```bash
python scripts/mirror.py          # fetch the live catalog; rashid needs local files
python scripts/build_demo.py      # metadata + documented columns
python scripts/gen_docs.py        # README.md + AGENTS.md
python scripts/add_styles.py      # MapLibre styles as assets
python scripts/verify_queries.py  # runs all 14 AGENTS.md queries against live data
```
