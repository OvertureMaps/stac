import json, urllib.request, concurrent.futures as cf

ROOT = "https://stac.overturemaps.org/2026-08-19.0/catalog.json"

def get(u):
    with urllib.request.urlopen(u, timeout=60) as r:
        return json.load(r)

def children(d):
    base = d["links"]
    return [l["href"] for l in base if l["rel"] == "child"]

rel = get(ROOT)
themes = children(rel)
print(f"themes: {len(themes)}")

colls = []
for t in themes:
    td = get(t)
    for c in children(td):
        colls.append(c)
print(f"type collections: {len(colls)}")

def probe(u):
    d = get(u)
    rels = {l["rel"] for l in d.get("links", [])}
    assets = d.get("assets", {}) or {}
    roles = set()
    for a in assets.values():
        roles.update(a.get("roles") or [])
    ext = d.get("stac_extensions", []) or []
    tc = d.get("table:columns", []) or []
    described = [c for c in tc if c.get("description")]
    typed = [c for c in tc if c.get("type")]
    te = (d.get("extent", {}).get("temporal", {}) or {}).get("interval") or []
    return {
        "url": u,
        "id": d.get("id"),
        "type": d.get("type"),
        "title": bool(d.get("title")),
        "description_len": len(d.get("description") or ""),
        "providers": len(d.get("providers") or []),
        "license": d.get("license"),
        "keywords": len(d.get("keywords") or []),
        "rels": sorted(rels),
        "asset_keys": sorted(assets.keys()),
        "asset_roles": sorted(roles),
        "extensions": ext,
        "portolan_ext": any("portolan" in e for e in ext),
        "n_columns": len(tc),
        "n_col_described": len(described),
        "n_col_typed": len(typed),
        "temporal": te,
        "row_count": d.get("table:row_count"),
    }

with cf.ThreadPoolExecutor(16) as ex:
    res = list(ex.map(probe, colls))

json.dump(res, open("/private/tmp/claude-501/-Users-danabauer-overturemaps-stac/84e50ea8-f658-4bb1-92eb-3748c46ffd17/scratchpad/audit.json","w"), indent=1)

n = len(res)
def cnt(f): return sum(1 for r in res if f(r))
print()
print(f"{'field':34} {'found':>10}  requirement")
rows = [
 ("title", cnt(lambda r: r["title"]), "MUST"),
 ("providers", cnt(lambda r: r["providers"]>0), "MUST"),
 ("license != proprietary/missing", cnt(lambda r: r["license"] not in (None,"proprietary")), "proprietary MUST NOT"),
 ("keywords", cnt(lambda r: r["keywords"]>0), "recommended"),
 ("rel: describedby", cnt(lambda r: "describedby" in r["rels"]), "MUST"),
 ("rel: agents", cnt(lambda r: "agents" in r["rels"]), "MUST"),
 ("rel: license", cnt(lambda r: "license" in r["rels"]), ""),
 ("portolan schema URI", cnt(lambda r: r["portolan_ext"]), "MUST"),
 ("any assets at all", cnt(lambda r: len(r["asset_keys"])>0), ""),
 ("style asset", cnt(lambda r: any("style" in k for k in r["asset_keys"])), "MUST"),
 ("default-role asset", cnt(lambda r: "default" in r["asset_roles"]), "MUST where >1 style"),
 ("documentation asset", cnt(lambda r: any(k in ("readme","documentation","agents") for k in r["asset_keys"])), ""),
 ("thumbnail", cnt(lambda r: "thumbnail" in r["asset_keys"] or "thumbnail" in r["asset_roles"]), ""),
 ("populated temporal extent", cnt(lambda r: r["temporal"] and r["temporal"][0] and any(r["temporal"][0])), ""),
 ("description > 200 chars", cnt(lambda r: r["description_len"]>200), ""),
 ("table:columns present", cnt(lambda r: r["n_columns"]>0), ""),
 ("columns WITH description", cnt(lambda r: r["n_columns"]>0 and r["n_col_described"]==r["n_columns"]), "all described"),
 ("columns WITH type", cnt(lambda r: r["n_columns"]>0 and r["n_col_typed"]==r["n_columns"]), "all typed"),
]
for name, c, req in rows:
    print(f"{name:34} {c:>4} of {n:<3}  {req}")

print()
tot = sum(r["n_columns"] for r in res)
desc = sum(r["n_col_described"] for r in res)
typed = sum(r["n_col_typed"] for r in res)
print(f"table:columns entries total: {tot}; with description: {desc}; with type: {typed}")
print()
print("distinct rel values across collections:", sorted({x for r in res for x in r['rels']}))
print("distinct asset keys:", sorted({x for r in res for x in r['asset_keys']}))
print("distinct licenses:", sorted({str(r['license']) for r in res}))
print("extensions seen:", sorted({e for r in res for e in r['extensions']}))
