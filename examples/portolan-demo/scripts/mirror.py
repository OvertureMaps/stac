import json, os, urllib.request, concurrent.futures as cf
BASE="https://stac.overturemaps.org/"
OUT="/private/tmp/claude-501/-Users-danabauer-overturemaps-stac/84e50ea8-f658-4bb1-92eb-3748c46ffd17/scratchpad/mirror"
seen=set()
def fetch(u):
    with urllib.request.urlopen(u,timeout=90) as r: return r.read()
def save(u,b):
    p=os.path.join(OUT,u[len(BASE):])
    os.makedirs(os.path.dirname(p),exist_ok=True)
    open(p,"wb").write(b)
def walk(urls):
    nxt=[]
    def one(u):
        b=fetch(u); save(u,b); d=json.loads(b)
        return [l["href"] for l in d.get("links",[]) if l["rel"] in ("child","item") and l["href"].startswith(BASE)]
    with cf.ThreadPoolExecutor(16) as ex:
        for kids in ex.map(one,urls): nxt.extend(kids)
    return [u for u in nxt if u not in seen and not seen.add(u)]
cur=["https://stac.overturemaps.org/2026-08-19.0/catalog.json"]
seen.update(cur)
n=0
while cur:
    n+=len(cur); print(f"fetched {n}...", flush=True)
    cur=walk(cur)
# root catalog too
save(BASE+"catalog.json", fetch(BASE+"catalog.json"))
print("done:",n+1)
