import json, urllib.request, concurrent.futures as cf
def get(u):
    with urllib.request.urlopen(u, timeout=90) as r: return json.load(r)
res = json.load(open("/private/tmp/claude-501/-Users-danabauer-overturemaps-stac/84e50ea8-f658-4bb1-92eb-3748c46ffd17/scratchpad/audit.json"))
def chk(x):
    d = get(x["url"])
    bb = d["extent"]["spatial"]["bbox"]
    nitems = sum(1 for l in d["links"] if l["rel"]=="item")
    name = "/".join(x["url"].split("/")[-3:-1])
    if len(bb) == 1:
        return (name, len(bb), nitems, "single bbox", None, None)
    sub = bb[1:]
    u = [min(b[0] for b in sub), min(b[1] for b in sub), max(b[2] for b in sub), max(b[3] for b in sub)]
    ok = bb[0][0]<=u[0] and bb[0][1]<=u[1] and bb[0][2]>=u[2] and bb[0][3]>=u[3]
    return (name, len(bb), nitems, "OK" if ok else "WRONG", bb[0], u)
with cf.ThreadPoolExecutor(8) as ex:
    out = list(ex.map(chk, res))
print(f"{'collection':28} {'bboxes':>7} {'items':>7}  overall-bbox")
tot=0
for name, n, ni, st, b0, u in sorted(out):
    tot+=ni
    print(f"{name:28} {n:>7} {ni:>7}  {st}")
print(f"\ntotal items: {tot}")
print("\nexample of the error:")
for name,n,ni,st,b0,u in sorted(out):
    if st=="WRONG":
        print(f"  {name}\n    declared: {[round(v,2) for v in b0]}\n    actual:   {[round(v,2) for v in u]}")
        break
