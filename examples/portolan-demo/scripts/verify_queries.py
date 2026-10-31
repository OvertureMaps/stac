import re, subprocess, sys
from pathlib import Path
blocks=[]
for f in sorted(Path("demo").rglob("AGENTS.md")):
    for m in re.finditer(r"```sql\n(.*?)```", f.read_text(), re.S):
        q=m.group(1).strip()
        if "SELECT" not in q.upper(): continue
        blocks.append((str(f), q))
print(f"{len(blocks)} queries to verify\n")
fails=0
for i,(f,q) in enumerate(blocks,1):
    label=f"{i:>2}. {f.replace('demo/','')}"
    r=subprocess.run(["duckdb","-noheader","-list","-c",q],capture_output=True,text=True,timeout=900)
    first=q.split("\n")[2][:64] if len(q.split("\n"))>2 else q[:64]
    if r.returncode!=0:
        fails+=1
        print(f"FAIL {label}\n     {first}\n     {r.stderr.strip().splitlines()[0][:160]}")
    else:
        rows=[l for l in r.stdout.strip().split("\n") if l]
        print(f"ok   {label}  -> {len(rows)} row(s)  {first}")
        if not rows: print(f"     WARNING: returned zero rows")
print(f"\n{len(blocks)-fails} passed, {fails} failed")
sys.exit(1 if fails else 0)
