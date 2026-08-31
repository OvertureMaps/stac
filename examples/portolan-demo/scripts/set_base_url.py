#!/usr/bin/env python3
"""Add absolute `self` links to the demo catalog once it has a home.

Portolan v0.2.0 (PORTO-CORE-081) recommends an absolute `self` link on the root
catalog of a catalog served from a fixed URL. Structural links stay relative.

Usage:
    python scripts/set_base_url.py https://<user>.github.io/<repo>
"""
import json
import sys
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit(__doc__)
base = sys.argv[1].rstrip("/")
root = Path(__file__).resolve().parent.parent / "demo"

n = 0
for p in sorted(root.rglob("*.json")):
    if "styles" in p.parts:
        continue
    d = json.loads(p.read_text())
    if d.get("type") not in ("Catalog", "Collection", "Feature"):
        continue
    rel_path = p.relative_to(root).as_posix()
    media = "application/geo+json" if d.get("type") == "Feature" else "application/json"
    d["links"] = [link for link in d.get("links", []) if link.get("rel") != "self"]
    d["links"].insert(0, {"rel": "self", "href": f"{base}/{rel_path}", "type": media})
    p.write_text(json.dumps(d, indent=2) + "\n")
    n += 1
print(f"set self href on {n} objects -> {base}")
