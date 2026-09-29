"""Pin pyproject.toml's wheel version to the tag being built.

Called from publish-pypi.yaml so tag-driven builds ship as the requested version
instead of falling back to Cargo.toml's crate version.
"""

import pathlib
import re
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: set_wheel_version.py <version>", file=sys.stderr)
        return 2
    version = sys.argv[1].strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+([.\-+][A-Za-z0-9.\-]+)?", version):
        print(f"error: not a valid PEP 440-ish version: {version!r}", file=sys.stderr)
        return 2
    path = pathlib.Path("pyproject.toml")
    text = path.read_text()
    new_text, n = re.subn(
        r'^dynamic = \["version"\]$',
        f'version = "{version}"',
        text,
        count=1,
        flags=re.M,
    )
    if n != 1:
        print('error: could not find `dynamic = ["version"]` in pyproject.toml', file=sys.stderr)
        return 1
    path.write_text(new_text)
    print(f"pinned wheel version to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
