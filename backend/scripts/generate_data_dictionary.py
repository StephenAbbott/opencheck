#!/usr/bin/env python3
"""Generate ``docs/data-dictionary.md`` from the code, or check it is current.

Usage (from ``backend/``)::

    uv run python scripts/generate_data_dictionary.py           # write the file
    uv run python scripts/generate_data_dictionary.py --check   # exit 1 when stale

The rendering lives in ``opencheck.data_dictionary``; see its docstring for
what is read from where. ``--check`` is what CI runs
(``.github/workflows/vendored-enum-drift.yml``), beside the OKF check.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencheck import data_dictionary  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 when the committed file differs from the rendering.",
    )
    args = ap.parse_args()
    path = data_dictionary.repo_doc_path()
    rendered = data_dictionary.render_markdown()
    if args.check:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current == rendered:
            print(f"{data_dictionary.DOC_PATH} is current.")
            return 0
        print(f"{data_dictionary.DOC_PATH} is stale — regenerate it:")
        print("    uv run python scripts/generate_data_dictionary.py")
        sys.stdout.writelines(
            difflib.unified_diff(
                current.splitlines(keepends=True),
                rendered.splitlines(keepends=True),
                fromfile="committed",
                tofile="rendered",
                n=1,
            )
        )
        return 1
    path.write_text(rendered, encoding="utf-8")
    print(f"wrote {path} ({len(rendered):,} characters)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
