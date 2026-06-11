#!/usr/bin/env python3
"""Re-vendor selected Companies House ``constants.yml`` enumerations.

Regenerates ``opencheck/bods/ch_constants.py`` from the upstream
``constants.yml`` so officer-role, company-type and company-status labels are
refreshed *reproducibly* instead of hand-maintained (mirrors the PSC re-vendor
script). Used by the CH → BODS mapper to map against the official enumerations
rather than ad-hoc substring matching / raw passthrough.

Blocks vendored:
  * ``officer_role``    → ``OFFICER_ROLE_LABELS``
  * ``company_type``    → ``COMPANY_TYPE_LABELS``
  * ``company_status``  → ``COMPANY_STATUS_LABELS``

Usage:
    python backend/scripts/revendor_ch_constants.py            # download + rewrite
    python backend/scripts/revendor_ch_constants.py --source <path-or-url>
    python backend/scripts/revendor_ch_constants.py --check    # CI: fail if stale

Source: https://github.com/companieshouse/api-enumerations
Public sector information licensed under the Open Government Licence v3.0.
"""
from __future__ import annotations

import argparse
import re
import urllib.request
from pathlib import Path

UPSTREAM_URL = (
    "https://raw.githubusercontent.com/companieshouse/api-enumerations/"
    "master/constants.yml"
)
TARGET = Path(__file__).resolve().parents[1] / "opencheck" / "bods" / "ch_constants.py"

_BLOCKS = {
    "officer_role": "OFFICER_ROLE_LABELS",
    "company_type": "COMPANY_TYPE_LABELS",
    "company_status": "COMPANY_STATUS_LABELS",
}

_BLOCK_RE = re.compile(r"^([A-Za-z_]+):\s*$")
_ENTRY_RE = re.compile(r"""^\s+(['"])(?P<key>.*?)\1\s*:\s*(['"])(?P<val>.*)\3\s*$""")


def parse_blocks(text: str) -> dict[str, dict[str, str]]:
    blocks: dict[str, dict[str, str]] = {}
    current: str | None = None
    for line in text.splitlines():
        if not line.strip():
            continue
        mb = _BLOCK_RE.match(line)
        if mb:
            current = mb.group(1)
            blocks.setdefault(current, {})
            continue
        me = _ENTRY_RE.match(line)
        if me and current is not None:
            blocks[current][me.group("key")] = me.group("val")
    return blocks


_HEADER = '''\
"""Companies House ``constants.yml`` enumerations → human-readable labels.

AUTO-GENERATED from the official Companies House ``constants.yml``:
https://github.com/companieshouse/api-enumerations/blob/master/constants.yml

Do NOT edit by hand. Regenerate with::

    python backend/scripts/revendor_ch_constants.py

Public sector information licensed under the Open Government Licence v3.0.
"""
from __future__ import annotations
'''

_FUNCTIONS = '''\
def describe_officer_role(code: str | None) -> str | None:
    """Official label for a CH officer-role code (or None if unknown)."""
    return OFFICER_ROLE_LABELS.get((code or "").lower())


def describe_company_type(code: str | None) -> str | None:
    """Official label for a CH company-type code (or None if unknown)."""
    return COMPANY_TYPE_LABELS.get((code or "").lower())


def describe_company_status(code: str | None) -> str | None:
    """Official label for a CH company-status code (or None if unknown)."""
    return COMPANY_STATUS_LABELS.get((code or "").lower())
'''


def render_dict(name: str, entries: dict[str, str]) -> str:
    lines = [f"{name}: dict[str, str] = {{"]
    for key, value in entries.items():
        lines.append(f"    {key!r}: {value!r},")
    lines.append("}")
    return "\n".join(lines)


def render_module(blocks: dict[str, dict[str, str]]) -> str:
    missing = [b for b in _BLOCKS if b not in blocks or not blocks[b]]
    if missing:
        raise SystemExit(f"upstream constants.yml missing expected block(s): {missing}")
    parts = [_HEADER]
    for block_name, dict_name in _BLOCKS.items():
        parts.append(render_dict(dict_name, blocks[block_name]))
    parts.append(_FUNCTIONS.rstrip("\n"))
    return "\n\n\n".join(parts) + "\n"


def load_source(source: str) -> str:
    if re.match(r"^https?://", source):
        with urllib.request.urlopen(source, timeout=30) as resp:  # noqa: S310
            return resp.read().decode("utf-8")
    return Path(source).read_text(encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default=UPSTREAM_URL, help="upstream YAML URL or local path")
    ap.add_argument("--check", action="store_true", help="exit 1 if the vendored file is stale")
    args = ap.parse_args()

    blocks = parse_blocks(load_source(args.source))
    generated = render_module(blocks)

    if args.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.exists() else ""
        if current != generated:
            print(f"STALE: {TARGET} differs from upstream. Run revendor_ch_constants.py.")
            return 1
        print(f"OK: {TARGET} is up to date with upstream.")
        return 0

    TARGET.write_text(generated, encoding="utf-8")
    counts = ", ".join(f"{dict_name}={len(blocks[block])}" for block, dict_name in _BLOCKS.items())
    print(f"Wrote {TARGET} ({counts}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
