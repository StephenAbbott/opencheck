#!/usr/bin/env python3
"""Re-vendor the Companies House PSC enumeration descriptions.

Regenerates ``opencheck/bods/psc_natures.py`` from the upstream
``psc_descriptions.yml`` so the vendored copy is refreshed *reproducibly*
instead of hand-maintained. This guards against silent drift when Companies
House adds or revises codes (e.g. the ROE / identity-verification additions).

We vendor three of the four blocks in the upstream file:
  * ``short_description``       → ``PSC_NATURE_DESCRIPTIONS``   (86 nature codes)
  * ``super_secure_description``→ ``SUPER_SECURE_DESCRIPTIONS`` (2 codes)
  * ``statement_description``   → ``PSC_STATEMENT_DESCRIPTIONS``(PSC statements)
The long-form ``description`` block is intentionally not vendored — the concise
``short_description`` is the better fit for BODS ``interest.details``.

Usage:
    python backend/scripts/revendor_psc_enums.py             # download + rewrite
    python backend/scripts/revendor_psc_enums.py --source <path-or-url>
    python backend/scripts/revendor_psc_enums.py --check     # CI: fail if stale

Source data: https://github.com/companieshouse/api-enumerations
Public sector information licensed under the Open Government Licence v3.0.
"""
from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path

UPSTREAM_URL = (
    "https://raw.githubusercontent.com/companieshouse/api-enumerations/"
    "master/psc_descriptions.yml"
)
TARGET = Path(__file__).resolve().parents[1] / "opencheck" / "bods" / "psc_natures.py"

# Upstream YAML block name → generated dict name.
_BLOCKS = {
    "short_description": "PSC_NATURE_DESCRIPTIONS",
    "super_secure_description": "SUPER_SECURE_DESCRIPTIONS",
    "statement_description": "PSC_STATEMENT_DESCRIPTIONS",
}

# psc_descriptions.yml is flat, regular YAML — a top-level block header followed
# by quoted 'key' : "value" pairs. We parse it directly (no PyYAML dependency).
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
            blocks[current] = {}
            continue
        me = _ENTRY_RE.match(line)
        if me and current is not None:
            blocks[current][me.group("key")] = me.group("val")
    return blocks


_HEADER = '''\
"""Companies House PSC enumeration descriptions → human-readable text.

AUTO-GENERATED from the official Companies House ``psc_descriptions.yml``:
https://github.com/companieshouse/api-enumerations/blob/master/psc_descriptions.yml

Do NOT edit by hand. Regenerate with::

    python backend/scripts/revendor_psc_enums.py

Public sector information licensed under the Open Government Licence v3.0.
"""
from __future__ import annotations
'''

_FUNCTIONS = '''\
_DEFAULT_SUPER_SECURE = SUPER_SECURE_DESCRIPTIONS["super-secure-persons-with-significant-control"]


def describe_nature(code: str) -> str | None:
    """Human-readable descriptor for a PSC nature-of-control code (or None)."""
    return PSC_NATURE_DESCRIPTIONS.get((code or "").lower())


def describe_super_secure(code: str | None) -> str:
    """Official explanatory text for a super-secure PSC.

    Falls back to the generic PSC wording for an unknown/empty code.
    """
    return SUPER_SECURE_DESCRIPTIONS.get((code or "").lower(), _DEFAULT_SUPER_SECURE)


def describe_statement(code: str) -> str | None:
    """Human-readable descriptor for a PSC *statement* code (or None)."""
    return PSC_STATEMENT_DESCRIPTIONS.get((code or "").lower())
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
        raise SystemExit(f"upstream YAML missing expected block(s): {missing}")
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
            print(f"STALE: {TARGET} differs from upstream. Run revendor_psc_enums.py.")
            return 1
        print(f"OK: {TARGET} is up to date with upstream.")
        return 0

    TARGET.write_text(generated, encoding="utf-8")
    counts = ", ".join(f"{dict_name}={len(blocks[block])}" for block, dict_name in _BLOCKS.items())
    print(f"Wrote {TARGET} ({counts}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
