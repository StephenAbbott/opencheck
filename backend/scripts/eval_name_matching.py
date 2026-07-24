"""Before/after eval for the Phase-D shared name scorer.

Compares the historical scorer (plain difflib ratio on normalised forms —
exactly what cross_check._name_score did before Phase D) against
``names.name_similarity`` over every pair of names in the committed demo
corpus, and reports the pairs whose classification at the product-wide 0.88
threshold changes. Run from ``backend/``:

    python3 scripts/eval_name_matching.py [--threshold 0.88]

The new scorer is recall-only relative to the old one (it takes a max over
the old ratio plus token-sort and rigour Levenshtein components), so the
"newly below threshold" section must always be empty — the script exits
non-zero if not.
"""

from __future__ import annotations

import argparse
import difflib
import itertools
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from opencheck import names  # noqa: E402


def _old_score(a: str, b: str) -> float:
    na, nb = names.normalise_name(a), names.normalise_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return difflib.SequenceMatcher(a=na, b=nb).ratio()


def _corpus_names() -> list[str]:
    demo = pathlib.Path(__file__).resolve().parents[2] / "data" / "demo"
    out: set[str] = set()
    for path in sorted(demo.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            rd = json.loads(line).get("recordDetails") or {}
            for block in rd.get("names") or []:
                n = (block.get("fullName") or "").strip()
                if n:
                    out.add(n)
            for n in rd.get("alternateNames") or []:
                if str(n).strip():
                    out.add(str(n).strip())
    return sorted(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=0.88)
    args = ap.parse_args()

    corpus = _corpus_names()
    print(f"corpus: {len(corpus)} distinct names → {len(corpus)*(len(corpus)-1)//2} pairs")

    newly_above: list[tuple[str, str, float, float]] = []
    newly_below: list[tuple[str, str, float, float]] = []
    for a, b in itertools.combinations(corpus, 2):
        old, new = _old_score(a, b), names.name_similarity(a, b)
        was, now = old >= args.threshold, new >= args.threshold
        if now and not was:
            newly_above.append((a, b, old, new))
        elif was and not now:
            newly_below.append((a, b, old, new))

    print(f"\nnewly ≥ {args.threshold} with the Phase-D scorer: {len(newly_above)}")
    for a, b, old, new in newly_above:
        print(f"  {old:.3f} → {new:.3f}  {a!r} ↔ {b!r}")

    print(f"\nnewly < {args.threshold} (must be zero): {len(newly_below)}")
    for a, b, old, new in newly_below:
        print(f"  {old:.3f} → {new:.3f}  {a!r} ↔ {b!r}")

    return 1 if newly_below else 0


if __name__ == "__main__":
    raise SystemExit(main())
