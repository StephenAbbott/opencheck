"""hard_positive_test.py — the decisive test the identifier-labelled eval can't do.

Cross-source duplicates in the corpus share their names, so the labelled
positives are all "easy" and a name-only baseline scores recall 1.0. This script
manufactures the *hard* case: take known-same entities and build a synthetic
copy with a **degraded name** but the **same jurisdiction / incorporation date /
address**, then score the (original, variant) pair with the trained Splink model
vs the current ``difflib >= 0.88`` baseline. Every pair is a true match by
construction, so:

* difflib must fail as the name degrades (it only sees the name);
* Splink can lean on the other (exactly-matching) features.

The gap between the two columns is the value proposition. ``compare_two_records``
is also exactly the Phase 8 real-time path, so we time it here too.

Run from ``backend/``::

    uv run python spikes/splink_er/hard_positive_test.py --max 20
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
import time
from pathlib import Path

from splink import DuckDBAPI, Linker

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

MODEL = HERE / "model.json"
BASELINE = 0.88

# Legal-form / generic tokens dropped to simulate "same company, named shorter".
_SUFFIXES = {
    "a/s", "as", "ltd", "limited", "plc", "gmbh", "inc", "llc", "ag", "sa", "nv",
    "bv", "oy", "ab", "spa", "srl", "pte", "co", "corp", "corporation", "company",
    "holding", "holdings", "group", "se", "kg", "oyj", "asa",
}


def drop_suffix(name: str) -> str:
    toks = name.split()
    for _ in range(2):  # strip up to two trailing legal-form / short tokens
        if toks and (toks[-1] in _SUFFIXES or len(toks[-1]) <= 2):
            toks = toks[:-1]
    return " ".join(toks) or name


def abbreviate(name: str) -> str:
    # drop interior vowels from longer words — simulates compressed/aliased names
    return " ".join(re.sub(r"(?<=.)[aeiou]", "", w) if len(w) > 3 else w for w in name.split())


def typo(name: str) -> str:
    if len(name) < 6:
        return name
    i = len(name) // 2
    return name[:i] + name[i + 2 : i + 3] + name[i] + name[i + 3 :]  # transpose a couple of chars


PERTURB = {"drop_suffix": drop_suffix, "abbreviate": abbreviate, "typo": typo}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=20, help="base entities to perturb")
    args = ap.parse_args()

    df = load_corpus()
    settings = json.loads(MODEL.read_text())
    linker = Linker(df, settings, db_api=DuckDBAPI())

    # one base record per LEI (prefer the most complete name)
    by_lei: dict[str, dict] = {}
    for r in df.to_dict("records"):
        lei = r.get("lei")
        if not lei or not r.get("name_norm"):
            continue
        if lei not in by_lei or len(r["name_norm"]) > len(by_lei[lei]["name_norm"]):
            by_lei[lei] = r
    bases = [
        r for r in by_lei.values()
        if r.get("jurisdiction") or r.get("inc_date") or r.get("address_norm")
    ][: args.max]
    print(f"base entities: {len(bases)} (perturbations: {', '.join(PERTURB)})")

    results = {k: {"n": 0, "splink": 0, "difflib": 0, "ratio_sum": 0.0} for k in PERTURB}
    t_total = 0.0
    t_n = 0
    for base in bases:
        for pname, fn in PERTURB.items():
            new_name = fn(base["name_norm"])
            if new_name == base["name_norm"]:
                continue
            variant = dict(base)
            variant["record_id"] = f"{base['record_id']}__{pname}"
            variant["name_norm"] = new_name
            ratio = difflib.SequenceMatcher(a=base["name_norm"], b=new_name).ratio()
            try:
                t0 = time.time()
                out = linker.inference.compare_two_records(base, variant).as_pandas_dataframe()
                t_total += time.time() - t0
                t_n += 1
                p = float(out["match_probability"].iloc[0])
            except Exception as e:  # noqa: BLE001
                print(f"compare_two_records failed: {type(e).__name__}: {e}")
                return 1
            R = results[pname]
            R["n"] += 1
            R["ratio_sum"] += ratio
            R["splink"] += int(p >= 0.5)
            R["difflib"] += int(ratio >= BASELINE)

    print(f"\n{'perturbation':<14}{'n':>4}{'avg name sim':>14}{'splink>=0.5':>13}{'difflib>=0.88':>15}")
    for k, R in results.items():
        if not R["n"]:
            continue
        avg = R["ratio_sum"] / R["n"]
        print(f"{k:<14}{R['n']:>4}{avg:>14.2f}{R['splink']:>13}{R['difflib']:>15}")
    print("\n(every pair is a TRUE match by construction — higher = better recovery)")
    if t_n:
        print(f"\nPhase 8 — real-time compare_two_records: {1000 * t_total / t_n:.0f} ms/pair "
              f"({t_n} pairs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
