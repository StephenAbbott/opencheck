"""reconcile_prototype.py — iteration 3 (b): the simpler name+jurisdiction rule.

Benchmarks four no-shared-identifier matchers on the corpus, using **reliable**
labels only: a cross-source pair is a POSITIVE if it shares an LEI/nat_reg, a
NEGATIVE if both carry an LEI and the LEIs differ. (Pairs with neither are left
out — we can't label them reliably.)

  * difflib>=0.88        — the current cross_check.py name primitive
  * splink>=0.5          — the trained Splink model
  * name_exact+juris     — normalised name equal AND jurisdiction equal
  * name_exact_only      — normalised name equal

The point: the BP corpus is full of distinct "BP Exploration Alpha/Beta/…"
subsidiaries, so a FUZZY name match (difflib) over-merges. An EXACT name match
should reject Alpha-vs-Beta (names differ) while still catching identical-name
cross-source duplicates.

    uv run python spikes/splink_er/reconcile_prototype.py
"""

from __future__ import annotations

import difflib
import json
import sys
from pathlib import Path

from splink import DuckDBAPI, Linker

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

MODEL = HERE / "model.json"


def main() -> int:
    df = load_corpus()
    by = {r["record_id"]: r for r in df.to_dict("records")}
    linker = Linker(df, json.loads(MODEL.read_text()), db_api=DuckDBAPI())
    preds = linker.inference.predict().as_pandas_dataframe()

    methods = {
        "difflib >= 0.88": lambda a, b, p: difflib.SequenceMatcher(
            a=a.get("name_norm") or "", b=b.get("name_norm") or "").ratio() >= 0.88,
        "splink >= 0.5": lambda a, b, p: p >= 0.5,
        "name_exact + juris": lambda a, b, p: bool(
            a.get("name_norm") and a["name_norm"] == b.get("name_norm")
            and a.get("jurisdiction") and a["jurisdiction"] == b.get("jurisdiction")),
        "name_exact only": lambda a, b, p: bool(
            a.get("name_norm") and a["name_norm"] == b.get("name_norm")),
    }
    stats = {m: {"tp": 0, "fp": 0, "fn": 0, "tn": 0} for m in methods}
    pos = neg = 0

    for _, r in preds.iterrows():
        a, b = by.get(str(r["record_id_l"])), by.get(str(r["record_id_r"]))
        if not a or not b or a["source_id"] == b["source_id"]:
            continue
        same = bool((a.get("lei") and a["lei"] == b.get("lei"))
                    or (a.get("nat_reg") and a["nat_reg"] == b.get("nat_reg")))
        diff = bool(a.get("lei") and b.get("lei") and a["lei"] != b["lei"]) and not same
        if not (same or diff):
            continue  # no reliable label
        pos += same
        neg += diff
        p = float(r["match_probability"])
        for m, fn in methods.items():
            pred = fn(a, b, p)
            s = stats[m]
            if same and pred:
                s["tp"] += 1
            elif same and not pred:
                s["fn"] += 1
            elif diff and pred:
                s["fp"] += 1
            else:
                s["tn"] += 1

    print(f"reliable cross-source pairs: {pos} positive (shared id), {neg} negative (different LEI)\n")
    print(f"{'method':<22}{'prec':>7}{'recall':>8}{'F1':>7}{'false-merges':>14}")
    for m, s in stats.items():
        prec = s["tp"] / (s["tp"] + s["fp"]) if s["tp"] + s["fp"] else 0.0
        rec = s["tp"] / (s["tp"] + s["fn"]) if s["tp"] + s["fn"] else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        print(f"{m:<22}{prec:>7.2f}{rec:>8.2f}{f1:>7.2f}{s['fp']:>10} /{neg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
