"""evaluate.py — Phase 4-6 of the Splink ER spike.

Loads the trained model (model.json), predicts pairwise match probabilities on
the soft features, then:

* **Phase 4** — predicts (all blocked candidate pairs).
* **Phase 5** — labels each pair using the held-out identifiers (two rows are
  the same entity if they share a non-null ``lei`` or ``nat_reg``) and reports
  precision / recall / F1 for Splink at several thresholds vs the current
  baseline (``difflib`` name ratio >= 0.88, as in ``cross_check.py``).
* **Phase 6** — saves waterfall charts for a few illustrative pairs.

IMPORTANT label caveat (see write-up): identifier-agreement gives *reliable
positives* but treats every other pair as a negative — yet some of those are
genuinely the same entity with no shared identifier (exactly the case we want
Splink for). So Splink's apparent "false positives" are under-counted truth and
**precision here is a lower bound** until a sample is eyeballed.

Run from ``backend/`` after training::

    uv run python spikes/splink_er/evaluate.py
"""

from __future__ import annotations

import difflib
import json
import sys
from itertools import combinations
from pathlib import Path

import pandas as pd
from splink import DuckDBAPI, Linker

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

MODEL = HERE / "model.json"
CHARTS = HERE / "charts"
BASELINE_THRESHOLD = 0.88  # cross_check.py's difflib cut-off


def same_entity(a: dict, b: dict) -> bool:
    """Ground truth: share a non-null strong identifier."""
    if a.get("lei") and a["lei"] == b.get("lei"):
        return True
    if a.get("nat_reg") and a["nat_reg"] == b.get("nat_reg"):
        return True
    return False


def prf(tp: int, predicted: int, actual: int) -> tuple[float, float, float]:
    p = tp / predicted if predicted else 0.0
    r = tp / actual if actual else 0.0
    f = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f


def main() -> int:
    CHARTS.mkdir(exist_ok=True)
    df = load_corpus()
    by_id = {r["record_id"]: r for r in df.to_dict("records")}
    n = len(df)
    print(f"corpus: {n} rows")

    # --- ground truth over ALL pairs (for blocking-recall) ---
    all_pos = {
        frozenset((a, b))
        for a, b in combinations(by_id, 2)
        if same_entity(by_id[a], by_id[b])
    }
    cross_pos = {
        pr for pr in all_pos
        if by_id[(a := tuple(pr))[0]]["source_id"] != by_id[a[1]]["source_id"]
    }
    print(f"true positive pairs (shared lei/nat_reg): {len(all_pos)} "
          f"({len(cross_pos)} cross-source)")

    # --- Splink prediction over blocked candidate pairs ---
    settings = json.loads(MODEL.read_text())
    linker = Linker(df, settings, db_api=DuckDBAPI())
    preds = linker.inference.predict().as_pandas_dataframe()
    print(f"blocked candidate pairs scored by Splink: {len(preds)}")

    # candidate set as (idl, idr) frozensets + per-pair features for baseline/label
    cand = []
    for _, row in preds.iterrows():
        idl, idr = str(row["record_id_l"]), str(row["record_id_r"])
        a, b = by_id.get(idl), by_id.get(idr)
        if not a or not b:
            continue
        cand.append({
            "pair": frozenset((idl, idr)),
            "p": float(row["match_probability"]),
            "label": same_entity(a, b),
            "cross": a["source_id"] != b["source_id"],
            "name_ratio": difflib.SequenceMatcher(
                a=a.get("name_norm") or "", b=b.get("name_norm") or ""
            ).ratio(),
        })

    pos_in_cand = {c["pair"] for c in cand if c["label"]}
    blocking_recall = len(pos_in_cand) / len(all_pos) if all_pos else 0.0
    print(f"blocking recall (true positives reachable after blocking): "
          f"{blocking_recall:.0%}\n")

    def evaluate(subset: list[dict], actual_pos: int, title: str) -> None:
        print(f"--- {title} (candidate pairs: {len(subset)}, true positives: {actual_pos}) ---")
        print(f"  {'method':<22}{'prec':>7}{'recall':>8}{'F1':>7}{'predicted':>11}")
        # Splink at thresholds
        for thr in (0.5, 0.9, 0.99):
            pred = [c for c in subset if c["p"] >= thr]
            tp = sum(1 for c in pred if c["label"])
            p, r, f = prf(tp, len(pred), actual_pos)
            print(f"  {'splink >= ' + str(thr):<22}{p:>7.2f}{r:>8.2f}{f:>7.2f}{len(pred):>11}")
        # Baseline: difflib name ratio >= 0.88
        pred = [c for c in subset if c["name_ratio"] >= BASELINE_THRESHOLD]
        tp = sum(1 for c in pred if c["label"])
        p, r, f = prf(tp, len(pred), actual_pos)
        print(f"  {'difflib >= 0.88':<22}{p:>7.2f}{r:>8.2f}{f:>7.2f}{len(pred):>11}\n")

    # recall denominators use positives reachable after blocking (fair to both,
    # since the baseline is also scored only on the candidate set)
    evaluate(cand, len(pos_in_cand), "ALL candidate pairs")
    cross = [c for c in cand if c["cross"]]
    cross_pos_in_cand = sum(1 for c in cross if c["label"])
    evaluate(cross, cross_pos_in_cand, "CROSS-SOURCE pairs only (the real ER task)")

    # --- The decisive cut: true matches whose NAMES disagree ---
    # A name-only baseline (difflib >= 0.88) misses these by construction; this
    # is where the extra features (jurisdiction / inc date / address) must earn
    # their keep. If Splink recovers some, that's the actual value proposition.
    hard = [c for c in cross if c["label"] and c["name_ratio"] < 0.90]
    print(f"--- HARD cross-source positives (true match, name ratio < 0.90): {len(hard)} ---")
    if hard:
        s = sum(1 for c in hard if c["p"] >= 0.5)
        d = sum(1 for c in hard if c["name_ratio"] >= BASELINE_THRESHOLD)
        print(f"  recovered by splink   >= 0.5 : {s}/{len(hard)} ({s/len(hard):.0%})")
        print(f"  recovered by difflib  >= 0.88: {d}/{len(hard)} ({d/len(hard):.0%})")
    else:
        print("  (none in this corpus yet — cross-source duplicates here mostly "
              "share names; need more corpus / name-variant cases to test the value prop)")
    print()

    # --- Phase 6: waterfalls for illustrative pairs ---
    rec = preds.copy()
    rec["pair"] = [frozenset((str(l), str(r))) for l, r in zip(rec["record_id_l"], rec["record_id_r"])]
    lab = {c["pair"]: c for c in cand}
    rec["label"] = rec["pair"].map(lambda p: lab.get(p, {}).get("label"))
    rec["name_ratio"] = rec["pair"].map(lambda p: lab.get(p, {}).get("name_ratio"))
    picks = pd.concat([
        rec[rec["label"]].nlargest(2, "match_probability"),                       # strong TPs
        rec[(rec["match_probability"] > 0.4) & (rec["match_probability"] < 0.7)].head(2),  # near-miss
        rec[(~rec["label"].astype(bool)) & (rec["name_ratio"] > 0.85)].nlargest(2, "match_probability"),  # name clash, not same id
    ]).drop_duplicates("pair")
    try:
        records = picks.drop(columns=["pair", "label", "name_ratio"]).to_dict("records")
        linker.visualisations.waterfall_chart(records).save(str(CHARTS / "waterfalls.html"))
        print(f"waterfalls ({len(records)} pairs) -> {CHARTS / 'waterfalls.html'}")
    except Exception as e:  # noqa: BLE001
        print(f"(waterfall chart skipped: {type(e).__name__}: {e})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
