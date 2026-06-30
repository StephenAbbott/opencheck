"""make_negatives_worksheet.py — iteration 3 task 2b: the PRECISION worksheet.

The first labelling round had zero negatives, so it measured recall only. This
samples name-similar pairs that are *likely different entities* — the cases that
test false-merge rate (difflib's known weakness) — for human labelling.

It also computes false-positive rates immediately on **reliable negatives**:
pairs where both sides carry an LEI and the LEIs *differ* are definitely
different entities (no human needed). Among those with high name similarity, how
often does each method wrongly match?

Run from ``backend/``::

    uv run python spikes/splink_er/make_negatives_worksheet.py --per-bucket 20
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import sys
from pathlib import Path

from splink import DuckDBAPI, Linker

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

MODEL = HERE / "model.json"
OUT = HERE / "corpus" / "negatives_worksheet.csv"
BASELINE = 0.88


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-bucket", type=int, default=20)
    args = ap.parse_args()

    df = load_corpus()
    by = {r["record_id"]: r for r in df.to_dict("records")}
    linker = Linker(df, json.loads(MODEL.read_text()), db_api=DuckDBAPI())
    preds = linker.inference.predict().as_pandas_dataframe()

    cand = []
    for _, r in preds.iterrows():
        a, b = by.get(str(r["record_id_l"])), by.get(str(r["record_id_r"]))
        if not a or not b or a["source_id"] == b["source_id"]:
            continue
        sim = difflib.SequenceMatcher(a=a.get("name_norm") or "", b=b.get("name_norm") or "").ratio()
        diff_lei = bool(a.get("lei") and b.get("lei") and a["lei"] != b["lei"])
        diff_jur = bool(a.get("jurisdiction") and b.get("jurisdiction") and a["jurisdiction"] != b["jurisdiction"])
        cand.append({"a": a, "b": b, "p": float(r["match_probability"]), "sim": sim,
                     "diff_lei": diff_lei, "diff_jur": diff_jur})

    # --- reliable negatives: different LEI => different entity (no labelling needed) ---
    neg = [c for c in cand if c["diff_lei"]]
    neg_sim = [c for c in neg if c["sim"] >= BASELINE]
    print(f"reliable negatives (different LEI, cross-source): {len(neg)}; "
          f"of which name-sim>=0.88: {len(neg_sim)}")
    if neg_sim:
        d_fp = sum(1 for c in neg_sim if c["sim"] >= BASELINE)        # difflib would match all of these
        s_fp = sum(1 for c in neg_sim if c["p"] >= 0.5)
        print(f"  difflib >= 0.88  false-merges: {d_fp}/{len(neg_sim)} ({d_fp/len(neg_sim):.0%})")
        print(f"  splink  >= 0.5   false-merges: {s_fp}/{len(neg_sim)} ({s_fp/len(neg_sim):.0%})  <- precision test")

    # --- worksheet: name-similar pairs that need a human eye (likely different) ---
    def bucket(c):
        if c["diff_lei"] and c["sim"] >= BASELINE:
            return "diff_lei_high_sim"          # reliable negative; confirm difflib false-merge
        if c["diff_jur"] and c["sim"] >= 0.9:
            return "diff_juris_high_sim"        # same name, different country
        if 0.80 <= c["sim"] < 0.93:
            return "partial_name_sim"           # similar-not-identical (e.g. Alpha vs Beta)
        return None

    rows: dict[str, list[dict]] = {}
    for c in cand:
        bk = bucket(c)
        if not bk:
            continue
        a, b = c["a"], c["b"]
        rows.setdefault(bk, []).append({
            "bucket": bk, "human_label": "", "notes": "",
            "splink_p": round(c["p"], 3), "name_sim": round(c["sim"], 2),
            "diff_lei": int(c["diff_lei"]),
            "name_a": a.get("name_raw"), "name_b": b.get("name_raw"),
            "juris_a": a.get("jurisdiction") or "", "juris_b": b.get("jurisdiction") or "",
            "date_a": a.get("inc_date") or "", "date_b": b.get("inc_date") or "",
            "source_a": a.get("source_id"), "source_b": b.get("source_id"),
            "record_id_a": a["record_id"], "record_id_b": b["record_id"],
        })

    fields = ["bucket", "human_label", "notes", "splink_p", "name_sim", "diff_lei",
              "name_a", "name_b", "juris_a", "juris_b", "date_a", "date_b",
              "source_a", "source_b", "record_id_a", "record_id_b"]
    OUT.parent.mkdir(exist_ok=True)
    total = 0
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for bk, items in rows.items():
            items.sort(key=lambda x: -x["name_sim"])
            for it in items[: args.per_bucket]:
                w.writerow(it)
                total += 1
    print(f"\nwrote {total} pairs to {OUT}")
    for bk, items in rows.items():
        print(f"  {bk:<22} available={len(items):4d} sampled={min(len(items), args.per_bucket)}")
    print("\nLabel `human_label` 1 (same) / 0 (different) / ? — these are the likely-DIFFERENT")
    print("pairs, so 0 = correct rejection. difflib precision = #1 / #(name_sim>=0.88).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
