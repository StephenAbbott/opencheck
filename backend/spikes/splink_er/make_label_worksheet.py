"""make_label_worksheet.py — export a hand-labelling worksheet (iteration 3 task 2).

Identifier-proxy labels are exhausted; a true precision/recall read needs human
judgement. This exports the pairs where the decision is genuinely informative —
method disagreements and Splink's confident/borderline calls — as a CSV with an
empty ``human_label`` column to fill in (1 = same entity, 0 = different, ? =
unsure).

Buckets sampled (capped per bucket so the sheet stays reviewable):
  * difflib_yes_splink_no  — difflib >= 0.88 but Splink < 0.5 (difflib's likely
    false positives; the precision case)
  * splink_yes_difflib_no  — Splink >= 0.9 but difflib < 0.88 (name-variant
    catches; the recall case)
  * splink_borderline      — 0.4 <= Splink < 0.6 (where a threshold would sit)
  * splink_high_confident  — Splink >= 0.95 (precision spot-check)

Run from ``backend/``::

    uv run python spikes/splink_er/make_label_worksheet.py --per-bucket 25
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
OUT = HERE / "corpus" / "label_worksheet.csv"
BASELINE = 0.88


def same(a: dict, b: dict) -> bool:
    return bool((a.get("lei") and a["lei"] == b.get("lei"))
                or (a.get("nat_reg") and a["nat_reg"] == b.get("nat_reg")))


def bucket(p: float, ratio: float) -> str | None:
    if ratio >= BASELINE and p < 0.5:
        return "difflib_yes_splink_no"
    if p >= 0.9 and ratio < BASELINE:
        return "splink_yes_difflib_no"
    if 0.4 <= p < 0.6:
        return "splink_borderline"
    if p >= 0.95:
        return "splink_high_confident"
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-bucket", type=int, default=25)
    args = ap.parse_args()

    df = load_corpus()
    by = {r["record_id"]: r for r in df.to_dict("records")}
    linker = Linker(df, json.loads(MODEL.read_text()), db_api=DuckDBAPI())
    preds = linker.inference.predict().as_pandas_dataframe()

    rows: dict[str, list[dict]] = {}
    for _, r in preds.iterrows():
        a, b = by.get(str(r["record_id_l"])), by.get(str(r["record_id_r"]))
        if not a or not b or a["source_id"] == b["source_id"]:
            continue  # cross-source only — the real ER task
        p = float(r["match_probability"])
        ratio = difflib.SequenceMatcher(a=a.get("name_norm") or "", b=b.get("name_norm") or "").ratio()
        bk = bucket(p, ratio)
        if not bk:
            continue
        rows.setdefault(bk, []).append({
            "bucket": bk,
            "splink_p": round(p, 3),
            "name_sim": round(ratio, 2),
            "shared_id_proxy": int(same(a, b)),  # the (unreliable) auto-label, for reference
            "name_a": a.get("name_raw") or a.get("name_norm"),
            "name_b": b.get("name_raw") or b.get("name_norm"),
            "juris_a": a.get("jurisdiction") or "",
            "juris_b": b.get("jurisdiction") or "",
            "date_a": a.get("inc_date") or "",
            "date_b": b.get("inc_date") or "",
            "source_a": a.get("source_id"),
            "source_b": b.get("source_id"),
            "record_id_a": a["record_id"],
            "record_id_b": b["record_id"],
            "human_label": "",   # <-- fill: 1 same / 0 different / ? unsure
            "notes": "",
        })

    fields = [
        "bucket", "human_label", "notes", "splink_p", "name_sim", "shared_id_proxy",
        "name_a", "name_b", "juris_a", "juris_b", "date_a", "date_b",
        "source_a", "source_b", "record_id_a", "record_id_b",
    ]
    OUT.parent.mkdir(exist_ok=True)
    total = 0
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for bk, items in rows.items():
            items.sort(key=lambda x: -x["splink_p"])
            for it in items[: args.per_bucket]:
                w.writerow(it)
                total += 1
    print(f"wrote {total} pairs to {OUT}")
    for bk, items in rows.items():
        print(f"  {bk:<24} available={len(items):4d} sampled={min(len(items), args.per_bucket)}")
    print("\nLabel `human_label` as 1 (same entity) / 0 (different) / ? (unsure), then")
    print("compute true precision = (#1 among splink>=0.5) / (#labelled among splink>=0.5).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
