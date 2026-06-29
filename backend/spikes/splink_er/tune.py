"""tune.py — iteration 3: compare comparison-set / m-estimation variants.

Iteration 2 found Splink false-negatives obvious matches (identical name, same
jurisdiction, shared id scored 0.06) because `inc_date`/`address` disagree
across sources and the model over-penalises that. This trains several model
variants and scores each on the identifier labels (cross-source precision /
recall at p>=0.5), so we can see which comparison setup recovers recall without
losing the precision win.

One variant per run (keeps under the shell time budget); results accumulate in
`corpus/tune_results.json` and the full table prints each run.

    uv run python spikes/splink_er/tune.py --variant floor_nj_label
"""

from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations
from pathlib import Path

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

RESULTS = HERE / "corpus" / "tune_results.json"


def comparisons(feats: list[str]) -> list:
    lib = {
        "name": cl.NameComparison("name_norm"),
        "juris": cl.ExactMatch("jurisdiction"),
        "date": cl.DateOfBirthComparison("inc_date", input_is_string=True),
        "addr": cl.JaroWinklerAtThresholds("address_local", [0.9, 0.7]),
    }
    return [lib[f] for f in feats]


# variant -> (feature list, m-method)
VARIANTS = {
    "it2_all_label": (["name", "juris", "date", "addr"], "label"),
    "floor_nj_label": (["name", "juris"], "label"),
    "nj_date_label": (["name", "juris", "date"], "label"),
    "nj_addr_label": (["name", "juris", "addr"], "label"),
    "all_em": (["name", "juris", "date", "addr"], "em"),
}


def same_entity(a: dict, b: dict) -> bool:
    return bool((a.get("lei") and a["lei"] == b.get("lei"))
                or (a.get("nat_reg") and a["nat_reg"] == b.get("nat_reg")))


def run(variant: str) -> dict:
    feats, m_method = VARIANTS[variant]
    df = load_corpus()
    by_id = {r["record_id"]: r for r in df.to_dict("records")}
    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        blocking_rules_to_generate_predictions=[
            block_on("substr(name_norm, 1, 4)"),
            block_on("jurisdiction", "substr(name_norm, 1, 1)"),
        ],
        comparisons=comparisons(feats),
        retain_intermediate_calculation_columns=True,
    )
    linker = Linker(df, settings, db_api=DuckDBAPI())
    linker.training.estimate_probability_two_random_records_match([block_on("lei")], recall=0.9)
    linker.training.estimate_u_using_random_sampling(max_pairs=5e5)
    if m_method == "label":
        linker.training.estimate_m_from_label_column("lei")
    else:
        linker.training.estimate_parameters_using_expectation_maximisation(block_on("name_norm"))

    preds = linker.inference.predict().as_pandas_dataframe()
    tp = fp = pos = 0
    for _, r in preds.iterrows():
        a, b = by_id.get(str(r["record_id_l"])), by_id.get(str(r["record_id_r"]))
        if not a or not b or a["source_id"] == b["source_id"]:
            continue
        if same_entity(a, b):
            pos += 1
        if r["match_probability"] >= 0.5:
            tp += int(same_entity(a, b))
            fp += int(not same_entity(a, b))
    # recall denominator: cross-source positives reachable in the candidate set
    # (computed as 'pos' above already restricts to scored cross-source pairs)
    predicted = tp + fp
    p = tp / predicted if predicted else 0.0
    rec = tp / pos if pos else 0.0
    f = 2 * p * rec / (p + rec) if (p + rec) else 0.0
    return {"variant": variant, "features": "+".join(feats), "m": m_method,
            "precision": round(p, 3), "recall": round(rec, 3), "f1": round(f, 3),
            "predicted": predicted, "cross_pos": pos}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", choices=list(VARIANTS), required=True)
    args = ap.parse_args()
    res = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
    res[args.variant] = run(args.variant)
    RESULTS.write_text(json.dumps(res, indent=2))
    print(f"\n{'variant':<16}{'features':<22}{'m':<7}{'prec':>6}{'recall':>8}{'F1':>6}{'pred':>7}")
    for v in res.values():
        print(f"{v['variant']:<16}{v['features']:<22}{v['m']:<7}"
              f"{v['precision']:>6}{v['recall']:>8}{v['f1']:>6}{v['predicted']:>7}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
