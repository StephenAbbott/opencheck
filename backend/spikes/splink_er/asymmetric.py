"""asymmetric.py — iteration 3 recall-tail fix: asymmetric date/address.

Diagnosis (see iteration-3 dev notes): `inc_date`'s intermediate levels were
untrained (m=None) so a *near-correct* date (off by ~1y) scored ~-4.4 bits —
strong evidence *against* a true match. And the "all other" levels were strongly
negative, punishing cross-source formatting differences.

Fix: make date/address **asymmetric** — reward agreement, treat disagreement as
~neutral:
  * `inc_date`  -> ExactMatch (exact / else), no sparse intermediate levels;
  * `address_local` -> JaroWinkler>=0.9 / else;
  * after training, clamp the "all other" level m=u (weight ~0) for both, so a
    mismatch neither helps nor hurts.

Compares: symmetric (trained) vs asymmetric (clamped). Run from `backend/`::

    uv run python spikes/splink_er/asymmetric.py
"""

from __future__ import annotations

import copy
import json
import sys
import tempfile
from pathlib import Path

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from train_model import load_corpus  # noqa: E402

OUT = HERE / "model_asymmetric.json"


def settings() -> SettingsCreator:
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        blocking_rules_to_generate_predictions=[
            block_on("substr(name_norm, 1, 4)"),
            block_on("jurisdiction", "substr(name_norm, 1, 1)"),
        ],
        comparisons=[
            cl.NameComparison("name_norm"),
            cl.ExactMatch("jurisdiction"),
            cl.ExactMatch("inc_date"),                       # exact / else (no sparse levels)
            cl.JaroWinklerAtThresholds("address_local", [0.9]),  # exact / >=0.9 / else
        ],
        retain_intermediate_calculation_columns=True,
    )


def same(a: dict, b: dict) -> bool:
    return bool((a.get("lei") and a["lei"] == b.get("lei"))
                or (a.get("nat_reg") and a["nat_reg"] == b.get("nat_reg")))


def clamp_disagreement(model: dict) -> dict:
    """Set the 'all other' level m=u (weight ~0) for date + address."""
    m = copy.deepcopy(model)
    for comp in m["comparisons"]:
        if comp.get("output_column_name") in ("inc_date", "address_local"):
            for lvl in comp["comparison_levels"]:
                if "all other" in (lvl.get("label_for_charts", "").lower()):
                    if lvl.get("u_probability"):
                        lvl["m_probability"] = lvl["u_probability"]
    return m


def main() -> int:
    df = load_corpus()
    by = {r["record_id"]: r for r in df.to_dict("records")}

    linker = Linker(df, settings(), db_api=DuckDBAPI())
    linker.training.estimate_probability_two_random_records_match([block_on("lei")], recall=0.9)
    linker.training.estimate_u_using_random_sampling(max_pairs=5e5)
    linker.training.estimate_m_from_label_column("lei")
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
        linker.misc.save_model_to_json(tf.name, overwrite=True)
        trained = json.loads(Path(tf.name).read_text())
    OUT.write_text(json.dumps(clamp_disagreement(trained), indent=2))

    def evaluate(model: dict, tag: str) -> None:
        preds = Linker(df, model, db_api=DuckDBAPI()).inference.predict().as_pandas_dataframe()
        tp = fp = pos = 0
        for _, r in preds.iterrows():
            a, b = by.get(str(r["record_id_l"])), by.get(str(r["record_id_r"]))
            if not a or not b or a["source_id"] == b["source_id"]:
                continue
            s = same(a, b)
            pos += s
            if r["match_probability"] >= 0.5:
                tp += s
                fp += not s
        p = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / pos if pos else 0.0
        f = 2 * p * rec / (p + rec) if (p + rec) else 0.0
        print(f"{tag:<34} prec={p:.3f} recall={rec:.3f} F1={f:.3f} pred={tp + fp} cross_pos={pos}")

    print("(it2 all-4 symmetric baseline was: prec=0.989 recall=0.827 F1=0.901)")
    evaluate(trained, "asym features, SYMMETRIC (trained)")
    evaluate(clamp_disagreement(trained), "asym features, CLAMPED (disagree~0)")

    # trace a near-miss true match: same name+juris+address, date off by ~1y
    lk = Linker(df, clamp_disagreement(trained), db_api=DuckDBAPI())
    r1 = {"record_id": "a", "name_norm": "acme holdings limited", "jurisdiction": "GB",
          "inc_date": "1990-01-01", "address_local": "1 king st london",
          "address_norm": "1 king st london", "lei": None, "nat_reg": None}
    r2 = {**r1, "record_id": "b", "inc_date": "1991-03-01"}  # date near-miss only
    p = float(lk.inference.compare_two_records(r1, r2).as_pandas_dataframe()["match_probability"].iloc[0])
    print(f"\nnear-miss trace (same name+juris+addr, date off ~1y) -> p={p:.3f} "
          f"(it2 would strongly penalise this)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
