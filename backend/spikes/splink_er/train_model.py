"""train_model.py — Phase 2-3 of the Splink ER spike: explore + train a model.

Loads the Phase 1 corpus (corpus/entities.csv), trains a Splink Fellegi-Sunter
model on the **soft features only** (name / jurisdiction / incorporation date /
address — identifiers held out), and saves ``model.json`` + the match-weights
chart. The identifier labels (``lei``) are used only to (a) seed the prior via a
deterministic rule and (b) cross-check ``m`` — never as a model feature.

Run from ``backend/`` (after ``uv sync --group spike``)::

    uv run python spikes/splink_er/train_model.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

HERE = Path(__file__).parent
CSV = HERE / "corpus" / "entities.csv"
MODEL_OUT = HERE / "model.json"
CHARTS = HERE / "charts"

# Columns that should be treated as "no value" (NULL) when blank, so Splink
# doesn't (a) block thousands of empty-string rows together or (b) score a
# blank-vs-blank agreement as evidence.
_NULLABLE = ["name_norm", "jurisdiction", "inc_date", "address_norm", "lei", "nat_reg"]


def load_corpus() -> pd.DataFrame:
    # pandas tolerates the quoted-newline fields that trip DuckDB's CSV sniffer.
    df = pd.read_csv(CSV, dtype=str, keep_default_na=False)
    df = df.drop_duplicates("record_id")            # legacy pre-dedupe rows
    df = df[df["name_norm"].str.len() > 0]          # name is the anchor feature
    for c in _NULLABLE:                              # blank -> NULL for Splink
        df[c] = df[c].replace("", None)
    return df.reset_index(drop=True)


def build_settings() -> SettingsCreator:
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        # Block prediction pairs so we don't score the full cartesian product.
        blocking_rules_to_generate_predictions=[
            block_on("substr(name_norm, 1, 4)"),
            block_on("jurisdiction", "substr(name_norm, 1, 1)"),
        ],
        comparisons=[
            cl.NameComparison("name_norm"),
            cl.ExactMatch("jurisdiction"),
            # inc_date is an ISO date string; DOB comparison gives exact / close
            # / far levels (the logic is identical for an incorporation date).
            cl.DateOfBirthComparison("inc_date", input_is_string=True),
            cl.JaroWinklerAtThresholds("address_norm", [0.9, 0.7]),
        ],
        retain_intermediate_calculation_columns=True,
    )


def main() -> int:
    CHARTS.mkdir(exist_ok=True)
    df = load_corpus()
    n = len(df)
    print(f"corpus: {n} entity rows (deduped, name present)")

    linker = Linker(df, build_settings(), db_api=DuckDBAPI())

    # 1) Prior: records that share an LEI are a match -> use it as a deterministic
    #    rule to estimate P(two random records match). LEI is NULL where absent,
    #    so this block can't explode on blanks. (Label used for the prior only.)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("lei")], recall=0.9
    )
    # 2) u (coincidence) from random sampling — no labels needed.
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    # 3) m (data quality) via unsupervised EM, blocked two ways for coverage.
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("name_norm"))
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("jurisdiction"))

    linker.misc.save_model_to_json(str(MODEL_OUT), overwrite=True)
    print(f"saved model -> {MODEL_OUT}")

    # m cross-check: estimate m straight from the LEI label and compare to EM.
    try:
        linker.training.estimate_m_from_label_column("lei")
        print("m-from-label estimated (compare with EM via parameter chart)")
    except Exception as e:  # noqa: BLE001
        print(f"(m-from-label skipped: {type(e).__name__}: {e})")

    # Charts (HTML; no extra renderer needed).
    try:
        linker.visualisations.match_weights_chart().save(str(CHARTS / "match_weights.html"))
        print(f"match-weights chart -> {CHARTS / 'match_weights.html'}")
    except Exception as e:  # noqa: BLE001
        print(f"(match-weights chart skipped: {type(e).__name__}: {e})")

    # Quick prediction sanity check.
    preds = linker.inference.predict(threshold_match_probability=0.5)
    pdf = preds.as_pandas_dataframe()
    print(f"\npredicted pairs >=0.5: {len(pdf)}")
    for thr in (0.5, 0.9, 0.99):
        print(f"  >= {thr}: {(pdf['match_probability'] >= thr).sum()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
