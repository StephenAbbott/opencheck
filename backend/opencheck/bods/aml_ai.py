"""Google AML AI export — project a BODS bundle into the AML AI input tables.

Thin wrapper over the `bods-aml-ai
<https://github.com/StephenAbbott/bods-aml-ai>`_ library (a pinned git
dependency) via its in-memory ``transform_statements()`` API — no files read
or written on the way through.

Google's AML AI data model is account- and transaction-centric with **no
native party-to-party relationship table**, so ownership is encoded through
its extensibility mechanisms: numeric ownership signals per party in
``party_supplementary_data`` (``bo_ownership_pct_<subject>``,
``bo_is_beneficial_owner``, per-interest-type flags…) and synthetic
"ownership accounts" in ``account_party_link`` (the owned entity as
``PRIMARY_HOLDER``, each owner as ``SUPPLEMENTARY_HOLDER``). The bundled
README explains the encoding to whoever loads the tables.
"""

from __future__ import annotations

import json
from typing import Any

from bods_aml_ai import transform_statements

# Table name → manifest count key. Order is the zip's file order.
_TABLES = {
    "party": "aml_ai_party_count",
    "party_supplementary_data": "aml_ai_supplementary_row_count",
    "account_party_link": "aml_ai_account_link_count",
}


def map_to_aml_ai(bods_statements: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Map BODS statements to AML AI table rows (``{table: [row, ...]}``)."""
    return transform_statements(bods_statements)


def aml_ai_counts(bods_statements: list[dict[str, Any]]) -> dict[str, int]:
    """Row counts per AML AI table for the export manifest."""
    tables = map_to_aml_ai(bods_statements)
    return {key: len(tables.get(table, [])) for table, key in _TABLES.items()}


def _readme(counts: dict[str, int]) -> str:
    return (
        "# OpenCheck → Google AML AI export\n"
        "\n"
        "This package projects an OpenCheck BODS v0.4 ownership bundle into the\n"
        "[Google AML AI input data model](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model),\n"
        "generated with [bods-aml-ai](https://github.com/StephenAbbott/bods-aml-ai).\n"
        "\n"
        "## Contents\n"
        "\n"
        f"- `party.ndjson` — {counts['aml_ai_party_count']} Party row(s): persons as\n"
        "  `CONSUMER`, entities as `COMPANY`\n"
        f"- `party_supplementary_data.ndjson` — {counts['aml_ai_supplementary_row_count']}\n"
        "  row(s) of numeric ownership signals per party\n"
        "  (`bo_ownership_pct_<subject>`, `bo_is_beneficial_owner`, `bo_is_direct`,\n"
        "  per-interest-type flags, and per-subject aggregates)\n"
        f"- `account_party_link.ndjson` — {counts['aml_ai_account_link_count']}\n"
        "  row(s) of synthetic \"ownership accounts\" (`bods-ownership-<subject>`):\n"
        "  the owned entity is linked as `PRIMARY_HOLDER`, each owner as\n"
        "  `SUPPLEMENTARY_HOLDER`\n"
        "- `LICENSES.md` — licence and attribution for every contributing source\n"
        "\n"
        "## Why ownership is encoded this way\n"
        "\n"
        "AML AI has no party-to-party relationship table — it scores party-level\n"
        "risk from accounts, transactions and party attributes. BODS\n"
        "ownership-or-control statements are therefore decomposed into\n"
        "supplementary-data signals, plus the synthetic accounts so AML AI's\n"
        "graph-based scoring can still see connections between parties sharing an\n"
        "ownership relationship.\n"
        "\n"
        "## Load into BigQuery\n"
        "\n"
        "```bash\n"
        "DATASET=my_project.my_aml_dataset\n"
        "bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \\\n"
        "  $DATASET.party party.ndjson\n"
        "bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \\\n"
        "  $DATASET.party_supplementary_data party_supplementary_data.ndjson\n"
        "bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \\\n"
        "  $DATASET.account_party_link account_party_link.ndjson\n"
        "```\n"
        "\n"
        "Transaction, RiskCaseEvent and InteractionEvent tables are not part of\n"
        "this export — beneficial ownership data cannot populate them.\n"
    )


def build_aml_ai_files(bods_statements: list[dict[str, Any]]) -> dict[str, str]:
    """The zip's members (path → text content), minus LICENSES.md.

    Empty tables are skipped so `bq load` never sees a zero-line file.
    """
    tables = map_to_aml_ai(bods_statements)
    files: dict[str, str] = {}
    for table in _TABLES:
        rows = tables.get(table, [])
        if rows:
            files[f"{table}.ndjson"] = (
                "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows) + "\n"
            )
    counts = {key: len(tables.get(table, [])) for table, key in _TABLES.items()}
    files["README.md"] = _readme(counts)
    return files
