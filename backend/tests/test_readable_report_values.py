"""Readable values in the exported report (the 23 Nov 2026 production check).

The Markdown report printed "As of 2026-11-23.", a Records compared signal as
"Record Consistency" attributed to "opencheck", and the derived identifier keys
as "No orgnr" / "Ocid". Each is pinned here, with the identifier-label table
checked against every key an adapter or the pipeline can put in
``derived_identifiers``.
"""

from __future__ import annotations

import ast
from pathlib import Path

from opencheck import disclosure, integrated_ownership, sanctions_ownership
from opencheck.identifier_labels import IDENTIFIER_LABELS, identifier_label
from opencheck.reporting import build_report_markdown
from opencheck.sources import REGISTRY
from opencheck.ui_dates import ui_date

_PACKAGE = Path(__file__).resolve().parents[1] / "opencheck"


def _derived_keys() -> set[str]:
    keys: set[str] = set()
    for adapter in REGISTRY.values():
        for d in getattr(adapter, "lookup_derivers", ()) or ():
            keys.add(d.derived_key)
        keys |= set(getattr(adapter, "lookup_dispatch_keys", ()) or ())
    # Keys the pipeline sets itself: ``ctx.derived["…"] = …``.
    for path in (_PACKAGE / "routers").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if (
                        isinstance(t, ast.Subscript)
                        and isinstance(t.value, ast.Attribute)
                        and t.value.attr == "derived"
                        and isinstance(t.slice, ast.Constant)
                        and isinstance(t.slice.value, str)
                    ):
                        keys.add(t.slice.value)
    return keys


def test_every_derived_identifier_key_has_a_label() -> None:
    keys = _derived_keys()
    assert {"no_orgnr", "ocid", "lei", "wikidata_qid"} <= keys
    missing = sorted(keys - set(IDENTIFIER_LABELS))
    assert not missing, f"identifier_labels.IDENTIFIER_LABELS is missing: {missing}"


def test_the_named_labels() -> None:
    assert identifier_label("no_orgnr") == "Norwegian organisation number"
    assert identifier_label("ocid") == "OpenCorporates ID"
    assert identifier_label("some_new_key") == "Some new key"


def test_ui_date() -> None:
    assert ui_date("2026-11-23") == "23 Nov 2026"
    assert ui_date("2026-09-16T23:30:00Z") == "16 Sept 2026"
    assert ui_date("") == ""
    assert ui_date("soon") == "soon"


def test_footers_say_dates_in_words() -> None:
    assert "As of 23 Nov 2026." in " ".join(
        disclosure.report_footer({"sources": [], "not_covered": [], "as_of": "2026-11-23"})
    )
    assert "stood on 23 Nov 2026." in " ".join(
        integrated_ownership.report_footer({"as_of": "2026-11-23"})
    )
    assert "stood on 23 Nov 2026." in " ".join(
        sanctions_ownership.report_footer({"evidence": {"as_of": "2026-11-23"}})
    )


def _report() -> dict:
    return {
        "lei": "1595T7RFSMR9TFCTJX53",
        "legal_name": "EIDE SEAFOOD AS",
        "jurisdiction": "NO",
        "derived_identifiers": {
            "lei": "1595T7RFSMR9TFCTJX53",
            "no_orgnr": "912345678",
            "ocid": "no/912345678",
        },
        "hits": [],
        "risk_signals": [
            {
                "code": "RECORD_CONSISTENCY",
                "kind": "context",
                "confidence": "high",
                "source_id": "opencheck",
                "summary": "Two sources describe this entity. They agree on jurisdiction.",
            }
        ],
        "license_notices": [],
        "bods": [],
        "disclosure_status": {"sources": [], "not_covered": ["wikidata"], "as_of": "2026-11-23"},
    }


def test_markdown_report_reads_without_codes_or_iso_dates() -> None:
    md = build_report_markdown(_report())
    assert "Norwegian organisation number" in md and "No orgnr" not in md
    assert "OpenCorporates ID" in md and "| Ocid |" not in md
    assert "**Records compared**" in md and "Record Consistency" not in md
    assert "*OpenCheck risk engine*" in md and "*opencheck*" not in md
    assert "As of 2026-11-23" not in md


def test_relationship_table_says_dates_and_types_in_words() -> None:
    from opencheck.reporting.diagram import _row_interest

    text = _row_interest(
        [
            {"type": "shareholding", "startDate": "2022-12-29", "endDate": "2024-03"},
            {"type": "votingRights", "startDate": "2019"},
        ]
    )
    assert text == "Shareholding, from 29 Dec 2022, to Mar 2024; Voting rights, from 2019"


def test_ui_date_keeps_the_source_precision() -> None:
    assert ui_date("2024-03") == "Mar 2024"
    assert ui_date("2019") == "2019"
    assert ui_date("2024-13") == "2024-13"
