"""Phase 259 — the SEC EDGAR data-quality ticket (issues 1–3).

1. EDGAR's state-and-country codes, from the SEC's published table. The old
   table knew X1, the states and X2 — read as Canada; X2 is Burkina Faso — so
   TCI Fund Management and Christopher Hohn (X0, United Kingdom) carried no
   nationality or jurisdiction.
2. Joint filings stay one relationship per reporting person, as filed, and
   say that their holdings may be the same shares (Stephen, 28 Sept 2026).
3. NON_EU_JURISDICTION is one note per lookup: pooled across sources, the
   sources named in ``evidence.reported_by``, and "including ended
   relationships" only for a country reached through ended links alone.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from opencheck.bods.mapper import map_sec_edgar
from opencheck.risk import (
    INCLUDING_ENDED,
    _non_eu_jurisdiction_signal,
    merge_non_eu_jurisdiction,
)
from opencheck.routers.lookup import _merge_signals
from opencheck.sources.edgar_codes import EDGAR_CODES, edgar_country
from opencheck.sources.sec_edgar import _edgar_citizenship, _parse_filing_xml

_FIXTURES = Path(__file__).parent / "fixtures" / "sec_edgar"
_MOODYS = "1059556"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. EDGAR codes
# ---------------------------------------------------------------------------


def test_every_code_in_the_secs_table_is_mapped_with_the_secs_name() -> None:
    published = json.loads(_fixture("edgar_state_country_codes.json"))
    assert len(published) == 309
    assert set(EDGAR_CODES) == {code for code, _ in published}
    for code, name in published:
        assert EDGAR_CODES[code][1] == name, code


@pytest.mark.parametrize(
    ("code", "iso"),
    [
        ("X0", "GB"),
        ("X1", "US"),
        ("X2", "BF"),  # Burkina Faso — the old table said Canada
        ("Z4", "CA"),
        ("A6", "CA"),  # Ontario
        ("E9", "KY"),  # Cayman Islands
        ("D8", "VG"),  # British Virgin Islands
        ("K3", "HK"),
        ("DE", "US"),
        ("GU", "GU"),
        ("PR", "PR"),
        ("VI", "VI"),
        ("V6", "SZ"),  # "SWAZILAND"
        ("W8", "TR"),  # "TURKEY"
        ("P8", ""),  # Netherlands Antilles — no current ISO code
        ("XX", ""),
    ],
)
def test_edgar_country(code: str, iso: str) -> None:
    assert edgar_country(code)[0] == iso


def test_every_iso_code_is_a_real_one() -> None:
    import pycountry

    for code, (iso, _name) in EDGAR_CODES.items():
        if iso:
            assert pycountry.countries.get(alpha_2=iso) is not None, code


def test_a_filer_writing_the_name_not_the_code() -> None:
    assert _edgar_citizenship("United Kingdom") == ("GB", "UNITED KINGDOM")
    assert _edgar_citizenship("Delaware") == ("US", "DELAWARE")
    assert _edgar_citizenship(" x0 ") == ("GB", "UNITED KINGDOM")
    assert _edgar_citizenship("Atlantis") == ("", "")
    assert _edgar_citizenship("") == ("", "")


def _moodys_bundle() -> dict:
    """Moody's three real 13G filings, parsed, as ``fetch`` assembles them."""
    filings = []
    for name, filed in (
        ("moodys_13ga_tci_hohn.xml", "2026-05-15"),
        ("moodys_13g_vanguard_capital_management.xml", "2026-04-30"),
        ("moodys_13ga_vanguard_group_exit.xml", "2026-03-27"),
    ):
        parsed = _parse_filing_xml(_fixture(name))
        assert parsed is not None
        reporters = parsed["reporters"]
        for r in reporters:
            filings.append(
                {
                    "reporter": r,
                    "joint_with": [o["name"] for o in reporters if o is not r],
                    "issuer": parsed["issuer"],
                    "filer_cik": parsed["filer_cik"],
                    "filing_url": f"https://www.sec.gov/{name}",
                    "form_type": "SCHEDULE 13G",
                    "filed": filed,
                    "event_date": parsed["event_date"],
                }
            )
    return {"source_id": "sec_edgar", "issuer_cik": _MOODYS, "filings": filings}


def _by_name(statements: list[dict]) -> dict[str, dict]:
    out = {}
    for s in statements:
        rd = s.get("recordDetails", {})
        if s["recordType"] == "entity":
            out[rd["name"]] = s
        elif s["recordType"] == "person":
            out[rd["names"][0]["fullName"]] = s
    return out


def test_tci_and_hohn_are_british() -> None:
    parsed = _parse_filing_xml(_fixture("moodys_13ga_tci_hohn.xml"))
    assert parsed is not None
    assert [r["citizenship_iso"] for r in parsed["reporters"]] == ["GB", "GB"]

    parties = _by_name(list(map_sec_edgar(_moodys_bundle())))
    hohn = parties["Christopher Hohn"]["recordDetails"]
    assert hohn["nationalities"] == [{"name": "United Kingdom", "code": "GB"}]
    tci = parties["TCI Fund Management Ltd"]["recordDetails"]
    assert tci["jurisdiction"] == {"name": "United Kingdom", "code": "GB"}
    # Vanguard (PA) is still the US.
    vcm = parties["Vanguard Capital Management"]["recordDetails"]
    assert vcm["jurisdiction"]["code"] == "US"


def test_a_place_with_no_iso_code_is_named_and_never_coded() -> None:
    bundle = {
        "source_id": "sec_edgar",
        "issuer_cik": _MOODYS,
        "filings": [
            {
                "issuer": {"cik": _MOODYS, "name": "Moody's Corporation"},
                "reporter": {
                    "name": "Antilles Holdings N.V.",
                    "type_code": "CO",
                    "citizenship_iso": "",
                    "citizenship_name": "NETHERLANDS ANTILLES",
                    "percent_of_class": 5.5,
                },
                "filing_url": "https://www.sec.gov/x",
                "form_type": "SCHEDULE 13G",
                "filed": "2026-05-01",
            }
        ],
    }
    parties = _by_name(list(map_sec_edgar(bundle)))
    jur = parties["Antilles Holdings N.V."]["recordDetails"]["jurisdiction"]
    assert jur == {"name": "Netherlands Antilles"}


# ---------------------------------------------------------------------------
# 2. Joint filings
# ---------------------------------------------------------------------------


def _interest_details(statements: list[dict]) -> dict[str, str]:
    names = {}
    for s in statements:
        rd = s.get("recordDetails", {})
        if s["recordType"] == "entity":
            names[s["statementId"]] = rd["name"]
        elif s["recordType"] == "person":
            names[s["statementId"]] = rd["names"][0]["fullName"]
    return {
        names[s["recordDetails"]["interestedParty"]]: s["recordDetails"]["interests"][0].get("details", "")
        for s in statements
        if s["recordType"] == "relationship"
    }


def test_joint_filers_stay_as_filed_and_say_so() -> None:
    statements = list(map_sec_edgar(_moodys_bundle()))
    rels = [s for s in statements if s["recordType"] == "relationship"]
    # One relationship per reporting person — no inferred control chain.
    assert len(rels) == 4
    assert not any(
        i["type"] != "shareholding" for s in rels for i in s["recordDetails"]["interests"]
    )
    details = _interest_details(statements)
    assert "Reported in one joint filing with Christopher Hohn" in details["TCI Fund Management Ltd"]
    assert "Reported in one joint filing with TCI Fund Management Ltd" in details["Christopher Hohn"]
    assert "not added together" in details["Christopher Hohn"]
    # A sole filer says nothing about joint filing.
    assert "joint" not in details["Vanguard Capital Management"]
    assert "joint" not in details["The Vanguard Group"]


def test_three_joint_filers_are_listed_as_a_sentence() -> None:
    bundle = _moodys_bundle()
    bundle["filings"][0]["joint_with"] = ["A LLC", "B LP"]
    details = _interest_details(list(map_sec_edgar(bundle)))
    assert "Reported in one joint filing with A LLC and B LP;" in details["TCI Fund Management Ltd"]


# ---------------------------------------------------------------------------
# 3. One "outside the EU/EEA" note per lookup
# ---------------------------------------------------------------------------


def _sig(source_id: str, jurisdictions: list[dict], ended_ids: list[str] | None = None) -> dict:
    ev: dict = {"jurisdictions": jurisdictions}
    if ended_ids:
        ev["includes_ended_relationships"] = True
        ev["ended_relationship_statement_ids"] = ended_ids
    return {
        "code": "NON_EU_JURISDICTION",
        "confidence": "low",
        "kind": "context",
        "summary": "x",
        "source_id": source_id,
        "hit_id": "h",
        "evidence": ev,
    }


def test_moodys_sec_bundle_reaches_the_us_and_gb_through_current_holdings() -> None:
    # The Vanguard Group's holding ended (exit filing), but the US is still
    # reached through Vanguard Capital Management's current one — so the note
    # carries no "including ended relationships". Before Phase 259 it did.
    statements = list(map_sec_edgar(_moodys_bundle()))
    sig = _non_eu_jurisdiction_signal("sec_edgar", _MOODYS, statements)
    assert sig is not None
    d = sig.to_dict()
    assert INCLUDING_ENDED not in d["summary"]
    assert "reaches jurisdictions outside the EU/EEA: GB, US." in d["summary"]
    assert "includes_ended_relationships" not in d["evidence"]
    ended_only = [j for j in d["evidence"]["jurisdictions"] if j.get("via_ended_only")]
    assert [j["code"] for j in ended_only] == ["US"]  # The Vanguard Group itself


def test_ended_only_country_keeps_the_qualifier() -> None:
    bundle = _moodys_bundle()
    # Make the exit filer the only US party: drop Vanguard Capital Management.
    bundle["filings"] = [
        f for f in bundle["filings"] if f["reporter"]["name"] != "Vanguard Capital Management"
    ]
    statements = list(map_sec_edgar(bundle))
    sig = _non_eu_jurisdiction_signal("sec_edgar", _MOODYS, statements)
    assert sig is not None
    d = sig.to_dict()
    assert f"({INCLUDING_ENDED})" in d["summary"]
    assert d["evidence"]["includes_ended_relationships"] is True


def test_two_sources_merge_into_one_note_that_names_both() -> None:
    wikidata = _sig("wikidata", [{"statement_id": "W-US", "code": "US", "name": "United States"}])
    sec = _sig(
        "sec_edgar",
        [
            {"statement_id": "S-US", "code": "US", "name": "United States"},
            {"statement_id": "S-GB", "code": "GB", "name": "United Kingdom"},
        ],
    )
    merged = [s for s in _merge_signals([wikidata], [sec]) if s["code"] == "NON_EU_JURISDICTION"]
    assert len(merged) == 1
    ev = merged[0]["evidence"]
    assert ev["reported_by"] == ["wikidata", "sec_edgar"]
    # Every node keeps its badge.
    assert {j["statement_id"] for j in ev["jurisdictions"]} == {"W-US", "S-US", "S-GB"}
    assert "outside the EU/EEA: GB, US." in merged[0]["summary"]
    assert INCLUDING_ENDED not in merged[0]["summary"]


def test_a_current_holding_in_another_source_clears_the_qualifier() -> None:
    wikidata = _sig("wikidata", [{"statement_id": "W-US", "code": "US"}])
    sec = _sig(
        "sec_edgar",
        [{"statement_id": "S-US", "code": "US", "via_ended_only": True}],
        ended_ids=["rel-1"],
    )
    merged = merge_non_eu_jurisdiction(wikidata, sec)
    assert INCLUDING_ENDED not in merged["summary"]
    assert "includes_ended_relationships" not in merged["evidence"]

    alone = merge_non_eu_jurisdiction(sec, _sig("gleif", [{"statement_id": "G-GB", "code": "GB"}]))
    assert f"({INCLUDING_ENDED})" in alone["summary"]
    assert alone["evidence"]["ended_relationship_statement_ids"] == ["rel-1"]


def test_merge_order_does_not_change_the_answer() -> None:
    a = _sig("wikidata", [{"statement_id": "W-US", "code": "US"}])
    b = _sig("sec_edgar", [{"statement_id": "S-GB", "code": "GB"}])
    ab = merge_non_eu_jurisdiction(a, b)
    ba = merge_non_eu_jurisdiction(b, a)
    assert ab["summary"] == ba["summary"]
    assert set(ab["evidence"]["reported_by"]) == set(ba["evidence"]["reported_by"])


def test_mcp_rows_and_the_report_name_every_source() -> None:
    from opencheck.mcp import shaping
    from opencheck.reporting.html_report import _signal_source_name
    from opencheck.sources import REGISTRY

    merged = merge_non_eu_jurisdiction(
        _sig("wikidata", [{"statement_id": "W-US", "code": "US"}]),
        _sig("sec_edgar", [{"statement_id": "S-US", "code": "US"}]),
    )
    rows = shaping._shape_risk([merged])
    assert rows[0]["sources"] == ["wikidata", "sec_edgar"]
    name = _signal_source_name(merged, REGISTRY)
    assert REGISTRY["wikidata"].info.name in name
    assert REGISTRY["sec_edgar"].info.name in name
