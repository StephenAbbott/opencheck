"""Record consistency (Phase 152, shadow mode): do independent sources agree
about the same entity, and are the outcomes counted honestly?

The tests named for Novo Nordisk and Shell run the real 2 Sept 2026 exports
and pin the two decisions the analysis rested on: Wikidata's inception is
never compared with a register's incorporation date, and OpenCorporates ids
are never treated as a clash — both would otherwise fire on every well-known
company.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import consistency, consistencystats
from opencheck.app import app
from opencheck.bods import liveness
from opencheck.bods.mapper import make_entity_statement
from opencheck.consistency import (
    AGREE,
    DISAGREE,
    IDENTIFIER_CLASH,
    MIRROR,
    ONE_MISSING,
    STALE,
    assess_consistency,
    one_per_entity_identifiers,
    referent_groups,
)

FIXTURES = Path(__file__).parent / "fixtures" / "consistency"
FRONTEND_RECONCILE = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "reconcile.ts"

LEI = "21380068P1DRHMJ8KU70"


def _stmt(
    source_id: str,
    *,
    lei: str | None = LEI,
    number: tuple[str, str] | None = None,
    jurisdiction: tuple[str, str] = ("United Kingdom", "GB"),
    founding: str | None = None,
    status: str | None = None,
    since: str | None = None,
    name: str = "SHELL PLC",
) -> dict:
    ids = []
    if lei:
        ids.append({"id": lei, "scheme": "XI-LEI"})
    if number:
        ids.append({"id": number[1], "scheme": number[0]})
    stmt = make_entity_statement(
        source_id=source_id,
        local_id=f"{source_id}-{name}",
        name=name,
        jurisdiction=jurisdiction,
        identifiers=ids,
        founding_date=founding,
    )
    if status:
        liveness.apply_register_status(stmt, source_label=source_id, liveness=status, raw=status, since=since)
    return stmt


def _items(result, field=None, relation=None):
    return [
        i for i in result.items
        if (field is None or i.field == field) and (relation is None or i.relation == relation)
    ]


# ---------------------------------------------------------------------
# Referents
# ---------------------------------------------------------------------


def test_referent_groups_need_a_shared_strong_identifier() -> None:
    a = _stmt("gleif")
    b = _stmt("companies_house", lei=None, number=("GB-COH", "04366849"))
    c = _stmt("wikidata", lei=None, name="SHELL PLC")  # same name, no identifier
    assert referent_groups([a, b, c]) == []  # nothing links a and b
    b2 = _stmt("companies_house", number=("GB-COH", "04366849"))
    groups = referent_groups([a, b2, c])
    assert len(groups) == 1 and {s["statementId"] for s in groups[0]} == {a["statementId"], b2["statementId"]}


def test_same_source_twice_is_not_compared() -> None:
    result = assess_consistency([_stmt("openaleph", founding="2002-02-05"), _stmt("openaleph")])
    assert result.groups and result.items == []


# ---------------------------------------------------------------------
# Liveness — the comparison this was started for
# ---------------------------------------------------------------------


def test_register_dissolved_vs_gleif_active_is_a_disagreement() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03")
    gl = _stmt("gleif", status=liveness.LIVE)
    (item,) = _items(assess_consistency([ch, gl]), "liveness")
    assert item.relation == DISAGREE
    assert set(item.sources) == {"companies_house", "gleif"}
    assert set(item.values) == {"terminal", "live"}


def test_register_dissolved_vs_opencorporates_active_is_stale_not_disagree() -> None:
    """OpenCorporates republishes Companies House: a difference is the copy
    lagging, counted as ``stale`` and by decision never shown."""
    ch = _stmt("companies_house", status=liveness.TERMINAL)
    oc = _stmt("opencorporates", status=liveness.LIVE)
    (item,) = _items(assess_consistency([ch, oc]), "liveness")
    assert item.relation == STALE
    agree = _items(assess_consistency([ch, _stmt("opencorporates", status=liveness.TERMINAL)]), "liveness")
    assert agree[0].relation == MIRROR


def test_silent_source_is_one_missing_not_agreement() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL)
    gl = _stmt("gleif")  # said nothing about liveness
    (item,) = _items(assess_consistency([ch, gl]), "liveness")
    assert item.relation == ONE_MISSING
    assert item.values == ("terminal", None)


def test_both_silent_produces_nothing() -> None:
    assert _items(assess_consistency([_stmt("companies_house"), _stmt("gleif")]), "liveness") == []


# ---------------------------------------------------------------------
# Founding date and jurisdiction
# ---------------------------------------------------------------------


def test_founding_date_compares_at_the_coarser_precision() -> None:
    a = _stmt("gleif", founding="2002-02-05")
    b = _stmt("companies_house", founding="2002")
    assert _items(assess_consistency([a, b]), "founding_date")[0].relation == AGREE
    c = _stmt("companies_house", founding="2003-02-05")
    assert _items(assess_consistency([a, c]), "founding_date")[0].relation == DISAGREE


def test_wikidata_inception_is_never_compared_with_incorporation_novo_nordisk() -> None:
    """Novo Nordisk: four sources say 1931-11-28, Wikidata says 1923-01-01.
    Neither is wrong — inception ≠ incorporation — so no item exists."""
    a = _stmt("gleif", founding="1931-11-28", jurisdiction=("Denmark", "DK"))
    w = _stmt("wikidata", founding="1923-01-01", jurisdiction=("Denmark", "DK"))
    assert _items(assess_consistency([a, w]), "founding_date") == []
    # Wikidata still takes part in comparisons where the concept IS shared.
    assert _items(assess_consistency([a, w]), "jurisdiction")[0].relation == AGREE


def test_jurisdiction_disagreement() -> None:
    a = _stmt("gleif")
    b = _stmt("wikidata", jurisdiction=("Netherlands", "NL"))
    assert _items(assess_consistency([a, b]), "jurisdiction")[0].relation == DISAGREE


# ---------------------------------------------------------------------
# Identifier clash
# ---------------------------------------------------------------------


def test_opencorporates_ids_are_per_registration_not_a_clash_shell() -> None:
    """Shell plc carries gb/04366849 (GLEIF) and nl/34179503 (Wikidata) —
    two registrations of one entity, not a conflation."""
    a = _stmt("gleif")
    a["recordDetails"]["identifiers"].append({"id": "gb/04366849", "scheme": "OpenCorporates"})
    b = _stmt("wikidata")
    b["recordDetails"]["identifiers"].append({"id": "nl/34179503", "scheme": "OpenCorporates"})
    assert "OPENCORPORATES" not in one_per_entity_identifiers(a)
    clash = _items(assess_consistency([a, b]), IDENTIFIER_CLASH)
    assert all(i.relation == AGREE for i in clash)  # the LEI, which they share
    assert all("OPENCORPORATES" not in v for i in clash for v in i.values)


def test_same_register_different_number_is_a_clash() -> None:
    """Two records bridged by LEI whose Companies House numbers differ —
    a mis-resolution or a successor case, either way worth knowing."""
    a = _stmt("gleif", number=("", "04366849"))  # GLEIF registeredAs, unschemed
    b = _stmt("companies_house", number=("GB-COH", "04366850"))
    clash = _items(assess_consistency([a, b]), IDENTIFIER_CLASH, DISAGREE)
    assert len(clash) == 1
    assert set(clash[0].values) == {"REGISTER:GB:04366849", "REGISTER:GB:04366850"}


def test_register_number_formatting_differences_are_not_a_clash() -> None:
    a = _stmt("gleif", number=("", "556056-6258"), jurisdiction=("Sweden", "SE"))
    b = _stmt("bolagsverket", number=("SE-BLV", "5560566258"), jurisdiction=("Sweden", "SE"))
    assert _items(assess_consistency([a, b]), IDENTIFIER_CLASH, DISAGREE) == []


def test_tax_and_securities_ids_are_never_register_schemes() -> None:
    stmt = _stmt("gleif", lei=None)
    stmt["recordDetails"]["identifiers"] = [
        {"id": "1234567890", "scheme": "PL-NIP"},
        {"id": "GB00B03MM408", "scheme": "ISIN"},
        {"id": "0000000001", "scheme": "US-SEC-CIK"},
        {"id": "12345678", "scheme": "GB-COH"},
    ]
    assert set(one_per_entity_identifiers(stmt)) == {"GB-COH", "REGISTER:GB"}


def test_non_register_segments_match_the_frontend() -> None:
    """The backend and ``reconcile.ts`` must agree on which scheme segments are
    tax/securities/classification, or the merge and the clash check drift."""
    text = FRONTEND_RECONCILE.read_text(encoding="utf-8")
    block = re.search(r"NON_REGISTER_SEGMENTS = new Set\(\[(.*?)\]\)", text, re.S).group(1)
    frontend = set(re.findall(r'"([A-Z]+)"', block))
    assert frontend == set(consistency.NON_REGISTER_SEGMENTS)


# ---------------------------------------------------------------------
# The real exports
# ---------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ["novo_nordisk_2026-09-02", "shell_plc_2026-09-02"])
def test_live_exports_have_no_disagreements(fixture: str) -> None:
    """Production exports from the day the analysis was done. Every source
    describing the subject is grouped, the comparisons run, and — because the
    two known semantic mismatches are excluded by design — nothing disagrees."""
    bods = json.loads((FIXTURES / f"{fixture}.json").read_text(encoding="utf-8"))
    result = assess_consistency(bods)
    assert result.groups, "the subject should be described by several sources"
    subject = max(result.groups, key=len)
    assert len(subject) >= 5
    assert result.by_relation(DISAGREE) == []
    assert result.by_relation(STALE) == []
    fields = {i.field for i in result.items}
    assert {"jurisdiction", "founding_date", IDENTIFIER_CLASH} <= fields
    # Mirror pairs exist (OpenCorporates / OpenSanctions / OpenAleph) and are
    # never counted as agreement.
    assert result.by_relation(MIRROR)


def test_fails_soft_on_garbage() -> None:
    assert assess_consistency([{"recordType": "entity", "statementId": 1, "recordDetails": "x"}]).items == []
    assert assess_consistency([]).items == []


# ---------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------


def test_consistencystats_counts_pairs_and_rates() -> None:
    consistencystats.reset()
    ch = _stmt("companies_house", status=liveness.TERMINAL, founding="2002-02-05")
    gl = _stmt("gleif", status=liveness.LIVE, founding="2002-02-05")
    consistencystats.record(assess_consistency([ch, gl]))
    consistencystats.record(assess_consistency([ch, _stmt("gleif", status=liveness.TERMINAL)]))
    consistencystats.record(assess_consistency([]))
    out = consistencystats.stats()
    assert out["lookups"] == 3 and out["lookups_with_groups"] == 2
    row = out["pairs"]["liveness|companies_house|gleif"]
    assert row["disagree"] == 1 and row["agree"] == 1 and row["disagree_rate"] == 0.5
    founding = out["pairs"]["founding_date|companies_house|gleif"]
    assert founding["agree"] == 1 and founding["disagree_rate"] == 0.0
    # No values, identifiers or names anywhere in the snapshot.
    dumped = json.dumps(out)
    assert "2002" not in dumped and LEI not in dumped and "SHELL" not in dumped
    consistencystats.reset()


def test_consistencystats_endpoint() -> None:
    consistencystats.reset()
    r = TestClient(app).get("/consistencystats")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["lookups"] == 0 and body["pairs"] == {}


def test_shadow_counter_moves_through_a_real_lookup(monkeypatch, tmp_path: Path) -> None:
    """End-to-end: the pipeline records a consistency result per run, and the
    public endpoint carries no entity name or LEI from it."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    consistencystats.reset()
    try:
        lei = "2138000000000000A001"
        secret = "ZQX SECRET CONSISTENCY LTD"
        target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "statementId": "e-subject",
                    "recordType": "entity",
                    "recordDetails": {
                        "name": secret,
                        "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                        "identifiers": [{"id": lei, "scheme": "XI-LEI"}],
                    },
                }
            )
            + "\n"
        )
        client = TestClient(app)
        assert client.get("/lookup", params={"lei": lei}).status_code == 200
        body = client.get("/consistencystats").json()
        assert body["lookups"] >= 1
        blob = json.dumps(body)
        assert secret not in blob and lei not in blob
    finally:
        get_settings.cache_clear()
        consistencystats.reset()
