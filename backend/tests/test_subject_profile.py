"""The subject profile — what the registers say the company is (Phase 154).

Facts assembled from the subject's own entity statements only, the register
preferred, the worst register status winning, sources counted for
independence. Never a finding.
"""

from __future__ import annotations

from typing import Any

from opencheck.bods import liveness
from opencheck.bods.mapper import SOURCE_NAMES
from opencheck.subject_profile import build_subject_profile, subject_statements

LEI = "21380068P1DRHMJ8KU70"


def _entity(
    source_id: str,
    sid: str,
    *,
    identifiers: list[dict[str, str]],
    details: str | None = None,
    legal_form_label: str | None = None,
    founding: str | None = None,
    address: str | None = None,
    address_type: str = "registered",
    status: str | None = None,
    since: str | None = None,
    jurisdiction: str = "GB",
) -> dict[str, Any]:
    rd: dict[str, Any] = {
        "isComponent": False,
        "entityType": {"type": "registeredEntity"},
        "name": "SHELL PLC",
        "identifiers": identifiers,
        "jurisdiction": {"code": jurisdiction, "name": "United Kingdom"},
    }
    if details:
        rd["entityType"]["details"] = details
    if legal_form_label:
        rd["legalFormLabel"] = legal_form_label
    if founding:
        rd["foundingDate"] = founding
    if address:
        rd["addresses"] = [
            {"type": address_type, "address": address, "country": {"code": "GB", "name": "United Kingdom"}}
        ]
    stmt = {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": rd,
        "source": {"description": SOURCE_NAMES[source_id]},
    }
    if status:
        liveness.apply_register_status(
            stmt, source_label=SOURCE_NAMES[source_id], liveness=status, since=since
        )
    return stmt


def _shell_bundle(ch_status: str = liveness.LIVE) -> list[dict[str, Any]]:
    gleif = _entity(
        "gleif", "s-gleif",
        identifiers=[{"scheme": "XI-LEI", "id": LEI}, {"scheme": "GB-COH", "id": "04366849"}],
        legal_form_label="Public Limited Company",
        founding="2002-02-05",
        address="Shell Centre, London, SE1 7NA, GB",
        status=liveness.LIVE,
    )
    ch = _entity(
        "companies_house", "s-ch",
        identifiers=[{"scheme": "GB-COH", "id": "04366849"}],
        details="Public limited company",
        founding="2002-02-05",
        address="Shell Centre, London, SE1 7NA, United Kingdom",
        status=ch_status,
        since="2026-01-31" if ch_status != liveness.LIVE else None,
    )
    oc = _entity(
        "opencorporates", "s-oc",
        identifiers=[{"scheme": "GB-COH", "id": "04366849"}],
        details="Public limited company",
        founding="2002",
        status=liveness.LIVE,
    )
    unrelated = _entity(
        "gleif", "sub-1",
        identifiers=[{"scheme": "XI-LEI", "id": "AAAA00000000000000A1"}],
        legal_form_label="Private Limited Company",
        founding="1999-01-01",
        jurisdiction="KY",
    )
    return [gleif, ch, oc, unrelated]


def test_subject_is_the_referent_group_holding_the_lei() -> None:
    ids = {s["statementId"] for s in subject_statements(LEI, _shell_bundle())}
    assert ids == {"s-gleif", "s-ch", "s-oc"}


def test_profile_prefers_the_register_and_lists_agreeing_sources() -> None:
    p = build_subject_profile(LEI, _shell_bundle())
    assert p is not None
    # Legal form: Companies House's wording wins; GLEIF's ELF label agrees
    # case-insensitively; OpenCorporates republishes Companies House.
    assert p["legal_form"]["value"] == "Public limited company"
    assert p["legal_form"]["sources"] == ["companies_house", "gleif", "opencorporates"]
    assert p["legal_form"]["independent_sources"] == 2
    assert p["legal_form"]["other_values"] == []
    # Founding: the most precise agreeing value is displayed; "2002" agrees.
    assert p["founding_date"]["value"] == "2002-02-05"
    assert p["founding_date"]["sources"] == ["companies_house", "gleif", "opencorporates"]
    # Address: the register's text, GLEIF agreeing after normalisation is
    # NOT claimed — "GB" and "United Kingdom" differ, and that is honest.
    assert p["registered_address"]["value"].startswith("Shell Centre")
    assert p["registered_address"]["country"] == "GB"
    assert "companies_house" in p["registered_address"]["sources"]
    assert p["jurisdiction"] == "GB"
    # The unrelated subsidiary never contaminates the profile.
    assert "1999" not in p["founding_date"]["value"]
    assert "sub-1" not in p["statement_ids"]


def test_register_status_names_the_register_when_all_agree() -> None:
    p = build_subject_profile(LEI, _shell_bundle())
    rs = p["register_status"]
    assert rs["liveness"] == "live"
    assert rs["source_id"] == "companies_house"
    assert rs["sources"] == ["companies_house", "gleif", "opencorporates"]
    assert rs["independent_sources"] == 2
    assert rs["other_values"] == []


def test_the_worst_register_status_wins_whoever_said_it() -> None:
    # A dissolved company with an ACTIVE LEI — the Phase 151 case.
    p = build_subject_profile(LEI, _shell_bundle(ch_status=liveness.TERMINAL))
    rs = p["register_status"]
    assert rs["liveness"] == "terminal"
    assert rs["source_id"] == "companies_house"
    assert rs["since"] == "2026-01-31"
    assert {o["source_id"] for o in rs["other_values"]} == {"gleif", "opencorporates"}
    # And GLEIF alone saying terminal still outranks a live register.
    bods = _shell_bundle()
    bods[0]["annotations"] = []
    liveness.apply_register_status(
        bods[0], source_label=SOURCE_NAMES["gleif"], liveness=liveness.PENDING
    )
    p = build_subject_profile(LEI, bods)
    assert p["register_status"]["liveness"] == "pending"
    assert p["register_status"]["source_id"] == "gleif"


def test_a_lone_gleif_statement_still_profiles() -> None:
    bods = _shell_bundle()[:1]
    p = build_subject_profile(LEI, bods)
    assert p is not None
    assert p["legal_form"]["value"] == "Public Limited Company"
    assert p["legal_form"]["sources"] == ["gleif"]
    assert p["register_status"]["source_id"] == "gleif"


def test_no_subject_statement_means_no_profile() -> None:
    assert build_subject_profile(LEI, []) is None
    assert build_subject_profile(LEI, _shell_bundle()[3:]) is None
    assert build_subject_profile("", _shell_bundle()) is None


def test_profile_carries_no_risk_vocabulary() -> None:
    import json

    p = build_subject_profile(LEI, _shell_bundle(ch_status=liveness.TERMINAL))
    text = json.dumps(p).lower()
    for word in ("risk", "suspicious", "flag", "warning"):
        assert word not in text
