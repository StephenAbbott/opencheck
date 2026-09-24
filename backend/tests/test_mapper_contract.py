"""Phase 239 — the risk engine's jurisdiction and identifier inputs.

Three defects reached production because nothing checked what the mappers
hand the risk engine:

* ``bods_gleif`` / ``bods_uk_psc`` (and the script that builds
  ``data/cache/bods_data/``) wrote v0.3's ``incorporatedInJurisdiction`` under
  ``bodsVersion: "0.4"``, and ``risk._entity_jurisdiction`` looked for the old
  key at the *top* of the statement — so the FATF, EU high-risk and non-EU
  checks never saw an Open Ownership bundle's entities. Bank Saderat PLC, a
  UK bank whose parents are Iranian, drew no FATF black-list signal;
* Wikidata fell back to the Q-ID when pycountry could not parse a country
  label: Rosneft exported ``{"name": "Russia", "code": "Q159"}``;
* the GLEIF mapper knew an org-id scheme for 24 RA codes and exported every
  other registration number with ``scheme: ""`` (Equinor's ``923 609 016``),
  so GLEIF and the register could not corroborate each other by identifier.

The contract itself — ISO jurisdiction codes, a scheme on every identifier
with an id, no v0.3 field names — is checked on **every statement every
source mapper produces anywhere in the suite** by the registry-wide guard in
``tests/_entity_subtype_guard.py``. This file pins the three fixes, the
contract function, and that every registered source's mapper actually went
through the guard.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from opencheck import bods_data, risk
from opencheck.bods import jurisdiction as bods_jurisdiction
from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    gleif_registration_scheme,
    map_gleif,
    normalise_registered_as,
)
from opencheck.bods.mappers.wikidata import _wikidata_country, map_wikidata
from opencheck.bods.statements import make_entity_statement, make_person_statement
from opencheck.bods.validator import (
    is_iso_jurisdiction_code,
    jurisdiction_input_issues,
)
from opencheck.ra_codes import is_ra_scheme
from opencheck.sources import REGISTRY
from opencheck.sources.bods_gleif import _build_entity_statement as gleif_entity
from opencheck.sources.bods_uk_psc import _build_entity_statement_psc as psc_entity
from opencheck.sources.wikidata import _FETCH_QUERY, _summarise_bindings
from tests import _entity_subtype_guard as guard

BANK_SADERAT = "2138008KTNTDICZU8L25"
DATA = Path(__file__).resolve().parents[2] / "data" / "cache" / "bods_data"


# ---------------------------------------------------------------------------
# The contract function
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", ["GB", "RU", "VG", "US-DE", "CA-NS", "GB-SCT"])
def test_iso_codes_are_accepted(code: str) -> None:
    assert is_iso_jurisdiction_code(code)


@pytest.mark.parametrize("code", ["Q159", "BVI", "UK", "gb", "XX", "US-ZZ", "", " GB"])
def test_non_iso_codes_are_refused(code: str) -> None:
    assert not is_iso_jurisdiction_code(code)


def _entity(**rd: Any) -> dict[str, Any]:
    stmt = make_entity_statement(source_id="test", local_id="1", name="ACME")
    stmt["recordDetails"].update(rd)
    return stmt


def test_a_conformant_entity_has_no_issue() -> None:
    stmt = _entity(
        jurisdiction={"name": "Norway", "code": "NO"},
        identifiers=[{"id": "923609016", "scheme": "NO-BRC"}],
    )
    assert jurisdiction_input_issues(stmt) == []


def test_a_jurisdiction_with_a_name_and_no_code_is_conformant() -> None:
    # ``code`` is optional in BODS; the fix for an unresolvable country is to
    # leave it out, never to put a stand-in there.
    assert jurisdiction_input_issues(_entity(jurisdiction={"name": "Soviet Union"})) == []


@pytest.mark.parametrize(
    "rd,fragment",
    [
        ({"jurisdiction": {"name": "Russia", "code": "Q159"}}, "'Q159' is not ISO"),
        ({"incorporatedInJurisdiction": {"name": "UK", "code": "GB"}}, "v0.3 field"),
        ({"identifiers": [{"id": "923 609 016", "scheme": ""}]}, "has no scheme"),
        ({"identifiers": [{"id": "380129866"}]}, "has no scheme"),
    ],
)
def test_each_breach_is_reported(rd: dict[str, Any], fragment: str) -> None:
    issues = jurisdiction_input_issues(_entity(**rd))
    assert any(fragment in i for i in issues), issues


def test_a_nationality_must_be_a_country_not_a_subdivision_or_qid() -> None:
    person = make_person_statement(
        source_id="test",
        local_id="p",
        full_name="A Person",
        nationalities=[{"name": "Russia", "code": "Q159"}, {"name": "Delaware", "code": "US-DE"}],
    )
    issues = jurisdiction_input_issues(person)
    assert len(issues) == 2


def test_an_identifier_with_only_a_uri_needs_no_scheme() -> None:
    assert jurisdiction_input_issues(_entity(identifiers=[{"uri": "https://x"}])) == []


def test_the_guard_records_a_contract_breach() -> None:
    def map_fake(bundle: dict[str, Any]):
        yield _entity(jurisdiction={"name": "Russia", "code": "Q159"})

    list(guard.wrap(map_fake)({}))
    (violation,) = guard.drain()
    assert violation[0] == "map_fake"
    assert "Q159" in violation[2]


# ---------------------------------------------------------------------------
# 1. v0.3 incorporatedInJurisdiction
# ---------------------------------------------------------------------------


def _oo_row() -> dict[str, str]:
    return {
        "statementid": "oo-1",
        "recorddetails_name": "Bank Saderat Iran",
        "recorddetails_entitytype_type": "registeredEntity",
        "recorddetails_jurisdiction_name": "Iran",
        "recorddetails_jurisdiction_code": "IR",
    }


@pytest.mark.parametrize(
    "build,mapper",
    [(gleif_entity, "map_bods_gleif"), (psc_entity, "map_bods_uk_psc")],
)
def test_open_ownership_adapters_write_the_v04_key(build, mapper) -> None:
    import opencheck.bods.mapper as mapper_mod

    stmt = build(_oo_row(), [], [])
    rd = stmt["recordDetails"]
    assert rd["jurisdiction"] == {"name": "Iran", "code": "IR"}
    assert "incorporatedInJurisdiction" not in rd
    # Through the (guarded) passthrough mapper too, so the registry-wide
    # contract sees what these adapters build, not only this assertion.
    assert list(getattr(mapper_mod, mapper)({"bods_statements": [stmt]})) == [stmt]


def test_the_reader_accepts_every_spelling_ever_written() -> None:
    v04 = {"recordDetails": {"jurisdiction": {"code": "IR"}}}
    v03_in_details = {"recordDetails": {"incorporatedInJurisdiction": {"code": "IR"}}}
    v03_top_level = {"incorporatedInJurisdiction": {"code": "IR"}, "recordDetails": {}}
    for stmt in (v04, v03_in_details, v03_top_level):
        assert risk._entity_jurisdiction(stmt) == {"code": "IR"}


def test_upgrade_renames_in_place_and_keeps_an_existing_v04_value() -> None:
    stmt = {"recordDetails": {"incorporatedInJurisdiction": {"name": "Iran", "code": "IR"}}}
    bods_jurisdiction.upgrade(stmt)
    assert stmt["recordDetails"] == {"jurisdiction": {"name": "Iran", "code": "IR"}}
    both = {"recordDetails": {"jurisdiction": {"code": "GB"}, "incorporatedInJurisdiction": {"code": "IR"}}}
    bods_jurisdiction.upgrade(both)
    assert both["recordDetails"] == {"jurisdiction": {"code": "GB"}}


def test_the_bundle_files_still_carry_the_v03_key_and_the_loader_serves_v04() -> None:
    raw = (DATA / "gleif" / f"{BANK_SADERAT}.jsonl").read_text(encoding="utf-8")
    assert "incorporatedInJurisdiction" in raw  # the committed extract, unchanged
    served = bods_data.gleif_bundle_for_lei(BANK_SADERAT)
    assert served
    assert "incorporatedInJurisdiction" not in json.dumps(served)
    assert all(jurisdiction_input_issues(s) == [] for s in served)


def test_bank_saderat_draws_the_jurisdiction_signals_it_never_drew() -> None:
    """The ticket's "done when": the high-risk checks see the passthrough
    entities. BP itself has nothing above it, so its NON_EU_JURISDICTION
    cannot name them; Bank Saderat PLC has Iranian parents, and before
    Phase 239 this bundle produced no jurisdiction signal at all."""
    bundle = bods_data.gleif_bundle_for_lei(BANK_SADERAT)
    signals = {s.code: s for s in risk.assess_amla("bods_gleif", {}, bundle, hit_id=BANK_SADERAT)}
    assert {"FATF_BLACK_LIST", "EU_HIGH_RISK_THIRD_COUNTRY", "NON_EU_JURISDICTION"} <= set(signals)
    entity_ids = {s["statementId"] for s in bundle if s.get("recordType") == "entity"}
    for code in ("FATF_BLACK_LIST", "NON_EU_JURISDICTION"):
        hits = signals[code].evidence["jurisdictions"]
        assert hits and {h["statement_id"] for h in hits} <= entity_ids
    assert {h["code"] for h in signals["FATF_BLACK_LIST"].evidence["jurisdictions"]} == {"IR"}


def test_bp_bundle_entities_are_visible_to_the_jurisdiction_checks() -> None:
    bundle = bods_data.gleif_bundle_for_lei("213800LH1BZH3DI6G760")
    entities = [s for s in bundle if s.get("recordType") == "entity"]
    with_code = [s for s in entities if (risk._entity_jurisdiction(s) or {}).get("code")]
    # 85 entity statements; before Phase 239 the risk engine read none of them.
    assert len(with_code) >= 80


# ---------------------------------------------------------------------------
# 2. Wikidata: ISO codes from P297, never a Q-ID
# ---------------------------------------------------------------------------


def test_the_fetch_query_reads_p297_for_country_and_citizenship() -> None:
    assert "?country wdt:P297 ?countryIso" in _FETCH_QUERY
    assert "?citizenship wdt:P297 ?citizenshipIso" in _FETCH_QUERY


def _uri(qid: str) -> dict[str, str]:
    return {"type": "uri", "value": f"http://www.wikidata.org/entity/{qid}"}


def _lit(value: str) -> dict[str, str]:
    return {"type": "literal", "value": value}


def test_summary_carries_the_iso_code_wikidata_states() -> None:
    summary = _summarise_bindings(
        "Q1141123",
        [
            {
                "label": _lit("Rosneft"),
                "instance": _uri("Q891723"),
                "country": _uri("Q159"),
                "countryLabel": _lit("Russia"),
                "countryIso": _lit("RU"),
            }
        ],
    )
    assert summary["country"] == {"qid": "Q159", "label": "Russia", "iso": "RU"}


@pytest.mark.parametrize(
    "country,expected",
    [
        # P297 wins, whatever the label.
        ({"qid": "Q159", "label": "Russia", "iso": "RU"}, ("Russia", "RU")),
        ({"qid": "Q43", "label": "Turkey", "iso": "TR"}, ("Turkey", "TR")),
        ({"qid": "Q974", "label": "Democratic Republic of the Congo", "iso": "CD"},
         ("Democratic Republic of the Congo", "CD")),
        # A summary cached before P297 was read: the label, then nothing.
        ({"qid": "Q145", "label": "United Kingdom"}, ("United Kingdom", "GB")),
        ({"qid": "Q159", "label": "Russia"}, ("Russia", None)),
        ({"qid": "Q15180", "label": "Soviet Union"}, ("Soviet Union", None)),
        # No English label: Wikidata's label service hands back the Q-ID.
        ({"qid": "Q159", "label": "Q159", "iso": "RU"}, ("Russian Federation", "RU")),
        ({"qid": "Q999999", "label": "Q999999"}, None),
    ],
)
def test_a_country_resolves_to_iso_or_to_a_name_alone(country, expected) -> None:
    assert _wikidata_country(country) == expected


def test_rosneft_never_exports_a_qid_as_a_code() -> None:
    bundle = {
        "source_id": "wikidata",
        "qid": "Q1141123",
        "summary": {
            "qid": "Q1141123",
            "label": "Rosneft",
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q891723", "label": "public company"}],
            "identifiers": {},
            "country": {"qid": "Q159", "label": "Russia"},  # cached, no iso
        },
    }
    entity = next(s for s in map_wikidata(bundle) if s["recordType"] == "entity")
    assert entity["recordDetails"]["jurisdiction"] == {"name": "Russia"}
    bundle["summary"]["country"]["iso"] = "RU"
    entity = next(s for s in map_wikidata(bundle) if s["recordType"] == "entity")
    assert entity["recordDetails"]["jurisdiction"] == {"name": "Russia", "code": "RU"}


# ---------------------------------------------------------------------------
# 3. GLEIF RA → scheme
# ---------------------------------------------------------------------------


def _adapter_ra_codes() -> dict[str, str]:
    """Every RA code an OpenCheck adapter dispatches on → the adapter id."""
    out: dict[str, str] = {}
    for adapter in REGISTRY.values():
        for deriver in adapter.lookup_derivers:
            for ra in deriver.ra_codes:
                out[ra] = adapter.id
    return out


def test_every_ra_code_an_adapter_dispatches_on_has_a_scheme() -> None:
    missing = sorted(ra for ra in _adapter_ra_codes() if ra not in _GLEIF_RA_TO_ORG_ID)
    assert missing == []


@pytest.mark.parametrize(
    "ra,scheme",
    [
        # The ticket's examples, each now the scheme its own adapter writes.
        ("RA000472", "NO-BRC"),
        ("RA000170", "DK-CVR"),
        ("RA000394", "IN-MCA"),
        ("RA000072", "CA-CORP"),
        ("RA000014", "AU-ACN"),
        ("RA000469", "NG-CAC"),
        ("RA000484", "PL-KRS"),
        ("RA000017", "AT-FB"),
        ("RA000526", "SK-RPO"),
        # Aligned to the adapters (Stephen, 24 Sept 2026).
        ("RA000181", "EE-ARIREGISTER"),
        ("RA000544", "SE-BLV"),
    ],
)
def test_ra_scheme(ra: str, scheme: str) -> None:
    assert _GLEIF_RA_TO_ORG_ID[ra][0] == scheme


def test_an_unmapped_authority_is_named_by_its_ra_code() -> None:
    # Cayman Islands General Registry — no org-id scheme, no adapter.
    assert gleif_registration_scheme("RA000087", "KY")[0] == "RA000087"
    # A Canadian province: never CA-CORP, never REG-CA.
    assert gleif_registration_scheme("RA000076", "CA-ON")[0] == "RA000076"
    # GLEIF's "not on the list" code, with the authority it names instead.
    assert gleif_registration_scheme("RA999999", "KY", "Cayman Monetary Authority") == (
        "RA999999",
        "Cayman Monetary Authority",
    )
    # US states keep their ISO 3166-2 scheme — but not for RA999999.
    assert gleif_registration_scheme("RA000602", "US-DE")[0] == "US-DE"
    assert gleif_registration_scheme("RA999999", "US-DE")[0] == "RA999999"


def test_the_ra_code_scheme_is_recognised_where_a_blank_one_was() -> None:
    assert is_ra_scheme("RA000087") and is_ra_scheme("ra000087")
    assert not is_ra_scheme("") and not is_ra_scheme("NO-BRC") and not is_ra_scheme("RA0001")


def _gleif(ra: str, registered_as: str, jurisdiction: str) -> dict[str, Any]:
    lei = "OWTXL3BVJKXN1CYZ6X67"
    return {
        "lei": lei,
        "record": {
            "id": lei,
            "attributes": {
                "lei": lei,
                "entity": {
                    "legalName": {"name": "Equinor ASA"},
                    "jurisdiction": jurisdiction,
                    "registeredAs": registered_as,
                    "registeredAt": {"id": ra, "other": None},
                    "legalAddress": {"country": jurisdiction.split("-")[0]},
                },
            },
        },
    }


def test_equinor_exports_a_schemed_unspaced_number() -> None:
    entity = next(s for s in map_gleif(_gleif("RA000472", "923 609 016", "NO")) if s["recordType"] == "entity")
    local = [i for i in entity["recordDetails"]["identifiers"] if i["scheme"] != "XI-LEI"]
    assert local[0]["id"] == "923609016"
    assert local[0]["scheme"] == "NO-BRC"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("923 609 016", "923609016"),   # Norway, as one LEI issuer typed it
        ("542 051 180", "542051180"),   # France (SIREN)
        ("29 596 587 150", "29596587150"),  # Australia (ABN)
        ("C 83807", "C 83807"),         # Malta: the space is part of the number
        ("C  83807 ", "C 83807"),
        ("33.805.882/0001-11", "33.805.882/0001-11"),  # left as filed
    ],
)
def test_registered_as_is_written_the_way_the_register_writes_it(raw, expected) -> None:
    assert normalise_registered_as(raw) == expected


def test_new_zealand_nzbn_and_company_number_take_their_own_schemes() -> None:
    assert gleif_registration_scheme("RA000466", "NZ", registered_as="9429037186104")[0] == "NZ-NZBN"
    assert gleif_registration_scheme("RA000466", "NZ", registered_as="8303602")[0] == "NZ-COH"


def test_gleif_mapper_emits_no_blank_scheme_for_an_unknown_authority() -> None:
    entity = next(s for s in map_gleif(_gleif("RA000087", "12345", "KY")) if s["recordType"] == "entity")
    local = [i for i in entity["recordDetails"]["identifiers"] if i["scheme"] != "XI-LEI"]
    assert local[0]["scheme"] == "RA000087"
    assert jurisdiction_input_issues(entity) == []


def test_ra_scheme_values_are_well_formed() -> None:
    for ra, (scheme, name) in _GLEIF_RA_TO_ORG_ID.items():
        assert scheme and scheme == scheme.strip().upper(), ra
        assert name.strip(), ra


# ---------------------------------------------------------------------------
# Every registered source's mapper went through the guard
# ---------------------------------------------------------------------------


def test_everypolitician_mapper_meets_the_contract() -> None:
    """The one registered mapper no other test called (the coverage check
    below found it)."""
    from opencheck.bods.mapper import map_everypolitician

    bundle = map_everypolitician(
        {
            "entity": {
                "id": "Q7747",
                "schema": "Person",
                "properties": {"name": ["Vladimir Putin"], "nationality": ["ru", "su"]},
            }
        }
    )
    (person,) = [s for s in bundle.statements if s["recordType"] == "person"]
    assert jurisdiction_input_issues(person) == []
    codes = [n.get("code") for n in person["recordDetails"].get("nationalities", [])]
    assert "RU" in codes and "Q7747" not in codes


def test_every_registered_source_mapper_was_checked(request: pytest.FixtureRequest) -> None:
    """The guard only sees mappers the suite runs. Moved to the end of the
    session by ``conftest.py``; meaningful only on a full run, so it skips
    when any test module was left out of the collection (``-k``, one file)."""
    config = request.config
    partial = bool(config.option.keyword) or bool(config.option.markexpr) or any(
        "::" in arg or arg.endswith(".py") for arg in config.args
    )
    if partial:
        pytest.skip("partial run — the mapper-coverage check needs the whole suite")
    expected = {f"map_{sid}" for sid in REGISTRY} | {"map_bods_gleif", "map_bods_uk_psc"}
    unchecked = sorted(expected - guard.EXERCISED)
    assert unchecked == [], (
        "these source mappers produced no statement anywhere in the suite, so "
        f"the mapper contract never checked them: {unchecked}"
    )
