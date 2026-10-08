"""Phase 309 — former legal names, said as such.

Barrick Mining Corporation (``0O4KBQCJZX82UKGCBV73``) as GLEIF files it on
8 Oct 2026: three ``PREVIOUS_LEGAL_NAME`` entries and one trading name,
renamed from Barrick Gold Corporation on 6 May 2025. Companies House dates
each former name itself (``ceased_on`` / ``effective_from``).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from opencheck import search_rank
from opencheck.bods import former_names as fn
from opencheck.bods.mapper import map_companies_house, map_gleif
from opencheck.mcp import shaping
from opencheck.sources import SearchKind, SourceHit
from opencheck.subject_profile import build_subject_profile

MINING = "0O4KBQCJZX82UKGCBV73"

OTHER_NAMES = [
    {"name": "American Barrick Resources Corporation", "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
    {"name": "SOCIETE AURIFERE BARRICK", "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
    {"name": "Barrick Gold Corporation", "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
    {"name": "SOCIETE MINIERE BARRICK", "language": "en", "type": "TRADING_OR_OPERATING_NAME"},
]


def _gleif_bundle(other_names: list[dict[str, Any]], events: list[dict] | None = None) -> dict[str, Any]:
    return {
        "lei": MINING,
        "record": {
            "attributes": {
                "lei": MINING,
                "entity": {
                    "legalName": {"name": "BARRICK MINING CORPORATION", "language": "en"},
                    "jurisdiction": "CA-BC",
                    "status": "ACTIVE",
                    "otherNames": other_names,
                    "transliteratedOtherNames": [],
                    "eventGroups": [
                        {"groupType": "STANDALONE", "events": [e]} for e in (events or [])
                    ],
                },
                "registration": {"status": "ISSUED", "lastUpdateDate": "2026-06-17T16:06:08Z"},
            }
        },
        "direct_parents": [],
        "ultimate_parents": [],
        "direct_children": [],
    }


RENAME = {
    "type": "CHANGE_LEGAL_NAME",
    "status": "COMPLETED",
    "effectiveDate": "2025-05-06T00:00:00Z",
    "recordedDate": "2025-05-06T20:09:08Z",
    "validationDocuments": "REGULATORY_FILING",
}


def _entity(stmts: Any) -> dict[str, Any]:
    return next(s for s in stmts if s["recordType"] == "entity")


# --- the annotation ---------------------------------------------------------------


def test_gleif_former_legal_names_are_annotated_and_trading_names_are_not() -> None:
    e = _entity(map_gleif(_gleif_bundle(OTHER_NAMES, [RENAME])).statements)
    # alternateNames is exactly what it was before this phase.
    assert e["recordDetails"]["alternateNames"] == [
        "American Barrick Resources Corporation",
        "SOCIETE AURIFERE BARRICK",
        "Barrick Gold Corporation",
        "SOCIETE MINIERE BARRICK",
    ]
    former = fn.former_names_of(e)
    assert [f["name"] for f in former] == [
        "American Barrick Resources Corporation",
        "SOCIETE AURIFERE BARRICK",
        "Barrick Gold Corporation",
    ]
    assert all(f["until"] is None and f["from"] is None for f in former)
    notes = [a for a in e["annotations"] if fn.FORMER_NAME_PROPERTY in a]
    assert notes[2]["statementPointerTarget"] == "/recordDetails/alternateNames/2"
    assert notes[2]["motivation"] == "commenting"
    assert notes[2]["description"] == "Former legal name, as filed: Barrick Gold Corporation."
    assert notes[2][fn.FORMER_NAME_PROPERTY] == {"name": "Barrick Gold Corporation"}


def test_a_former_name_equal_to_the_current_name_gets_no_annotation() -> None:
    e = _entity(
        map_gleif(
            _gleif_bundle([{"name": "BARRICK MINING CORPORATION", "type": "PREVIOUS_LEGAL_NAME"}])
        ).statements
    )
    assert "alternateNames" not in e["recordDetails"]
    assert fn.former_names_of(e) == []


def test_companies_house_dates_each_former_name() -> None:
    bundle = {
        "company_number": "00109244",
        "profile": {
            "company_number": "00109244",
            "company_name": "THE ARSENAL FOOTBALL CLUB LIMITED",
            "date_of_creation": "1910-04-26",
            "previous_company_names": [
                {
                    "name": "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY",
                    "effective_from": "1991-08-23",
                    "ceased_on": "2019-03-29",
                }
            ],
        },
        "pscs": {},
        "officers": {},
    }
    e = _entity(map_companies_house(bundle).statements)
    former = fn.former_names_of(e)
    assert former == [
        {
            "name": "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY",
            "until": "2019-03-29",
            "from": "1991-08-23",
        }
    ]
    note = next(a for a in e["annotations"] if fn.FORMER_NAME_PROPERTY in a)
    assert note["description"] == (
        "Former legal name, as filed: THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY "
        "(from 23 August 1991 until 29 March 2019)."
    )


def test_a_drifted_annotation_is_dropped_not_trusted() -> None:
    e = _entity(map_gleif(_gleif_bundle(OTHER_NAMES)).statements)
    e["recordDetails"]["alternateNames"][2] = "Something else"
    assert "Barrick Gold Corporation" not in [f["name"] for f in fn.former_names_of(e)]
    assert len(fn.former_names_of(e)) == 2


# --- the profile --------------------------------------------------------------------


def test_profile_lists_former_names_and_dates_the_change() -> None:
    bods = list(map_gleif(_gleif_bundle(OTHER_NAMES, [RENAME])))
    profile = build_subject_profile(MINING, bods)
    assert [f["name"] for f in profile["former_names"]] == [
        "American Barrick Resources Corporation",
        "SOCIETE AURIFERE BARRICK",
        "Barrick Gold Corporation",
    ]
    assert profile["former_names"][0]["sources"] == ["gleif"]
    assert profile["name_changed_on"] == "2025-05-06"
    # A trading name is never a former name.
    assert "SOCIETE MINIERE BARRICK" not in str(profile["former_names"])


def test_profile_merges_across_sources_dated_first() -> None:
    gleif_stmt = _entity(map_gleif(_gleif_bundle(OTHER_NAMES)).statements)
    ch = {
        "company_number": "00000001",
        "profile": {
            "company_number": "00000001",
            "company_name": "BARRICK MINING CORPORATION",
            "previous_company_names": [
                {"name": "BARRICK GOLD CORPORATION", "ceased_on": "2025-05-06"},
                {"name": "OLDER NAME LIMITED", "ceased_on": "2010-01-01"},
            ],
        },
        "pscs": {},
        "officers": {},
    }
    ch_stmt = _entity(map_companies_house(ch).statements)
    # The two statements must share an identifier to be one subject; give the
    # CH entity the LEI as the GLEIF statement carries it.
    ch_stmt["recordDetails"]["identifiers"].append({"id": MINING, "scheme": "XI-LEI", "schemeName": "LEI"})
    profile = build_subject_profile(MINING, [gleif_stmt, ch_stmt])
    rows = profile["former_names"]
    assert rows[0] == {"name": "Barrick Gold Corporation", "until": "2025-05-06", "from": None, "sources": ["gleif", "companies_house"]}
    assert rows[1]["name"] == "OLDER NAME LIMITED" and rows[1]["until"] == "2010-01-01"
    assert [r["name"] for r in rows[2:]] == ["American Barrick Resources Corporation", "SOCIETE AURIFERE BARRICK"]
    assert profile["name_changed_on"] is None


def test_profile_with_no_former_names_is_empty_not_absent() -> None:
    profile = build_subject_profile(MINING, list(map_gleif(_gleif_bundle([]))))
    assert profile["former_names"] == [] and profile["name_changed_on"] is None


# --- search -------------------------------------------------------------------------


def _hit(name: str, raw: dict | None = None, **ids: str) -> SourceHit:
    return SourceHit(
        source_id="gleif",
        hit_id=ids.get("lei", "X"),
        kind=SearchKind.ENTITY,
        name=name,
        summary=f"LEI {ids.get('lei', 'X')} · CA-BC · active",
        identifiers=ids,
        raw=raw or {},
        is_stub=False,
        liveness="live",
    )


def _gleif_raw(other_names: list[dict]) -> dict:
    return {"attributes": {"lei": MINING, "entity": {"legalName": {"name": "BARRICK MINING CORPORATION"}, "otherNames": other_names}}}


def test_a_former_legal_name_ranks_the_renamed_company_first() -> None:
    renamed = _hit("BARRICK MINING CORPORATION", _gleif_raw(OTHER_NAMES), lei=MINING)
    other = _hit("BARRICK GOLD INC.", _gleif_raw([]), lei="5493002CWGHR03YL8X75")
    query = "Barrick Gold Corporation"
    assert search_rank.former_names_of_hit(renamed) == [
        "American Barrick Resources Corporation",
        "SOCIETE AURIFERE BARRICK",
        "Barrick Gold Corporation",
    ]
    assert search_rank.best_match(query, renamed) == (0, "Barrick Gold Corporation")
    assert search_rank.best_match(query, other)[1] is None
    ranked = search_rank.rank_hits(query, [other, renamed])
    assert [h.name for h in ranked] == ["BARRICK MINING CORPORATION", "BARRICK GOLD INC."]
    # A trading name never wins a tier.
    trading = _hit("SOMETHING ELSE", _gleif_raw([{"name": "Barrick Gold Corporation", "type": "TRADING_OR_OPERATING_NAME"}]))
    assert search_rank.best_match(query, trading) == (4, None)


def test_mcp_candidates_say_which_former_name_matched() -> None:
    renamed = _hit("BARRICK MINING CORPORATION", _gleif_raw(OTHER_NAMES), lei=MINING)
    other = _hit("BARRICK GOLD INC.", _gleif_raw([]), lei="5493002CWGHR03YL8X75")
    query = "Barrick Gold Corporation"
    payload = SimpleNamespace(query=query, kind=SearchKind.ENTITY, hits=search_rank.rank_hits(query, [other, renamed]))
    out = shaping.shape_search(payload)
    first, second = out["candidates"][0], out["candidates"][1]
    assert first["name"] == "BARRICK MINING CORPORATION" and first["match"] == "exact"
    assert first["matched_former_name"] == "Barrick Gold Corporation"
    assert first["former_names"] == [
        "American Barrick Resources Corporation",
        "SOCIETE AURIFERE BARRICK",
        "Barrick Gold Corporation",
    ]
    assert "former_names" not in second and "matched_former_name" not in second
    assert "matched_former_name" in out["hint"]
