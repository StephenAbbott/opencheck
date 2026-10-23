"""The Nasdaq CSD account prefix is not part of an Estonian party's name.

The register files some holders under their securities account and puts the
account type before the name: ``Omanikukonto: EESTI ENERGIA AS`` (an owner
account), ``Lihtkonto: Rahandusministeerium`` (a simple account). In the
19 Oct 2026 dataset 1,227 people and 143 companies were named that way.
Stephen's decisions (20 Oct 2026, Notion "Strip the Nasdaq CSD account
prefix"): strip both prefixes; keep the register's text in a
``transformation`` annotation only (no line in the interest's details); never
feed the account type to the risk engine.

One test per path that reads names — the index, the printable page, REST and
the SOAP history — and each checks the graph is otherwise unchanged: the same
statement ids, the same relationships.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.mapper import map_ariregister
from opencheck.bods.mappers import estonia
from opencheck.bods.mappers.estonia import ee_strip_account_prefix
from opencheck.sources.ariregister_history import history_from_responses
from opencheck.sources.ariregister_rest_company import bundle_from_rest
from tests.test_ariregister_index import (
    HOLD,
    _build,
    _fetch,
    _point_at,
    _shareholders,
)
from tests.test_ariregister_person_index import _PAGE_ONE_PERSON, _page_statements

_HISTORY = Path(__file__).parent / "fixtures" / "ariregister_rest" / "history"
_REST = Path(__file__).parent / "fixtures" / "ariregister_rest"


def _names(statements: list[dict[str, Any]]) -> list[str]:
    out = []
    for s in statements:
        rd = s.get("recordDetails") or {}
        if s["recordType"] == "entity":
            out.append(rd.get("name") or "")
        elif s["recordType"] == "person":
            out += [n.get("fullName") or "" for n in rd.get("names") or []]
    return out


def _prefixed(statements: list[dict[str, Any]]) -> list[str]:
    return [n for n in _names(statements) if ee_strip_account_prefix(n)[1]]


def _prefix_notes(statement: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        a
        for a in statement.get("annotations") or []
        if a["motivation"] == "transformation"
        and "Nasdaq CSD securities account" in a["description"]
    ]


def _shape(statements: list[dict[str, Any]]) -> tuple[list[str], list[tuple[Any, ...]]]:
    """What the graph is apart from names: the ids, and every edge."""
    ids = sorted(s["recordId"] for s in statements)
    edges = sorted(
        (s["recordDetails"]["subject"], s["recordDetails"]["interestedParty"], s["recordId"])
        for s in statements
        if s["recordType"] == "relationship"
    )
    return ids, edges


def _map(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.recording() as rec:
        provenance.record_live("test")
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        return list(map_ariregister(copy.deepcopy(bundle)))


def _without_stripping(monkeypatch: pytest.MonkeyPatch, run: Callable[[], Any]) -> Any:
    """Run the mapping as it was before the fix, for the shape comparison."""
    with monkeypatch.context() as m:
        m.setattr(estonia, "_ee_strip_statement_name", lambda statement: None)
        return run()


# --- the helper ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("filed", "name", "kinds"),
    [
        ("Omanikukonto: EESTI ENERGIA AS", "EESTI ENERGIA AS", ["omanikukonto"]),
        ("Lihtkonto: Rahandusministeerium", "Rahandusministeerium", ["lihtkonto"]),
        ("  OMANIKUKONTO :  Jaan Tamm ", "Jaan Tamm", ["omanikukonto"]),
        ("lihtkonto:Mari Mets", "Mari Mets", ["lihtkonto"]),
        ("Omanikukonto: Lihtkonto: X OÜ", "X OÜ", ["omanikukonto", "lihtkonto"]),
        ("Jaan Tamm", "Jaan Tamm", []),
        ("Omanikukonto OÜ", "Omanikukonto OÜ", []),  # no colon: a name, not a prefix
        ("Omanikukonto:", "Omanikukonto:", []),  # nothing left: left as filed
        (None, "", []),
    ],
)
def test_the_prefix_is_removed_in_any_case_or_spacing(
    filed: str | None, name: str, kinds: list[str]
) -> None:
    assert ee_strip_account_prefix(filed) == (name, kinds)


def test_the_name_key_still_sets_the_prefix_aside() -> None:
    assert estonia.ee_name_key(None, "Omanikukonto: EVA MARIA KAERMA") == "eva maria kaerma"


# --- the index -------------------------------------------------------------------


def _prefixed_shareholders() -> list[dict[str, Any]]:
    """The fixture's holding OÜ with its Latvian company and its Danish
    shareholder (who is also its procurist and beneficial owner, one node
    on the register's hash) filed through Nasdaq CSD accounts."""
    out = _shareholders()
    for company in out:
        if company["ariregistri_kood"] == int(HOLD):
            for row in company["osanikud"]:
                if row["kirje_id"] == 32:
                    row["nimi_arinimi"] = "Omanikukonto: SIA Ziemeļi"
                if row["kirje_id"] == 33:
                    row["eesnimi"] = "Lihtkonto: Lars"
    return out


async def _index_statements(
    tmp_path: Path, shareholders: list[dict[str, Any]], name: str
) -> list[dict[str, Any]]:
    out, _ = _build(tmp_path / name, shareholders=shareholders)
    _point_at(out)
    try:
        bundle = await _fetch(HOLD)
        assert bundle["tier"] == "index"
        return _map(bundle)
    finally:
        _point_at(None)


async def test_the_index_path_strips_the_prefix(tmp_path: Path) -> None:
    statements = await _index_statements(tmp_path, _prefixed_shareholders(), "prefixed")
    assert _prefixed(statements) == []

    sia = next(
        s
        for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "SIA Ziemeļi"
    )
    [note] = _prefix_notes(sia)
    assert note["statementPointerTarget"] == "/recordDetails/name"
    assert '"Omanikukonto: SIA Ziemeļi"' in note["description"]
    assert "an owner account (omanikukonto)" in note["description"]
    assert note["transformedContent"] == "SIA Ziemeļi"

    lars = next(
        s
        for s in statements
        if s["recordType"] == "person"
        and s["recordDetails"]["names"][0]["fullName"] == "Lars Jensen"
    )
    [note] = _prefix_notes(lars)
    assert note["statementPointerTarget"] == "/recordDetails/names"
    assert '"Lihtkonto: Lars Jensen"' in note["description"]
    assert "a simple account (lihtkonto)" in note["description"]

    # Annotation only (decision 2): the interest says what it said before.
    plain = await _index_statements(tmp_path, _shareholders(), "plain")
    assert _shape(statements) == _shape(plain), "same nodes, same edges"
    details = lambda sts: sorted(  # noqa: E731
        json.dumps(s["recordDetails"]["interests"], sort_keys=True)
        for s in sts
        if s["recordType"] == "relationship"
    )
    assert details(statements) == details(plain)


async def test_the_account_type_feeds_no_signal(tmp_path: Path) -> None:
    from opencheck.risk import assess_bundle

    prefixed = await _index_statements(tmp_path, _prefixed_shareholders(), "prefixed")
    plain = await _index_statements(tmp_path, _shareholders(), "plain")

    def codes(statements: list[dict[str, Any]]) -> list[tuple[str, str]]:
        return sorted(
            (s.code, s.confidence)
            for s in assess_bundle("ariregister", {}, statements, hit_id=HOLD)
        )

    assert codes(prefixed) == codes(plain)


# --- the printable page ----------------------------------------------------------


_PAGE_WITH_ACCOUNTS = _PAGE_ONE_PERSON.replace(
    "<tr><td>40.00%</td><td>1000.00 EUR</td><td>Haudemaja OÜ</td>",
    "<tr><td>40.00%</td><td>1000.00 EUR</td><td>Lihtkonto: Haudemaja OÜ</td>",
)


async def test_the_page_path_strips_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert "Lihtkonto: Haudemaja OÜ" in _PAGE_WITH_ACCOUNTS
    statements = await _page_statements(tmp_path, _PAGE_WITH_ACCOUNTS)
    assert _prefixed(statements) == []
    haude = next(
        s
        for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "Haudemaja OÜ"
    )
    assert '"Lihtkonto: Haudemaja OÜ"' in _prefix_notes(haude)[0]["description"]

    # The name merge is unchanged: Eva-Maria Kaerma is still one node with
    # three edges, named with the registry card's spelling.
    eva = next(
        s
        for s in statements
        if s["recordType"] == "person"
        and s["recordDetails"]["names"][0]["fullName"] == "Eva-Maria Kaerma"
    )
    assert (
        len(
            [
                s
                for s in statements
                if s["recordType"] == "relationship"
                and s["recordDetails"]["interestedParty"] == eva["recordId"]
            ]
        )
        == 3
    )

    with monkeypatch.context() as m:
        m.setattr(estonia, "_ee_strip_statement_name", lambda statement: None)
        before = await _page_statements(tmp_path, _PAGE_WITH_ACCOUNTS)
    assert _prefixed(before)
    assert _shape(before) == _shape(statements)


# --- REST (an index miss) --------------------------------------------------------


def _rest_body() -> dict[str, Any]:
    body = json.loads((_REST / "rest_company_10078240.json").read_text(encoding="utf-8"))
    # Every row of one former shareholder, so whichever the history reads first carries it.
    surname = body["relatedPersons"]["shareholders"][0]["person"]["surnameLegalEntityName"]
    for rows in body["relatedPersons"].values():
        for row in rows if isinstance(rows, list) else []:
            person = row.get("person") or {}
            if person.get("surnameLegalEntityName") == surname:
                person["firstName"] = "Omanikukonto: " + (person.get("firstName") or "")
    return body


def test_the_rest_path_strips_the_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = bundle_from_rest(_rest_body(), "10078240")
    statements = _map(bundle)
    assert _prefixed(statements) == []
    assert sum(len(_prefix_notes(s)) for s in statements) == 1
    before = _without_stripping(monkeypatch, lambda: _map(bundle))
    assert _prefixed(before), "the fixture does carry a prefixed name"
    assert _shape(before) == _shape(statements)


# --- the SOAP history --------------------------------------------------------------


def _soap_with_accounts() -> str:
    text = (_HISTORY / "soap_detail_10421629.xml").read_text(encoding="utf-8")
    assert ">Rahandusministeerium<" in text
    return text.replace(">Rahandusministeerium<", ">Lihtkonto: Rahandusministeerium<")


def test_the_soap_history_path_strips_the_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    bo = (_HISTORY / "soap_bo_10421629.xml").read_text(encoding="utf-8")
    history = history_from_responses(_soap_with_accounts(), bo, {})
    bundle = {
        "tier": "index",
        "registry_code": "10421629",
        "name": "Eesti Energia AS",
        "cut": "2026-10-12",
        "history": history,
    }
    statements = _map(bundle)
    assert _prefixed(statements) == []
    ministry = [
        s
        for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "Rahandusministeerium"
    ]
    assert len(ministry) == 1
    before = _without_stripping(monkeypatch, lambda: _map(bundle))
    assert _prefixed(before)
    assert _shape(before) == _shape(statements)


# --- the dataset check -------------------------------------------------------------


def test_the_dataset_check_refuses_a_prefixed_name() -> None:
    from tests.test_sweep_estonia import sweep

    person = {
        "statementId": "p1",
        "recordId": "p1",
        "recordType": "person",
        "source": {"opencheckSourceId": "ariregister"},
        "recordDetails": {
            "personType": "knownPerson",
            "names": [{"type": "legal", "fullName": "Omanikukonto: JAAN TAMM"}],
        },
    }
    entity = {
        "statementId": "e1",
        "recordId": "e1",
        "recordType": "entity",
        "source": {"opencheckSourceId": "ariregister"},
        "recordDetails": {"name": "lihtkonto : KESKO OYJ"},
    }
    result = sweep.check_statements([person, entity])
    assert result["problem_count"] == 2
    assert all("Nasdaq CSD account prefix" in p for p in result["problems"])
    entity["recordDetails"]["name"] = "KESKO OYJ"
    person["recordDetails"]["names"][0]["fullName"] = "JAAN TAMM"
    assert sweep.check_statements([person, entity])["problem_count"] == 0
