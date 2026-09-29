"""Phase 261 — the Subsidiaries tab's rows carry the Phase 255 data.

``/subsidiaries`` rows gain the child LEI's own registration status, the
direct parent of an ultimate-only child (and whether it is in the network),
the relationship's ``RELATIONSHIP_PERIOD``, and the response gains
``enriched``. Shapes are the Phase 255 fixtures (Shell, 28 Sept 2026).
"""

from __future__ import annotations

from unittest.mock import patch

from opencheck import subsidiaries as subs
from opencheck.routers.subsidiaries import SubsidiariesResponse

from tests.test_subsidiaries_phase255 import (  # noqa: F401 — _live is a fixture
    BG_GROUP,
    DEEP,
    DIRECT,
    SHELL,
    STRANDED,
    _CM,
    _Client,
    _l1,
    _live,
    _Resp,
    _rr,
    _shell_children,
    _shell_routes,
)

_LAPSED = {
    "status": "LAPSED",
    "nextRenewalDate": "2019-10-19T00:00:00Z",
    "lastUpdateDate": "2024-02-01T00:00:00Z",
    "initialRegistrationDate": "2014-01-01T00:00:00Z",
    "managingLou": "213800WAVVOPS85N2205",
}


def _rows_by_lei(res: dict) -> dict[str, dict]:
    return {r["lei"]: r for r in res["children"]}


async def _assemble(monkeypatch, children: list[dict], **extra) -> dict:
    async def _build(lei):
        return {"lei": lei, "subject_attrs": {}, "direct_total": 1, "ultimate_total": 3,
                "children": children, **extra}

    monkeypatch.setattr(subs, "_build", _build)
    return await subs.assemble_subsidiaries(SHELL)


async def test_rows_carry_the_direct_parent_and_whether_it_is_in_the_network(_live, monkeypatch):
    rows = _rows_by_lei(await _assemble(monkeypatch, _shell_children()))
    assert rows[DEEP]["direct_parent_lei"] == DIRECT
    assert rows[DEEP]["direct_parent_in_network"] is True
    # BG Group is a lapsed holding company outside the network.
    assert rows[STRANDED]["direct_parent_lei"] == BG_GROUP
    assert rows[STRANDED]["direct_parent_in_network"] is False
    # A "both" child sits under the head; nothing to say.
    assert rows[DIRECT]["direct_parent_lei"] is None
    assert rows[DIRECT]["direct_parent_in_network"] is None


async def test_a_parent_named_as_the_head_itself_is_not_a_path(_live, monkeypatch):
    kids = [{"record": _l1(DEEP, "SHELL DEEP B.V."), "relations": ["ultimate"],
             "direct_parent": {"lei": SHELL, "rel": _rr(DEEP, SHELL)}}]
    rows = _rows_by_lei(await _assemble(monkeypatch, kids))
    assert rows[DEEP]["direct_parent_lei"] is None
    assert rows[DEEP]["direct_parent_in_network"] is None


async def test_rows_carry_the_relationship_period(_live, monkeypatch):
    kids = _shell_children()
    kids.append({
        "record": _l1("5493000000000000END1", "SHELL FORMER LIMITED"), "relations": ["direct"],
        "rels": {"direct": _rr("5493000000000000END1", SHELL, start="2001-04-01T00:00:00Z",
                               end="2021-03-03T00:00:00Z")},
    })
    rows = _rows_by_lei(await _assemble(monkeypatch, kids))
    # "both": the direct record's period, the edge the graph draws.
    assert rows[DIRECT]["relationship_start"] == "2013-11-01"
    assert rows[DIRECT]["relationship_end"] is None
    # ultimate-only: the ultimate record's period.
    assert rows[DEEP]["relationship_start"] == "2020-11-02"
    assert rows["5493000000000000END1"]["relationship_start"] == "2001-04-01"
    assert rows["5493000000000000END1"]["relationship_end"] == "2021-03-03"
    # No relationship record: no date, never an invented one.
    assert rows[STRANDED]["relationship_start"] is None


async def test_rows_carry_the_lei_registration_not_the_entity_status(_live, monkeypatch):
    kids = [
        {"record": _l1(DIRECT, "SHELL DIRECT HOLDINGS LIMITED",
                       reg={"status": "ISSUED", "nextRenewalDate": "2027-01-01T00:00:00Z"}),
         "relations": ["direct", "ultimate"]},
        {"record": _l1(DEEP, "SHELL DEEP B.V.", reg=_LAPSED), "relations": ["ultimate"]},
        {"record": _l1(STRANDED, "BG INTERNATIONAL LIMITED"), "relations": ["ultimate"]},
    ]
    rows = _rows_by_lei(await _assemble(monkeypatch, kids))
    assert rows[DEEP]["status"] == "ACTIVE"  # the company
    assert rows[DEEP]["lei_registration"] == {
        "status": "LAPSED", "label": "Lapsed", "flag": True,
        "since": "2019-10-19", "next_renewal_date": "2019-10-19",
    }
    assert rows[DIRECT]["lei_registration"]["flag"] is False
    # No registration block: absence, never ISSUED.
    assert rows[STRANDED]["lei_registration"] is None


async def test_enriched_is_on_the_response(_live, monkeypatch):
    res = await _assemble(monkeypatch, _shell_children(), enriched=False)
    assert res["enriched"] is False
    res = await _assemble(monkeypatch, _shell_children(), enriched=True)
    assert res["enriched"] is True


async def test_a_live_refusal_of_the_relationship_records_reaches_the_response(_live):
    client = _Client(_shell_routes(**{
        f"/{SHELL}/direct-child-relationships": _Resp(429),
    }))
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["enriched"] is False
    rows = _rows_by_lei(res)
    # The direct list is whole; its dates could not be read.
    assert rows[DIRECT]["relationship_start"] is None
    # The live parent lookup still placed the deep child.
    assert rows[DEEP]["direct_parent_lei"] == DIRECT


async def test_a_whole_live_network_is_enriched(_live):
    client = _Client(_shell_routes())
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["enriched"] is True
    assert _rows_by_lei(res)[DIRECT]["relationship_start"] == "2013-11-01"


async def test_the_response_model_keeps_the_new_fields(_live, monkeypatch):
    kids = _shell_children()
    kids[1]["record"] = _l1(DEEP, "SHELL DEEP B.V.", reg=_LAPSED)
    res = await _assemble(monkeypatch, kids, enriched=False)
    model = SubsidiariesResponse.model_validate(res).model_dump()
    assert model["enriched"] is False
    deep = next(c for c in model["children"] if c["lei"] == DEEP)
    assert deep["direct_parent_lei"] == DIRECT and deep["direct_parent_in_network"] is True
    assert deep["relationship_start"] == "2020-11-02"
    assert deep["lei_registration"]["status"] == "LAPSED"


async def test_the_empty_offline_response_says_nothing_was_refused(monkeypatch):
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        res = await subs.assemble_subsidiaries(SHELL)
    finally:
        get_settings.cache_clear()
    assert res["enriched"] is True and res["children"] == []


def test_a_mirror_without_relationship_rows_is_not_enriched(monkeypatch):
    from types import SimpleNamespace

    from opencheck import entity_pages

    def _store(has_rels: bool):
        return SimpleNamespace(
            is_mirror=True,
            has_relationships=has_rels,
            get=lambda lei: SimpleNamespace(),
            children=lambda lei, limit, kind: ([], 0),
            watermark=lambda: None,
            relationship=lambda lei, kind: None,
        )

    monkeypatch.setattr(entity_pages, "gleif_record_from_row",
                        lambda row: {"type": "lei-records", "id": SHELL, "attributes": {}})
    monkeypatch.setattr(subs, "_store_direct_parents", lambda leis: {})
    for has_rels in (False, True):
        monkeypatch.setattr(entity_pages, "get_store", lambda h=has_rels: _store(h))
        out = subs._mirror_network(SHELL)
        assert out is not None and out["enriched"] is has_rels

