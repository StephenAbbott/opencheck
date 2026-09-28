"""Phase 255 — the subsidiary network's BODS, and the validator clean-ups the
28 Sept 2026 five-network check (Shell, Unilever, Quantexa, Moody's, Novo
Nordisk) turned up.

Shapes are the ones GLEIF published that day: Shell's "both" children, its
ultimate-only children whose direct parent is another Shell company, and the
ones whose direct parent is a lapsed holding company outside the network
(BG Group, Unilever N.V.). No network.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from opencheck import subsidiaries as subs
from opencheck.bods import map_gleif_subsidiaries
from opencheck.bods.annotations import (
    dropped_partial_date,
    person_identifiers_from_annotations,
)
from opencheck.bods.ftm import map_to_ftm
from opencheck.bods.mapper import _gleif_entity_statement, map_meip
from opencheck.bods.senzing import map_to_senzing
from opencheck.bods.statements import make_entity_statement, make_person_statement
from opencheck.config import get_settings
from opencheck.consistency import _extract_founding
from opencheck.mcp import server as mcp_server

SHELL = "21380068P1DRHMJ8KU70"
DIRECT = "213800C2Y6KDQCD2WZ09"      # a direct child of Shell
DEEP = "549300XNL1VRVIODFM92"        # ultimate-only; its direct parent is DIRECT
STRANDED = "5493000000000000BG01"    # ultimate-only; direct parent outside the set
BG_GROUP = "213800AAAAAAAAAAAA99"    # lapsed, outside the network

_REPO = Path(__file__).resolve().parents[2]


def _l1(lei: str, name: str, *, jur: str = "GB", reg: dict | None = None,
        legal_form: dict | None = None) -> dict:
    entity: dict = {"legalName": {"name": name}, "jurisdiction": jur, "status": "ACTIVE"}
    if legal_form:
        entity["legalForm"] = legal_form
    attrs: dict = {"lei": lei, "entity": entity}
    if reg:
        attrs["registration"] = reg
    return {"type": "lei-records", "id": lei, "attributes": attrs}


def _rr(child: str, parent: str, kind: str = "direct", *, start: str | None = None,
        end: str | None = None, updated: str | None = None) -> dict:
    rel: dict = {
        "startNode": {"id": child, "type": "LEI"},
        "endNode": {"id": parent, "type": "LEI"},
        "type": "IS_DIRECTLY_CONSOLIDATED_BY" if kind == "direct" else "IS_ULTIMATELY_CONSOLIDATED_BY",
        "status": "ACTIVE",
    }
    if start or end:
        period: dict = {"type": "RELATIONSHIP_PERIOD"}
        if start:
            period["startDate"] = start
        if end:
            period["endDate"] = end
        rel["periods"] = [
            {"type": "ACCOUNTING_PERIOD", "startDate": "2019-01-01T00:00:00Z",
             "endDate": "2019-12-31T00:00:00Z"},
            period,
        ]
    attrs: dict = {"relationship": rel}
    if updated:
        attrs["registration"] = {"lastUpdateDate": updated, "status": "PUBLISHED"}
    return {"type": "relationship-records", "attributes": attrs}


def _shell_children() -> list[dict]:
    return [
        {"record": _l1(DIRECT, "SHELL DIRECT HOLDINGS LIMITED"), "relations": ["direct", "ultimate"],
         "rels": {"direct": _rr(DIRECT, SHELL, start="2013-11-01T00:00:00Z",
                                updated="2022-06-07T06:58:39Z")}},
        {"record": _l1(DEEP, "SHELL DEEP B.V.", jur="NL"), "relations": ["ultimate"],
         "rels": {"ultimate": _rr(DEEP, SHELL, "ultimate", start="2020-11-02T00:00:00Z",
                                  updated="2023-07-31T17:34:15Z")},
         "direct_parent": {"lei": DIRECT, "rel": _rr(DEEP, DIRECT, start="2020-11-02T00:00:00Z",
                                                     updated="2023-07-31T17:34:15Z")}},
        {"record": _l1(STRANDED, "BG INTERNATIONAL LIMITED"), "relations": ["ultimate"],
         "direct_parent": {"lei": BG_GROUP, "rel": _rr(STRANDED, BG_GROUP)}},
    ]


def _shell_subject() -> dict:
    return {"lei": SHELL, "entity": {"legalName": {"name": "SHELL PLC"}, "jurisdiction": "GB"}}


def _rels(stmts: list[dict]) -> list[dict]:
    return [s for s in stmts if s["recordType"] == "relationship"]


def _sid_of(stmts: list[dict], lei: str) -> str:
    for s in stmts:
        for i in (s.get("recordDetails") or {}).get("identifiers") or []:
            if i.get("scheme") == "XI-LEI" and i.get("id") == lei:
                return s["statementId"]
    raise AssertionError(lei)


# ---------------------------------------------------------------------------
# Fixes 1–3: one relationship per pair, the path, the dates
# ---------------------------------------------------------------------------


def test_one_relationship_per_pair_and_the_deep_child_gets_its_path():
    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children())
    rels = _rels(stmts)
    shell, direct, deep, stranded = (
        _sid_of(stmts, x) for x in (SHELL, DIRECT, DEEP, STRANDED)
    )
    pairs = [(r["recordDetails"]["subject"], r["recordDetails"]["interestedParty"]) for r in rels]
    # no pair twice
    assert len(pairs) == len(set(pairs))
    assert (direct, shell) in pairs          # the "both" child: one statement
    assert (deep, direct) in pairs           # the path GLEIF gives
    assert (deep, shell) in pairs            # the ultimate fact, still stated
    assert (stranded, shell) in pairs
    assert len(rels) == 4

    by_pair = dict(zip(pairs, rels))
    deep_direct = by_pair[(deep, direct)]["recordDetails"]["interests"][0]
    assert deep_direct["directOrIndirect"] == "direct"
    deep_ultimate = by_pair[(deep, shell)]["recordDetails"]["interests"][0]
    assert deep_ultimate["directOrIndirect"] == "indirect"


def test_the_deep_edge_uses_the_parents_own_network_id():
    """``{parent}:direct-child:{child}`` — the statement the parent's own
    network would emit, so the two collapse when both are in one export."""
    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children())
    from_parent = map_gleif_subsidiaries(
        DIRECT, {"entity": {"legalName": {"name": "SHELL DIRECT HOLDINGS LIMITED"}}},
        [{"record": _l1(DEEP, "SHELL DEEP B.V.", jur="NL"), "relations": ["direct"],
          "rels": {"direct": _rr(DEEP, DIRECT, start="2020-11-02T00:00:00Z",
                                 updated="2023-07-31T17:34:15Z")}}],
    )
    ids = {r["statementId"] for r in _rels(stmts)}
    assert {r["statementId"] for r in _rels(from_parent)} <= ids


def test_a_direct_parent_outside_the_network_is_named_not_drawn():
    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children())
    stranded = _sid_of(stmts, STRANDED)
    (rel,) = [r for r in _rels(stmts) if r["recordDetails"]["subject"] == stranded]
    notes = [a["description"] for a in rel.get("annotations") or []]
    assert any(BG_GROUP in n and "SHELL PLC" in n for n in notes)
    assert all(a["statementPointerTarget"] == "/recordDetails" for a in rel["annotations"])


def test_dates_come_from_the_relationship_record():
    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children())
    direct = _sid_of(stmts, DIRECT)
    (rel,) = [r for r in _rels(stmts) if r["recordDetails"]["subject"] == direct]
    assert rel["statementDate"] == "2022-06-07"
    (interest,) = rel["recordDetails"]["interests"]
    assert interest["startDate"] == "2013-11-01"
    assert "endDate" not in interest
    assert "also its ultimate consolidating parent" in interest["details"]


def test_a_relationship_period_end_is_carried():
    children = [{"record": _l1(DIRECT, "X"), "relations": ["direct"],
                 "rels": {"direct": _rr(DIRECT, SHELL, start="2013-11-01T00:00:00Z",
                                        end="2024-03-31T00:00:00Z")}}]
    (rel,) = _rels(map_gleif_subsidiaries(SHELL, _shell_subject(), children))
    assert rel["recordDetails"]["interests"][0]["endDate"] == "2024-03-31"


def test_no_relationship_record_leaves_the_interest_undated():
    children = [{"record": _l1(DIRECT, "X"), "relations": ["direct"]}]
    (rel,) = _rels(map_gleif_subsidiaries(SHELL, _shell_subject(), children))
    assert "startDate" not in rel["recordDetails"]["interests"][0]


def test_the_frontend_recognises_the_single_both_statement():
    """``bodsGraph.ts`` labels a pair "Controls (direct + ultimate)" from the
    details clause; the two must not drift apart."""
    ts = (_REPO / "frontend/src/lib/bodsGraph.ts").read_text(encoding="utf-8")
    m = re.search(r'export const ALSO_ULTIMATE = "([^"]+)"', ts)
    assert m, "ALSO_ULTIMATE not found in bodsGraph.ts"
    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children()[:1])
    (rel,) = _rels(stmts)
    assert m[1] in rel["recordDetails"]["interests"][0]["details"].lower()


def test_the_network_validates_under_libcovebods():
    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import validate_bods_statements

    stmts = map_gleif_subsidiaries(SHELL, _shell_subject(), _shell_children())
    report = validate_bods_statements(stmts)
    assert report["json_errors"] == [], report["json_errors"]
    assert report["additional_errors"] == [], report["additional_errors"]


# ---------------------------------------------------------------------------
# The data layer: relationship records and direct parents
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int, payload: dict | None = None) -> None:
        self.status_code = status
        self._payload = payload or {}

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        return self._payload


def _page(data: list[dict]) -> _Resp:
    return _Resp(200, {"data": data, "meta": {"pagination": {"total": len(data), "lastPage": 1}}})


class _Client:
    def __init__(self, routes: dict[str, _Resp]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    async def get(self, url: str, **kwargs):
        self.calls.append(url)
        for suffix, resp in self.routes.items():
            if url.endswith(suffix):
                return resp
        return _Resp(404)


class _CM:
    def __init__(self, client) -> None:
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _shell_routes(**extra: _Resp) -> dict[str, _Resp]:
    routes = {
        f"/lei-records/{SHELL}": _Resp(200, {"data": _l1(SHELL, "SHELL PLC")}),
        f"/{SHELL}/direct-children": _page([_l1(DIRECT, "SHELL DIRECT HOLDINGS LIMITED")]),
        f"/{SHELL}/ultimate-children": _page([
            _l1(DIRECT, "SHELL DIRECT HOLDINGS LIMITED"), _l1(DEEP, "SHELL DEEP B.V.", jur="NL"),
        ]),
        f"/{SHELL}/direct-child-relationships": _page([
            _rr(DIRECT, SHELL, start="2013-11-01T00:00:00Z", updated="2022-06-07T06:58:39Z"),
        ]),
        f"/{SHELL}/ultimate-child-relationships": _page([
            _rr(DIRECT, SHELL, "ultimate"), _rr(DEEP, SHELL, "ultimate", updated="2023-07-31T00:00:00Z"),
        ]),
        f"/{DEEP}/direct-parent-relationship": _Resp(200, {"data": _rr(DEEP, DIRECT)}),
    }
    routes.update(extra)
    return routes


async def test_live_network_carries_relationship_records_and_direct_parents(_live):
    client = _Client(_shell_routes())
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL, include_bods=True)
    stmts = res["bods"]
    pairs = {(r["recordDetails"]["subject"], r["recordDetails"]["interestedParty"])
             for r in _rels(stmts)}
    assert (_sid_of(stmts, DEEP), _sid_of(stmts, DIRECT)) in pairs
    assert len(_rels(stmts)) == 3  # both → 1, deep → direct + indirect
    # the result was complete, so it was cached in the new shape
    cached = subs._cache.get_payload(f"{subs._CACHE_NS}/{SHELL}")
    assert cached and cached[0]["shape"] == subs._SHAPE and cached[0]["complete"] is True


async def test_the_golden_copy_places_a_parent_before_gleif_is_asked(_live):
    client = _Client(_shell_routes())
    seen: list[list[str]] = []

    def _store(leis):
        seen.append(list(leis))
        return {DEEP: _rr(DEEP, DIRECT)} if DEEP in leis else {}

    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", _store):
            await subs.assemble_subsidiaries(SHELL)
    assert seen and DEEP in seen[0]
    assert not any(c.endswith("/direct-parent-relationship") for c in client.calls)


async def test_live_parent_lookups_are_capped_and_the_rest_not_cached(_live, monkeypatch):
    monkeypatch.setattr(subs, "_PARENT_LOOKUP_CAP", 1)
    many = [f"5493000000000000X{i:03d}" for i in range(3)]
    routes = _shell_routes(**{
        f"/{SHELL}/ultimate-children": _page([_l1(x, f"C{x}") for x in many]),
        f"/{SHELL}/direct-children": _Resp(404),
    })
    client = _Client(routes)
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            await subs.assemble_subsidiaries(SHELL)
    assert sum(c.endswith("/direct-parent-relationship") for c in client.calls) == 1
    # cached — the children are whole — but marked unenriched, so it is
    # trusted for an hour only
    cached = subs._cache.get_payload(f"{subs._CACHE_NS}/{SHELL}")
    assert cached and cached[0]["complete"] is True and cached[0]["enriched"] is False


async def test_refused_relationship_records_cost_the_dates_not_the_children(_live):
    client = _Client(_shell_routes(**{
        f"/{SHELL}/direct-child-relationships": _Resp(429),
    }))
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["direct_available"] is True and res["degraded_detail"] is None
    assert res["distinct_fetched"] == 2
    cached = subs._cache.get_payload(f"{subs._CACHE_NS}/{SHELL}")
    assert cached and cached[0]["enriched"] is False


async def test_an_unenriched_entry_is_rebuilt_after_an_hour(_live, monkeypatch):
    key = f"{subs._CACHE_NS}/{SHELL}"
    subs._cache.put(key, {
        "lei": SHELL, "subject_attrs": {}, "direct_total": 0, "ultimate_total": 0,
        "children": [], "complete": True, "shape": subs._SHAPE, "enriched": False,
    })
    client = _Client(_shell_routes())
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            fresh = await subs.assemble_subsidiaries(SHELL)
            assert fresh["distinct_fetched"] == 0 and not client.calls  # within the hour
            monkeypatch.setattr(subs, "_UNENRICHED_MAX_AGE_DAYS", -1)
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["distinct_fetched"] == 2 and client.calls


async def test_a_refused_parent_lookup_stops_the_rest(_live):
    from opencheck.gleif_throttle import GleifRateLimitedError

    class _Refusing(_Client):
        async def get(self, url, **kwargs):
            if url.endswith("/direct-parent-relationship"):
                self.calls.append(url)
                raise GleifRateLimitedError("held", reason="held_for_lookups")
            return await super().get(url, **kwargs)

    many = [f"5493000000000000Y{i:03d}" for i in range(8)]
    client = _Refusing(_shell_routes(**{
        f"/{SHELL}/ultimate-children": _page([_l1(x, f"C{x}") for x in many]),
        f"/{SHELL}/direct-children": _Resp(404),
    }))
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["distinct_fetched"] == 8 and res["degraded_detail"] is None
    # at most one call per concurrent slot before the refusal is seen
    assert sum(c.endswith("/direct-parent-relationship") for c in client.calls) <= subs._PARENT_LOOKUP_CONCURRENCY


async def test_old_shape_cache_entries_are_rebuilt(_live):
    subs._cache.put(f"{subs._CACHE_NS}/{SHELL}", {
        "lei": SHELL, "subject_attrs": {}, "direct_total": 0, "ultimate_total": 0,
        "children": [], "complete": True,
    })
    client = _Client(_shell_routes())
    with patch.object(subs, "build_client", lambda: _CM(client)):
        with patch.object(subs, "_store_direct_parents", lambda leis: {}):
            res = await subs.assemble_subsidiaries(SHELL)
    assert res["distinct_fetched"] == 2 and client.calls


async def test_countries_roll_subdivisions_up(_live, monkeypatch):
    kids = [
        {"record": _l1("A" * 18 + "01", "A", jur="CA"), "relations": ["direct"]},
        {"record": _l1("A" * 18 + "02", "B", jur="CA-AB"), "relations": ["direct"]},
        {"record": _l1("A" * 18 + "03", "C", jur="US-DE"), "relations": ["direct"]},
    ]

    async def _build(lei):
        return {"lei": lei, "subject_attrs": {}, "direct_total": 3, "ultimate_total": 0,
                "children": kids}

    monkeypatch.setattr(subs, "_build", _build)
    res = await subs.assemble_subsidiaries(SHELL)
    assert {j["code"] for j in res["jurisdictions"]} == {"CA", "CA-AB", "US-DE"}
    assert res["countries"] == [{"code": "CA", "count": 2}, {"code": "US", "count": 1}]


def test_store_direct_parents_reads_the_golden_copy(monkeypatch):
    from opencheck import entity_pages

    class _Row:
        direct_parent_lei = DIRECT

    class _Held:
        parent_lei = DIRECT
        child_lei = DEEP
        relationship_type = "IS_DIRECTLY_CONSOLIDATED_BY"
        relationship_status = "ACTIVE"
        registration_status = "PUBLISHED"
        period_start = "2020-11-02"
        period_end = None
        last_updated = "2023-07-31"

    class _Store:
        def get_many(self, leis):
            return {DEEP: _Row()} if DEEP in leis else {}

        def relationship(self, lei, kind):
            return _Held()

    monkeypatch.setattr(entity_pages, "get_store", lambda: _Store())
    out = subs._store_direct_parents([DEEP, STRANDED])
    assert set(out) == {DEEP}
    rel = out[DEEP]["attributes"]["relationship"]
    assert rel["endNode"]["id"] == DIRECT
    assert rel["periods"][0]["startDate"] == "2020-11-02"


# ---------------------------------------------------------------------------
# Fixes 5–7: the GLEIF entity statement
# ---------------------------------------------------------------------------


def test_a_lapsed_lei_is_said_and_worded_as_the_records_status():
    rec = _l1(DEEP, "SHELL DEEP B.V.", reg={
        "status": "LAPSED", "nextRenewalDate": "2022-10-28T09:20:00Z",
        "lastUpdateDate": "2023-07-31T17:34:15Z",
    })["attributes"]
    stmt = _gleif_entity_statement(DEEP, rec["entity"], "u", attrs=rec)
    notes = [a["description"] for a in stmt["annotations"] if a["motivation"] == "commenting"]
    lapsed = [n for n in notes if "LEI" in n and "lapsed" in n.lower()]
    assert lapsed and "not of the company" in lapsed[0]
    # liveness is untouched: GLEIF still records the entity as active
    assert any("records this entity as active" in n for n in notes)


def test_an_issued_lei_gets_no_registration_note():
    rec = _l1(DIRECT, "X", reg={"status": "ISSUED"})["attributes"]
    stmt = _gleif_entity_statement(DIRECT, rec["entity"], "u", attrs=rec)
    assert not any("LEI record" in a["description"] for a in stmt.get("annotations") or [])


@pytest.mark.parametrize(
    ("legal_form", "label"),
    [
        ({"id": "9999", "other": "SOCIEDAD ANONIMA DE CAPITAL VARIABLE"},
         "SOCIEDAD ANONIMA DE CAPITAL VARIABLE"),
        ({"id": "9999"}, None),
        ({"id": "ZZZZ", "other": "COMPANY"}, "COMPANY"),
    ],
)
def test_legal_form_falls_back_to_gleifs_free_text(legal_form, label):
    rec = _l1(DIRECT, "X", legal_form=legal_form)["attributes"]
    stmt = _gleif_entity_statement(DIRECT, rec["entity"], "u", attrs=rec)
    assert stmt["recordDetails"].get("legalFormLabel") == label


@pytest.mark.parametrize(
    ("ra", "scheme"),
    [("RA000439", "MY-SSM"), ("RA000483", "PH-SEC"), ("RA000407", "IT-RI"),
     ("RA000406", "IL-ROC"), ("RA000478", "PA-PRP"), ("RA000540", "LK-DRC"),
     ("RA000475", "PK-SEC"), ("RA001075", "JP-JCN"), ("RA000449", "MX-RFC")],
)
def test_ra_codes_with_an_org_id_code_use_it(ra, scheme):
    from opencheck.bods.mapper import gleif_registration_scheme

    assert gleif_registration_scheme(ra, None)[0] == scheme


def test_the_new_schemes_add_no_register_hops():
    """No adapter claims these RA codes, so FullCheck cannot hop on them."""
    from opencheck.register_hops import hop_schemes

    for scheme in ("MY-SSM", "PH-SEC", "IT-RI", "IL-ROC", "PA-PRP", "LK-DRC",
                   "PK-SEC", "JP-JCN", "MX-RFC"):
        assert scheme not in hop_schemes()


# ---------------------------------------------------------------------------
# Fix 9: the lookup's direct children are paged
# ---------------------------------------------------------------------------


async def test_lookup_direct_children_read_every_page(monkeypatch):
    from opencheck.sources.gleif import GleifAdapter

    pages = {
        1: {"data": [_l1(f"P1{i:018d}", "a") for i in range(100)],
            "meta": {"pagination": {"total": 105, "lastPage": 2}}},
        2: {"data": [_l1(f"P2{i:018d}", "b") for i in range(5)],
            "meta": {"pagination": {"total": 105, "lastPage": 2}}},
    }
    asked: list[str] = []

    async def _get_optional(self, path, *, cache_key, max_age_days=None):
        asked.append(cache_key)
        page = int(re.search(r"page\[number\]=(\d+)", path)[1])
        return pages.get(page)

    monkeypatch.setattr(GleifAdapter, "_get_optional", _get_optional)
    records, total = await GleifAdapter()._fetch_direct_children(SHELL)
    assert total == 105 and len(records) == 105
    # page 1 keeps its pre-255 cache key
    assert asked[0].endswith("direct-children-p1-s100")


async def test_a_failed_later_page_keeps_the_first(monkeypatch):
    from opencheck.sources.gleif import GleifAdapter

    async def _get_optional(self, path, *, cache_key, max_age_days=None):
        if "page[number]=1" in path:
            return {"data": [_l1("P" * 20, "a")], "meta": {"pagination": {"total": 150, "lastPage": 2}}}
        raise RuntimeError("429")

    monkeypatch.setattr(GleifAdapter, "_get_optional", _get_optional)
    records, total = await GleifAdapter()._fetch_direct_children(SHELL)
    assert len(records) == 1 and total == 150


# ---------------------------------------------------------------------------
# Fixes 12–14: partial dates, person identifiers, MEIP order
# ---------------------------------------------------------------------------


def test_a_year_only_founding_date_is_left_out_and_recorded():
    stmt = make_entity_statement(
        source_id="opensanctions", local_id="NK-x", name="昭和シェル石油株式会社",
        founding_date="1942",
    )
    assert "foundingDate" not in stmt["recordDetails"]
    (note,) = [a for a in stmt["annotations"] if a["motivation"] == "transformation"]
    assert "“1942”" in note["description"] and note["statementPointerTarget"] == "/recordDetails"
    # the consistency check still compares it at its own precision
    assert dropped_partial_date(stmt, "foundingDate") == "1942"
    assert _extract_founding(stmt) == "1942"


def test_a_full_date_or_datetime_is_kept_as_a_date():
    a = make_entity_statement(source_id="gleif", local_id="a", name="A", founding_date="2016-03-07")
    b = make_entity_statement(source_id="gleif", local_id="b", name="B",
                              founding_date="2016-03-07T00:00:00Z")
    assert a["recordDetails"]["foundingDate"] == b["recordDetails"]["foundingDate"] == "2016-03-07"
    assert "annotations" not in a and "annotations" not in b


def test_person_identifiers_keep_documents_and_annotate_the_rest():
    stmt = make_person_statement(
        source_id="wikidata", local_id="Q7747", full_name="Vladimir Putin",
        identifiers=[
            {"id": "Q7747", "scheme": "WIKIDATA", "schemeName": "Wikidata Q identifier",
             "uri": "https://www.wikidata.org/wiki/Q7747"},
            {"id": "123456789", "scheme": "GBR-PASSPORT", "schemeName": "UK passport"},
        ],
    )
    assert [i["scheme"] for i in stmt["recordDetails"]["identifiers"]] == ["GBR-PASSPORT"]
    (moved,) = person_identifiers_from_annotations(stmt)
    assert moved == {"id": "Q7747", "scheme": "WIKIDATA", "schemeName": "Wikidata Q identifier",
                     "uri": "https://www.wikidata.org/wiki/Q7747"}


def test_exports_still_carry_the_moved_person_identifier():
    person = make_person_statement(
        source_id="wikidata", local_id="Q7747", full_name="Vladimir Putin",
        identifiers=[{"id": "Q7747", "scheme": "WIKIDATA", "schemeName": "Wikidata"}],
    )
    (ftm,) = [e for e in map_to_ftm([person]) if e.get("schema") == "Person"]
    assert "Q7747" in (ftm["properties"].get("wikidataId") or [])
    (sz,) = map_to_senzing([person])
    assert "Q7747" in repr(sz["FEATURES"])


def test_backgroundcheck_reads_the_same_description_shape():
    """``annotations.ts`` parses the sentence ``person_identifier_note`` writes."""
    ts = (_REPO / "frontend/src/lib/annotations.ts").read_text(encoding="utf-8")
    # Phase 256 shape, and the Phase 255 one still read for saved reports.
    assert "^Identifier (\\S+) \\(scheme ([^;]+); (.+?)\\)\\. BODS keeps" in ts
    assert "identifier (\\S+) \\(scheme ([^)]+)\\)" in ts


def test_meip_parties_precede_relationships():
    raw = {"bods_statements": [
        {"statementId": "r", "recordType": "relationship"},
        {"statementId": "e1", "recordType": "entity"},
        {"statementId": "e2", "recordType": "entity"},
    ]}
    assert [s["statementId"] for s in map_meip(raw)] == ["e1", "e2", "r"]


# ---------------------------------------------------------------------------
# Fix 10: subsidiaries through MCP
# ---------------------------------------------------------------------------


async def test_mcp_subsidiaries_tool_summary_and_bods(monkeypatch):
    async def _assemble(lei, *, include_bods=False):
        return {"lei": lei, "available": True, "direct_total": 1, "children": [{"lei": DIRECT}],
                "bods": [{"statementId": "s"}] if include_bods else None}

    monkeypatch.setattr("opencheck.subsidiaries.assemble_subsidiaries", _assemble)
    summary = await mcp_server.opencheck_subsidiaries(lei=SHELL)
    assert "bods" not in summary and summary["children"] == [{"lei": DIRECT}]
    assert summary["license_notices"][0]["source_id"] == "gleif"
    assert "accounting consolidation" in summary["measures"]

    bods = await mcp_server.opencheck_subsidiaries(lei=SHELL, format="bods")
    assert bods["bods"] == [{"statementId": "s"}] and bods["statement_count"] == 1


async def test_mcp_subsidiaries_tool_declared_lists(monkeypatch):
    async def _assemble(lei, *, include_bods=False):
        return {"lei": lei, "children": [], "bods": None}

    async def _declared(lei):
        return {"lei": lei, "sources": [
            {"id": "meip", "covered": True, "rows": [{"name": "X"}]},
            {"id": "eiti_assessment", "covered": False, "rows": []},
        ], "covered": 1, "listed": 1, "with_lei": 0}

    monkeypatch.setattr("opencheck.subsidiaries.assemble_subsidiaries", _assemble)
    monkeypatch.setattr("opencheck.subsidiaries_declared.assemble_declared", _declared)
    out = await mcp_server.opencheck_subsidiaries(lei=SHELL, include_declared=True)
    assert out["declared"]["sources"][0]["id"] == "meip"
    assert [n["source_id"] for n in out["license_notices"]] == ["gleif", "meip"]


@pytest.mark.parametrize(("lei", "fmt"), [("NOTANLEI", "summary"), (SHELL, "csv")])
async def test_mcp_subsidiaries_tool_refuses_bad_input(lei, fmt):
    out = await mcp_server.opencheck_subsidiaries(lei=lei, format=fmt)
    assert "error" in out


async def test_mcp_export_can_include_subsidiaries(monkeypatch):
    base = [{"statementId": "e1", "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Shell"}}]

    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=base, bods_issues=[], license_notices=[])

    async def _merge(payload):
        merged = SimpleNamespace(**{**vars(payload), "bods": payload.bods + [{"statementId": "x"}]})
        return merged, 1

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake_lookup)
    monkeypatch.setattr("opencheck.routers.export._merge_subsidiaries", _merge)
    plain = await mcp_server.opencheck_export_bods(lei=SHELL)
    assert plain["statement_count"] == 1 and plain["subsidiary_statement_count"] == 0
    merged = await mcp_server.opencheck_export_bods(lei=SHELL, include_subsidiaries=True)
    assert merged["statement_count"] == 2 and merged["subsidiary_statement_count"] == 1
