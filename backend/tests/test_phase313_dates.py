"""Phase 313 — the wrong date claims found by the BODS dates audit (9 Oct 2026).

The audit ("📆 Audit of dates captured across OpenCheck") measured every
source's ``statementDate`` / ``publicationDate`` / ``retrievedAt`` against the
BODS v0.4 dates guidance. Phase A fixes the claims that were false rather
than merely thin:

* **GLEIF Level 2 edges were dated by the wrong record.** The adapter read the
  other party's Level 1 record and never the relationship (RR) record, so all
  107 of Shell plc's relationships carried the subject's date and no period.
  The mapper side is pinned in ``test_register_statement_dates.py``; the
  adapter side — where the RR records come from — is pinned here.
* **Two mapper call sites ran outside any provenance scope**, so a live fetch
  produced statements claiming no retrieval and dated today.
* **ClimateTRACE's GEM ownership rows inherited a live API timestamp** for
  CSVs that may be months old.
* **``source.type``**: OpenCorporates is an aggregator (``thirdParty``) and
  GLEIF the official register of LEIs (``officialRegister``) — pinned in
  ``test_bods_statement_metadata.py``.
"""

from __future__ import annotations

import ast
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import opencheck.bods as bods_pkg
from opencheck import entity_pages, gleif_throttle, provenance
from opencheck import subsidiaries as subs
from opencheck.config import get_settings
from opencheck.entity_pages import RelationshipRow
from opencheck.gleif_throttle import GleifRateLimitedError
from opencheck.sources import climatetrace as ct
from opencheck.sources.gleif import GleifAdapter

TODAY = date.today().isoformat()
_PKG = Path(__file__).resolve().parents[1] / "opencheck"

SUBJECT = "213800LH1BZH3DI6G760"
PARENT = "5493001KJTIIGC8Y1R12"
CHILD = "CHILDAAAAAAAAAAAAA01"


@pytest.fixture
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _row(child: str, parent: str, kind: str = "IS_DIRECTLY_CONSOLIDATED_BY",
         *, updated: str = "2025-11-03T09:12:44Z", start: str | None = "2014-01-01",
         registration: str = "PUBLISHED") -> RelationshipRow:
    return RelationshipRow(
        child_lei=child, relationship_type=kind, parent_lei=parent,
        relationship_status="ACTIVE", registration_status=registration,
        period_start=start, period_end=None, last_updated=updated,
    )


def _fake_store(rows: list[RelationshipRow]) -> SimpleNamespace:
    by_key = {
        (r.child_lei, "direct" if "DIRECTLY" in r.relationship_type else "ultimate"): r
        for r in rows
    }
    return SimpleNamespace(
        has_relationships=True,
        relationship=lambda lei, kind: by_key.get((lei, kind)),
    )


def _l1(lei: str) -> dict:
    return {"id": lei, "attributes": {"lei": lei, "entity": {"legalName": {"name": lei}}}}


def _bundle() -> dict:
    return {
        "lei": SUBJECT,
        "record": _l1(SUBJECT),
        "direct_parent": _l1(PARENT),
        "ultimate_parent": None,
        "direct_children": [_l1(CHILD)],
    }


def _live_rr(child: str, parent: str, updated: str = "2026-02-02T00:00:00Z") -> dict:
    return {
        "type": "relationship-records",
        "attributes": {
            "relationship": {
                "startNode": {"id": child, "type": "LEI"},
                "endNode": {"id": parent, "type": "LEI"},
                "type": "IS_DIRECTLY_CONSOLIDATED_BY",
            },
            "registration": {"lastUpdateDate": updated},
        },
    }


# ---------------------------------------------------------------------------
# GLEIF — where the relationship records come from
# ---------------------------------------------------------------------------


class TestGleifRelationshipRecords:
    async def test_the_mirror_supplies_them_without_a_live_call(self, _live, monkeypatch):
        monkeypatch.setattr(
            entity_pages, "get_store",
            lambda: _fake_store([_row(SUBJECT, PARENT), _row(CHILD, SUBJECT)]),
        )
        adapter = GleifAdapter()

        async def _no_live(*a, **k):
            raise AssertionError("a mirror hit must not go live")

        monkeypatch.setattr(adapter, "_get_optional", _no_live)
        bundle = _bundle()
        await adapter._attach_relationship_records(SUBJECT, bundle)
        rr = bundle["direct_parent_relationship"]["attributes"]
        assert rr["registration"]["lastUpdateDate"] == "2025-11-03T09:12:44Z"
        assert rr["relationship"]["periods"][0]["startDate"] == "2014-01-01"
        assert CHILD in bundle["direct_child_relationships"]
        assert "ultimate_parent_relationship" not in bundle

    async def test_a_mirror_record_naming_another_parent_is_not_used(self, _live, monkeypatch):
        """A mirror hours behind must not date an edge GLEIF has re-pointed —
        the live record is read instead."""
        monkeypatch.setattr(
            entity_pages, "get_store",
            lambda: _fake_store([_row(SUBJECT, "OTHERPARENTXXXXXXXXX")]),
        )
        adapter = GleifAdapter()
        calls: list[str] = []

        async def _get_optional(path, **kwargs):
            calls.append(path)
            assert gleif_throttle._discretionary.get(), "enrichment must be discretionary"
            if path.endswith("/direct-parent-relationship"):
                return {"data": _live_rr(SUBJECT, PARENT)}
            return {"data": [_live_rr(CHILD, SUBJECT, "2026-03-03T00:00:00Z")]}

        monkeypatch.setattr(adapter, "_get_optional", _get_optional)
        bundle = _bundle()
        await adapter._attach_relationship_records(SUBJECT, bundle)
        assert (
            bundle["direct_parent_relationship"]["attributes"]["registration"]["lastUpdateDate"]
            == "2026-02-02T00:00:00Z"
        )
        assert CHILD in bundle["direct_child_relationships"]
        assert any("direct-child-relationships" in c for c in calls)

    async def test_a_refusal_costs_the_dates_never_the_anchor(self, _live, monkeypatch):
        monkeypatch.setattr(entity_pages, "get_store", lambda: None)
        adapter = GleifAdapter()

        async def _refused(*a, **k):
            raise GleifRateLimitedError("held for lookups", reason="held_for_lookups")

        monkeypatch.setattr(adapter, "_get_optional", _refused)
        bundle = _bundle()
        await adapter._attach_relationship_records(SUBJECT, bundle)
        assert "direct_parent_relationship" not in bundle
        assert "direct_child_relationships" not in bundle
        # …and the mapper still dates both edges from the reporter's record.
        out = bods_pkg.map_gleif(bundle)
        assert all(s["statementDate"] for s in out)

    async def test_a_live_record_for_another_pair_is_dropped(self, _live, monkeypatch):
        monkeypatch.setattr(entity_pages, "get_store", lambda: None)
        adapter = GleifAdapter()

        async def _get_optional(path, **kwargs):
            if path.endswith("/direct-parent-relationship"):
                return {"data": _live_rr(SUBJECT, "SOMEONEELSEXXXXXXXXX")}
            return {"data": [_live_rr(CHILD, "SOMEONEELSEXXXXXXXXX")]}

        monkeypatch.setattr(adapter, "_get_optional", _get_optional)
        bundle = _bundle()
        await adapter._attach_relationship_records(SUBJECT, bundle)
        assert "direct_parent_relationship" not in bundle
        assert "direct_child_relationships" not in bundle

    def test_the_mirror_bundle_carries_the_store_records(self, monkeypatch):
        """The snapshot/mirror anchor reads RR records from the same store."""
        child_row = SimpleNamespace(lei=CHILD)
        store = _fake_store([_row(SUBJECT, PARENT), _row(CHILD, SUBJECT)])
        subject_row = SimpleNamespace(
            lei=SUBJECT, direct_parent_lei=PARENT, ultimate_parent_lei=None
        )
        store.get = lambda lei: subject_row if lei == SUBJECT else None
        store.get_many = lambda leis: {}
        store.children = lambda lei, limit=100: ([child_row], 1)
        store.exceptions = lambda lei: {}
        store.watermark = lambda: datetime(2026, 10, 9, tzinfo=timezone.utc)
        store.retrieved_at = lambda: None
        monkeypatch.setattr(entity_pages, "get_store", lambda: store)
        monkeypatch.setattr(entity_pages, "gleif_record_from_row", lambda r: _l1(r.lei))
        bundle = GleifAdapter()._snapshot_bundle(SUBJECT, reason="mirror")
        assert bundle["direct_parent_relationship"]["type"] == "relationship-records"
        assert CHILD in bundle["direct_child_relationships"]


# ---------------------------------------------------------------------------
# Subsidiary networks — un-enriched children
# ---------------------------------------------------------------------------


def test_an_unenriched_subsidiary_edge_carries_the_childs_own_date():
    """With no RR record the child — the reporter — dates the edge, never
    the day OpenCheck read it (Phase 313)."""
    child = {
        "record": {"attributes": {
            "lei": CHILD,
            "entity": {"legalName": {"name": "CHILD"}},
            "registration": {"lastUpdateDate": "2024-02-05T00:00:00Z"},
        }},
        "relations": ["direct"],
    }
    out = bods_pkg.map_gleif_subsidiaries(SUBJECT, {"lei": SUBJECT}, [child])
    rel = next(s for s in out if s["recordType"] == "relationship")
    assert rel["statementDate"] == "2024-02-05"


# ---------------------------------------------------------------------------
# Provenance scopes around every mapper call
# ---------------------------------------------------------------------------

#: Mapper calls that may run outside ``mapping_provenance`` — each one never
#: publishes the statements it builds. Keyed (module, enclosing function).
_UNSCOPED_ALLOWED = {
    # Counts EveryPolitician's statements for the row's button label; the
    # statements themselves are discarded.
    ("routers/lookup.py", "_everypolitician_row_counts"),
}


def _mapper_calls() -> list[tuple[str, int, str, str | None, bool]]:
    mappers = {
        n for n in dir(bods_pkg) if n.startswith("map_") and not n.startswith("map_to_")
    }
    found = []
    for path in sorted(_PKG.rglob("*.py")):
        rel = path.relative_to(_PKG).as_posix()
        if rel.startswith("bods/"):
            continue  # mappers calling their own helpers
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents: dict[ast.AST, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.id if isinstance(func, ast.Name)
                else func.attr if isinstance(func, ast.Attribute) else None
            )
            if name not in mappers and name != "mapper":
                continue
            scoped, enclosing, cur = False, None, node
            while cur in parents:
                cur = parents[cur]
                if isinstance(cur, (ast.With, ast.AsyncWith)) and any(
                    "mapping_provenance" in ast.unparse(item.context_expr)
                    for item in cur.items
                ):
                    scoped = True
                if enclosing is None and isinstance(
                    cur, (ast.FunctionDef, ast.AsyncFunctionDef)
                ):
                    enclosing = cur.name
            found.append((rel, node.lineno, name, enclosing, scoped))
    return found


def test_every_mapper_call_runs_inside_a_provenance_scope():
    """A mapper outside ``mapping_provenance`` reads STUB_PROVENANCE: its
    statements claim no retrieval and fall back to today's date, whatever the
    fetch actually was. Two call sites did exactly that until Phase 313
    (the subsidiary network, the person-appointments view)."""
    calls = _mapper_calls()
    assert len(calls) >= 6, "the scan found too few call sites to mean anything"
    unscoped = [
        f"{rel}:{line} {name}() in {fn}"
        for rel, line, name, fn, scoped in calls
        if not scoped and (rel, fn) not in _UNSCOPED_ALLOWED
    ]
    assert not unscoped, "mapper called outside mapping_provenance:\n" + "\n".join(unscoped)


def test_the_allowlist_is_not_stale():
    seen = {(rel, fn) for rel, _, _, fn, scoped in _mapper_calls() if not scoped}
    assert _UNSCOPED_ALLOWED <= seen


async def test_a_fetched_subsidiary_network_says_when_it_was_read(_live, monkeypatch):
    async def _build(lei):
        provenance.record_live("test GLEIF call")
        return {
            "lei": lei, "subject_attrs": {"lei": lei}, "direct_total": 1,
            "ultimate_total": 0, "children": [{"record": _l1(CHILD), "relations": ["direct"]}],
        }

    monkeypatch.setattr(subs, "_build", _build)
    outer = provenance.Recorder()
    token = provenance._CURRENT.set(outer)
    try:
        res = await subs.assemble_subsidiaries(SUBJECT, include_bods=True)
    finally:
        provenance._CURRENT.reset(token)
    assert res["bods"]
    assert all("retrievedAt" in s["source"] for s in res["bods"])
    # An outer scope (a FullCheck hop inside a lookup) still sees the call.
    assert [o.liveness for o in outer.observations] == ["live"]


async def test_person_appointments_say_when_companies_house_was_read(monkeypatch):
    from opencheck.routers import person_check
    from opencheck.sources import REGISTRY

    from tests.test_person_check import _OFFICER_BUNDLE

    async def fake_fetch(hit_id):
        provenance.record_live("test CH call")
        return dict(_OFFICER_BUNDLE)

    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", fake_fetch)
    resp = await person_check._person_appointments_impl(_OFFICER_BUNDLE["officer_id"])
    assert resp.bods
    assert all("retrievedAt" in s["source"] for s in resp.bods)


# ---------------------------------------------------------------------------
# ClimateTRACE — GEM release dates
# ---------------------------------------------------------------------------


class TestGemRelease:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("ownership/ownership_all_entities_050826.csv", "2026-08-05"),
            ("ownership_all_entities_020626.csv", "2026-06-02"),
            ("ownership_all_entities_310226.csv", None),  # 31 Feb: not a date
            ("all_entities.csv", None),
            (None, None),
        ],
    )
    def test_release_date_from_the_dated_filename(self, name, expected):
        assert ct._release_date_from_name(name) == expected

    def test_the_sidecar_names_the_release_of_the_cached_csv(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
        get_settings.cache_clear()
        try:
            path = ct._gem_csv_path("entities")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Entity ID,Full Name\n")
            assert ct.gem_release()["release_date"] is None  # no sidecar: not guessed
            ct._write_gem_release(
                "ownership/ownership_all_entities_050826.csv", "2026-08-07T10:00:00Z"
            )
            release = ct.gem_release()
            assert release["release_date"] == "2026-08-05"
            assert release["downloaded_at"]
            assert json.loads(ct._gem_release_path().read_text())["gcs_updated"]
        finally:
            get_settings.cache_clear()

    def test_the_release_is_declared_as_the_snapshot_date(self):
        with provenance.recording() as rec:
            ct._declare_gem_snapshot(
                {"release_date": "2026-08-05", "downloaded_at": "2026-09-01T00:00:00+00:00"}
            )
        (obs,) = rec.observations
        assert obs.liveness == "snapshot"
        # Two clocks (Phase 314): GEM's release is the cut, the download ours.
        assert obs.source_as_of == datetime(2026, 8, 5, tzinfo=timezone.utc)
        assert obs.retrieved_at == datetime(2026, 9, 1, tzinfo=timezone.utc)

    def test_without_a_release_date_the_download_time_is_declared(self):
        with provenance.recording() as rec:
            ct._declare_gem_snapshot(
                {"release_date": None, "downloaded_at": "2026-09-01T12:00:00+00:00"}
            )
        assert rec.observations[0].retrieved_at == datetime(
            2026, 9, 1, 12, tzinfo=timezone.utc
        )
        assert rec.observations[0].source_as_of is None

    def test_every_gem_statement_is_dated_by_the_release(self):
        from tests.test_bods_climatetrace import _entity_with_parent_bundle

        bundle = _entity_with_parent_bundle()
        bundle["gem_release"] = "2026-08-05"
        out = list(bods_pkg.map_climatetrace(bundle))
        assert len(out) >= 3
        assert {s["statementDate"] for s in out} == {"2026-08-05"}
        assert all(s["publicationDetails"]["publicationDate"] == TODAY for s in out)

    def test_the_sweep_expects_a_snapshot_now(self):
        from opencheck.sources.probes import PROBES

        assert PROBES["climatetrace"].expect_liveness == frozenset({"snapshot"})


def test_climatetrace_bundle_resolves_to_a_snapshot(monkeypatch, tmp_path):
    """End to end through the adapter: the GEM CSVs are declared, so the live
    emissions call can no longer stand in for them."""
    monkeypatch.setattr(ct, "gem_release", lambda: {"release_date": "2026-08-05",
                                                    "downloaded_at": None})
    monkeypatch.setattr(ct, "_get_indexes", lambda: ({}, {"E1": {"Full Name": "X"}}))
    monkeypatch.setattr(ct, "_get_relationship_indexes", lambda: ({}, {}))
    monkeypatch.setattr(ct, "_get_geot_data", lambda: {"meta": {}, "entities": {}})

    async def _ct_get(self, *a, **k):
        provenance.record_live("Climate TRACE API")
        return {}

    monkeypatch.setattr(ct.ClimateTRACEAdapter, "_ct_get", _ct_get)
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    import asyncio

    try:
        with provenance.recording() as rec:
            bundle = asyncio.run(
                ct.ClimateTRACEAdapter()._fetch_entity_data("E1", "climatetrace/test")
            )
    finally:
        get_settings.cache_clear()
    resolved = rec.resolve()
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of == datetime(2026, 8, 5, tzinfo=timezone.utc)
    assert bundle["gem_release"] == "2026-08-05"
