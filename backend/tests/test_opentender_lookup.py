"""Lookup-integration + issue #29 tests for the OpenTender adapter.

Covers the identifier-first dispatch over ``body_ids`` (country-scoped, restricted
to registration-bearing id_types), the name-equivalence gate on the name path
(issue #21), cross-country non-matching, and graceful degradation when the DB
artifact is absent.

Fixtures are built through the extractor's OWN machinery (``_DDL`` +
``_insert_tender``), so the synthetic ``body_ids`` / ``tenders`` rows are shaped
exactly like a real ``opentender.db`` — including the raw DIGIWHIST ``id_type``
values the allowlist keys on.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck.config import get_settings
from opencheck.routers.lookup import (
    _dispatch,
    _LookupCtx,
    _opentender_strategies,
)
from opencheck.sources import REGISTRY
from opencheck.sources.opentender import OpenTenderAdapter

# The build script isn't an importable package member — load it by path,
# exactly as the projection tests (#38) and the runtime do.
_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extract_opentender = _load("extract_opentender")

# Orange S.A.'s real SIREN — the maintainer's own worked example on issue #29.
_SIREN = "380129866"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENTENDER_DB_FILE", raising=False)
    monkeypatch.delenv("OPENTENDER_S3_URL", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fixture DB helpers — built through the extractor's own _DDL / _insert_tender
# ---------------------------------------------------------------------------

def _build_db(tmp_path: Path, tenders: list[dict[str, Any]], *, slim: bool = True) -> Path:
    db_path = tmp_path / "opentender.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(extract_opentender._DDL)
    cur = conn.cursor()
    for tender in tenders:
        assert extract_opentender._insert_tender(cur, tender, slim=slim)
    conn.commit()
    conn.close()
    return db_path


def _configure(monkeypatch, db_path: Path) -> None:
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()


def _buyer_tender(
    pid: str, country: str, name: str, id_type: str, id_value: str, *, title: str = "Public contract"
) -> dict[str, Any]:
    return {
        "persistentId": pid,
        "country": country,
        "title": title,
        "buyers": [
            {"name": name, "bodyIds": [{"type": id_type, "scope": country, "id": id_value}]}
        ],
    }


def _bidder_tender(
    pid: str,
    country: str,
    name: str,
    *,
    id_type: str | None = None,
    id_value: str | None = None,
    title: str = "Telecom services",
) -> dict[str, Any]:
    body: dict[str, Any] = {"name": name}
    if id_type and id_value:
        body["bodyIds"] = [{"type": id_type, "scope": country, "id": id_value}]
    return {
        "persistentId": pid,
        "country": country,
        "title": title,
        "lots": [{"bids": [{"isWinning": True, "bidders": [body]}]}],
    }


# ---------------------------------------------------------------------------
# (a) identifier hits — country-scoped, over an allowlisted id_type
# ---------------------------------------------------------------------------

async def test_fetch_by_registration_returns_country_scoped_organization_id_tenders(
    monkeypatch, tmp_path: Path
) -> None:
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-orange-1", "FR", "Orange S.A.", id_type="ORGANIZATION_ID", id_value=_SIREN),
            _buyer_tender("FR-orange-2", "FR", "Orange S.A.", "ORGANIZATION_ID", _SIREN, title="Network build"),
            # Same SIREN in a different country — must NOT leak into an FR query.
            _bidder_tender("CZ-clash", "CZ", "Orange Czech", id_type="ORGANIZATION_ID", id_value=_SIREN),
            # An unrelated FR tender with a different id — must NOT match.
            _bidder_tender("FR-other", "FR", "Bouygues", id_type="ORGANIZATION_ID", id_value="552032534"),
        ],
    )
    _configure(monkeypatch, db)
    hits = await OpenTenderAdapter().fetch_by_registration("FR", _SIREN)
    assert {h.hit_id for h in hits} == {"FR-orange-1", "FR-orange-2"}


@pytest.mark.parametrize("id_type", ["ORGANIZATION_ID", "TRADE_REGISTER", "HEADER_ICO", "TAX_ID"])
async def test_fetch_by_registration_accepts_every_registration_id_type(
    monkeypatch, tmp_path: Path, id_type: str
) -> None:
    db = _build_db(
        tmp_path,
        [_bidder_tender("FR-1", "FR", "Some Co", id_type=id_type, id_value="A1B2C3")],
    )
    _configure(monkeypatch, db)
    hits = await OpenTenderAdapter().fetch_by_registration("FR", "A1B2C3")
    assert {h.hit_id for h in hits} == {"FR-1"}


# ---------------------------------------------------------------------------
# (b) exclusion — internal/near-useless id_types must not match
# ---------------------------------------------------------------------------

async def test_fetch_by_registration_excludes_non_registration_id_types(
    monkeypatch, tmp_path: Path
) -> None:
    """SOURCE_ID / BVD_ID / ETALON_ID / VAT rows carrying the queried value must
    NOT match — they are internal keys or near-useless (issue #29). Mutation
    guard: dropping the ``id_type IN (...)`` allowlist makes this return the four
    decoys, so this assertion fails."""
    decoy_value = "999888777"
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-src", "FR", "Decoy Source", id_type="SOURCE_ID", id_value=decoy_value),
            _bidder_tender("FR-bvd", "FR", "Decoy BvD", id_type="BVD_ID", id_value=decoy_value),
            _bidder_tender("FR-etalon", "FR", "Decoy Etalon", id_type="ETALON_ID", id_value=decoy_value),
            _bidder_tender("FR-vat", "FR", "Decoy VAT", id_type="VAT", id_value=decoy_value),
            # A genuine registration row with a DIFFERENT value proves the query
            # itself does find allowlisted rows (guards against a vacuous pass).
            _bidder_tender("FR-ok", "FR", "Real Co", id_type="ORGANIZATION_ID", id_value="111222333"),
        ],
    )
    _configure(monkeypatch, db)
    adapter = OpenTenderAdapter()
    assert await adapter.fetch_by_registration("FR", decoy_value) == []
    assert {h.hit_id for h in await adapter.fetch_by_registration("FR", "111222333")} == {"FR-ok"}


# ---------------------------------------------------------------------------
# (c) name gate — "Orange S.A." must not attach "Red-Orange e.U." tenders
# ---------------------------------------------------------------------------

async def test_fetch_by_name_rejects_bodies_that_do_not_bear_the_name(
    monkeypatch, tmp_path: Path
) -> None:
    """The maintainer's real false-positive class: an FTS token match on
    "Orange" surfaces "Red-Orange e.U." and "Orange controls s.r.o.", neither of
    which IS Orange S.A. The gate rejects both. Mutation guard: bypassing the
    ``_tender_bears_name`` gate makes ``fetch_by_name`` return both, so this
    fails."""
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-redorange", "FR", "Red-Orange e.U."),
            _bidder_tender("FR-orangecontrols", "FR", "Orange controls s.r.o."),
        ],
    )
    _configure(monkeypatch, db)
    adapter = OpenTenderAdapter()

    # Both decoys ARE surfaced by the underlying FTS search (so the gate — not an
    # empty candidate set — is what rejects them).
    conn = adapter._conn()
    assert conn is not None
    candidate_ids = {h.hit_id for h in adapter._db_search_impl(conn, "Orange S.A.")}
    assert candidate_ids == {"FR-redorange", "FR-orangecontrols"}

    # The gate rejects both.
    assert await adapter.fetch_by_name("Orange S.A.") == []


async def test_fetch_by_name_keeps_a_body_that_bears_the_name(
    monkeypatch, tmp_path: Path
) -> None:
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-genuine", "FR", "Orange S.A."),
            _bidder_tender("FR-redorange", "FR", "Red-Orange e.U."),
        ],
    )
    _configure(monkeypatch, db)
    hits = await OpenTenderAdapter().fetch_by_name("Orange S.A.")
    assert {h.hit_id for h in hits} == {"FR-genuine"}


# ---------------------------------------------------------------------------
# (d) cross-country non-match — same number, different country
# ---------------------------------------------------------------------------

async def test_fetch_by_registration_is_country_scoped_both_ways(
    monkeypatch, tmp_path: Path
) -> None:
    value = "12345678"
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-x", "FR", "Foo FR", id_type="ORGANIZATION_ID", id_value=value),
            _bidder_tender("CZ-x", "CZ", "Foo CZ", id_type="ORGANIZATION_ID", id_value=value),
        ],
    )
    _configure(monkeypatch, db)
    adapter = OpenTenderAdapter()
    assert {h.hit_id for h in await adapter.fetch_by_registration("FR", value)} == {"FR-x"}
    assert {h.hit_id for h in await adapter.fetch_by_registration("CZ", value)} == {"CZ-x"}


# ---------------------------------------------------------------------------
# (e) graceful degradation — DB artifact absent
# ---------------------------------------------------------------------------

async def test_adapter_methods_degrade_gracefully_without_db(monkeypatch) -> None:
    """With no OPENTENDER_DB_FILE configured the adapter must never raise — both
    new lookup methods return an empty list (clean no-results)."""
    adapter = OpenTenderAdapter()
    assert adapter.info.live_available is False
    assert await adapter.fetch_by_registration("FR", _SIREN) == []
    assert await adapter.fetch_by_name("Orange S.A.") == []


# ---------------------------------------------------------------------------
# Lookup-pipeline wiring — _opentender_strategies + _dispatch
# ---------------------------------------------------------------------------

async def test_strategies_identifier_first_and_dispatched_when_registered(
    monkeypatch, tmp_path: Path
) -> None:
    db = _build_db(
        tmp_path,
        [_bidder_tender("FR-orange-1", "FR", "Orange S.A.", id_type="ORGANIZATION_ID", id_value=_SIREN)],
    )
    _configure(monkeypatch, db)
    monkeypatch.setitem(REGISTRY, "opentender", OpenTenderAdapter())

    ctx = _LookupCtx(lei="X", legal_name="Orange S.A.", derived={"siren": _SIREN})
    hits = await _opentender_strategies(ctx)
    assert {h.hit_id for h in hits} == {"FR-orange-1"}

    # Registration makes opentender part of the dispatch fan-out.
    tasks = _dispatch(ctx)
    ids = {sid for sid, _ in tasks}
    for _sid, coro in tasks:
        coro.close()  # we only inspect membership; don't leave coroutines pending
    assert "opentender" in ids


async def test_strategies_identifier_hit_suppresses_name_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-byid", "FR", "Orange S.A.", id_type="ORGANIZATION_ID", id_value=_SIREN),
            _bidder_tender("FR-redorange", "FR", "Red-Orange e.U."),
        ],
    )
    _configure(monkeypatch, db)
    monkeypatch.setitem(REGISTRY, "opentender", OpenTenderAdapter())
    ctx = _LookupCtx(lei="X", legal_name="Orange S.A.", derived={"siren": _SIREN})
    hits = await _opentender_strategies(ctx)
    # Identifier path found FR-byid → the name fallback is never consulted, so
    # the FTS-adjacent Red-Orange tender is not attached.
    assert {h.hit_id for h in hits} == {"FR-byid"}


async def test_strategies_name_fallback_runs_only_when_identifier_empty(
    monkeypatch, tmp_path: Path
) -> None:
    db = _build_db(
        tmp_path,
        [
            _bidder_tender("FR-genuine", "FR", "Orange S.A."),
            _bidder_tender("FR-redorange", "FR", "Red-Orange e.U."),
        ],
    )
    _configure(monkeypatch, db)
    monkeypatch.setitem(REGISTRY, "opentender", OpenTenderAdapter())
    # No derived national id → identifier path empty → gated name fallback runs.
    ctx = _LookupCtx(lei="X", legal_name="Orange S.A.", derived={})
    hits = await _opentender_strategies(ctx)
    assert {h.hit_id for h in hits} == {"FR-genuine"}


async def test_strategies_degrade_without_db_when_registered(monkeypatch) -> None:
    monkeypatch.setitem(REGISTRY, "opentender", OpenTenderAdapter())
    ctx = _LookupCtx(lei="X", legal_name="Orange S.A.", derived={"siren": _SIREN})
    assert await _opentender_strategies(ctx) == []


async def test_strategies_noop_when_not_registered(monkeypatch, tmp_path: Path) -> None:
    # Baseline registry has no opentender → strategies return [] and dispatch
    # omits it, so the (dormant) wiring cannot fire on a DB-less deployment.
    assert "opentender" not in REGISTRY
    ctx = _LookupCtx(lei="X", legal_name="Orange S.A.", derived={"siren": _SIREN})
    assert await _opentender_strategies(ctx) == []
    tasks = _dispatch(ctx)
    ids = {sid for sid, _ in tasks}
    for _sid, coro in tasks:
        coro.close()
    assert "opentender" not in ids
