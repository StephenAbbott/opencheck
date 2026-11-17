"""OpenAleph name-only matches are shown, never mapped into the subject's BODS.

The BODS quality sweep failed every week from 26 Oct 2026 on
``declaration_subject_not_subject|openaleph``: OpenAleph's name steps (FtM
match, percolation, ``q=``) returned procurement records of the subject
(Regulatory DataCorp Limited, Equinor ASA) carrying nothing but their own
OpenAleph id. Nothing could join them to the subject's referent group, so each
was deepened as its own network and the source named two subjects. Stephen's
decision (17 Nov 2026, option 3 on the ticket "OpenAleph records of Equinor
that join no referent group"): keep such a record off the subject's BODS.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers import lookup as lookup_mod
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.base import SourceHit

_LEI = "894500LD30X1VN839203"


def _hit(hit_id: str, **identifiers: str) -> SourceHit:
    return SourceHit(
        source_id="openaleph", hit_id=hit_id, kind=SearchKind.ENTITY,
        name="REGULATORY DATACORP LIMITED", summary="collection: OCDS",
        identifiers={"aleph_id": hit_id, **identifiers}, raw={}, is_stub=False,
    )


def _stub_steps(monkeypatch, *, lei=(), match=(), percolate=(), name=()) -> None:
    adapter = REGISTRY["openaleph"]

    def returning(hits):
        async def step(*_a, **_kw):
            return list(hits)
        return step

    async def no_mentions(*_a, **_kw):
        return None

    monkeypatch.setattr(adapter, "fetch_by_lei", returning(lei))
    monkeypatch.setattr(adapter, "fetch_by_oc_url", returning(()), raising=False)
    monkeypatch.setattr(adapter, "fetch_by_registration", returning(()))
    monkeypatch.setattr(adapter, "match_entity", returning(match))
    monkeypatch.setattr(adapter, "fetch_by_name_percolate", returning(percolate))
    monkeypatch.setattr(adapter, "fetch_by_name", returning(name))
    monkeypatch.setattr(adapter, "fetch_mentions", no_mentions)


def _ctx() -> "lookup_mod._LookupCtx":
    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = "Regulatory DataCorp Limited"
    return ctx


def _flagged(hits: list[SourceHit]) -> dict[str, bool]:
    return {h.hit_id: bool(h.raw.get(lookup_mod.OPENALEPH_NAME_ONLY)) for h in hits}


@pytest.mark.parametrize("step", ["match", "percolate", "name"])
async def test_a_name_step_flags_records_with_nothing_to_join_on(monkeypatch, step):
    _stub_steps(monkeypatch, **{step: [
        _hit("ocds-uk-body"),
        _hit("ocds-fts", registration_number="08739364"),
        _hit("oc-record", lei=_LEI),
    ]})
    result = await lookup_mod._openaleph_strategies(_ctx())
    assert _flagged(result) == {
        "ocds-uk-body": True, "ocds-fts": False, "oc-record": False,
    }


async def test_an_identifier_corroborated_match_is_never_name_only(monkeypatch):
    corroborated = _hit("ocds-corroborated")
    corroborated.raw["identifier_corroborated"] = True
    _stub_steps(monkeypatch, match=[corroborated])
    result = await lookup_mod._openaleph_strategies(_ctx())
    assert _flagged(result) == {"ocds-corroborated": False}


async def test_a_record_found_by_lei_is_never_name_only(monkeypatch):
    # The leiCode filter is an identifier step: the record was found by the
    # subject's own LEI even when the mapper reads no other identifier from it.
    _stub_steps(monkeypatch, lei=[_hit("lei-filtered")])
    result = await lookup_mod._openaleph_strategies(_ctx())
    assert _flagged(result) == {"lei-filtered": False}


@pytest.fixture
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{_LEI}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({
        "statementId": "e-subject", "recordId": "e-subject",
        "recordType": "entity",
        "recordDetails": {
            "name": "Regulatory DataCorp Limited",
            "jurisdiction": {"name": "United Kingdom", "code": "GB"},
            "identifiers": [{"id": _LEI, "scheme": "XI-LEI"}],
        },
    }) + "\n")
    yield
    get_settings.cache_clear()


def test_the_lookup_keeps_the_row_and_maps_no_statements(monkeypatch, _isolated):
    name_only = _hit("ocds-uk-body")
    numbered = _hit("ocds-fts", registration_number="08739364")
    name_only.raw[lookup_mod.OPENALEPH_NAME_ONLY] = True

    async def strategies(_ctx):
        return [name_only, numbered]

    monkeypatch.setattr(lookup_mod, "_openaleph_strategies", strategies)

    mapped: list[tuple[str, str]] = []
    real_deepen, real_count = lookup_mod._safe_deepen, lookup_mod._count_only

    async def deepen(source_id, hit_id, **kw):
        mapped.append((source_id, hit_id))
        if source_id == "openaleph":
            return None
        return await real_deepen(source_id, hit_id, **kw)

    async def count(source_id, hit_id, **kw):
        mapped.append((source_id, hit_id))
        if source_id == "openaleph":
            return None
        return await real_count(source_id, hit_id, **kw)

    monkeypatch.setattr(lookup_mod, "_safe_deepen", deepen)
    monkeypatch.setattr(lookup_mod, "_count_only", count)

    payload = TestClient(app).get(
        "/lookup", params={"lei": _LEI, "refresh": "true"}
    ).json()
    shown = {h["hit_id"] for h in payload["hits"] if h["source_id"] == "openaleph"}
    assert shown == {"ocds-uk-body", "ocds-fts"}
    assert ("openaleph", "ocds-uk-body") not in mapped
    assert ("openaleph", "ocds-fts") in mapped
