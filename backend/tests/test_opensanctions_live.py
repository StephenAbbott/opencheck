"""Live OpenSanctions adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.risk import (
    _DEBARMENT_TOPICS,
    _INFORMATIONAL_TOPICS,
    _KNOWN_EXPORT_TOPICS,
    _KNOWN_SANCTION_TOPICS,
    _PEP_TOPICS,
)
from opencheck.sources import SearchKind
from opencheck.sources.opensanctions import _RISK_TOPICS, _TOPIC_PARAMS, OpenSanctionsAdapter

_API = "https://api.opensanctions.org"

# OpenSanctions' ``target_topics`` as published in
# https://data.opensanctions.org/meta/model.json on 2026-08-05. The adapter's
# ``_RISK_TOPICS`` is meant to mirror this exactly; the drift canary below
# fails if someone narrows the list by hand again. When OpenSanctions adds a
# target topic, update this constant in the same commit as ``_RISK_TOPICS``.
_PUBLISHED_TARGET_TOPICS = frozenset(
    {
        "corp.disqual",
        "crime",
        "crime.boss",
        "crime.fin",
        "crime.fraud",
        "crime.terror",
        "crime.theft",
        "crime.traffick",
        "crime.war",
        "debarment",
        "export.control",
        "export.control.linked",
        "export.risk",
        "invest.ban",
        "invest.risk",
        "mare.detained",
        "mare.shadow",
        "poi",
        "reg.action",
        "reg.warn",
        "role.oligarch",
        "role.pep",
        "role.rca",
        "sanction",
        "sanction.control",
        "sanction.counter",
        "sanction.linked",
        "wanted",
    }
)


def test_risk_topics_mirror_opensanctions_target_topics() -> None:
    """The screening scope must not silently narrow.

    A hand-picked subset is how ``us_bis_mieu`` (``export.control`` only) came
    to be filtered out of screening results entirely — see the comment on
    ``_RISK_TOPICS``. This is an offline canary against the last-verified
    published list; ``tests/live`` covers drift against the live model.
    """
    assert set(_RISK_TOPICS) == _PUBLISHED_TARGET_TOPICS
    assert len(_RISK_TOPICS) == len(set(_RISK_TOPICS)), "duplicate topic"


def test_sanction_family_is_fully_classified() -> None:
    """Every published ``sanction.*`` topic must have an explicit class.

    Retrieving a topic is not the same as understanding it. ``sanction.control``
    was inside ``_RISK_TOPICS`` (so those entities were fetched) while every
    classifier funnelled it through a ``startswith("sanction")`` catch-all into
    the softest bucket — a subsidiary of a designated party read as ordinary
    adjacency. This canary fails the build when OpenSanctions adds a
    sanction-family subtopic, forcing a decision about what it *means* instead
    of letting the fallback answer silently.

    Classify the new topic in ``risk._KNOWN_SANCTION_TOPICS`` (and give it a
    signal if it deserves one), then add it here in the same commit.
    """
    published = {t for t in _PUBLISHED_TARGET_TOPICS if t.startswith("sanction")}
    assert published == set(_KNOWN_SANCTION_TOPICS)


def test_export_family_is_fully_classified() -> None:
    """Every published ``export.*`` topic must have an explicit class.

    The export family repeated the ``sanction.control`` failure one family
    over: all three topics sat in ``_RISK_TOPICS`` (so ``us_bis_mieu``'s
    entities were fetched) while ``risk.py`` classified none of them — a hit
    card with the topics visible in its summary line and no risk chip at all.
    """
    published = {t for t in _PUBLISHED_TARGET_TOPICS if t.startswith("export")}
    assert published == set(_KNOWN_EXPORT_TOPICS)


def test_every_risk_topic_is_classified_or_allowlisted() -> None:
    """The canary that closes the whole class, not one family.

    Every topic in ``_RISK_TOPICS`` must either map to a signal family or
    appear on the explicit informational allowlist in ``risk.py``. "We fetch
    it but don't understand it" is thereby a build failure, not something
    discovered by reading a blog post: it would have caught both
    ``sanction.control`` (Phase 98) and the export family (Phase 118), and it
    catches the next one without anyone having to notice.

    When OpenSanctions publishes a new target topic: add it to
    ``_RISK_TOPICS`` and ``_PUBLISHED_TARGET_TOPICS``, then either classify
    it into a family (give it a signal) or add it to
    ``risk._INFORMATIONAL_TOPICS`` — a decision, recorded in code, either way.
    """
    classified = (
        set(_KNOWN_SANCTION_TOPICS)
        | set(_KNOWN_EXPORT_TOPICS)
        | set(_DEBARMENT_TOPICS)
        # _PEP_TOPICS includes role.spouse / role.family, which OpenSanctions
        # does not publish as target topics — intersect so the assertion
        # covers exactly the published surface.
        | (set(_PEP_TOPICS) & _PUBLISHED_TARGET_TOPICS)
    )
    overlap = classified & _INFORMATIONAL_TOPICS
    assert not overlap, f"topics both classified and allowlisted: {sorted(overlap)}"
    assert set(_RISK_TOPICS) == classified | _INFORMATIONAL_TOPICS


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/default?q=rosneft&schema=LegalEntity&limit=10&{_TOPIC_PARAMS}",
        match_headers={"Authorization": "ApiKey test-key"},
        json={
            "results": [
                {
                    "id": "NK-rosneft",
                    "schema": "Company",
                    "caption": "Rosneft Oil Company",
                    "properties": {
                        "leiCode": ["253400VC22A0KFSOPB29"],
                        "wikidataId": ["Q219617"],
                    },
                    "datasets": ["eu_fsf", "us_ofac_sdn"],
                    "topics": ["sanction"],
                }
            ]
        },
    )

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("rosneft", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Rosneft Oil Company"
    assert hit.hit_id == "NK-rosneft"
    assert hit.identifiers["lei"] == "253400VC22A0KFSOPB29"
    assert hit.identifiers["wikidata_qid"] == "Q219617"
    assert "sanction" in hit.summary


async def test_person_search(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/search/default?q=putin&schema=Person&limit=10&{_TOPIC_PARAMS}",
        json={
            "results": [
                {
                    "id": "NK-putin",
                    "schema": "Person",
                    "caption": "Vladimir Putin",
                    "properties": {"wikidataId": ["Q7747"]},
                    "topics": ["role.pep", "sanction"],
                    "datasets": ["eu_fsf"],
                }
            ]
        },
    )

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("putin", SearchKind.PERSON)
    assert len(hits) == 1
    assert hits[0].identifiers["wikidata_qid"] == "Q7747"


async def test_fetch_entity_bundle(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/NK-rosneft",
        json={
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {"name": ["Rosneft Oil Company"]},
        },
    )

    adapter = OpenSanctionsAdapter()
    bundle = await adapter.fetch("NK-rosneft")
    assert bundle["entity_id"] == "NK-rosneft"
    assert bundle["entity"]["caption"] == "Rosneft Oil Company"


async def test_stub_path_when_no_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True
