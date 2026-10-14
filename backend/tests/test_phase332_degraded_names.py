"""Phase 332 — a degraded screen is named by what it is, never by a raw id.

Found in the Phase 320 post-deploy check (10 Oct 2026): when the
related-party screens hit the per-lookup limit (Phase 279), the evidence
packet's gap read "Screening against opencheck did not fully run because
OpenCheck's per-lookup limit on related parties was reached", on five of
the six curated examples. The degradation's ``source_id`` is "opencheck",
which ``REGISTRY`` cannot name, and the packet fell back to the slug. The
PDF/Markdown "Screening limitations" block printed **opencheck** and
**icij** the same way, and the batch row listed "opencheck opencheck".
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck import cross_check, icij_check, openaleph_check
from opencheck.config import get_settings
from opencheck.cross_check import assess_cross_source_names
from opencheck.icij_check import _RECONCILE_URL, assess_icij_names
from opencheck.narrative.packet import build_evidence_packet
from opencheck.reporting.html_report import _degraded_block
from opencheck.reporting.markdown_report import _degraded_lines
from opencheck.risk import (
    DEGRADED_CHECK_LABELS,
    DEGRADED_TIMEOUT,
    DEGRADED_TRUNCATED,
    DEGRADED_UPSTREAM_ERROR,
    OPENCHECK_SOURCE_ID,
    DegradedSource,
    degraded_screen_lead,
    degraded_source_name,
)
from opencheck.sources import _UNREGISTERED_NAMES, REGISTRY, SearchKind, SourceHit

LEI = "5493001KJTIIGC8Y1R12"
_FRONTEND_VOCAB = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "vocab.ts"


# ---------------------------------------------------------------------
# Bundles: thirty former directors and one current — past both caps
# ---------------------------------------------------------------------


def _person(sid: str, name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "person",
        "recordDetails": {
            "personType": "knownPerson",
            "names": [{"type": "individual", "fullName": name}],
        },
    }


def _rel(sid: str, subject: str, party: str, *, ended: bool) -> dict[str, Any]:
    interest: dict[str, Any] = {"type": "boardMember", "directOrIndirect": "direct"}
    if ended:
        interest["endDate"] = "2020-01-01"
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "relationship",
        "recordStatus": "closed" if ended else "new",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": party,
            "interests": [interest],
        },
    }


def _big_bundle() -> list[dict[str, Any]]:
    bods: list[dict[str, Any]] = [
        {
            "statementId": "s",
            "recordId": "s",
            "recordType": "entity",
            "recordDetails": {
                "entityType": {"type": "registeredEntity"},
                "name": "Subject Co",
                "identifiers": [{"scheme": "XI-LEI", "id": LEI}],
            },
        }
    ]
    for i in range(30):
        bods.append(_person(f"f{i}", f"Former Director{i} Smithson"))
        bods.append(_rel(f"rf{i}", "s", f"f{i}", ended=True))
    bods.append(_person("wd", "Katherine Baicker"))
    bods.append(_rel("rw", "s", "wd", ended=False))
    return bods


class _Empty:
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []


@pytest.fixture
def _live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _report(degraded: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "lei": LEI,
        "legal_name": "Subject Co",
        "bods": [],
        "risk_signals": [],
        "degraded_sources": degraded,
    }


def _gaps(degraded: list[dict[str, Any]]) -> list[str]:
    # Only the degradation gaps: an empty bundle also has no disclosed owner.
    return [
        g.statement
        for g in build_evidence_packet(_report(degraded)).gaps
        if not g.statement.startswith("No beneficial owner")
    ]


def _raw_unregistered_ids(text: str) -> list[str]:
    """Source-id-shaped words in ``text`` that no adapter owns.

    Every id a degradation can carry that is not in ``REGISTRY``, matched
    case-sensitively as a whole word — "OpenCheck's" is the brand, "opencheck"
    is the slug.
    """
    ids = set(_UNREGISTERED_NAMES) | {"made_up_source"}
    return [i for i in sorted(ids) if re.search(rf"(?<![\w.-]){re.escape(i)}(?![\w-])", text)]


# ---------------------------------------------------------------------
# The real screens, capped, through the packet
# ---------------------------------------------------------------------


async def test_capped_screens_read_by_their_own_names_in_the_packet(
    monkeypatch, httpx_mock, _live
) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _Empty())
    monkeypatch.setitem(REGISTRY, "everypolitician", _Empty())
    httpx_mock.add_callback(
        lambda request: httpx.Response(
            200,
            json={
                k: {"result": []}
                for k in json.loads(
                    dict(httpx.QueryParams(request.content.decode())).get("queries") or "{}"
                )
                or {}
            },
        ),
        url=_RECONCILE_URL,
        is_reusable=True,
    )
    degraded: list[DegradedSource] = []
    bundle = _big_bundle()
    await assess_cross_source_names(bundle, degraded=degraded, subject_lei=LEI)
    await assess_icij_names(bundle, degraded=degraded, subject_lei=LEI)
    capped = [d.to_dict() for d in degraded if d.reason == DEGRADED_TRUNCATED]
    assert {d["check"] for d in capped} == {"cross_source_names", "icij_offshore_leaks"}
    assert all(d["source_id"] == OPENCHECK_SOURCE_ID for d in capped)

    monkeypatch.undo()  # the packet reads REGISTRY's names
    gaps = _gaps(capped)
    assert len(gaps) == 2
    text = " ".join(gaps)
    # The ticket's wording, led by the check.
    assert gaps[0].startswith(
        "Related-party sanctions and PEP screening covered the 25 highest-ranked of "
        "31 related parties"
    )
    assert gaps[1].startswith("Offshore Leaks screening covered the 30 highest-ranked of 31")
    assert "Screening against" not in text
    assert "not a clean screen of them" in text
    assert _raw_unregistered_ids(text) == []
    # Counts only: no related party is named.
    assert "Smithson" not in text and "Baicker" not in text


# ---------------------------------------------------------------------
# Every shape a degradation can take, through the packet
# ---------------------------------------------------------------------


def _every_degradation() -> list[dict[str, Any]]:
    """One record per source id a degradation can name, per reason shape."""
    out: list[dict[str, Any]] = []
    for sid in sorted(REGISTRY):
        out.append(
            DegradedSource(sid, "source_fetch", [], "Fetch failed.", DEGRADED_UPSTREAM_ERROR).to_dict()
        )
    for check in sorted(DEGRADED_CHECK_LABELS):
        for reason in (DEGRADED_TRUNCATED, DEGRADED_UPSTREAM_ERROR):
            out.append(
                DegradedSource(
                    OPENCHECK_SOURCE_ID, check, ["RELATED_PEP"], "Counts only.", reason
                ).to_dict()
            )
    out.append(
        DegradedSource("icij", "icij_offshore_leaks", ["OFFSHORE_LEAKS"], "", DEGRADED_TIMEOUT).to_dict()
    )
    # A check no label covers, and an id nothing names.
    out.append(DegradedSource(OPENCHECK_SOURCE_ID, "brand_new_check", [], "", DEGRADED_TIMEOUT).to_dict())
    out.append(DegradedSource("made_up_source", "brand_new_check", [], "", DEGRADED_TIMEOUT).to_dict())
    out.append({"check": "cross_source_names", "reason": DEGRADED_TIMEOUT})
    return out


def test_no_packet_gap_names_a_source_id_the_registry_lacks() -> None:
    gaps = _gaps(_every_degradation())
    offenders = [(g, _raw_unregistered_ids(g)) for g in gaps if _raw_unregistered_ids(g)]
    assert offenders == []


def test_an_upstream_keeps_screening_against_its_name() -> None:
    [gap] = _gaps(
        [
            DegradedSource(
                "opensanctions", "cross_source_names", ["RELATED_SANCTIONED"],
                "Search failed for 2 of 2 related-party name(s).", DEGRADED_TIMEOUT,
            ).to_dict()
        ]
    )
    assert gap.startswith("Screening against OpenSanctions did not fully run because")
    assert "not a clean screen." in gap


def test_icij_reads_as_icij_offshore_leaks() -> None:
    [gap] = _gaps(
        [DegradedSource("icij", "icij_offshore_leaks", ["OFFSHORE_LEAKS"], "", DEGRADED_TIMEOUT).to_dict()]
    )
    assert gap.startswith("Screening against ICIJ Offshore Leaks did not fully run")
    # No detail means no doubled space where it would have been.
    assert "  " not in gap


def test_an_opencheck_failure_that_is_not_the_cap_leads_with_the_check() -> None:
    [gap] = _gaps(
        [
            DegradedSource(
                OPENCHECK_SOURCE_ID, "cross_source_names", ["RELATED_PEP"],
                "Screening errored for 2 of 30 related-party name(s) before reaching "
                "the upstream sources.", DEGRADED_UPSTREAM_ERROR,
            ).to_dict()
        ]
    )
    assert gap.startswith(
        "Related-party sanctions and PEP screening did not fully run because the "
        "upstream service errored."
    )


def test_unknown_ids_and_checks_fall_back_to_true_words() -> None:
    assert degraded_source_name({"source_id": "made_up_source"}) == "an upstream source"
    assert degraded_source_name({}) == "an upstream source"
    assert degraded_screen_lead({"source_id": "opencheck", "check": "nope"}) == "An OpenCheck screen"


# ---------------------------------------------------------------------
# The labels belong to the screens that emit the records
# ---------------------------------------------------------------------


def test_every_derived_screen_has_a_label() -> None:
    for module in (cross_check, icij_check, openaleph_check):
        assert module.CHECK_NAME in DEGRADED_CHECK_LABELS, module.__name__


# ---------------------------------------------------------------------
# The other surfaces
# ---------------------------------------------------------------------


def _capped_and_icij() -> dict[str, Any]:
    return _report(
        [
            DegradedSource(
                OPENCHECK_SOURCE_ID, "cross_source_names", ["RELATED_PEP"],
                "Related-party sanctions and PEP screening covered the 25 highest-ranked "
                "of 96 related parties; 71 were not screened (71 former).",
                DEGRADED_TRUNCATED,
            ).to_dict(),
            DegradedSource("icij", "icij_offshore_leaks", ["OFFSHORE_LEAKS"], "x.", DEGRADED_TIMEOUT).to_dict(),
        ]
    )


def test_markdown_limitations_name_opencheck_and_icij() -> None:
    text = "\n".join(_degraded_lines(_capped_and_icij()))
    assert "**OpenCheck** — Related-party sanctions and PEP screening covered" in text
    assert "**ICIJ Offshore Leaks**" in text
    assert _raw_unregistered_ids(text) == []


def test_html_limitations_name_opencheck_and_icij() -> None:
    html = _degraded_block(_capped_and_icij())
    assert "<strong>OpenCheck</strong>" in html
    assert "<strong>ICIJ Offshore Leaks</strong>" in html
    assert _raw_unregistered_ids(html) == []


def test_frontend_names_the_same_unregistered_ids() -> None:
    src = _FRONTEND_VOCAB.read_text(encoding="utf-8")
    m = re.search(
        r"export const UNREGISTERED_NAMES: Record<string, string> = \{(.*?)\};", src, re.S
    )
    assert m, "UNREGISTERED_NAMES not found in frontend/src/lib/vocab.ts"
    pairs = dict(re.findall(r'^\s*(\w+):\s*"([^"]+)",?\s*$', m.group(1), re.M))
    assert "opencheck" in pairs
    # A subset, word for word: the frontend may not yet name every id the
    # backend does (icij), but where it names one it must agree.
    assert {k: _UNREGISTERED_NAMES.get(k) for k in pairs} == pairs
