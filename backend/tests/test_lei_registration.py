"""Phase 242 — the LEI record's own registration status on every surface.

Found by the Opus 5.5 check (DQ-4): American Foreign Policy Council
(``549300W96W2VKSMVDF81``) has been LAPSED since its renewal fell due on
19 Oct 2017, and the lookup, MCP, PDF and frontend said nothing — only the SEO
entity page did. The record shapes below are GLEIF's own, as read from the
live API on 24 Sept 2026.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import lei_registration as lr
from opencheck.bods import liveness
from opencheck.bods.mapper import map_gleif
from opencheck.config import get_settings
from opencheck.entity_pages import EntityRow, gleif_record_from_row
from opencheck.mcp.shaping import shape_batch_row, shape_lookup
from opencheck.reporting.html_report import _identifiers as html_identifiers
from opencheck.reporting.markdown_report import _identifiers as md_identifiers
from opencheck.subject_profile import build_subject_profile

AFPC = "549300W96W2VKSMVDF81"

#: GLEIF's record for AFPC, trimmed to what the mapper and this phase read.
AFPC_RECORD = {
    "type": "lei-records",
    "id": AFPC,
    "attributes": {
        "lei": AFPC,
        "entity": {
            "legalName": {"name": "American Foreign Policy Council", "language": "en"},
            "jurisdiction": "US-DC",
            "status": "ACTIVE",
            "registeredAt": {"id": "RA000601", "other": None},
            "registeredAs": "L21249",
            "legalAddress": {
                "addressLines": ["509 C Street NE"],
                "city": "Washington",
                "region": "US-DC",
                "country": "US",
                "postalCode": "20002",
            },
        },
        "registration": {
            "initialRegistrationDate": "2016-10-21T01:57:00Z",
            "lastUpdateDate": "2026-04-08T20:16:13Z",
            "status": "LAPSED",
            "nextRenewalDate": "2017-10-19T17:57:00Z",
            "managingLou": "5493001KJTIIGC8Y1R12",
            "corroborationLevel": "FULLY_CORROBORATED",
        },
    },
}


def _record(status: str, **reg) -> dict:
    rec = copy.deepcopy(AFPC_RECORD)
    rec["attributes"]["registration"]["status"] = status
    rec["attributes"]["registration"].update(reg)
    return rec


# --- reading GLEIF's block ------------------------------------------------------


def test_a_lapsed_lei_is_dated_by_the_renewal_it_missed() -> None:
    reg = lr.from_gleif_record(AFPC_RECORD)
    assert reg["status"] == "LAPSED" and reg["label"] == "Lapsed" and reg["flag"] is True
    assert reg["since"] == "2017-10-19"
    assert reg["next_renewal_date"] == "2017-10-19"
    assert reg["last_update_date"] == "2026-04-08"
    assert reg["initial_registration_date"] == "2016-10-21"
    assert reg["managing_lou"] == "5493001KJTIIGC8Y1R12"
    assert reg["source_id"] == "gleif"
    assert "19 Oct 2017" in reg["sentence"]
    assert reg["sentence"].endswith(lr.NOT_ENTITY_STATUS)


def test_last_update_date_is_never_called_a_validation() -> None:
    """GLEIF edited AFPC's record in 2026, nine years after it lapsed: the
    date is carried as a record update, never as a validation."""
    sentence = lr.from_gleif_record(AFPC_RECORD)["sentence"]
    assert "validat" not in sentence.lower()
    assert "last updated the record on 8 Apr 2026" in sentence


def test_an_issued_lei_is_stated_but_not_flagged() -> None:
    reg = lr.from_gleif_record(_record("ISSUED", nextRenewalDate="2027-03-21T00:00:00Z"))
    assert reg["flag"] is False and reg["since"] is None
    assert lr.short_line(reg) == "Issued — renews 21 Mar 2027"
    assert lr.NOT_ENTITY_STATUS not in reg["sentence"]


@pytest.mark.parametrize(
    "status", ["RETIRED", "MERGED", "ANNULLED", "DUPLICATE", "PENDING_TRANSFER", "PENDING_ARCHIVAL"]
)
def test_only_a_lapse_gets_a_since_date(status: str) -> None:
    """GLEIF publishes no date on which a retirement or merger took effect —
    ``lastUpdateDate`` is only when it last edited the record."""
    reg = lr.from_gleif_record(_record(status))
    assert reg["flag"] is True and reg["since"] is None
    assert reg["sentence"].endswith(lr.NOT_ENTITY_STATUS)
    assert lr.short_line(reg) == lr.LABELS[status]


def test_an_unknown_status_is_carried_not_guessed() -> None:
    reg = lr.from_gleif_record(_record("SOMETHING_NEW"))
    assert reg["status"] == "SOMETHING_NEW" and reg["label"] == "Something new"
    assert reg["flag"] is True


@pytest.mark.parametrize("record", [None, {}, {"attributes": {}}, {"attributes": {"registration": {}}}])
def test_no_registration_block_is_none_never_issued(record) -> None:
    assert lr.from_gleif_record(record) is None


@pytest.mark.parametrize("status", sorted(lr.LABELS))
def test_no_sentence_passes_judgement(status: str) -> None:
    sentence = lr.from_gleif_record(_record(status))["sentence"].lower()
    for word in lr.BANNED_WORDS:
        assert word not in sentence, (status, word)


def test_the_mirror_record_carries_the_same_block() -> None:
    """The Golden Copy mirror serves the anchor on production (Phase 179);
    its record must yield the same reading as the live API's."""
    row = EntityRow(
        lei=AFPC, name="American Foreign Policy Council", slug="american-foreign-policy-council",
        entity_status="ACTIVE", registration_status="LAPSED", jurisdiction="US-DC",
        legal_form=None, city="Washington", region="US-DC", country="US",
        first_registered="2016-10-21T01:57:00Z", last_updated="2026-04-08T20:16:13Z",
        successor_lei=None, direct_parent_lei=None, ultimate_parent_lei=None,
        detail={"nextRenewalDate": "2017-10-19T17:57:00Z", "managingLou": "5493001KJTIIGC8Y1R12"},
    )
    assert lr.from_gleif_record(gleif_record_from_row(row)) == lr.from_gleif_record(AFPC_RECORD)


# --- the profile: beside register status, never instead of it ----------------------


def _afpc_bods() -> list[dict]:
    return list(map_gleif({"source_id": "gleif", "lei": AFPC, "record": AFPC_RECORD}))


def test_a_lapsed_lei_leaves_register_status_live() -> None:
    """The ticket's "do not change liveness": entity.status ACTIVE stays live."""
    reg = lr.from_gleif_record(AFPC_RECORD)
    profile = build_subject_profile(AFPC, _afpc_bods(), lei_registration=reg)
    assert profile["register_status"]["liveness"] == liveness.LIVE
    assert profile["lei_registration"] == reg
    assert build_subject_profile(AFPC, _afpc_bods())["lei_registration"] is None


# --- MCP ----------------------------------------------------------------------------


class _Payload:
    def __init__(self, reg: dict | None) -> None:
        self.lei = AFPC
        self.legal_name = "American Foreign Policy Council"
        self.jurisdiction = "US-DC"
        self.hits = []
        self.errors = []
        self.bods = _afpc_bods()
        self.risk_signals = []
        self.derived_identifiers = {}
        self.license_notices = []
        self.degraded_sources = []
        self.sources_applicable = []
        self.verdict = None
        self.subject_profile = build_subject_profile(AFPC, self.bods, lei_registration=reg)


def test_mcp_says_a_lapse_up_front_and_carries_the_profile() -> None:
    out = shape_lookup(_Payload(lr.from_gleif_record(AFPC_RECORD)))
    assert out["profile"]["lei_registration"]["status"] == "LAPSED"
    assert (
        "LEI registration (GLEIF): Lapsed — renewal was due 19 Oct 2017 — the status of "
        "the LEI record, not of the company." in out["summary"]
    )
    assert out["summary"].index("LEI registration") < out["summary"].index("Risk signals")
    assert "LAPSED" not in str(out["risk_signals"])


def test_mcp_summary_is_silent_on_an_issued_lei() -> None:
    reg = lr.from_gleif_record(_record("ISSUED", nextRenewalDate="2027-03-21T00:00:00Z"))
    out = shape_lookup(_Payload(reg))
    assert "LEI registration" not in out["summary"]
    assert out["profile"]["lei_registration"]["status"] == "ISSUED"


def test_batch_row_carries_status_and_date() -> None:
    row = shape_batch_row(_Payload(lr.from_gleif_record(AFPC_RECORD)))
    assert row["lei_registration"] == {"status": "LAPSED", "since": "2017-10-19"}
    assert row["register_status"]["liveness"] == liveness.LIVE
    assert shape_batch_row(_Payload(None))["lei_registration"] is None


# --- the report ---------------------------------------------------------------------


def _report(reg: dict | None) -> dict:
    return {"lei": AFPC, "subject_profile": {"lei_registration": reg}}


def test_both_reports_state_the_lapse_with_its_date() -> None:
    reg = lr.from_gleif_record(AFPC_RECORD)
    md = "\n".join(md_identifiers(_report(reg), None))
    assert "| LEI registration (GLEIF) | GLEIF records this LEI as lapsed" in md
    assert "19 Oct 2017" in md and lr.NOT_ENTITY_STATUS in md
    html = html_identifiers(_report(reg), None)
    # A sentence, so set in the body face — not the identifiers' monospace.
    assert '<th scope="row">LEI registration (GLEIF)</th><td>GLEIF records' in html


def test_reports_say_issued_briefly_and_nothing_when_unknown() -> None:
    reg = lr.from_gleif_record(_record("ISSUED", nextRenewalDate="2027-03-21T00:00:00Z"))
    assert "| LEI registration (GLEIF) | Issued — renews 21 Mar 2027 |" in "\n".join(
        md_identifiers(_report(reg), None)
    )
    assert "LEI registration" not in "\n".join(md_identifiers(_report(None), None))
    assert "LEI registration" not in html_identifiers({"lei": AFPC}, None)


# --- the pipeline -------------------------------------------------------------------


@pytest.fixture
def afpc_client(monkeypatch, tmp_path: Path):
    """The offline pipeline with GLEIF answering AFPC's live record."""
    import opencheck.sources.climatetrace as _ct
    from opencheck.app import app
    from opencheck.sources import REGISTRY

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()

    async def _fetch(lei: str) -> dict:
        return {"source_id": "gleif", "lei": lei, "record": copy.deepcopy(AFPC_RECORD),
                "is_stub": False}

    monkeypatch.setattr(REGISTRY["gleif"], "fetch", _fetch)
    yield TestClient(app)
    get_settings.cache_clear()


def test_the_pipeline_freezes_the_status_into_subject_profile(afpc_client) -> None:
    from tests.test_lookup_pipeline import _stream_body, _stream_events

    events = _stream_events(_stream_body(afpc_client, AFPC))
    profile = dict(events)["subject_profile"]["profile"]
    assert profile["lei_registration"]["status"] == "LAPSED"
    assert profile["lei_registration"]["since"] == "2017-10-19"
    assert profile["register_status"]["liveness"] == liveness.LIVE

    sync = afpc_client.get("/lookup", params={"lei": AFPC}).json()
    assert sync["subject_profile"]["lei_registration"] == profile["lei_registration"]


def test_the_lei_is_listed_once_in_the_identifiers_table() -> None:
    """Found while checking this phase's PDF: the derived identifiers carry
    the LEI too, so the table printed it twice, either side of the new row."""
    report = {**_report(lr.from_gleif_record(AFPC_RECORD)), "derived_identifiers": {"lei": AFPC, "us_dc": "L21249"}}
    assert "\n".join(md_identifiers(report, None)).count(f"`{AFPC}`") == 1
    assert html_identifiers(report, None).count(AFPC) == 1
