"""Phase 316 — the weekly BODS quality sweep, and the defects its first run found.

The sweep (``opencheck/bods_quality.py``, ``scripts/bods_quality.py``,
``.github/workflows/bods-quality.yml``) reads production and checks every
statement. Its first run against production on 9 Oct 2026 found four live
defects, fixed in this phase and pinned at the bottom of this file:
``country`` as a bare code on ANAF and Firmenbuch addresses, Firmenbuch
dates of birth as ``YYYYMMDD``, ΓΕΜΗ's future term expiries published as
``endDate``, and EITI SOE asserting beneficial ownership by a state body.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

import opencheck.bods as bods_pkg
from opencheck import bods_quality as bq
from opencheck import provenance
from opencheck.bods.statements import OFFICIAL_REGISTER_SOURCES
from opencheck.provenance import Provenance

TODAY = "2026-10-09"


def _stmt(**over: Any) -> dict[str, Any]:
    st: dict[str, Any] = {
        "statementId": "s1",
        "recordType": "entity",
        "statementDate": "2026-09-01",
        "publicationDetails": {"publicationDate": TODAY},
        "source": {
            "type": ["officialRegister"],
            "opencheckSourceId": "companies_house",
            "retrievedAt": "2026-10-09T08:00:00Z",
        },
        "recordDetails": {},
    }
    st.update(over)
    return st


def _checks(findings: list[bq.Finding]) -> list[str]:
    return sorted(f.check for f in findings)


def _run(statements: list[dict[str, Any]], **kw: Any) -> list[bq.Finding]:
    kw.setdefault("official_registers", OFFICIAL_REGISTER_SOURCES)
    return bq.check_statements(statements, today=TODAY, **kw)


# ---------------------------------------------------------------------------
# The rules
# ---------------------------------------------------------------------------


class TestDateRules:
    def test_a_clean_statement_has_no_findings(self):
        assert _run([_stmt()], liveness={"companies_house": "live"}) == []

    def test_dates_after_the_run_fail(self):
        st = _stmt(statementDate="2026-10-10", recordDetails={
            "foundingDate": "2027-01-01",
            "interests": [{"type": "shareholding", "endDate": "2028-10-27"}],
        })
        found = _run([st])
        assert _checks(found).count("date_in_future") == 3
        assert all(f.severity == bq.FAIL for f in found if f.check == "date_in_future")

    def test_claim_or_download_after_publication_fails(self):
        st = _stmt(
            statementDate="2026-10-09",
            publicationDetails={"publicationDate": "2026-10-08"},
            source={"type": ["officialRegister"], "opencheckSourceId": "companies_house",
                    "retrievedAt": "2026-10-09T01:00:00Z"},
        )
        checks = _checks(_run([st]))
        assert "statement_after_publication" in checks
        assert "retrieved_after_publication" in checks

    def test_an_interest_ending_before_it_starts_fails(self):
        st = _stmt(recordType="relationship", recordStatus="closed", recordDetails={
            "interests": [{"type": "shareholding", "startDate": "2020-01-01", "endDate": "2019-01-01"}],
        })
        assert "start_after_end" in _checks(_run([st]))

    def test_a_bulk_source_dated_today_fails_only_under_bulk_liveness(self):
        st = _stmt(statementDate=TODAY)
        assert "bulk_dated_today" in _checks(_run([st], liveness={"companies_house": "snapshot"}))
        assert "bulk_dated_today" in _checks(_run([st], liveness={"companies_house": "curated"}))
        assert "bulk_dated_today" not in _checks(_run([st], liveness={"companies_house": "live"}))


class TestProvenanceRules:
    def test_cut_as_retrieval_needs_a_snapshot_with_no_declared_cut(self):
        st = _stmt(statementDate="2026-09-02", source={
            "type": ["officialRegister"], "opencheckSourceId": "companies_house",
            "retrievedAt": "2026-09-02T00:00:00Z",
        })
        live = {"companies_house": "snapshot"}
        assert "cut_as_retrieval" in _checks(_run([st], liveness=live, no_cut=frozenset({"companies_house"})))
        # A date-only build beside a separately declared cut is not the defect.
        assert "cut_as_retrieval" not in _checks(_run([st], liveness=live))

    def test_a_read_source_without_retrieved_at_warns(self):
        st = _stmt(source={"type": ["officialRegister"], "opencheckSourceId": "companies_house"})
        found = _run([st], liveness={"companies_house": "live"})
        assert _checks(found) == ["no_retrieved_at"]
        assert found[0].severity == bq.WARN
        # A stub claims no read, so nothing is owed.
        assert _run([st], liveness={"companies_house": "stub"}) == []

    def test_the_publisher_verbatim_source_is_exempt(self):
        st = _stmt(source={"type": ["thirdParty"], "opencheckSourceId": "meip"})
        assert _run([st], liveness={"meip": "snapshot"}) == []

    def test_a_source_block_without_an_opencheck_id_warns(self):
        st = _stmt(source={"type": ["officialRegister"], "description": "GLEIF",
                           "retrievedAt": "2026-10-09T08:00:00Z"})
        assert _checks(_run([st])) == ["no_source_id"]

    def test_source_type_must_match_the_official_register_set(self):
        st = _stmt(source={"type": ["officialRegister"], "opencheckSourceId": "opencorporates",
                           "retrievedAt": "2026-10-09T08:00:00Z"})
        assert _checks(_run([st])) == ["source_type"]
        st["source"]["type"] = ["thirdParty"]
        assert _run([st]) == []

    def test_an_ended_relationship_left_open_warns(self):
        st = _stmt(recordType="relationship", recordStatus="new", recordDetails={
            "interests": [{"type": "shareholding", "endDate": "2026-03-13"}],
        })
        assert _checks(_run([st])) == ["ended_not_closed"]
        st["recordStatus"] = "closed"
        assert _run([st]) == []
        # One interest still running: the relationship has not ended.
        st["recordStatus"] = "new"
        st["recordDetails"]["interests"].append({"type": "votingRights"})
        assert _run([st]) == []


class TestSchema:
    def test_lib_cove_bods_errors_are_failures_named_by_source(self):
        pytest.importorskip("libcovebods")
        bad = _stmt(recordDetails={
            "isComponent": False, "entityType": {"type": "registeredEntity"},
            "name": "X", "addresses": [{"type": "registered", "address": "A", "country": "RO"}],
        })
        bad["source"]["opencheckSourceId"] = "anaf_romania"
        found = bq.validate_schema([bad])
        assert found and all(f.severity == bq.FAIL for f in found)
        assert any(f.check == "schema" and f.source_id == "anaf_romania" for f in found)

    def test_nothing_to_validate_is_no_finding(self):
        assert bq.validate_schema([]) == []


class TestProfile:
    def test_dating_shape_per_source(self):
        a = _stmt(statementDate="2026-10-09")  # dated by its retrieval day
        b = _stmt(statementId="s2", statementDate="2026-09-01")
        c = _stmt(statementId="s3", source={"opencheckSourceId": "zefix"})
        prof = bq.dating_profile([a, b, c])
        assert prof["companies_house"] == {
            "statements": 2, "with_retrieved_at": 2, "dated_by_retrieval": 1,
            "distinct_statement_dates": 2,
        }
        assert prof["zefix"]["with_retrieved_at"] == 0
        merged = bq.merge_profiles([prof, prof])
        assert merged["companies_house"]["statements"] == 4


# ---------------------------------------------------------------------------
# Subjects, the run, the diff
# ---------------------------------------------------------------------------


def test_deepen_subjects_take_only_what_deepen_can_address():
    from opencheck.sources.probes import PROBES

    subjects = {s.source_id: s for s in bq.deepen_subjects(PROBES)}
    assert subjects["prh"].hit_id == "1852302-9"
    assert subjects["eiti_assessment"].hit_id == "21380068P1DRHMJ8KU70"  # fetch_by_lei
    assert "bce_belgium" not in subjects  # inactive
    assert "openaleph" not in subjects  # its fetch takes an entity id, not the LEI
    assert "opensanctions" not in subjects  # a search probe
    assert subjects["asp_moldova"].expect_liveness == frozenset({"snapshot"})


class _Resp:
    def __init__(self, status: int, payload: Any = None, headers: dict | None = None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _Client:
    def __init__(self, routes: dict[str, list[_Resp]]):
        self.routes = routes
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None):
        self.calls.append((url, dict(params or {})))
        key = url.rsplit("/", 1)[-1]
        return self.routes[key].pop(0)


def test_the_run_reads_lookups_and_deepens_and_reports(monkeypatch):
    monkeypatch.setattr(bq, "validate_schema", lambda statements: [])
    lookup_payload = {
        "bods": [_stmt(statementDate=TODAY, source={
            "type": ["officialRegister"], "opencheckSourceId": "onrc_romania",
            "retrievedAt": "2026-10-09T05:00:00Z"})],
        "source_liveness": {"onrc_romania": {"liveness": "snapshot", "source_as_of": None}},
    }
    client = _Client({
        "lookup": [_Resp(429, headers={"Retry-After": "2"}), _Resp(200, lookup_payload)],
        "deepen": [_Resp(500, {"detail": "Internal server error: 'str' object has no attribute 'get'"})],
    })
    waits: list[float] = []
    report = bq.run(
        client=client, base_url="https://api.example",
        lookup_subjects=[{"lei": "LEI1", "name": "Subject"}],
        deepen=[bq.DeepenSubject("prh", "1852302-9", "Neste", frozenset())],
        official_registers=OFFICIAL_REGISTER_SOURCES,
        sleep=waits.append, today=TODAY, now=lambda: "2026-10-12T10:00:00Z",
    )
    assert waits == [2.0]  # honoured Retry-After, then retried
    assert client.calls[0][1] == {"lei": "LEI1", "refresh": "true"}
    assert client.calls[-1][1] == {"source": "prh", "hit_id": "1852302-9"}
    t = report["totals"]
    assert t["by_check"] == {"bulk_dated_today": 1, "deepen_failed": 1}
    assert bq.failed(report)
    deepen = report["subjects"][1]
    assert "object has no attribute" in deepen["error"]
    md = bq.render_markdown(report)
    assert "`deepen_failed` | `prh`" in md
    assert "No comparison available" in md


def test_the_diff_is_per_check_and_source_against_the_published_report():
    prev = {"generated_at": "2026-10-05T16:00:00Z", "totals": {"by_check_source": {
        "schema|anaf_romania": 2, "ended_not_closed|nz_companies": 40}}}
    report = {"subjects": [{"name": "x", "findings": [
        {"check": "ended_not_closed", "severity": "warn", "source_id": "nz_companies", "message": "m"},
        {"check": "deepen_failed", "severity": "fail", "source_id": "cro", "message": "m"},
    ]}]}
    d = bq.diff(report, prev)
    assert d["new"] == {"deepen_failed|cro": 1}
    assert d["resolved"] == {"schema|anaf_romania": 2}
    assert d["changed"] == {"ended_not_closed|nz_companies": [40, 1]}
    assert bq.diff(report, None) == {"available": False}


# ---------------------------------------------------------------------------
# The defects the first production run found
# ---------------------------------------------------------------------------


def _map(mapper: Any, bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.mapping_provenance(
        Provenance(liveness="live", retrieved_at=dt.datetime.now(dt.timezone.utc))
    ):
        return list(mapper(bundle))


def test_anaf_addresses_carry_a_country_object():
    out = _map(bods_pkg.map_anaf_romania, {"cui": "14399840", "record": {
        "date_generale": {"cui": 14399840, "denumire": "OMV PETROM SA", "data": "2026-10-01"},
        "adresa_sediu_social": {"sdenumire_Strada": "Str. Coralilor", "snumar_Strada": "22",
                                "sdenumire_Localitate": "Bucuresti"},
    }})
    for address in out[0]["recordDetails"].get("addresses") or []:
        assert address["country"] == {"name": "Romania", "code": "RO"}


def test_firmenbuch_country_object_and_compact_birth_dates():
    from tests.test_bods_firmenbuch import _bundle

    bundle = _bundle()
    for officer in bundle["extract"].get("officers") or []:
        officer["dob"] = "19700301"
    out = _map(bods_pkg.map_firmenbuch, bundle)
    entity = next(s for s in out if s["recordType"] == "entity")
    for address in entity["recordDetails"].get("addresses") or []:
        assert address["country"] == {"name": "Austria", "code": "AT"}
    births = {s["recordDetails"].get("birthDate") for s in out if s["recordType"] == "person"}
    assert births - {None} == {"1970-03-01"}
    pytest.importorskip("libcovebods")
    assert bq.validate_schema(out) == []


def test_gemi_future_term_expiry_is_not_an_end_date():
    from tests.test_gemi_greece import _FIXTURES, _bundle, _split

    _, _, rels = _split(_map(bods_pkg.map_gemi_greece, _bundle("ae_board")))
    expiries = {r["dtTo"][:10] for r in _FIXTURES["ae_board"]["persons"]}
    for rel in rels:
        for interest in rel["recordDetails"]["interests"]:
            assert "endDate" not in interest
            assert "term runs to" in interest.get("details", "")
    assert any(e in i.get("details", "") for r in rels
               for i in r["recordDetails"]["interests"] for e in expiries)


async def test_eiti_soe_state_control_asserts_no_beneficial_ownership(monkeypatch):
    from opencheck.sources import eiti_soe
    from tests import test_eiti_soe as soe_tests

    # The in-memory index test_eiti_soe.py points the adapter at.
    monkeypatch.setattr(eiti_soe, "_index", dict(soe_tests._FIXTURE))
    monkeypatch.setattr(eiti_soe, "get_settings", lambda: type("S", (), {"allow_live": False})())
    try:
        bundle = await eiti_soe.EitiSoeAdapter().fetch_by_lei(soe_tests._LEI_MATCH)
    finally:
        eiti_soe._reset_index_for_tests()
    out = _map(bods_pkg.map_eiti_soe, bundle)
    (rel,) = [s for s in out if s["recordType"] == "relationship"]
    assert rel["recordDetails"]["interests"][0]["beneficialOwnershipOrControl"] is False
    pytest.importorskip("libcovebods")
    assert bq.validate_schema(out) == []


def test_the_workflow_runs_the_sweep_and_publishes_the_data_slot():
    """The weekly Claude check reads the release this job publishes; if the
    asset names drift, it reads last week's report and says nothing."""
    from pathlib import Path

    wf = (Path(__file__).resolve().parents[2] / ".github/workflows/bods-quality.yml").read_text()
    assert "scripts/bods_quality.py" in wf
    assert "TAG=bods-quality-latest" in wf
    assert "bods-quality.json bods-quality.md bods-quality-history.json" in wf
    assert "continue-on-error: true" in wf  # report first, fail last
    for line in wf.splitlines():
        if line.strip().startswith("- uses:"):
            ref = line.split("@", 1)[1].split()[0]
            assert len(ref) == 40, f"action not pinned to a commit: {line.strip()}"
