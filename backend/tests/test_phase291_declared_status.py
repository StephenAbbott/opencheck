"""Phase 291 — GLEIF's entity status on an unmaintained LEI is ``declared``.

Bentcard Import LLP (``54930007FGRO3F0RZ382``): GLEIF entity status ACTIVE,
registration LAPSED since its renewal fell due on 5 Feb 2015, jurisdiction AZ.
Companies House dissolved the UK LLP (OC369315) on 19 Apr 2016. The profile
returned ``liveness: "live", raw: "ACTIVE", source_id: "gleif"`` — a 2015
declaration nobody has re-checked, shown as a live register status.

Rules pinned here:

* GLEIF's ``live`` becomes ``declared`` when the LEI is not maintained
  (anything but ISSUED and the two pending-transfer states); dated by the
  missed renewal, never by ``lastUpdateDate``.
* Never ``terminal`` on a lapse alone (Phase 242), and a GLEIF INACTIVE stays
  terminal.
* A source that reads the register — Companies House, OpenCorporates —
  outranks the declaration in either direction.
* An ISSUED LEI (Danske Bank) is unchanged.
* The BODS is untouched: ``declared`` is a profile class only.
"""

from __future__ import annotations

import copy

import pytest

from opencheck import lei_registration as lr
from opencheck.bods import liveness
from opencheck.bods.mapper import map_gleif
from opencheck.mcp.shaping import shape_batch_row, shape_lookup
from opencheck.bods.mapper import SOURCE_NAMES
from opencheck.subject_profile import DECLARED, build_subject_profile
from opencheck.watchlist import diff_snapshots

BENTCARD = "54930007FGRO3F0RZ382"
DANSKE = "MAES062Z21O4RZ2U7M96"

#: GLEIF's live record for Bentcard on 5 Oct 2026, trimmed to what is read.
BENTCARD_RECORD = {
    "type": "lei-records",
    "id": BENTCARD,
    "attributes": {
        "lei": BENTCARD,
        "entity": {
            "legalName": {"name": "BENTCARD IMPORT LLP", "language": "az"},
            "jurisdiction": "AZ",
            "legalForm": {"id": "8888", "other": "LIMITED LIABILITY PARTNERSHIP"},
            "status": "ACTIVE",
            "registeredAs": None,
            "registeredAt": {"id": "RA999999", "other": None},
            "legalAddress": {
                "addressLines": ["BAKIKHANOV ST.18"],
                "city": "BAKU",
                "country": "AZ",
                "postalCode": "AZ 1035",
            },
            "expiration": {"date": None, "reason": None},
        },
        "registration": {
            "initialRegistrationDate": "2014-02-07T01:18:00Z",
            "lastUpdateDate": "2023-07-31T17:11:36Z",
            "status": "LAPSED",
            "nextRenewalDate": "2015-02-05T09:24:00Z",
            "managingLou": "213800WAVVOPS85N2205",
            "corroborationLevel": "ENTITY_SUPPLIED_ONLY",
        },
    },
}

DANSKE_RECORD = {
    "type": "lei-records",
    "id": DANSKE,
    "attributes": {
        "lei": DANSKE,
        "entity": {
            "legalName": {"name": "Danske Bank A/S", "language": "da"},
            "jurisdiction": "DK",
            "status": "ACTIVE",
            "registeredAt": {"id": "RA000170", "other": None},
            "registeredAs": "61126228",
            "legalAddress": {
                "addressLines": ["Bernstorffsgade 40"],
                "city": "København V",
                "country": "DK",
                "postalCode": "1577",
            },
        },
        "registration": {
            "initialRegistrationDate": "2012-06-06T15:52:00Z",
            "lastUpdateDate": "2026-05-20T08:00:00Z",
            "status": "ISSUED",
            "nextRenewalDate": "2027-06-07T00:00:00Z",
            "managingLou": "5299000J2N45DDNE4Y28",
        },
    },
}

BENTCARD_SENTENCE = (
    "GLEIF holds the entity status as ACTIVE, last declared to the LEI issuer before "
    "5 Feb 2015; the LEI has not been renewed since, so no issuer has re-checked it. "
    "This is not a current register reading."
)


def _record(base: dict, *, status: str | None = None, entity_status: str | None = None) -> dict:
    rec = copy.deepcopy(base)
    if status is not None:
        rec["attributes"]["registration"]["status"] = status
    if entity_status is not None:
        rec["attributes"]["entity"]["status"] = entity_status
    return rec


def _bods(record: dict) -> list[dict]:
    return list(
        map_gleif({"source_id": "gleif", "lei": record["id"], "record": record})
    )


def _profile(record: dict, extra: list[dict] | None = None) -> dict:
    bods = _bods(record) + (extra or [])
    return build_subject_profile(
        record["id"], bods, lei_registration=lr.from_gleif_record(record)
    )


def _register_stmt(record: dict, source_id: str, liveness_class, raw: str, since=None) -> dict:
    """A second statement for the same entity (shares the LEI identifier),
    as a register adapter would emit it."""
    gleif = next(s for s in _bods(record) if s.get("recordType") == "entity")
    stmt = copy.deepcopy(gleif)
    stmt["statementId"] = f"{source_id}-{record['id']}"
    stmt["recordId"] = f"{source_id}-{record['id']}"
    stmt["source"] = {
        **(stmt.get("source") or {}),
        "description": SOURCE_NAMES[source_id],
        "opencheckSourceId": source_id,
    }
    stmt["annotations"] = []
    stmt["recordDetails"].pop("dissolutionDate", None)
    liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES[source_id],
        liveness=liveness_class,
        raw=raw,
        since=since,
    )
    return stmt


# --- the ticket's case ------------------------------------------------------------


def test_bentcard_shows_a_declared_status_dated_2015() -> None:
    status = _profile(BENTCARD_RECORD)["register_status"]
    assert status["liveness"] == DECLARED == "declared"
    assert status["since"] == "2015-02-05"
    assert status["raw"] == "ACTIVE"
    assert status["source_id"] == "gleif"
    assert status["lei_registration_status"] == "LAPSED"
    assert status["sentence"] == BENTCARD_SENTENCE


def test_the_date_is_the_missed_renewal_never_the_last_update() -> None:
    status = _profile(BENTCARD_RECORD)["register_status"]
    assert "2023" not in status["sentence"]  # lastUpdateDate 31 Jul 2023
    assert "validat" not in status["sentence"].lower()


def test_danske_with_an_issued_lei_is_unchanged() -> None:
    status = _profile(DANSKE_RECORD)["register_status"]
    assert status["liveness"] == liveness.LIVE
    assert status["source_id"] == "gleif"
    assert "sentence" not in status
    assert "lei_registration_status" not in status
    assert status["since"] is None


# --- which registration statuses count ----------------------------------------------


@pytest.mark.parametrize(
    "reg_status", ["RETIRED", "MERGED", "ANNULLED", "DUPLICATE", "CANCELLED", "PENDING_VALIDATION"]
)
def test_every_unmaintained_status_is_declared_without_a_date(reg_status: str) -> None:
    status = _profile(_record(BENTCARD_RECORD, status=reg_status))["register_status"]
    assert status["liveness"] == DECLARED
    # GLEIF publishes a "since" only for a lapse; nothing is estimated.
    assert status["since"] is None
    assert status["sentence"].startswith("GLEIF holds the entity status as ACTIVE")
    assert status["sentence"].endswith("This is not a current register reading.")


@pytest.mark.parametrize("reg_status", ["ISSUED", "PENDING_TRANSFER", "PENDING_ARCHIVAL"])
def test_a_maintained_lei_keeps_gleif_live(reg_status: str) -> None:
    status = _profile(_record(BENTCARD_RECORD, status=reg_status))["register_status"]
    assert status["liveness"] == liveness.LIVE


def test_no_registration_block_is_never_read_as_a_lapse() -> None:
    profile = build_subject_profile(BENTCARD, _bods(BENTCARD_RECORD), lei_registration=None)
    assert profile["register_status"]["liveness"] == liveness.LIVE
    assert lr.entity_status_is_declared(None) is False
    assert lr.entity_status_is_declared({"status": ""}) is False


@pytest.mark.parametrize("reg_status", list(lr.LABELS))
def test_no_declared_sentence_passes_judgement(reg_status: str) -> None:
    reg = lr.from_gleif_record(_record(BENTCARD_RECORD, status=reg_status))
    text = lr.declared_sentence("ACTIVE", reg).lower()
    for word in lr.BANNED_WORDS:
        assert word not in text, (reg_status, word)


# --- never terminal on a lapse; INACTIVE stays terminal -----------------------------


def test_a_lapse_alone_is_never_terminal() -> None:
    profile = _profile(BENTCARD_RECORD)
    assert profile["register_status"]["liveness"] != liveness.TERMINAL


def test_gleif_inactive_on_a_lapsed_lei_stays_terminal() -> None:
    status = _profile(_record(BENTCARD_RECORD, entity_status="INACTIVE"))["register_status"]
    assert status["liveness"] == liveness.TERMINAL
    assert "sentence" not in status


# --- the register wins, either way --------------------------------------------------


def test_a_register_saying_dissolved_outranks_the_declaration() -> None:
    ch = _register_stmt(
        BENTCARD_RECORD, "companies_house", liveness.TERMINAL, "dissolved", since="2016-04-19"
    )
    status = _profile(BENTCARD_RECORD, [ch])["register_status"]
    assert status["liveness"] == liveness.TERMINAL
    assert status["source_id"] == "companies_house"
    assert status["since"] == "2016-04-19"
    assert {"source_id": "gleif", "value": DECLARED} in status["other_values"]
    assert "sentence" not in status


def test_a_source_reading_the_register_as_live_outranks_the_declaration() -> None:
    oc = _register_stmt(BENTCARD_RECORD, "opencorporates", liveness.LIVE, "Active")
    status = _profile(BENTCARD_RECORD, [oc])["register_status"]
    assert status["liveness"] == liveness.LIVE
    assert status["source_id"] == "opencorporates"
    assert {"source_id": "gleif", "value": DECLARED} in status["other_values"]


# --- the BODS is untouched ----------------------------------------------------------


def test_the_gleif_statement_still_says_what_gleif_says() -> None:
    gleif = next(s for s in _bods(BENTCARD_RECORD) if s.get("recordType") == "entity")
    read = liveness.read_register_status(gleif)
    assert read["liveness"] == liveness.LIVE
    assert read["raw"] == "ACTIVE"


# --- MCP, batch ---------------------------------------------------------------------


class _Payload:
    def __init__(self, record: dict) -> None:
        self.lei = record["id"]
        self.legal_name = record["attributes"]["entity"]["legalName"]["name"]
        self.jurisdiction = record["attributes"]["entity"]["jurisdiction"]
        self.hits = []
        self.errors = []
        self.bods = _bods(record)
        self.risk_signals = []
        self.derived_identifiers = {}
        self.license_notices = []
        self.degraded_sources = []
        self.sources_applicable = []
        self.verdict = None
        self.subject_profile = build_subject_profile(
            self.lei, self.bods, lei_registration=lr.from_gleif_record(record)
        )


def test_mcp_summary_says_the_status_is_a_declaration() -> None:
    out = shape_lookup(_Payload(BENTCARD_RECORD))
    assert f"Register status: {BENTCARD_SENTENCE}" in out["summary"]
    assert out["summary"].index("Register status") < out["summary"].index("Risk signals")
    assert out["profile"]["register_status"]["liveness"] == DECLARED


def test_mcp_summary_is_silent_for_danske() -> None:
    out = shape_lookup(_Payload(DANSKE_RECORD))
    assert "Register status" not in out["summary"]


def test_batch_row_carries_declared_and_its_sentence() -> None:
    row = shape_batch_row(_Payload(BENTCARD_RECORD))
    assert row["register_status"] == {
        "liveness": DECLARED,
        "since": "2015-02-05",
        "raw": "ACTIVE",
        "source_id": "gleif",
        "sentence": BENTCARD_SENTENCE,
    }
    assert shape_batch_row(_Payload(DANSKE_RECORD))["register_status"] == {
        "liveness": liveness.LIVE,
        "raw": "ACTIVE",
        "source_id": "gleif",
    }


# --- watchlist ----------------------------------------------------------------------


def _snap(liveness_class: str) -> dict:
    return {"register_status": {"liveness": liveness_class, "source_id": "gleif"}}


def test_watchlist_does_not_report_the_reclassification_as_news() -> None:
    """live → declared is the same entity status; the LEI registration change
    that causes it is already a ``gleif_field`` change."""
    assert not [c for c in diff_snapshots(_snap("live"), _snap("declared")) if c["kind"] == "register_status"]
    assert not [c for c in diff_snapshots(_snap("declared"), _snap("live")) if c["kind"] == "register_status"]


def test_watchlist_still_reports_a_real_status_change() -> None:
    changes = diff_snapshots(_snap("declared"), _snap("terminal"))
    assert [c["kind"] for c in changes] == ["register_status"]
