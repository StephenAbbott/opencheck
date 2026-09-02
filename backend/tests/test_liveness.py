"""Register liveness (Phase 151): one vocabulary for "does the register still
treat this entity as live?", written by ``apply_register_status`` and read
back by ``read_register_status``.

Most of these pin the two invariants the module exists for:

* ``dissolutionDate`` is only ever a real ``YYYY-MM-DD`` — never ``"unknown"``,
  never ``null`` (both were in tree before this phase);
* the annotation grammar round-trips, so Phase C can read a register's class
  without a second copy of every register's vocabulary.
"""

from __future__ import annotations

import pytest

from opencheck.bods import liveness
from opencheck.bods.annotations import validate_annotations
from opencheck.bods.mapper import (
    make_entity_statement,
    map_companies_house,
    map_gleif,
    map_opencorporates,
)


def _entity() -> dict:
    return make_entity_statement(
        source_id="companies_house",
        local_id="04366849",
        name="SHELL PLC",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[{"id": "04366849", "scheme": "GB-COH"}],
        founding_date="2002-02-05",
    )


# ---------------------------------------------------------------------
# classify
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("active", "live"),
        ("Active", "live"),
        ("  ACTIVE ", "live"),
        ("liquidation", "pending"),
        ("dissolved", "terminal"),
        ("", "unknown"),
        (None, "unknown"),
        ("something the register invented", "unknown"),
    ],
)
def test_classify(raw, expected) -> None:
    assert (
        liveness.classify(raw, live=("active",), pending=("liquidation",), terminal=("dissolved",))
        == expected
    )


def test_classify_matches_whole_labels_not_substrings() -> None:
    """'inactive' must not match a 'live' vocabulary containing 'active'."""
    assert liveness.classify("inactive", live=("active",), terminal=("inactive",)) == "terminal"
    assert liveness.classify("inactive", live=("active",)) == "unknown"


# ---------------------------------------------------------------------
# apply_register_status
# ---------------------------------------------------------------------


def test_terminal_with_full_date_sets_dissolution_date_and_annotation() -> None:
    stmt = liveness.apply_register_status(
        _entity(),
        source_label="UK Companies House",
        liveness=liveness.TERMINAL,
        raw="dissolved",
        since="2019-04-03",
    )
    assert stmt["recordDetails"]["dissolutionDate"] == "2019-04-03"
    assert validate_annotations(stmt) == []
    status = liveness.read_register_status(stmt)
    assert status == {
        "source": "UK Companies House",
        "liveness": "terminal",
        "since": "2019-04-03",
        "raw": "dissolved",
    }
    (annotation,) = stmt["annotations"]
    assert annotation["motivation"] == "commenting"
    assert annotation["statementPointerTarget"] == "/recordDetails"
    assert annotation["description"] == (
        "UK Companies House records this entity as dissolved since 2019-04-03"
        " — register status: “dissolved”."
    )


def test_terminal_without_date_never_writes_a_sentinel() -> None:
    """The MCA / ABR / ACRA precedents wrote "unknown" or null. Never again."""
    stmt = liveness.apply_register_status(
        _entity(), source_label="MCA", liveness=liveness.TERMINAL, raw="Strike Off"
    )
    assert "dissolutionDate" not in stmt["recordDetails"]
    status = liveness.read_register_status(stmt)
    assert status["liveness"] == "terminal" and status["since"] is None
    assert status["raw"] == "Strike Off"


@pytest.mark.parametrize(
    ("since", "iso", "unit"),
    [("2019", "2019-01-01", "month and day"), ("2019-04", "2019-04-01", "day")],
)
def test_partial_dissolution_date_is_rounded_and_annotated(since, iso, unit) -> None:
    stmt = liveness.apply_register_status(
        _entity(), source_label="X", liveness=liveness.TERMINAL, since=since
    )
    assert stmt["recordDetails"]["dissolutionDate"] == iso
    kinds = {a["motivation"] for a in stmt["annotations"]}
    assert kinds == {"commenting", "transformation"}
    rounding = next(a for a in stmt["annotations"] if a["motivation"] == "transformation")
    assert rounding["statementPointerTarget"] == "/recordDetails/dissolutionDate"
    assert f"no {unit}" in rounding["description"]
    assert validate_annotations(stmt) == []
    # The status annotation carries the rounded date, matching the field.
    assert liveness.read_register_status(stmt)["since"] == iso


def test_garbage_date_is_dropped_not_written() -> None:
    stmt = liveness.apply_register_status(
        _entity(), source_label="X", liveness=liveness.TERMINAL, since="not a date"
    )
    assert "dissolutionDate" not in stmt["recordDetails"]
    assert liveness.read_register_status(stmt)["since"] is None


def test_pending_and_live_never_set_dissolution_date() -> None:
    for cls, word in ((liveness.PENDING, "in a terminal process"), (liveness.LIVE, "active")):
        stmt = liveness.apply_register_status(
            _entity(), source_label="X", liveness=cls, raw="whatever", since="2020-01-01"
        )
        assert "dissolutionDate" not in stmt["recordDetails"]
        assert liveness.read_register_status(stmt)["liveness"] == cls
        assert f"records this entity as {word}" in stmt["annotations"][0]["description"]


def test_unknown_writes_nothing() -> None:
    stmt = liveness.apply_register_status(
        _entity(), source_label="X", liveness=liveness.UNKNOWN, raw="?", since="2020-01-01"
    )
    assert "annotations" not in stmt
    assert "dissolutionDate" not in stmt["recordDetails"]
    assert liveness.read_register_status(stmt) is None


def test_second_call_replaces_rather_than_stacks() -> None:
    stmt = liveness.apply_register_status(
        _entity(), source_label="X", liveness=liveness.TERMINAL, since="2020-01-01"
    )
    stmt = liveness.apply_register_status(stmt, source_label="X", liveness=liveness.LIVE, raw="active")
    status_annotations = [a for a in stmt["annotations"] if liveness._is_status_annotation(a)]
    assert len(status_annotations) == 1
    assert liveness.read_register_status(stmt)["liveness"] == "live"
    # A live status clears the stale dissolution date from the earlier call.
    assert "dissolutionDate" not in stmt["recordDetails"]


def test_other_annotations_survive() -> None:
    stmt = _entity()
    stmt["annotations"] = [
        {
            "statementPointerTarget": "/recordDetails/name",
            "motivation": "commenting",
            "description": "Something unrelated.",
            "createdBy": {"name": "OpenCheck"},
        }
    ]
    liveness.apply_register_status(stmt, source_label="X", liveness=liveness.LIVE)
    assert len(stmt["annotations"]) == 2
    assert liveness.read_register_status(stmt)["liveness"] == "live"


def test_raw_label_with_quotes_and_newlines_round_trips() -> None:
    stmt = liveness.apply_register_status(
        _entity(),
        source_label="ΓΕΜΗ — Greek General Commercial Registry (Γενικό Εμπορικό Μητρώο)",
        liveness=liveness.TERMINAL,
        raw='Διαγραφή  "λόγω"\n συγχώνευσης',
    )
    status = liveness.read_register_status(stmt)
    assert status["source"].startswith("ΓΕΜΗ")
    assert status["raw"] == 'Διαγραφή "λόγω" συγχώνευσης'


def test_bare_dissolution_date_reads_as_terminal() -> None:
    """A bulk dataset (or a pre-151 cache entry) that carried the field
    verbatim, with no annotation, still reads back in the one shape."""
    stmt = _entity()
    stmt["recordDetails"]["dissolutionDate"] = "2011-06-30"
    assert liveness.read_register_status(stmt) == {
        "source": "UK Companies House",
        "liveness": "terminal",
        "since": "2011-06-30",
        "raw": None,
    }
    stmt["recordDetails"]["dissolutionDate"] = "unknown"  # the old sentinel
    assert liveness.read_register_status(stmt) is None


# ---------------------------------------------------------------------
# Through the mappers
# ---------------------------------------------------------------------


def _ch_bundle(status: str, cessation: str | None = None) -> dict:
    profile = {
        "company_name": "OLD SHELL LIMITED",
        "company_number": "01234567",
        "company_status": status,
        "date_of_creation": "1990-01-01",
        "type": "ltd",
    }
    if cessation:
        profile["date_of_cessation"] = cessation
    return {"company_number": "01234567", "profile": profile, "officers": [], "pscs": [], "is_stub": False}


def _ch_entity(bundle: dict) -> dict:
    return next(
        s for s in map_companies_house(bundle) if s.get("recordType") == "entity"
        and s["recordDetails"]["name"] == "OLD SHELL LIMITED"
    )


def test_companies_house_dissolved_with_cessation_date() -> None:
    entity = _ch_entity(_ch_bundle("dissolved", "2019-04-03"))
    assert entity["recordDetails"]["dissolutionDate"] == "2019-04-03"
    status = liveness.read_register_status(entity)
    assert status["liveness"] == "terminal" and status["raw"] == "dissolved"
    assert status["source"] == "UK Companies House"


def test_companies_house_liquidation_is_pending() -> None:
    entity = _ch_entity(_ch_bundle("liquidation"))
    assert "dissolutionDate" not in entity["recordDetails"]
    assert liveness.read_register_status(entity)["liveness"] == "pending"


def test_companies_house_active_is_live_not_silent() -> None:
    """A source that SAYS active is distinguishable from one that says nothing
    — the distinction Phase C needs."""
    entity = _ch_entity(_ch_bundle("active"))
    assert liveness.read_register_status(entity)["liveness"] == "live"
    assert "dissolutionDate" not in entity["recordDetails"]


def _gleif_bundle(status: str, expiration: dict | None = None) -> dict:
    entity = {
        "legalName": {"name": "GONE GMBH"},
        "legalAddress": {"country": "DE", "city": "Berlin"},
        "jurisdiction": "DE",
        "status": status,
        "creationDate": "2001-01-01",
    }
    if expiration:
        entity["expiration"] = expiration
    return {
        "lei": "529900T8BM49AURSDO55",
        "record": {
            "id": "529900T8BM49AURSDO55",
            "attributes": {
                "lei": "529900T8BM49AURSDO55",
                "entity": entity,
                "registration": {"status": "RETIRED", "lastUpdateDate": "2020-05-01T00:00:00Z"},
            },
        },
        "is_stub": False,
    }


def _gleif_entity(bundle: dict) -> dict:
    return next(s for s in map_gleif(bundle) if s.get("recordType") == "entity")


def test_gleif_inactive_with_expiration() -> None:
    entity = _gleif_entity(_gleif_bundle("INACTIVE", {"date": "2020-04-30T00:00:00Z", "reason": "DISSOLVED"}))
    assert entity["recordDetails"]["dissolutionDate"] == "2020-04-30"
    status = liveness.read_register_status(entity)
    assert status["liveness"] == "terminal"
    assert status["raw"] == "INACTIVE (DISSOLVED)"
    assert status["source"] == "GLEIF"


def test_gleif_inactive_without_expiration_is_still_visible() -> None:
    entity = _gleif_entity(_gleif_bundle("INACTIVE"))
    assert "dissolutionDate" not in entity["recordDetails"]
    assert liveness.read_register_status(entity)["liveness"] == "terminal"


def test_gleif_active_is_live_and_lapse_is_not_liveness() -> None:
    """registration.status RETIRED / LAPSED is about the LEI record, not the
    legal entity, and must not read as dissolution."""
    entity = _gleif_entity(_gleif_bundle("ACTIVE"))
    status = liveness.read_register_status(entity)
    assert status["liveness"] == "live"
    assert "RETIRED" not in (status["raw"] or "")
    assert "dissolutionDate" not in entity["recordDetails"]


def _oc_bundle(**company) -> dict:
    base = {
        "name": "SHELL PLC",
        "company_number": "04366849",
        "jurisdiction_code": "gb",
        "incorporation_date": "2002-02-05",
        "opencorporates_url": "https://opencorporates.com/companies/gb/04366849",
    }
    base.update(company)
    return {"ocid": "gb/04366849", "company": base, "officers": [], "is_stub": False}


def _oc_entity(bundle: dict) -> dict:
    return next(s for s in map_opencorporates(bundle) if s.get("recordType") == "entity")


def test_opencorporates_dissolution_date_is_no_longer_dropped() -> None:
    entity = _oc_entity(_oc_bundle(inactive=True, current_status="Dissolved", dissolution_date="2015-03-31"))
    assert entity["recordDetails"]["dissolutionDate"] == "2015-03-31"
    status = liveness.read_register_status(entity)
    assert status["liveness"] == "terminal" and status["raw"] == "Dissolved"


def test_opencorporates_inactive_flag_without_date() -> None:
    entity = _oc_entity(_oc_bundle(inactive=True, current_status="Struck off"))
    assert "dissolutionDate" not in entity["recordDetails"]
    assert liveness.read_register_status(entity)["liveness"] == "terminal"


def test_opencorporates_active_and_unknown() -> None:
    assert liveness.read_register_status(_oc_entity(_oc_bundle(inactive=False, current_status="Active")))["liveness"] == "live"
    assert liveness.read_register_status(_oc_entity(_oc_bundle())) is None


def test_status_annotated_output_validates_against_the_bods_schema() -> None:
    """The three shapes this phase writes — terminal with date, terminal with
    a rounded partial date (two annotations), INACTIVE GLEIF — are schema-valid
    BODS 0.4. This is the check the old ``"unknown"`` / ``null`` sentinels
    would have failed."""
    import json
    import tempfile
    from pathlib import Path

    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    stmts = (
        list(map_companies_house(_ch_bundle("dissolved", "2019-04-03")))
        + list(map_gleif(_gleif_bundle("INACTIVE", {"date": "2020-04-30T00:00:00Z", "reason": "DISSOLVED"})))
        + list(map_opencorporates(_oc_bundle(inactive=True, current_status="Dissolved", dissolution_date="2015")))
    )
    assert sum(1 for s in stmts if liveness.read_register_status(s)) == 3
    path = Path(tempfile.mkdtemp()) / "out.json"
    path.write_text(json.dumps(stmts))
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]
