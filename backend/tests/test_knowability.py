"""Phase 223 — the per-jurisdiction "what is knowable" statement.

Three families of guard:

* **coverage** — every jurisdiction an adapter reads has a row, every row
  has a name, a code and a source; the EU/EEA set is complete;
* **voice** — no sentence uses a word that turns a register fact into a
  judgement, absence is stated rather than omitted, and the sentence flips
  on the dates in the data with no code change;
* **parity** — ``bo_access.notice_for`` (now a view over the same data) and
  ``bo_regimes`` thresholds agree with what the table says.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods.bo_regimes import REGIMES
from opencheck.knowability import (
    ACCESS_STATUSES,
    BANNED_VOCABULARY,
    JURISDICTIONS,
    BoRegister,
    Jurisdiction,
    KnowabilityStatement,
    chain_jurisdictions,
    opencheck_reads,
    statement_for,
    statements_for,
)
from opencheck.sources import REGISTRY

_TODAY = date(2026, 9, 18)
_EU_EEA = set(
    "AT BE BG HR CY CZ DK EE FI FR DE GR HU IE IT LV LT LU MT NL PL PT RO SK SI ES SE IS LI NO".split()
)


# ----------------------------------------------------------------------
# Coverage
# ----------------------------------------------------------------------
def test_every_adapter_jurisdiction_has_a_row() -> None:
    missing = sorted(
        {
            (a.info.country or "").upper()
            for a in REGISTRY.values()
            if a.info.country and a.info.country.upper() not in JURISDICTIONS
        }
    )
    assert not missing, f"adapter jurisdictions with no row in jurisdictions.json: {missing}"


def test_every_eu_eea_state_has_a_row() -> None:
    assert _EU_EEA <= set(JURISDICTIONS), sorted(_EU_EEA - set(JURISDICTIONS))


def test_chain_jurisdictions_from_the_ticket_have_rows() -> None:
    # KY, VG, BM, JE, GG, IM, LU — the Phase 223 scope decision (18 Sept 2026).
    for code in ("KY", "VG", "BM", "JE", "GG", "IM", "LU"):
        assert code in JURISDICTIONS, code


def test_rows_are_well_formed() -> None:
    for code, j in JURISDICTIONS.items():
        assert code == j.code
        assert j.name.strip(), code
        if j.bo_register.access is not None:
            assert j.bo_register.access in ACCESS_STATUSES, code
        for s in j.sources:
            assert s.url.startswith("https://") or s.url.startswith("http://"), (code, s.url)


def test_a_verified_row_carries_the_date_and_an_unverified_row_says_so() -> None:
    statuses = {j.review_status for j in JURISDICTIONS.values()}
    assert statuses <= {"verified", "unverified"}
    for j in JURISDICTIONS.values():
        st = statement_for(j.code, _TODAY)
        assert st.review_status == j.review_status
        assert (st.last_verified is not None) == (j.last_verified is not None)


def test_data_file_is_generated_not_hand_edited() -> None:
    raw = json.loads(
        (Path(__file__).resolve().parents[1] / "opencheck" / "data" / "jurisdictions.json").read_text()
    )
    assert "sync_jurisdictions.py" in raw["_comment"]
    assert raw["notion_data_source_id"] == "b6be86df-6d76-422b-93bd-f1ece5a99781"
    assert raw["generated_at"]


# ----------------------------------------------------------------------
# Voice
# ----------------------------------------------------------------------
_BANNED_RE = re.compile(r"\b(" + "|".join(re.escape(w) for w in BANNED_VOCABULARY) + r")\w*\b", re.I)


@pytest.mark.parametrize("code", sorted(JURISDICTIONS) + ["XX", "ZZ-1"])
def test_no_sentence_uses_risk_vocabulary(code: str) -> None:
    st = statement_for(code, _TODAY)
    hit = _BANNED_RE.search(st.sentence)
    assert hit is None, f"{code}: {hit.group(0)!r} in {st.sentence!r}"


def test_every_sentence_is_capitalised_and_full_stopped() -> None:
    for code in JURISDICTIONS:
        st = statement_for(code, _TODAY)
        assert st.sentences, code
        for s in st.sentences:
            assert s[0].isupper() and s.endswith("."), (code, s)
        assert st.sentence == " ".join(st.sentences)


def test_unknown_jurisdiction_is_a_stated_absence_not_silence() -> None:
    st = statement_for("XX", _TODAY)
    assert st.stated_absence is True
    assert st.review_status == "absent"
    assert "holds no register notes" in st.sentence
    assert "says nothing about what" in st.sentence
    assert st.opencheck_reads == []


def test_unknown_jurisdiction_is_named_when_pycountry_knows_it() -> None:
    st = statement_for("AR", _TODAY)  # Argentina has no row in v1
    assert st.stated_absence is True
    assert "Argentina" in st.sentence or "AR" in st.sentence


def test_a_subdivision_falls_back_to_its_country() -> None:
    assert statement_for("US-DE", _TODAY).code == "US"


def test_absence_of_an_adapter_is_stated_in_the_reads_sentence() -> None:
    st = statement_for("KY", _TODAY)
    assert st.opencheck_reads == []
    assert "OpenCheck reads no Cayman Islands register" in st.sentence
    assert "says nothing about what Cayman Islands holds" in st.sentence


def test_an_adapter_that_reads_beneficial_owners_is_said_to() -> None:
    st = statement_for("GB", _TODAY)
    ids = {s.source_id for s in st.opencheck_reads}
    assert "companies_house" in ids
    assert any(s.reads_beneficial_owners for s in st.opencheck_reads if s.source_id == "companies_house")
    assert "OpenCheck reads beneficial owners from" in st.sentence


def test_an_adapter_that_reads_only_the_company_register_is_not_credited_with_owners() -> None:
    # KvK: the UBO register is a separate, restricted product OpenCheck does not read.
    st = statement_for("NL", _TODAY)
    assert st.opencheck_reads and not any(s.reads_beneficial_owners for s in st.opencheck_reads)
    assert "not beneficial owners" in st.sentence


def test_opencheck_reads_is_derived_from_the_registry() -> None:
    for code in JURISDICTIONS:
        expected = sorted(
            sid for sid, a in REGISTRY.items() if (a.info.country or "").upper() == code
        )
        assert [s.source_id for s in opencheck_reads(code)] == expected, code


def test_reads_beneficial_owners_agrees_with_bo_regimes() -> None:
    for code in JURISDICTIONS:
        for s in opencheck_reads(code):
            regime = REGIMES.get(s.source_id)
            expected = bool(regime) and "assert_true" in regime.record_kinds.values()
            assert s.reads_beneficial_owners == expected, s.source_id


# ----------------------------------------------------------------------
# The sentence flips on the data, never on code
# ----------------------------------------------------------------------
def _fixture(**bo: object) -> Jurisdiction:
    return Jurisdiction(code="EE", name="Estonia", bo_register=BoRegister(**bo))


def test_estonia_shape_public_then_restricted_flips_on_the_date(monkeypatch) -> None:
    """The Estonia fixture: public today, a restriction announced for a
    future date, then restricted. The sentence changes with the date alone."""
    public = _fixture(name="e-Business Register", access="public", next_change_expected=date(2030, 7, 10))
    monkeypatch.setitem(JURISDICTIONS, "EE", public)
    before = statement_for("EE", date(2030, 7, 1))
    assert "is open to the public" in before.sentence
    assert "a change is announced for 10 Jul 2030" in before.sentence
    on_day = statement_for("EE", date(2030, 7, 10))
    assert "a change is announced" not in on_day.sentence  # the date has passed; the table must be updated

    restricted = _fixture(
        name="e-Business Register", access="legitimate_interest", access_since=date(2030, 7, 10)
    )
    monkeypatch.setitem(JURISDICTIONS, "EE", restricted)
    after = statement_for("EE", date(2030, 8, 1))
    assert (
        "accessible to authorities and obliged entities, and to others on legitimate interest, "
        "since 10 Jul 2030"
    ) in after.sentence


def test_lower_rungs_name_the_amld_floor(monkeypatch) -> None:
    """Every tier below public says that authorities and obliged entities can
    see the register — the ladder agreed 18 Sept 2026 — so "legitimate
    interest" is never read as "banks are locked out too". Sweden's shape
    (LIA in law, no route yet) is said in those words, not as "closed"."""
    monkeypatch.setitem(
        JURISDICTIONS, "SE", _fixture(name="BO register", access="restricted_no_lia_route_yet")
    )
    se = statement_for("SE", _TODAY).sentence
    assert "accessible to authorities and obliged entities" in se
    assert "legitimate-interest route is provided for in law but is not yet open" in se
    assert "closed" not in se

    ie_fixture = _fixture(
        access="authorities_and_obliged_entities_only", next_change_expected=date(2026, 11, 10)
    )
    monkeypatch.setitem(JURISDICTIONS, "IE", ie_fixture)
    ie = statement_for("IE", date(2026, 9, 18)).sentence
    assert "open to authorities and obliged entities only" in ie
    assert "a change is announced for 10 Nov 2026" in ie


@pytest.mark.parametrize("access", ACCESS_STATUSES)
def test_every_access_status_has_a_sentence(access: str, monkeypatch) -> None:
    monkeypatch.setitem(JURISDICTIONS, "EE", _fixture(name="Register", access=access))
    st = statement_for("EE", _TODAY)
    assert st.sentences[0].endswith(".")
    assert _BANNED_RE.search(st.sentence) is None


def test_unrecorded_access_is_said_to_be_unrecorded(monkeypatch) -> None:
    monkeypatch.setitem(JURISDICTIONS, "EE", _fixture())
    st = statement_for("EE", _TODAY)
    assert st.access is None
    assert "is not yet recorded here" in st.sentence


def test_recorded_sentence_reads_the_fields(monkeypatch) -> None:
    monkeypatch.setitem(
        JURISDICTIONS,
        "EE",
        _fixture(
            name="Register",
            access="public",
            threshold_wording="more than 25 %",
            fields_published=["names", "percentage_bands"],
            reporting_basis="first_qualifying_link",
            verification="identity_verified",
        ),
    )
    st = statement_for("EE", _TODAY)
    s2 = " ".join(st.sentences[1:3])
    assert "holders of more than 25 %" in s2
    assert "in percentage bands" in s2
    assert "first qualifying link" in s2
    assert "Identities are verified but the holdings claimed are not." in s2


def test_no_register_has_no_recorded_sentence(monkeypatch) -> None:
    monkeypatch.setitem(JURISDICTIONS, "EE", _fixture(access="no_register", threshold_wording="25 %"))
    st = statement_for("EE", _TODAY)
    assert len(st.sentences) == 2  # access + reads; nothing is "recorded" by a register that does not exist


def test_statements_for_dedupes_and_keeps_order() -> None:
    sts = statements_for(["gb", "KY", "GB", "", "ky"], _TODAY)
    assert [s.code for s in sts] == ["GB", "KY"]


# ----------------------------------------------------------------------
# Parity with the source-keyed registries
# ----------------------------------------------------------------------
def test_thresholds_agree_with_bo_regimes_where_both_are_recorded() -> None:
    """A source-level regime and the jurisdiction row must not quote two
    different thresholds for the same register."""
    for regime in REGIMES.values():
        if not regime.jurisdiction_code or not regime.threshold_wording:
            continue
        j = JURISDICTIONS.get(regime.jurisdiction_code)
        if j is None or not j.bo_register.threshold_wording:
            continue
        assert _norm(j.bo_register.threshold_wording) == _norm(regime.threshold_wording), (
            regime.source_id,
            j.bo_register.threshold_wording,
            regime.threshold_wording,
        )


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", s.lower().replace("%", "").replace("percent", ""))


def test_bo_access_is_a_view_over_the_same_data() -> None:
    from opencheck.bo_access import BO_ACCESS, notice_for

    for code, entry in BO_ACCESS.items():
        j = JURISDICTIONS[code]
        assert j.eu_eea
        access = j.bo_register.access
        if access in {"legitimate_interest", "restricted_no_lia_route_yet", "authorities_and_obliged_entities_only"}:
            assert entry.restricted_from == j.bo_register.access_since
            assert notice_for(code, _TODAY) is not None
        else:
            assert access in {"public", "public_with_registration_or_justification"}
            assert entry.restricted_from == j.bo_register.next_change_expected
    # A public register with no announced change has no footnote.
    for code, j in JURISDICTIONS.items():
        if j.eu_eea and j.bo_register.access == "public" and j.bo_register.next_change_expected is None:
            assert notice_for(code, _TODAY) is None, code


# ----------------------------------------------------------------------
# Chain helper (Phase C wires it; the walk ships now)
# ----------------------------------------------------------------------
def _entity(sid: str, code: str) -> dict:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "entity",
        "recordDetails": {"entityType": {"type": "registeredEntity"}, "jurisdiction": {"code": code}},
    }


def _rel(sid: str, subject: str, party: str, *, ended: bool = False) -> dict:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "relationship",
        "recordStatus": "closed" if ended else "new",
        "recordDetails": {
            "subject": subject,
            "interestedParty": party,
            "interests": [{"type": "shareholding"}],
        },
    }


def test_chain_jurisdictions_walks_upwards_subject_first_including_ended_links() -> None:
    bods = [
        _entity("subj", "GB"),
        _entity("hold", "KY"),
        _entity("top", "BM"),
        _entity("former", "VG"),
        _entity("sub", "FR"),  # a subsidiary — below the subject, never on the path
        _rel("r1", "subj", "hold"),
        _rel("r2", "hold", "top"),
        _rel("r3", "subj", "former", ended=True),
        _rel("r4", "sub", "subj"),
    ]
    codes = chain_jurisdictions(bods, "subj")
    assert codes[0] == "GB"
    assert set(codes) == {"GB", "KY", "BM", "VG"}
    assert "FR" not in codes


def test_chain_jurisdictions_without_a_subject_is_empty() -> None:
    assert chain_jurisdictions([_entity("a", "GB")], None) == []
    assert chain_jurisdictions([_entity("a", "GB")], "nope") == []


# ----------------------------------------------------------------------
# The endpoint
# ----------------------------------------------------------------------
@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_endpoint_answers_known_and_unknown_codes_alike(client: TestClient) -> None:
    r = client.get("/knowability", params={"jurisdictions": "gb,KY,XX"})
    assert r.status_code == 200
    body = r.json()
    codes = [s["code"] for s in body["statements"]]
    assert codes == ["GB", "KY", "XX"]
    assert body["statements"][2]["stated_absence"] is True
    assert "GB" in body["known_codes"]
    assert body["as_of"]
    assert "max-age" in r.headers.get("cache-control", "")
    KnowabilityStatement.model_validate(body["statements"][0])


def test_endpoint_requires_a_code(client: TestClient) -> None:
    assert client.get("/knowability").status_code == 422


# ----------------------------------------------------------------------
# The review document is generated — a stale copy fails the build
# ----------------------------------------------------------------------
def test_docs_knowability_md_is_in_sync_with_the_data() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "generate_knowability_doc",
        Path(__file__).resolve().parents[1] / "scripts" / "generate_knowability_doc.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    current = mod.OUT_PATH.read_text(encoding="utf-8")
    assert mod._strip_dates(current) == mod._strip_dates(mod.render()), (
        "docs/knowability.md is stale — run python3 backend/scripts/generate_knowability_doc.py"
    )


# ----------------------------------------------------------------------
# Phase 224 — the ``knowability`` lookup event and its fold
# ----------------------------------------------------------------------
def test_knowability_event_payload_is_the_statement_plus_as_of() -> None:
    from opencheck.routers.lookup import _knowability_payload

    day = date(2026, 9, 18)
    payload = _knowability_payload("GB", day)
    assert payload["as_of"] == "2026-09-18"
    assert payload["code"] == "GB"
    # Round-trips through the JSON a saved report stores, and validates
    # back into the model (``as_of`` is an extra the event adds).
    again = json.loads(json.dumps(payload))
    again.pop("as_of")
    assert KnowabilityStatement.model_validate(again).sentence == payload["sentence"]


def test_knowability_event_falls_back_from_a_us_state_to_the_country() -> None:
    from opencheck.routers.lookup import _knowability_payload

    assert _knowability_payload("US-DE", _TODAY)["code"] == "US"


def test_fold_carries_the_knowability_event_as_recorded(monkeypatch) -> None:
    """A saved report replays the sentence that was true on the day it ran:
    the fold copies the event payload and never re-renders it from today's
    table or today's clock (Phase 218's no-clock rule)."""
    from opencheck.routers import lookup as lookup_mod

    frozen = lookup_mod._knowability_payload("EE", date(2026, 1, 1))
    frozen["sentence"] = "Frozen sentence from the day of the run."

    def _boom(*_a, **_k):  # pragma: no cover - the assertion is that it is not called
        raise AssertionError("fold must not re-render the knowability statement")

    monkeypatch.setattr(lookup_mod, "knowability_statement_for", _boom)
    events = [
        ("gleif_done", {"lei": "X", "legal_name": "Co", "jurisdiction": "EE", "derived_identifiers": {}}),
        ("knowability", frozen),
        ("risk_signals", {"signals": [], "degraded_sources": [], "verdict": None}),
        ("done", {"bods_issues": [], "license_notices": [], "run_completed_at": "2026-01-01T00:00:00Z"}),
    ]
    folded = lookup_mod.fold_lookup_events("X", events)
    assert folded.knowability == frozen
    assert folded.knowability["sentence"] == "Frozen sentence from the day of the run."


def test_fold_without_the_event_leaves_the_field_none() -> None:
    """Payloads recorded before Phase 224 still validate, with no statement."""
    from opencheck.routers.lookup import fold_lookup_events

    folded = fold_lookup_events(
        "X",
        [
            ("gleif_done", {"lei": "X", "legal_name": None, "jurisdiction": None, "derived_identifiers": {}}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert folded.knowability is None
