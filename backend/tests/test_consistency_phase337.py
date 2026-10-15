"""Phase 337 — Phase D of the record-consistency plan: the ``RECORD_CONSISTENCY``
observation, and the three instrument changes the 15 Oct 2026 reading asked
for (a lapsed LEI's ACTIVE is not a current status; the identifier gate's
denominator; a canonical form for Romanian numbers), plus OpenSanctions out
of ``founding_date``.

Every test is named for what the reading showed or for a rule the plan
pinned in advance: one context signal per lookup, never a risk, never in the
verdict's risk clause, never scoped to a graph node, agreement said in the
same voice as disagreement, and the exported bundle carrying each shown
disagreement on both statements.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from opencheck import consistencystats
from opencheck.bods import liveness
from opencheck.bods.annotations import commenting, pointer, validate_all
from opencheck.consistency import (
    AGREE,
    DISAGREE,
    IDENTIFIER_CLASH,
    ONE_MISSING,
    RECORD_CONSISTENCY,
    annotate_disagreements,
    assess_consistency,
    build_signal,
    one_per_entity_identifiers,
)
from opencheck.consistency_allowlist import (
    JURISDICTION_PAIRS,
    LIVENESS_PAIRS,
    READING,
    is_shown,
)
from opencheck.lei_registration import (
    registration_status_from_statement,
    sentence,
    statement_entity_status_is_declared,
)
from opencheck.verdict import _RISK_CLAUSES, build_verdict

from tests.test_consistency import LEI, _items, _stmt

FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src"


def _lapsed(stmt: dict, status: str = "LAPSED") -> dict:
    """Stamp the Phase 255 annotation the GLEIF mapper writes for an LEI
    whose registration is not ISSUED."""
    reg = {
        "status": status,
        "label": {"LAPSED": "Lapsed", "RETIRED": "Retired", "PENDING_TRANSFER": "Pending transfer"}[status],
        "next_renewal_date": "2015-02-05",
        "last_update_date": "2026-04-08",
    }
    stmt.setdefault("annotations", []).append(commenting(pointer("recordDetails"), sentence(reg)))
    return stmt


def _signal(*stmts: dict, subject: str = LEI, degraded: list | None = None) -> dict | None:
    result = assess_consistency(list(stmts))
    subject_ids = {s["statementId"] for s in stmts if s.get("_subject", True)}
    return build_signal(result, list(stmts), subject=subject, subject_statement_ids=subject_ids, degraded=degraded)


# ---------------------------------------------------------------------
# Finding 1 — GLEIF's ACTIVE on an LEI no issuer maintains
# ---------------------------------------------------------------------


def test_the_registration_status_is_read_back_off_the_gleif_statement() -> None:
    gl = _lapsed(_stmt("gleif", status=liveness.LIVE))
    assert registration_status_from_statement(gl) == "LAPSED"
    assert statement_entity_status_is_declared(gl)
    # An ISSUED LEI carries no annotation; absence is never a lapse.
    issued = _stmt("gleif", status=liveness.LIVE)
    assert registration_status_from_statement(issued) is None
    assert not statement_entity_status_is_declared(issued)
    # A change of issuer is a maintained record.
    moving = _lapsed(_stmt("gleif", status=liveness.LIVE), "PENDING_TRANSFER")
    assert registration_status_from_statement(moving) == "PENDING_TRANSFER"
    assert not statement_entity_status_is_declared(moving)


def test_a_lapsed_leis_active_is_not_a_side_of_the_liveness_comparison() -> None:
    """Bentcard Import LLP: GLEIF ACTIVE, LAPSED since 5 Feb 2015; Companies
    House dissolved the LLP on 19 Apr 2016. Before Phase 337 that was a
    ``disagree``; it is GLEIF repeating a 2015 declaration."""
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2016-04-19")
    gl = _lapsed(_stmt("gleif", status=liveness.LIVE))
    (item,) = _items(assess_consistency([ch, gl]), "liveness")
    assert item.relation == ONE_MISSING
    assert item.values[item.sources.index("gleif")] is None


def test_a_lapsed_leis_inactive_stays_terminal() -> None:
    """Phase 242's rule holds: only ``live`` is demoted. A GLEIF INACTIVE on
    a lapsed LEI still compares — a register still listing the company as
    live is then the disagreement worth seeing."""
    ch = _stmt("companies_house", status=liveness.LIVE)
    gl = _lapsed(_stmt("gleif", status=liveness.TERMINAL, since="2016-04-19"))
    (item,) = _items(assess_consistency([ch, gl]), "liveness")
    assert item.relation == DISAGREE


def test_an_issued_leis_active_still_compares() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03")
    gl = _stmt("gleif", status=liveness.LIVE)
    (item,) = _items(assess_consistency([ch, gl]), "liveness")
    assert item.relation == DISAGREE


# ---------------------------------------------------------------------
# Finding 3 — Romanian numbers
# ---------------------------------------------------------------------


def test_a_cui_filed_under_the_trade_register_ra_code_is_compared_as_a_cui() -> None:
    """GLEIF's ``registeredAs`` under RA000497 reads ``38462171`` on some
    Romanian records (live sample, 15 Oct 2026) — the fiscal code, not the
    J-number. 293 of ~640 gleif|onrc pairs "clashed" on it."""
    gl = _stmt("gleif", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "38462171"))
    assert one_per_entity_identifiers(gl)["RO-CUI"] == "38462171"
    assert "RO-ONRC" not in one_per_entity_identifiers(gl)
    onrc = _stmt("onrc_romania", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "J2017015812405"))
    onrc["recordDetails"]["identifiers"].append({"id": "38462171", "scheme": "RO-CUI"})
    (item,) = _items(assess_consistency([gl, onrc]), IDENTIFIER_CLASH)
    assert item.relation == AGREE and item.values == ("RO-CUI:38462171", "RO-CUI:38462171")


def test_old_and_new_j_numbers_are_one_registration() -> None:
    old = _stmt("gleif", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "J40/15812/2017"))
    new = _stmt("onrc_romania", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "J2017015812405"))
    assert one_per_entity_identifiers(old)["RO-ONRC"] == "J201701581240"
    assert one_per_entity_identifiers(new)["RO-ONRC"] == "J201701581240"
    (item,) = _items(assess_consistency([old, new]), IDENTIFIER_CLASH)
    assert item.relation == AGREE
    # A different registration is still the clash worth finding.
    other = _stmt("onrc_romania", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "J2022006841234"))
    (item,) = _items(assess_consistency([old, other]), IDENTIFIER_CLASH)
    assert item.relation == DISAGREE


def test_the_vat_prefix_is_not_part_of_the_cui() -> None:
    anaf = _stmt("anaf_romania", jurisdiction=("Romania", "RO"), number=("RO-CUI", "RO14399840"))
    assert one_per_entity_identifiers(anaf)["RO-CUI"] == "14399840"


def test_an_opencorporates_style_value_is_still_not_a_register_number() -> None:
    gl = _stmt("gleif", number=("GB-COH", "gb/04366849"))
    assert "GB-COH" not in one_per_entity_identifiers(gl)


# ---------------------------------------------------------------------
# Finding 2 — the denominator
# ---------------------------------------------------------------------


def test_compared_pairs_are_counted_per_source_pair() -> None:
    consistencystats.reset()
    try:
        gl = _stmt("gleif", status=liveness.LIVE)
        ch = _stmt("companies_house", status=liveness.LIVE)
        oc = _stmt("opencorporates", status=liveness.LIVE)
        result = assess_consistency([gl, ch, oc])
        assert sorted(result.compared) == [
            ("companies_house", "gleif"),
            ("companies_house", "opencorporates"),
            ("gleif", "opencorporates"),
        ]
        consistencystats.record(result)
        consistencystats.record(result)
        stats = consistencystats.stats()
        assert stats["compared"]["gleif|opencorporates"] == 2
        row = stats["pairs"]["liveness|gleif|opencorporates"]
        assert row["compared"] == 2 and row["disagree_per_compared"] == 0.0
        assert not any(k.startswith("_compared") for k in stats["pairs"])
    finally:
        consistencystats.reset()


# ---------------------------------------------------------------------
# The comparison table
# ---------------------------------------------------------------------


def test_opensanctions_incorporation_date_is_not_compared_with_a_registers() -> None:
    """INPI↔OpenSanctions never agreed (0/7); CVR↔OpenSanctions 0/3; across
    eleven thin rows 25/13. Whatever its originating dataset said."""
    inpi = _stmt("inpi", founding="2015-03-02")
    osn = _stmt("opensanctions", founding="2014-11-20")
    assert _items(assess_consistency([inpi, osn]), "founding_date") == []


# ---------------------------------------------------------------------
# The allowlist
# ---------------------------------------------------------------------


def test_the_allowlist_is_the_15_oct_2026_reading() -> None:
    assert READING == "2026-10-15"
    assert len(LIVENESS_PAIRS) == 15 and len(JURISDICTION_PAIRS) == 66
    assert all(a < b for a, b in LIVENESS_PAIRS | JURISDICTION_PAIRS)
    # The rows the reading held out, by name.
    for held_out in (("firmenbuch", "gleif"), ("cnpj_brazil", "gleif"), ("corporations_canada", "gleif"), ("gleif", "zefix")):
        assert not is_shown("liveness", *held_out)
    assert is_shown("liveness", "gleif", "companies_house")  # order-insensitive
    assert not is_shown("founding_date", "ares", "openaleph")  # passed numerically, held as a field
    assert not is_shown("identifier_clash", "gleif", "opencorporates")
    assert not is_shown("jurisdiction", "cr_hongkong", "openaleph")  # 5/2, under the floor


# ---------------------------------------------------------------------
# The signal
# ---------------------------------------------------------------------


def test_one_context_signal_per_lookup_about_the_subject() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03")
    gl = _stmt("gleif", status=liveness.LIVE)
    sig = _signal(ch, gl)
    assert sig is not None
    assert sig["code"] == RECORD_CONSISTENCY and sig["kind"] == "context"
    assert sig["source_id"] == "opencheck" and sig["hit_id"] == LEI
    assert sig["confidence"] == "high"
    assert sig["summary"] == (
        "Two sources describe this entity (two independent). "
        "They agree on jurisdiction. "
        "One disagreement: UK Companies House records the register status as terminal "
        "(since 3 Apr 2019); GLEIF records live."
    )
    ev = sig["evidence"]
    assert ev["sources"] == 2 and ev["independent_sources"] == 2
    assert ev["agreed_fields"] == ["jurisdiction"] and ev["disagreed_fields"] == ["liveness"]
    assert ev["reading"] == READING
    (dis,) = [i for i in ev["items"] if i["relation"] == DISAGREE]
    assert dis["field"] == "liveness" and sorted(dis["sources"]) == ["companies_house", "gleif"]
    # Nothing a graph badge could scope to (decision pinned on the plan).
    for key in ("statement_id", "subject_statement_id", "subject_statement_ids", "matches", "jurisdictions", "longest_path"):
        assert key not in ev


def test_agreement_is_said_so_silence_never_reads_as_clean() -> None:
    sig = _signal(_stmt("companies_house", status=liveness.LIVE), _stmt("gleif", status=liveness.LIVE))
    assert sig is not None
    assert sig["summary"] == (
        "Two sources describe this entity (two independent). "
        "They agree on register status and jurisdiction."
    )
    assert sig["evidence"]["disagreed_fields"] == []


def test_a_mirror_is_not_a_second_source_agreeing() -> None:
    """Companies House and OpenCorporates (its republisher) on one subject:
    two sources, none independent of the other, nothing to compare."""
    sig = _signal(_stmt("companies_house", status=liveness.LIVE), _stmt("opencorporates", status=liveness.LIVE))
    assert sig is not None
    assert sig["summary"] == (
        "Two sources describe this entity (none independent of the others). "
        "No field could be compared between independent sources."
    )


def test_a_comparison_the_gate_held_out_is_counted_but_not_said() -> None:
    """Firmenbuch↔GLEIF liveness is 11.1 % on the reading: the disagreement
    stays in the counters and the log and never reaches the sentence."""
    fb = _stmt("firmenbuch", jurisdiction=("Austria", "AT"), status=liveness.TERMINAL)
    gl = _stmt("gleif", jurisdiction=("Austria", "AT"), status=liveness.LIVE)
    result = assess_consistency([fb, gl])
    assert [i.relation for i in _items(result, "liveness")] == [DISAGREE]
    sig = build_signal(result, [fb, gl], subject=LEI, subject_statement_ids={fb["statementId"], gl["statementId"]})
    assert sig is not None
    assert "disagreement" not in sig["summary"]
    assert sig["evidence"]["items"] == [
        {
            "field": "jurisdiction",
            "relation": AGREE,
            "sources": ["firmenbuch", "gleif"],
            "statement_ids": [fb["statementId"], gl["statementId"]],
            "values": ["AT", "AT"],
            "said": ["AT", "AT"],
        }
    ]


def test_founding_date_and_identifier_clash_stay_out_of_the_sentence() -> None:
    ares = _stmt("ares", jurisdiction=("Czechia", "CZ"), founding="1992-05-01", number=("CZ-ICO", "45274649"))
    oa = _stmt("openaleph", jurisdiction=("Czechia", "CZ"), founding="1992-06-01", number=("CZ-ICO", "45274650"))
    result = assess_consistency([ares, oa])
    assert _items(result, "founding_date", DISAGREE) and _items(result, IDENTIFIER_CLASH, DISAGREE)
    sig = build_signal(result, [ares, oa], subject=LEI, subject_statement_ids={ares["statementId"]})
    assert sig is not None and "disagreement" not in sig["summary"]
    assert {i["field"] for i in sig["evidence"]["items"]} <= {"jurisdiction"}


def test_a_source_that_did_not_answer_is_named_as_not_compared() -> None:
    gl = _stmt("gleif", status=liveness.LIVE)
    ch = _stmt("companies_house", status=liveness.LIVE)
    degraded = [
        {"source_id": "opencorporates", "check": "source_fetch", "reason": "timeout"},
        {"source_id": "openaleph", "check": "related_party_screen", "reason": "timeout"},
    ]
    sig = _signal(gl, ch, degraded=degraded)
    assert sig is not None
    assert sig["summary"].endswith("OpenCorporates did not answer, so its records were not compared.")
    assert sig["evidence"]["not_compared"] == [{"source_id": "opencorporates", "reason": "did not answer"}]


def test_one_source_is_nothing_to_compare_and_no_signal() -> None:
    assert _signal(_stmt("gleif", status=liveness.LIVE)) is None
    # Two statements, one source: still one source.
    assert _signal(_stmt("openaleph", founding="2002-02-05"), _stmt("openaleph")) is None


def test_the_signal_is_about_the_subject_not_its_owners() -> None:
    """An owner the bundle describes twice gets no sentence of its own; the
    one signal per lookup is the subject's."""
    subject_gl = _stmt("gleif", status=liveness.LIVE)
    subject_ch = _stmt("companies_house", status=liveness.LIVE)
    owner_gl = _stmt("gleif", lei="5493001KJTIIGC8Y1R12", status=liveness.LIVE, name="OWNER")
    owner_ch = _stmt("companies_house", lei="5493001KJTIIGC8Y1R12", status=liveness.TERMINAL, name="OWNER")
    result = assess_consistency([subject_gl, subject_ch, owner_gl, owner_ch])
    assert _items(result, "liveness", DISAGREE)  # the owner's, counted
    sig = build_signal(
        result,
        [subject_gl, subject_ch, owner_gl, owner_ch],
        subject=LEI,
        subject_statement_ids={subject_gl["statementId"], subject_ch["statementId"]},
    )
    assert sig is not None and "disagreement" not in sig["summary"]
    assert set(sig["evidence"]["subject_statements"]) == {subject_gl["statementId"], subject_ch["statementId"]}


def test_build_signal_fails_soft() -> None:
    result = assess_consistency([_stmt("gleif"), _stmt("companies_house")])
    assert build_signal(result, [{"statementId": None}, "not a statement"], subject=LEI, subject_statement_ids=["x"]) is None  # type: ignore[list-item]


# ---------------------------------------------------------------------
# The verdict, the scope
# ---------------------------------------------------------------------


def test_the_code_is_in_no_risk_clause_and_the_verdict_still_says_nothing_surfaced() -> None:
    assert all(RECORD_CONSISTENCY not in codes for codes, _, _ in _RISK_CLAUSES)
    sig = _signal(_stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03"), _stmt("gleif", status=liveness.LIVE))
    assert sig is not None
    assert build_verdict([sig], []) == "No risk signals surfaced across the sources that answered."
    assert build_verdict([sig], [{"source_id": "kvk", "check": "source_fetch", "reason": "timeout"}]) == (
        "No risk signals surfaced, but one source did not answer, so its records were not consulted."
    )


def test_the_frontend_does_not_scope_it_to_a_node_and_labels_it_records_compared() -> None:
    scope = (FRONTEND / "lib" / "signalScope.ts").read_text(encoding="utf-8")
    assert 'code.startsWith("RELATED_")' in scope or "startsWith(\"RELATED_\")" in scope
    chip = (FRONTEND / "components" / "risk" / "RiskChip.tsx").read_text(encoding="utf-8")
    assert "RECORD_CONSISTENCY" in chip
    block = chip[chip.index("RECORD_CONSISTENCY"):]
    assert 'label: "Records compared"' in block[: block.index("}")]
    assert "classes: CONTEXT_CLASSES" in block[: block.index("}")]


# ---------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------


def test_each_shown_disagreement_is_annotated_on_both_statements() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03")
    gl = _stmt("gleif", status=liveness.LIVE)
    sig = _signal(ch, gl)
    before = json.dumps([ch, gl], sort_keys=True)
    out = annotate_disagreements([ch, gl], [sig])
    assert json.dumps([ch, gl], sort_keys=True) == before  # the originals are untouched
    assert validate_all(out) == []
    notes = {
        s["statementId"]: [a for a in s["annotations"] if a["description"].startswith("Record consistency")]
        for s in out
    }
    assert len(notes[ch["statementId"]]) == 1 and len(notes[gl["statementId"]]) == 1
    ch_note = notes[ch["statementId"]][0]
    assert ch_note["motivation"] == "commenting"
    assert ch_note["statementPointerTarget"] == "/recordDetails/dissolutionDate"
    assert "GLEIF" in ch_note["description"] and "records live" in ch_note["description"]
    gl_note = notes[gl["statementId"]][0]
    assert gl_note["statementPointerTarget"] == "/recordDetails"
    assert "Companies House" in gl_note["description"]


def test_a_bundle_with_no_disagreement_is_returned_as_is() -> None:
    ch = _stmt("companies_house", status=liveness.LIVE)
    gl = _stmt("gleif", status=liveness.LIVE)
    sig = _signal(ch, gl)
    out = annotate_disagreements([ch, gl], [sig])
    assert out == [ch, gl] and all(not a["description"].startswith("Record consistency") for s in out for a in s.get("annotations", []))
    assert annotate_disagreements([ch, gl], None) == [ch, gl]


def test_jurisdiction_disagreement_points_at_the_jurisdiction() -> None:
    gl = _stmt("gleif", jurisdiction=("United Kingdom", "GB"))
    wd = _stmt("wikidata", jurisdiction=("Netherlands", "NL"))
    sig = _signal(gl, wd)
    assert sig is not None and "GLEIF records the jurisdiction as GB; Wikidata records NL" in sig["summary"]
    out = annotate_disagreements([gl, wd], [sig])
    targets = {a["statementPointerTarget"] for s in out for a in s["annotations"] if a["description"].startswith("Record consistency")}
    assert targets == {"/recordDetails/jurisdiction"}
    assert validate_all(out) == []


@pytest.mark.parametrize("field", ["liveness", "jurisdiction"])
def test_every_shown_field_has_a_noun(field: str) -> None:
    from opencheck.consistency import FIELD_NOUNS

    assert field in FIELD_NOUNS


def test_the_frontend_names_the_same_fields() -> None:
    """``FIELD_LABELS`` in ``lib/recordConsistency.ts`` ↔ ``FIELD_NOUNS`` here
    (the ``test_ra_codes.py`` pattern: the TypeScript is parsed, not trusted)."""
    import re

    from opencheck.consistency import FIELD_NOUNS

    ts = (FRONTEND / "lib" / "recordConsistency.ts").read_text(encoding="utf-8")
    block = ts[ts.index("export const FIELD_LABELS"):]
    block = block[: block.index("};")]
    keys = re.findall(r"^\s*(\w+):\s*\"", block, re.M)
    assert keys == list(FIELD_NOUNS)


def test_the_share_card_counts_no_records_compared_signal() -> None:
    """Absent from the OG card and share description by ``kind``, like every
    context signal — pinned here so the one new code is checked by name."""
    from opencheck.og_image import card_alt_text

    sig = _signal(_stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03"), _stmt("gleif", status=liveness.LIVE))
    assert sig is not None
    alt = card_alt_text("SHELL PLC", LEI, [sig])
    assert "no risk signals found" in alt
    assert "Records compared" not in alt and "RECORD_CONSISTENCY" not in alt


@pytest.mark.parametrize(
    ("fixture", "lei"),
    [("novo_nordisk_2026-09-02", "549300DAQ1CVT6CXN342"), ("shell_plc_2026-09-02", "21380068P1DRHMJ8KU70")],
)
def test_the_curated_exports_show_the_observation_with_no_disagreement(fixture: str, lei: str) -> None:
    """The plan's acceptance case: the Novo Nordisk and Shell exports of
    2 Sept 2026 show the observation, and it names no disagreement (Wikidata's
    inception is excluded; the registers and GLEIF agree)."""
    from opencheck.subject_identity import subject_identity

    bods = json.loads((Path(__file__).parent / "fixtures" / "consistency" / f"{fixture}.json").read_text())
    identity = subject_identity(lei, bods)
    assert identity.statement_ids
    result = assess_consistency(bods)
    sig = build_signal(result, bods, subject=lei, subject_statement_ids=identity.statement_ids)
    assert sig is not None and sig["kind"] == "context"
    assert sig["evidence"]["sources"] >= 2
    assert "disagreement" not in sig["summary"]
    assert "They agree on" in sig["summary"]
