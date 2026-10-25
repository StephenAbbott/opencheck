"""Release files: many networks, one record per party (the bulk-script
stamping ticket, 25 Oct 2026).

The Estonia and laundromat datasets hold one declared network per company
mapped and one record per party. A party in several networks is published
once and names one of them (Stephen's decision): a relationship keeps its
bundle's subject, a network's subject names itself, and any other party names
the first, by ``recordId``, of the networks whose relationships reference it.
The network subjects come from the build, never from the file, because in the
pre-phase shape every party names itself and any rule read from the file's
own values would accept that.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest

from opencheck.bods.declaration import (
    declaration_problem,
    network_subjects,
    relationship_networks,
    release_declaration_problems,
    settle_party,
    settle_shared_parties,
    stamp_bundle,
    stamp_merged,
)
from opencheck.bods.statements import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


def _company(code: str, name: str) -> dict[str, Any]:
    return make_entity_statement(
        source_id="ariregister",
        local_id=code,
        name=name,
        jurisdiction=("Estonia", "EE"),
        identifiers=[{"id": code, "scheme": "EE-ARIREGISTER"}],
    )


def _person(key: str, name: str) -> dict[str, Any]:
    return make_person_statement(source_id="ariregister", local_id=key, full_name=name)


def _board(company: dict[str, Any], person: dict[str, Any], tag: str) -> dict[str, Any]:
    return make_relationship_statement(
        source_id="ariregister",
        local_id=f"{company['recordId']}:{tag}",
        subject_statement_id=company["recordId"],
        interested_party_statement_id=person["recordId"],
        interests=[{"type": "boardMember"}],
    )


def _holding(company: dict[str, Any], holder: dict[str, Any], tag: str) -> dict[str, Any]:
    return make_relationship_statement(
        source_id="ariregister",
        local_id=f"{company['recordId']}:{tag}",
        subject_statement_id=company["recordId"],
        interested_party_statement_id=holder["recordId"],
        interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )


def _networks() -> dict[str, Any]:
    """Two companies, A and B, each its own network. P sits on both boards; Q
    only on A's. B also holds shares in A, so B is a party of A's network and
    the subject of its own. Each bundle is stamped as a lookup stamps one."""
    a = _company("10000001", "Alpha OÜ")
    b = _company("10000002", "Beta AS")
    p = _person("p-shared", "Piret Shared")
    q = _person("q-single", "Quentin Single")
    bundle_a = stamp_bundle(
        [a, p, q, b, _board(a, p, "p"), _board(a, q, "q"), _holding(a, b, "b")],
        source_id="ariregister", hit_id="10000001", record=False,
    )
    bundle_b = stamp_bundle(
        [b, p, _board(b, p, "p")], source_id="ariregister", hit_id="10000002", record=False,
    )
    return {"a": a, "b": b, "p": p, "q": q, "bundle_a": bundle_a, "bundle_b": bundle_b,
            "subjects": {a["recordId"], b["recordId"]}}


def _merged(n: dict[str, Any], *, primaries_last: bool = False) -> list[dict[str, Any]]:
    """The release file's shape: one statement per party recordId, one per
    relationship statementId. A's bundle comes first, so B's thin copy from
    A's shareholder list is the one kept unless ``primaries_last`` puts B's
    own record after it (the sweep's rank keeps a primary either way; the
    rule must not depend on which copy survived)."""
    out: dict[str, dict[str, Any]] = {}
    order = n["bundle_a"] + n["bundle_b"]
    for st in order:
        key = st["recordId"] if st["recordType"] != "relationship" else st["statementId"]
        if primaries_last or key not in out:
            out[key] = st
    return list(out.values())


def _by_record(statements: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {s["recordId"]: s for s in statements if s["recordType"] != "relationship"}


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def test_a_bundle_stamp_alone_leaves_a_shared_party_naming_whichever_copy_survived() -> None:
    """The problem, before settling: P is kept from A's bundle and names A;
    B's own record, kept from A's shareholder list, names A too."""
    n = _networks()
    kept = _by_record(_merged(n))
    assert kept[n["p"]["recordId"]]["declarationSubject"] == n["a"]["recordId"]
    assert kept[n["b"]["recordId"]]["declarationSubject"] == n["a"]["recordId"]
    problems = release_declaration_problems(_merged(n), n["subjects"])
    assert any("a network subject's record names" in p for p in problems)


@pytest.mark.parametrize("primaries_last", [False, True])
def test_settling_names_one_network_by_a_rule_independent_of_build_order(
    primaries_last: bool,
) -> None:
    n = _networks()
    settled = settle_shared_parties(_merged(n, primaries_last=primaries_last), n["subjects"])
    kept = _by_record(settled)
    a, b = n["a"]["recordId"], n["b"]["recordId"]
    # Each network's subject names itself, whichever copy of it was kept.
    assert kept[a]["declarationSubject"] == a
    assert kept[b]["declarationSubject"] == b
    # The shared person names the first of its networks by recordId.
    assert kept[n["p"]["recordId"]]["declarationSubject"] == min(a, b)
    # A party in one network names that network.
    assert kept[n["q"]["recordId"]]["declarationSubject"] == a
    # Relationships keep the subject of the bundle they were mapped in.
    for st in settled:
        if st["recordType"] == "relationship":
            assert st["declarationSubject"] == st["recordDetails"]["subject"]
    assert release_declaration_problems(settled, n["subjects"]) == []


def test_settling_changes_no_statement_id_and_copies_only_what_changes() -> None:
    n = _networks()
    merged = _merged(n)
    settled = settle_shared_parties(merged, n["subjects"])
    assert [s["statementId"] for s in settled] == [s["statementId"] for s in merged]
    for before, after in zip(merged, settled):
        if before.get("declarationSubject") == after.get("declarationSubject"):
            assert after is before
        else:
            assert after is not before
    # The input is never mutated.
    assert _by_record(merged)[n["b"]["recordId"]]["declarationSubject"] == n["a"]["recordId"]


def test_a_party_no_network_references_keeps_the_network_its_bundle_gave_it() -> None:
    """A succession partner the register names in a note: an entity statement
    with no relationship. Its bundle's stamp stands."""
    n = _networks()
    partner = _company("10000099", "Gamma OÜ")
    bundle = stamp_bundle(
        [n["a"], partner], source_id="ariregister", hit_id="10000001", record=False
    )
    networks = relationship_networks(bundle, n["subjects"])
    out = settle_party(bundle[1], n["subjects"], networks)
    assert out is bundle[1]
    assert out["declarationSubject"] == n["a"]["recordId"]
    assert declaration_problem(out, n["subjects"], relationship_networks(bundle)) is None


def test_another_publishers_statements_are_left_alone() -> None:
    oo = {
        "statementId": "oo-1", "recordId": "oo-person", "recordType": "person",
        "declarationSubject": "oo-person",
        "publicationDetails": {"publisher": {"name": "Open Ownership"}},
        "recordDetails": {"personType": "knownPerson"},
    }
    assert settle_party(oo, {"x"}, {"oo-person": {"x"}}) is oo
    assert declaration_problem(oo, {"x"}, {}) is None


# ---------------------------------------------------------------------------
# The check
# ---------------------------------------------------------------------------


def _factory_default(n: dict[str, Any]) -> list[dict[str, Any]]:
    """The pre-phase shape: every party names itself, every relationship its
    immediate subject — what a release built without stamping carries."""
    out: dict[str, dict[str, Any]] = {}
    for st in [n["a"], n["b"], n["p"], n["q"], _board(n["a"], n["p"], "p"),
               _board(n["a"], n["q"], "q"), _board(n["b"], n["p"], "p")]:
        out[st["statementId"]] = st
    return list(out.values())


def test_the_check_refuses_the_pre_phase_shape() -> None:
    n = _networks()
    problems = release_declaration_problems(_factory_default(n), n["subjects"])
    names = {n["p"]["statementId"], n["q"]["statementId"]}
    flagged = {p.split(":")[0] for p in problems}
    assert names <= flagged
    assert all("is not a network subject" in p for p in problems)


def test_the_check_refuses_a_party_naming_a_network_it_is_not_in() -> None:
    n = _networks()
    settled = settle_shared_parties(_merged(n), n["subjects"])
    q = dict(_by_record(settled)[n["q"]["recordId"]])
    q["declarationSubject"] = n["b"]["recordId"]  # Q sits only on A's board
    networks = relationship_networks(settled)
    problem = declaration_problem(q, n["subjects"], networks)
    assert problem and "none of the relationships referencing it" in problem


# ---------------------------------------------------------------------------
# A lookup's networks, as the laundromat build reads them
# ---------------------------------------------------------------------------


def test_network_subjects_are_the_members_stamp_merged_stamps() -> None:
    gleif = make_entity_statement(
        source_id="gleif", local_id="5299001ABCDEFGHIJK12", name="Alpha OÜ",
        identifiers=[{"id": "5299001ABCDEFGHIJK12", "scheme": "XI-LEI"},
                     {"id": "10000001", "scheme": "EE-ARIREGISTER"}],
        jurisdiction=("Estonia", "EE"),
    )
    n = _networks()
    merged = [gleif, *n["bundle_a"]]
    subjects = network_subjects(merged, "5299001ABCDEFGHIJK12")
    assert subjects == {"gleif": gleif["recordId"], "ariregister": n["a"]["recordId"]}
    stamped = stamp_merged(merged, "5299001ABCDEFGHIJK12")
    assert {s["declarationSubject"] for s in stamped} == set(subjects.values())


# ---------------------------------------------------------------------------
# The Estonia sweep's assemble stage, driven directly
# ---------------------------------------------------------------------------


def test_the_sweep_settles_a_person_on_two_boards(tmp_path: Path, monkeypatch) -> None:
    """The fixture build has no person on two boards, so the settle step is
    driven here through ``stage_assemble`` and ``stage_check`` themselves:
    the streaming two-pass path the real file goes through."""
    import argparse

    from tests.test_sweep_estonia import sweep

    n = _networks()
    work, out = tmp_path / "work", tmp_path / "out"
    work.mkdir()
    rows = [(i == 0, s) for i, s in enumerate(n["bundle_a"])] + [
        (i == 0, s) for i, s in enumerate(n["bundle_b"])
    ]
    (work / "gleif.jsonl").write_text("")
    (work / "register.jsonl").write_text(
        "".join(json.dumps({"p": p, "s": s}) + "\n" for p, s in rows)
    )
    (work / "gleif-subjects.txt").write_text("")
    (work / "register-subjects.txt").write_text("\n".join(sorted(n["subjects"])) + "\n")
    (work / "selection.json").write_text(json.dumps({
        "stats": {}, "sources": {"ariregister": {"cut": "2026-10-12"}},
        "lei_code": {}, "missing_codes": [],
    }))
    monkeypatch.setattr(sweep, "write_missing_csv", lambda path, sel: 0)
    args = argparse.Namespace(
        work_dir=str(work), out_dir=str(out), no_beneficial_owners=False, libcove=False
    )
    manifest = sweep.stage_assemble(args)
    with gzip.open(out / f"{manifest['dataset']}.jsonl.gz", "rt", encoding="utf-8") as fh:
        kept = _by_record([json.loads(line) for line in fh])
    a, b = n["a"]["recordId"], n["b"]["recordId"]
    assert kept[n["p"]["recordId"]]["declarationSubject"] == min(a, b)
    assert kept[b]["declarationSubject"] == b
    assert manifest["network_subjects"] == 2
    assert any("declarationSubject" in note for note in manifest["notes"])
    assert sweep.stage_check(args)["problem_count"] == 0


def test_the_sweep_refuses_a_work_directory_without_recorded_subjects(tmp_path: Path) -> None:
    from tests.test_sweep_estonia import sweep

    with pytest.raises(SystemExit, match="predates declarationSubject stamping"):
        sweep.read_subjects(tmp_path)


# ---------------------------------------------------------------------------
# The laundromat build's assemble, on raws cached before the stamping
# ---------------------------------------------------------------------------


def _ch_company(lei: str, number: str, name: str) -> dict[str, Any]:
    return make_entity_statement(
        source_id="companies_house", local_id=number, name=name,
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[{"id": number, "scheme": "GB-COH"}, {"id": lei, "scheme": "XI-LEI"}],
    )


def test_the_laundromat_build_stamps_old_raws_and_settles_a_shared_director() -> None:
    """Two LEI subjects whose Companies House records share a director, in
    raws cached before the declaration-subject phase (every party names
    itself). The build stamps each subject's bundle, then settles the shared
    director to one of the two networks."""
    from tests.test_build_laundromat_dataset import bld

    lei_a, lei_b = "549300AAAAAAAAAAAA11", "549300BBBBBBBBBBBB22"
    a = _ch_company(lei_a, "OC000001", "Alpha LLP")
    b = _ch_company(lei_b, "OC000002", "Beta LLP")
    director = make_person_statement(
        source_id="companies_house", local_id="officer-xyz", full_name="Dana Director"
    )

    def rel(company: dict[str, Any]) -> dict[str, Any]:
        return make_relationship_statement(
            source_id="companies_house", local_id=f"{company['recordId']}:dana",
            subject_statement_id=company["recordId"],
            interested_party_statement_id=director["recordId"],
            interests=[{"type": "seniorManagingOfficial"}],
        )

    # The factory default: the director names itself.
    assert director["declarationSubject"] == director["recordId"]
    subjects = [
        {"key": f"lei:{lei_a}", "kind": "lei", "lei": lei_a},
        {"key": f"lei:{lei_b}", "kind": "lei", "lei": lei_b},
    ]
    raws = {
        f"lei:{lei_a}": {"status": "ok", "bods": [a, director, rel(a)]},
        f"lei:{lei_b}": {"status": "ok", "bods": [b, director, rel(b)]},
    }
    out = bld.assemble(subjects, raws)
    kept = _by_record(out["statements"])
    assert kept[director["recordId"]]["declarationSubject"] == min(a["recordId"], b["recordId"])
    assert kept[a["recordId"]]["declarationSubject"] == a["recordId"]
    assert kept[b["recordId"]]["declarationSubject"] == b["recordId"]
    assert out["network_subjects"] == 2
    assert out["declaration_subject_problems"] == 0


def test_the_psc_stream_harness_stamps_each_event_as_its_companys_network() -> None:
    """``scripts/map_psc_stream.py`` maps one company's PSC per event; every
    statement it writes names that company's record, as a lookup's would."""
    import importlib.util

    from tests.test_psc_stream import _event

    script = Path(__file__).resolve().parents[1] / "scripts" / "map_psc_stream.py"
    spec = importlib.util.spec_from_file_location("_map_psc_stream", script)
    assert spec and spec.loader
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)

    statements, outcome = harness.map_events(
        [_event("00675216", "pid-1"), _event("00102498", "pid-2", name="Other Owner")],
        fetch_profiles=False,
    )
    assert sum(v for k, v in outcome.items() if k.startswith("mapped")) == 2
    companies = {
        s["recordId"]: s for s in statements
        if s["recordType"] == "entity"
        and any(i.get("scheme") == "GB-COH" for i in s["recordDetails"].get("identifiers") or [])
    }
    assert len(companies) == 2
    for st in statements:
        assert st["declarationSubject"] in companies
        if st["recordType"] == "relationship":
            assert st["declarationSubject"] == st["recordDetails"]["subject"]
    owners = [s for s in statements if s["recordType"] == "person"]
    assert owners and all(o["declarationSubject"] != o["recordId"] for o in owners)
