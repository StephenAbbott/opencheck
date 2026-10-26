#!/usr/bin/env python3
"""Build the Estonia BODS dataset from local data only: the GLEIF Golden Copy
mirror and the daily e-Business Register index.

Every Estonian legal entity holding a Legal Entity Identifier, with the
global LEI graph around it and the national register's shareholders,
officers, supervisory boards and beneficial owners, as BODS v0.4. **No API
calls**: both layers are read from the two SQLite files a production
OpenCheck server reads.

1. **GLEIF layer — the Golden Copy mirror** (``entity_pages.sqlite``, the
   ``entity-pages-latest`` release asset; ``--refresh-mirror`` first brings
   it up to GLEIF's latest publish with the delta files, the in-process
   refresh's own code). Every LEI whose legal jurisdiction is ``EE`` is
   mapped with OpenCheck's own GLEIF mapper from the mirror bundle a lookup
   is served from (``GleifAdapter._snapshot_bundle``), so the statements are
   the ones a lookup publishes: ``opencheckSourceId``, the mirror's two
   provenance clocks, Legal Entity Event dates (Phase 305), the LEI-status
   note (Phase 255), reporting exceptions as filed. A foreign parent is a
   boundary entity on its Estonian child's edge; a foreign company consolidated
   by an Estonian one contributes its own record and its edges to Estonian
   parents only. Each GLEIF edge is published once, from its child — the
   side GLEIF's relationship record names as the start node.
2. **Register layer — the e-Business Register index** (``ariregister.sqlite``,
   the ``ariregister-index`` release asset). Every LEI holder whose GLEIF
   record files an ``RA000181`` registry code, plus every Estonian company
   above one in the index's shareholder lists (the upward closure, without
   the lookup's caps), is mapped with ``map_ariregister`` on the index tier:
   current lists, off-card persons (supervisory boards, AS shareholders,
   founders), general data, rulings and pledges. The ended-ownership history
   is **not** read — it comes from RIK's credentialed services, and this
   dataset stays bulk-only.

The layers join on ``EE-ARIREGISTER`` — the registry code the GLEIF entity
statement and the register's own record both carry.

Stages (each resumable; ``all`` runs them in order)::

    select    → work/selection.json   (the LEIs, their registry codes, the
                                       foreign reporters, the closure)
    gleif     → work/gleif.jsonl
    register  → work/register.jsonl
    assemble  → <out>/estonia-<cut>.jsonl.gz, manifest.json, LICENCES.md
    check     → <out>/checks.json      (references, identifiers, personal data;
                                       --libcove adds lib-cove-bods)

Usage (from ``backend/``)::

    uv run python scripts/sweep_estonia.py --stage all \\
        --entity-pages-db ../data/entity_pages.sqlite --refresh-mirror \\
        --ariregister-index ../data/ariregister.sqlite \\
        --out-dir ../data/estonia [--libcove]

Licence of the combined output: **CC-BY-4.0** (GLEIF CC0 + RIK CC-BY-4.0;
most restrictive wins). Every statement names its source
(``source.opencheckSourceId``), so the RDF projection stamps a per-statement
licence without ``--fallback-license``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))  # the opencheck package

#: The registry code's scheme — the one ``gleif_registration_scheme`` maps
#: RA000181 to and the register's own mapper writes (Phase 239).
EE_SCHEME = "EE-ARIREGISTER"
#: GLEIF's registration authority for the Estonian Commercial Register.
EE_RA = "RA000181"
#: The scheme the July 2026 build carried for the registry code. Kept here
#: only so ``check`` can say it is gone (the register still files the VAT
#: number under it, which is correct).
VAT_SCHEME = "EE-KMKR"

#: LEI registration states that are not a record of a company at all.
_UNUSABLE_REGISTRATION = frozenset({"ANNULLED", "DUPLICATE"})

DATASET_PREFIX = "estonia"


# ---------------------------------------------------------------------------
# Configuration: point the package at the two files before it reads settings
# ---------------------------------------------------------------------------


def configure(entity_pages_db: Path, ariregister_index: Path) -> None:
    """Name the two files in the environment the package's settings read,
    and turn live calls off — nothing in this build may reach a network."""
    os.environ["OPENCHECK_ENTITY_PAGES_DB_FILE"] = str(entity_pages_db)
    os.environ["ARIREGISTER_DB_FILE"] = str(ariregister_index)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "false"
    from opencheck import entity_pages
    from opencheck.config import get_settings
    from opencheck.sources import ariregister

    get_settings.cache_clear()
    entity_pages.reset_store_for_tests()
    ariregister.reset_connection()


# ---------------------------------------------------------------------------
# Stage: refresh the mirror (optional, the only stage that downloads)
# ---------------------------------------------------------------------------


def refresh_mirror(db_path: Path) -> dict[str, Any]:
    """Bring the mirror up to GLEIF's latest Golden Copy publish with the
    smallest delta that covers the gap — ``mirror_refresh.apply_delta``, the
    code the server runs every few hours. A gap wider than a month needs a
    fresh release asset; this says so rather than guessing."""
    from opencheck import entity_pages as ep
    from opencheck import mirror_build as mb
    from opencheck import mirror_refresh as mr

    store = ep.EntityStore(db_path)
    watermark = store.watermark()
    publish = mb._latest_publish()
    publish_at = mr.parse_publish_datetime(publish.get("publish_date"))
    if watermark is None or publish_at is None:
        raise SystemExit("refresh-mirror: the mirror or the publish API has no date")
    gap = publish_at - watermark
    if gap <= dt.timedelta(0):
        return {"mirror_refresh": "up_to_date", "publish": publish["publish_date"]}
    delta = mr.choose_delta(gap)
    if delta is None:
        raise SystemExit(
            f"refresh-mirror: the mirror is {gap} behind GLEIF, wider than any delta; "
            "download a fresh entity-pages-latest asset first"
        )
    counts = mr.apply_delta(db_path, publish, delta)
    ep.reset_store_for_tests()
    return {
        "mirror_refresh": "applied",
        "delta": delta,
        "publish": publish["publish_date"],
        "rows": counts,
    }


# ---------------------------------------------------------------------------
# Stage: select
# ---------------------------------------------------------------------------


def registry_code_of(detail: dict[str, Any] | None) -> str | None:
    """The Estonian registry code a GLEIF record files, or None.

    Only a number filed under RA000181 is read: GLEIF records some Estonian
    entities under another authority (a fund's, a bank's), and their
    ``registeredAs`` is not a Commercial Register code.
    """
    from opencheck.sources.ariregister import normalise_registry_code

    detail = detail or {}
    if ((detail.get("registeredAt") or {}).get("id") or "") != EE_RA:
        return None
    raw = str(detail.get("registeredAs") or "").strip()
    if not raw:
        return None
    try:
        code = normalise_registry_code(raw)
    except ValueError:
        return None
    return code if code.isdigit() else None


def register_closure(
    codes: Iterable[str], *, guard: int = 200_000
) -> tuple[list[str], dict[str, int]]:
    """Every Estonian company in the index above ``codes`` in a shareholder
    list (the chain walk's holders — OÜ shareholders, AS shareholders on the
    off-card list, partners), followed to the top with no depth cap.

    Returns the codes the index holds — the inputs first, in input order,
    then the companies above them, sorted — and counts of the holders that
    could not be followed (a foreign register, or a code the index lacks).
    """
    from opencheck.sources import ariregister

    inputs = list(dict.fromkeys(codes))
    seen = set(inputs)
    frontier = list(inputs)
    held: set[str] = set()
    stats: Counter[str] = Counter()
    while frontier:
        code = frontier.pop()
        record = ariregister.entity_record(code)
        if record is None:
            if code not in inputs:
                stats["holders_not_in_index"] += 1
            continue
        held.add(code)
        for holder, _row in ariregister._corporate_holders(record):
            if holder is None:
                stats["foreign_holders"] += 1
            elif holder not in seen:
                seen.add(holder)
                frontier.append(holder)
        if len(seen) > guard:  # pragma: no cover - a guard, not a cap
            raise SystemExit("register closure grew past the guard; check the index")
    above = sorted(held - set(inputs))
    return [c for c in inputs if c in held] + above, dict(stats)


def stage_select(args: argparse.Namespace) -> dict[str, Any]:
    from opencheck import entity_pages as ep
    from opencheck.sources import ariregister

    store = ep.get_store()
    if store is None or not store.is_mirror:
        raise SystemExit("select: --entity-pages-db must be a Golden Copy mirror (schema 2)")
    if not ariregister.index_available():
        raise SystemExit("select: --ariregister-index is not a readable index")

    conn = sqlite3.connect(f"file:{store.path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT lei, entity_status, registration_status FROM entities "
        "WHERE jurisdiction = ? ORDER BY lei",
        (args.jurisdiction,),
    ).fetchall()
    excluded: Counter[str] = Counter()
    scope: list[str] = []
    for lei, entity_status, registration_status in rows:
        if (registration_status or "").upper() in _UNUSABLE_REGISTRATION:
            excluded[f"registration_{registration_status.lower()}"] += 1
            continue
        if (entity_status or "").upper() != "ACTIVE" and not args.include_inactive:
            excluded[f"entity_{(entity_status or 'null').lower()}"] += 1
            continue
        scope.append(lei)
    if args.limit:
        scope = scope[: args.limit]
    scope_set = set(scope)

    # Foreign companies GLEIF records as consolidated by an Estonian one.
    # Their edges are published from their side (the child is the start node
    # of GLEIF's relationship record), as every other GLEIF edge here is.
    reporters: list[str] = []
    for lei, direct, ultimate in conn.execute(
        "SELECT lei, direct_parent_lei, ultimate_parent_lei FROM entities "
        "WHERE (direct_parent_lei IS NOT NULL OR ultimate_parent_lei IS NOT NULL) "
        "AND (jurisdiction IS NULL OR jurisdiction != ?) ORDER BY lei",
        (args.jurisdiction,),
    ):
        if direct in scope_set or ultimate in scope_set:
            reporters.append(lei)
    conn.close()

    lei_code: dict[str, str] = {}
    other_ra = 0
    for chunk_start in range(0, len(scope), 500):
        batch = store.get_many(scope[chunk_start : chunk_start + 500])
        for lei, row in batch.items():
            code = registry_code_of(row.detail)
            if code:
                lei_code[lei] = code
            else:
                other_ra += 1

    codes = sorted(set(lei_code.values()))
    closure, closure_stats = register_closure(codes)
    held = set(closure)
    missing_codes = [c for c in codes if c not in held]

    meta = ariregister.read_meta()
    watermark = store.watermark()
    selection = {
        "scope": scope,
        "foreign_reporters": reporters,
        "lei_code": lei_code,
        "register_codes": closure,
        "missing_codes": missing_codes,
        "stats": {
            "gleif_jurisdiction": args.jurisdiction,
            "gleif_records_in_jurisdiction": len(rows),
            "gleif_entities_in_scope": len(scope),
            "gleif_excluded": dict(sorted(excluded.items())),
            "gleif_foreign_consolidated_entities": len(reporters),
            "lei_with_registry_code": len(lei_code),
            "lei_without_ra000181_code": other_ra,
            "ariregister_companies_matched": len(codes) - len(missing_codes),
            "ariregister_companies_missing": len(missing_codes),
            "ariregister_chain_companies": len(closure) - (len(codes) - len(missing_codes)),
            "ariregister_chain": closure_stats,
        },
        "sources": {
            "gleif": {
                "via": "OpenCheck Golden Copy mirror (entity_pages.sqlite)",
                "watermark": watermark.isoformat() if watermark else None,
                "refreshed_at": (
                    store.retrieved_at().isoformat() if store.retrieved_at() else None
                ),
                "mode": store.meta().get("mode"),
            },
            "ariregister": {
                "via": "OpenCheck e-Business Register index (ariregister.sqlite)",
                "cut": meta.get("cut"),
                "built_at": meta.get("built_at"),
                "source_files": (meta.get("source_files") or "").split(",")
                if meta.get("source_files")
                else [],
            },
        },
    }
    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    (work / "selection.json").write_text(json.dumps(selection))
    print(json.dumps(selection["stats"], indent=1))
    return selection


# ---------------------------------------------------------------------------
# Stage: gleif
# ---------------------------------------------------------------------------


def _declared(
    statements: list[dict[str, Any]],
    *,
    source_id: str,
    ref: str | None = None,
    hit_id: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """One bundle stamped as its own declared network (``stamp_bundle``, the
    rule a lookup applies) and the ``recordId`` of its subject, or ``None``
    when the subject could not be found — the build records the subjects so
    the assemble and check stages know which records are networks."""
    from opencheck.bods.declaration import find_subject, stamp_bundle

    subject, _outcome = find_subject(statements, ref=ref, hit_id=hit_id)
    rid = subject.get("recordId") if subject else None
    stamped = stamp_bundle(
        statements, source_id=source_id, ref=ref, hit_id=hit_id, record=False
    )
    return stamped, rid


#: The network subjects each mapping stage recorded, one ``recordId`` a line.
SUBJECT_FILES = ("gleif-subjects.txt", "register-subjects.txt")


def read_subjects(work: Path) -> set[str]:
    """Every network subject the gleif and register stages recorded. Raises
    when a stage wrote none — a work directory built before the
    declaration-subject stamping cannot be assembled or checked."""
    out: set[str] = set()
    for name in SUBJECT_FILES:
        path = work / name
        if not path.exists():
            raise SystemExit(
                f"{path} is missing: re-run the gleif and register stages "
                "(the work directory predates declarationSubject stamping)"
            )
        out.update(line.strip() for line in path.read_text().splitlines() if line.strip())
    return out


def _lei_of(statement: dict[str, Any]) -> str | None:
    for ident in (statement.get("recordDetails") or {}).get("identifiers") or []:
        if ident.get("scheme") == "XI-LEI":
            return str(ident.get("id") or "").upper() or None
    return None


def gleif_statements(lei: str, scope: set[str], *, reporter: bool = False) -> list[dict[str, Any]]:
    """The GLEIF statements one LEI contributes (see ``gleif_network``)."""
    return gleif_network(lei, scope, reporter=reporter)[0]


def gleif_network(
    lei: str, scope: set[str], *, reporter: bool = False
) -> tuple[list[dict[str, Any]], str | None]:
    """The GLEIF statements one LEI contributes, from the mirror bundle a
    lookup is served from, without its children.

    Children are left out because each child publishes its own edge (the
    lookup keys a parent edge ``<child>:<kind>-parent:<parent>`` and a child
    edge ``<parent>:direct-child:<child>``: two statement ids for one GLEIF
    relationship record). A ``reporter`` — a foreign company consolidated
    by an Estonian one — contributes its own record and only the edges to
    Estonian parents, not its other parents or its reporting exceptions.
    """
    from opencheck import provenance
    from opencheck.bods import map_gleif
    from opencheck.sources.gleif import GleifAdapter

    with provenance.recording() as recorder:
        bundle = GleifAdapter()._snapshot_bundle(lei, reason="mirror")
    if bundle is None:
        return [], None
    bundle["direct_children"] = []
    bundle.pop("direct_child_relationships", None)
    if reporter:
        for kind in ("direct", "ultimate"):
            parent = bundle.get(f"{kind}_parent") or {}
            parent_lei = (
                (parent.get("attributes") or {}).get("lei") or parent.get("id") or ""
            ).upper()
            if parent_lei not in scope:
                bundle[f"{kind}_parent"] = None
                bundle.pop(f"{kind}_parent_relationship", None)
            bundle[f"{kind}_parent_exception"] = None
    with provenance.mapping_provenance(recorder.resolve(is_stub=False)):
        statements = list(map_gleif(bundle).statements)
    return _declared(statements, source_id="gleif", ref=lei)


def stage_gleif(args: argparse.Namespace) -> dict[str, int]:
    work = Path(args.work_dir)
    sel = json.loads((work / "selection.json").read_text())
    scope = set(sel["scope"])
    counts: Counter[str] = Counter()
    with open(work / "gleif.jsonl", "w", encoding="utf-8") as fh, open(
        work / "gleif-subjects.txt", "w", encoding="utf-8"
    ) as subjects:
        for i, lei in enumerate(sel["scope"] + sel["foreign_reporters"]):
            reporter = lei not in scope
            statements, subject = gleif_network(lei, scope, reporter=reporter)
            if not statements:
                counts["not_in_mirror"] += 1
                continue
            if subject:
                subjects.write(subject + "\n")
            else:
                counts["subject_unresolved"] += 1
            for n, statement in enumerate(statements):
                # The subject's own entity statement is what it published of
                # itself; the parent statements beside it are copies.
                primary = n == 0
                fh.write(json.dumps({"p": primary, "s": statement}, ensure_ascii=False) + "\n")
                counts[statement.get("recordType") or "unknown"] += 1
            if i and i % 5000 == 0:
                print(f"gleif: {i:,} LEIs mapped", flush=True)
    print("gleif stage done:", dict(counts))
    return dict(counts)


# ---------------------------------------------------------------------------
# Stage: register
# ---------------------------------------------------------------------------


def register_statements(code: str, *, include_bo: bool = True) -> list[dict[str, Any]] | None:
    """The register's statements for one company (see ``register_network``)."""
    network = register_network(code, include_bo=include_bo)
    return None if network is None else network[0]


def register_network(
    code: str, *, include_bo: bool = True
) -> tuple[list[dict[str, Any]], str | None] | None:
    """The register's statements for one company, as a lookup maps them on
    the index tier — without the chain walk (the dataset carries every
    company above as its own record) and without the ended-ownership
    history (it is read from RIK's credentialed services, not the files).
    None when the index does not hold the code."""
    from opencheck import provenance
    from opencheck.bods import map_ariregister
    from opencheck.sources import ariregister
    from opencheck.sources.schemas import validate_raw
    from opencheck.sources.schemas.ariregister import AriregisterBundle

    record = ariregister.entity_record(code)
    if record is None:
        return None
    meta = ariregister.read_meta()
    with provenance.recording() as recorder:
        ariregister.declare_snapshot(meta)
    bundle = ariregister.AriregisterAdapter._index_bundle(code, record, meta)
    if not include_bo:
        bundle["beneficial_owners"] = []
    validate_raw("ariregister", AriregisterBundle, bundle)
    with provenance.mapping_provenance(recorder.resolve(is_stub=False)):
        statements = list(map_ariregister(bundle))
    return _declared(statements, source_id="ariregister", hit_id=code)


def stage_register(args: argparse.Namespace) -> dict[str, int]:
    work = Path(args.work_dir)
    sel = json.loads((work / "selection.json").read_text())
    counts: Counter[str] = Counter()
    include_bo = not args.no_beneficial_owners
    with open(work / "register.jsonl", "w", encoding="utf-8") as fh, open(
        work / "register-subjects.txt", "w", encoding="utf-8"
    ) as subjects:
        for i, code in enumerate(sel["register_codes"]):
            network = register_network(code, include_bo=include_bo)
            if network is None:
                counts["not_in_index"] += 1
                continue
            statements, subject = network
            if subject:
                subjects.write(subject + "\n")
            else:
                counts["subject_unresolved"] += 1
            for n, statement in enumerate(statements):
                # The first statement is the company's own register record;
                # a shareholder row elsewhere builds a thinner statement under
                # the same id, which the record supersedes.
                fh.write(json.dumps({"p": n == 0, "s": statement}, ensure_ascii=False) + "\n")
                counts[statement.get("recordType") or "unknown"] += 1
            if i and i % 5000 == 0:
                print(f"register: {i:,} companies mapped", flush=True)
    print("register stage done:", dict(counts))
    return dict(counts)


# ---------------------------------------------------------------------------
# Stage: assemble
# ---------------------------------------------------------------------------

LICENCES_MD = """# Data licences — OpenCheck Estonia dataset

This dataset combines two bulk sources. The combined output is licensed
under the more restrictive of the two: **CC BY 4.0**. Every statement names
its source (`source.opencheckSourceId`: `gleif` or `ariregister`), so the
licence of each statement can be read from the statement itself.

## GLEIF Level 1 & Level 2 data — CC0 1.0

Source: the [GLEIF Golden Copy](https://www.gleif.org/en/lei-data/gleif-golden-copy)
(LEI records, relationship records and reporting exceptions), read from
OpenCheck's local mirror of it. Licence:
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). No attribution
legally required; acknowledging GLEIF is good practice.

## Estonian e-Business Register open data — CC BY 4.0

Source: open data of the e-Business Register, published by the Centre of
Registers and Information Systems (RIK):
[avaandmed.ariregister.rik.ee](https://avaandmed.ariregister.rik.ee/en)
Licence: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

> Data from the Estonian e-Business Register (e-Äriregister), published
> by the Centre of Registers and Information Systems (RIK), CC BY 4.0.

## Personal data / GDPR

The Estonian layer contains the names of shareholders, officers, supervisory
board members, founders and (where included) beneficial owners, lawfully
published as open data by RIK. The open-data files carry no personal
identification codes (removed since 1 November 2024); OpenCheck keeps a
person apart from another of the same name only by the register's
pseudonymous person key, which is published as an annotation, never as an
identifier. No person's street or e-mail address is carried. Re-users are
obliged to comply with the GDPR when further processing personal data (see
RIK's terms: https://avaandmed.ariregister.rik.ee/en/terms-service). This
dataset is a dated snapshot — always check the live registers for current
data.
"""


def _dedupe_db(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE s (id TEXT PRIMARY KEY, rtype TEXT, rank INTEGER, body TEXT)")
    return conn


def _parts(work: Path) -> Iterator[tuple[bool, str, dict[str, Any]]]:
    for name in ("gleif.jsonl", "register.jsonl"):
        with open(work / name, encoding="utf-8") as fh:
            for line in fh:
                item = json.loads(line)
                statement = item["s"]
                yield bool(item.get("p")), json.dumps(statement, ensure_ascii=False), statement


def dataset_day(sel: dict[str, Any]) -> str:
    """The dataset's date: the register cut, the later of the two clocks
    (the mirror is refreshed to the same day by ``--refresh-mirror``)."""
    cut = (sel.get("sources") or {}).get("ariregister", {}).get("cut")
    return cut or dt.date.today().isoformat()


def stage_assemble(args: argparse.Namespace) -> dict[str, Any]:
    work = Path(args.work_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sel = json.loads((work / "selection.json").read_text())
    day = dataset_day(sel)
    name = f"{DATASET_PREFIX}-{day}"

    # One statement per record. A company's own record outranks a thinner
    # statement another company's list built for the same party; between two
    # of the same rank the longer wins. A party is keyed on its recordId — the
    # statementId is content-addressed (the record-ID ticket's phase B), so
    # the thin and the full statement of one party carry different ones — and
    # a relationship on its statementId, as ``unique_statements`` does.
    conn = _dedupe_db(work / "dedupe.sqlite")
    total_rows = 0
    for primary, body, statement in _parts(work):
        if statement.get("recordType") in ("entity", "person"):
            sid = statement.get("recordId") or statement.get("statementId")
        else:
            sid = statement.get("statementId")
        if not sid:
            continue
        total_rows += 1
        rank = (1 if primary else 0) * 10_000_000 + len(body)
        conn.execute(
            "INSERT INTO s VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "rtype = excluded.rtype, rank = excluded.rank, body = excluded.body "
            "WHERE excluded.rank > s.rank",
            (sid, statement.get("recordType") or "unknown", rank, body),
        )
    kept = conn.execute("SELECT COUNT(*) FROM s").fetchone()[0]
    duplicates = total_rows - kept

    # One record per party, many networks: a party kept from one bundle may
    # sit in others too. Each relationship keeps its bundle's subject; each
    # party is settled to one network by the rule in ``bods.declaration``
    # (Stephen, 25 Oct 2026). The networks are the subjects the gleif and
    # register stages recorded, never values read back from the file.
    from opencheck.bods.declaration import relationship_networks, settle_party

    subjects = read_subjects(work)
    networks = relationship_networks(
        (
            json.loads(body)
            for (body,) in conn.execute("SELECT body FROM s WHERE rtype = 'relationship'")
        ),
        subjects,
    )
    settled = 0

    counts: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    out_path = out_dir / f"{name}.jsonl.gz"
    with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as gz:
        # Entities and persons before relationships, each in id order, so
        # every reference points backwards and a rebuild is byte-stable.
        for clause in ("rtype != 'relationship'", "rtype = 'relationship'"):
            for rtype, body in conn.execute(
                f"SELECT rtype, body FROM s WHERE {clause} ORDER BY id"
            ):
                if rtype != "relationship":
                    statement = json.loads(body)
                    party = settle_party(statement, subjects, networks)
                    if party is not statement:
                        settled += 1
                        body = json.dumps(party, ensure_ascii=False)
                gz.write(body + "\n")
                counts[rtype] += 1
                source_id = (
                    (json.loads(body).get("source") or {}).get("opencheckSourceId")
                ) or "none"
                by_source[source_id] += 1
    conn.close()

    manifest = {
        "dataset": name,
        "generated": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "bods_version": "0.4",
        "licence": "CC-BY-4.0 (combined; GLEIF layer CC0-1.0, RIK layer CC-BY-4.0)",
        "statement_counts": dict(counts),
        "statements_by_source": dict(by_source),
        "total_statements": sum(counts.values()),
        "duplicate_statements_merged": duplicates,
        "network_subjects": len(subjects),
        "parties_settled_to_one_network": settled,
        **sel["stats"],
        "beneficial_owners_included": not args.no_beneficial_owners,
        "ownership_history_included": False,
        "registry_code_scheme": EE_SCHEME,
        "sources": sel["sources"],
        "notes": [
            "Built by backend/scripts/sweep_estonia.py from local data only: no API calls.",
            "GLEIF layer: every LEI with legal jurisdiction EE (entity status ACTIVE unless "
            "--include-inactive; ANNULLED and DUPLICATE registrations left out), mapped with "
            "OpenCheck's GLEIF mapper from the Golden Copy mirror. Foreign parents are boundary "
            "entities; a foreign company consolidated by an Estonian one carries only its own "
            "record and its edges to Estonian parents. Each GLEIF edge is published once, from "
            "the child.",
            "The mirror holds the Golden Copy, not GLEIF's mapping files: the OpenCorporates, "
            "S&P, BIC, MIC and QCC cross-reference identifiers a live lookup adds are not carried.",
            "Register layer: every LEI holder's registry code (RA000181) plus every Estonian "
            "company above one in the index's shareholder lists, each mapped on the index tier "
            "(current lists, off-card persons, general data, rulings, pledges). The register's "
            "ended-ownership history is not included: it comes from RIK's credentialed services.",
            "register-missing.csv lists the LEIs whose registry code the register's open data "
            "does not hold: GLEIF records them as active, the register has deleted the company "
            "(the open data carries no deleted company) or entered it after the cut. Their GLEIF "
            "statements are kept; they have no register layer.",
            "The two layers join on the shared EE-ARIREGISTER identifier (the registry code); "
            "EE-KMKR is the VAT number only.",
            "Entity and person statements precede all relationship statements.",
            "declarationSubject: each company mapped is its own declared network (the "
            "rule a lookup applies to one source's bundle). A relationship names the subject "
            "of the network it was mapped in, and a network's subject names itself. A party "
            "that appears in several networks (a person on two boards, a company that holds "
            "shares in several others) is published once, as one record, and names one "
            "network: the first, by recordId, of the networks whose relationships reference "
            "it. BODS's Record identifiers guidance would give such a party one record per "
            "network; the dataset keeps one so the party stays one node across companies.",
        ],
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=1, ensure_ascii=False) + "\n"
    )
    (out_dir / "LICENCES.md").write_text(LICENCES_MD)
    write_missing_csv(out_dir / "register-missing.csv", sel)
    print(
        json.dumps(
            {
                k: manifest[k]
                for k in (
                    "dataset",
                    "statement_counts",
                    "total_statements",
                    "duplicate_statements_merged",
                )
            },
            indent=1,
        )
    )
    return manifest


def write_missing_csv(path: Path, sel: dict[str, Any]) -> int:
    """One row per LEI whose registry code the register's open data does
    not hold: GLEIF still records the company, the register has deleted it
    (the files carry no deleted company) or entered it after the cut. The
    LEI's own registration status says how stale GLEIF's record is."""
    import csv

    from opencheck import entity_pages as ep

    missing = set(sel.get("missing_codes") or [])
    store = ep.get_store()
    rows = []
    for lei, code in sorted(sel["lei_code"].items()):
        if code not in missing:
            continue
        row = store.get(lei) if store is not None else None
        rows.append(
            [
                lei,
                code,
                row.name if row else "",
                row.registration_status if row else "",
                row.entity_status if row else "",
                (row.last_updated or "")[:10] if row else "",
            ]
        )
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "lei",
                "registry_code",
                "gleif_legal_name",
                "lei_registration_status",
                "gleif_entity_status",
                "gleif_last_update",
            ]
        )
        writer.writerows(rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Stage: check
# ---------------------------------------------------------------------------

_PERSONAL_CODE = re.compile(r"(?<!\d)([1-6]\d{10})(?!\d)")

#: A Nasdaq CSD account prefix ("Omanikukonto:", "Lihtkonto:") at the start
#: of a name — the mapper removes it (keeping the register's text in an
#: annotation), so one in the file means a statement bypassed the mapper.
_ACCOUNT_PREFIX = re.compile(r"^\s*(?:omanikukonto|lihtkonto)\s*:", re.IGNORECASE)


def isikukood_valid(code: str) -> bool:
    """An Estonian personal identification code's own checks: a real
    birth date in digits 2–7 and the check digit (weights 1..9,1 then
    3..9,1,2,3)."""
    if not re.fullmatch(r"[1-6]\d{10}", code):
        return False
    century = {"1": 1800, "2": 1800, "3": 1900, "4": 1900, "5": 2000, "6": 2000}[code[0]]
    try:
        dt.date(century + int(code[1:3]), int(code[3:5]), int(code[5:7]))
    except ValueError:
        return False
    digits = [int(c) for c in code]
    for weights in ((1, 2, 3, 4, 5, 6, 7, 8, 9, 1), (3, 4, 5, 6, 7, 8, 9, 1, 2, 3)):
        total = sum(d * w for d, w in zip(digits[:10], weights, strict=True))
        if total % 11 != 10:
            return total % 11 == digits[10]
    return digits[10] == 0


def check_statements(
    statements: Iterable[dict[str, Any]],
    subjects: set[str] | None = None,
    networks: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    """What the release promises, counted over the file: every reference
    resolves, the registry code is ``EE-ARIREGISTER`` throughout, no personal
    code or person's address is carried, and — given the network ``subjects``
    the build recorded — every ``declarationSubject`` follows the
    one-record-one-network rule (``bods.declaration.declaration_problem``).
    ``networks`` is that rule's relationship pass over the same file; without
    it the statements are read into memory to compute it. ``problems`` empty
    means the file can be published."""
    from opencheck.bods.declaration import declaration_problem, relationship_networks

    if subjects is not None and networks is None:
        statements = list(statements)
        networks = relationship_networks(statements)
    ids: set[str] = set()
    record_ids: set[str] = set()
    relationships: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    problems: list[str] = []
    for st in statements:
        sid = st.get("statementId")
        if sid in ids:
            problems.append(f"duplicate statementId {sid}")
        if subjects is not None and networks is not None:
            declared = declaration_problem(st, subjects, networks)
            if declared:
                counts["declaration_subject_problems"] += 1
                problems.append(declared)
        ids.add(sid)
        if st.get("recordId"):
            record_ids.add(st["recordId"])
        rd = st.get("recordDetails") or {}
        rtype = st.get("recordType")
        counts[rtype or "unknown"] += 1
        if not (st.get("source") or {}).get("opencheckSourceId"):
            counts["no_source_id"] += 1
        if rtype == "relationship":
            relationships.append(rd)
            continue
        if rtype == "entity":
            names = [rd.get("name")]
        else:
            names = [n.get("fullName") for n in rd.get("names") or []]
        if any(_ACCOUNT_PREFIX.match(str(n or "")) for n in names):
            problems.append(f"{sid}: a name starts with a Nasdaq CSD account prefix")
        for ident in rd.get("identifiers") or []:
            scheme = ident.get("scheme")
            if scheme == EE_SCHEME:
                counts["ee_ariregister_identifiers"] += 1
            elif scheme == VAT_SCHEME:
                counts["ee_kmkr_identifiers"] += 1
                # The VAT number is "EE" + nine digits; a registry code is
                # eight. An eight-digit value under EE-KMKR is the July bug.
                if re.fullmatch(r"\d{8}", str(ident.get("id") or "")):
                    problems.append(f"{sid}: a registry code under {VAT_SCHEME}")
        if rtype == "person":
            # A country of residence is what the files say; a street, a
            # postcode or any address text is what the index never holds.
            if any(a.get("address") or a.get("postCode") for a in rd.get("addresses") or []):
                problems.append(f"{sid}: a person statement carries a street address")
            elif rd.get("addresses"):
                counts["person_country_of_residence"] += 1
            for ident in rd.get("identifiers") or []:
                if isikukood_valid(str(ident.get("id") or "")):
                    problems.append(f"{sid}: a personal code in identifiers")
            for note in st.get("annotations") or []:
                for found in _PERSONAL_CODE.findall(json.dumps(note, ensure_ascii=False)):
                    if isikukood_valid(found):
                        problems.append(f"{sid}: a personal code in an annotation")
    dangling = 0
    for rd in relationships:
        for key in ("subject", "interestedParty"):
            ref = rd.get(key)
            if isinstance(ref, str) and ref not in ids and ref not in record_ids:
                dangling += 1
    if dangling:
        problems.append(f"{dangling} relationship references resolve to no statement")
    counts["dangling_references"] = dangling
    return {"counts": dict(counts), "problems": problems[:200], "problem_count": len(problems)}


def run_libcove(path: Path, work: Path) -> dict[str, Any]:
    """lib-cove-bods over the whole file: schema errors and additional
    checks, counted by type."""
    import libcovebods.data_reader as dr
    import libcovebods.jsonschemavalidate as jsv
    import libcovebods.run_tasks as rt
    from libcovebods.config import LibCoveBODSConfig
    from libcovebods.schema import SchemaBODS

    array = work / "libcove-input.json"
    with gzip.open(path, "rt", encoding="utf-8") as src, open(array, "w", encoding="utf-8") as out:
        out.write("[")
        for i, line in enumerate(src):
            out.write(("," if i else "") + line.strip())
        out.write("]")
    config = LibCoveBODSConfig()
    reader = dr.DataReader(str(array))
    schema = SchemaBODS(data_reader=reader, lib_cove_bods_config=config)
    errors = jsv.JSONSchemaValidator(schema).validate(reader)
    additional = rt.process_additional_checks(reader, config, schema)
    schema_errors: Counter[str] = Counter()
    for e in errors:
        try:
            d = e.json()
            schema_errors[f"{d.get('validator')}: {d.get('message')}"[:200]] += 1
        except Exception:  # noqa: BLE001
            schema_errors[str(e)[:200]] += 1
    checks: Counter[str] = Counter(
        c.get("type", "?") for c in additional.get("additional_checks", [])
    )
    array.unlink()
    return {
        "schema_errors": dict(schema_errors),
        "additional_checks": dict(checks),
        "statistics": {
            k: v
            for k, v in (additional.get("statistics") or {}).items()
            if isinstance(v, (int, float, str))
        },
    }


def stage_check(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    work = Path(args.work_dir)
    manifest = json.loads((out_dir / "manifest.json").read_text())
    path = out_dir / f"{manifest['dataset']}.jsonl.gz"
    from opencheck.bods.declaration import relationship_networks

    subjects = read_subjects(work)
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        networks = relationship_networks(json.loads(line) for line in fh)
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        result = check_statements(
            (json.loads(line) for line in fh), subjects=subjects, networks=networks
        )
    if args.libcove:
        result["libcove"] = run_libcove(path, work)
    (out_dir / "checks.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    print(json.dumps({k: result[k] for k in ("counts", "problem_count")}, indent=1))
    if result["problem_count"]:
        print("\n".join(result["problems"][:20]))
        raise SystemExit("check: the dataset breaks a release rule; see checks.json")
    return result


# ---------------------------------------------------------------------------


STAGES = ("select", "gleif", "register", "assemble", "check")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--stage", default="all", choices=[*STAGES, "all"])
    ap.add_argument(
        "--entity-pages-db",
        required=True,
        type=Path,
        help="the Golden Copy mirror (entity_pages.sqlite)",
    )
    ap.add_argument(
        "--refresh-mirror",
        action="store_true",
        help="apply GLEIF's latest delta to the mirror first (downloads)",
    )
    ap.add_argument(
        "--ariregister-index",
        required=True,
        type=Path,
        help="the e-Business Register index (ariregister.sqlite)",
    )
    ap.add_argument("--out-dir", default="../data/estonia", type=Path)
    ap.add_argument("--work-dir", default=None, type=Path)
    ap.add_argument("--jurisdiction", default="EE")
    ap.add_argument(
        "--include-inactive", action="store_true", help="also map LEIs GLEIF records as INACTIVE"
    )
    ap.add_argument("--limit", type=int, default=None, help="cap the LEIs in scope (testing)")
    ap.add_argument("--no-beneficial-owners", action="store_true")
    ap.add_argument("--libcove", action="store_true", help="run lib-cove-bods in the check stage")
    args = ap.parse_args(argv)
    if args.work_dir is None:
        args.work_dir = args.out_dir / "work"

    configure(args.entity_pages_db, args.ariregister_index)
    if args.refresh_mirror:
        print(json.dumps(refresh_mirror(args.entity_pages_db)))
        configure(args.entity_pages_db, args.ariregister_index)
    stages = STAGES if args.stage == "all" else (args.stage,)
    for stage in stages:
        {
            "select": stage_select,
            "gleif": stage_gleif,
            "register": stage_register,
            "assemble": stage_assemble,
            "check": stage_check,
        }[stage](args)


if __name__ == "__main__":
    main()
