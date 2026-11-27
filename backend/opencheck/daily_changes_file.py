"""The daily BODS file: what GLEIF changed on one day, as BODS v0.4.

The "Daily deltas for OpenCheck" ticket, step C. ``daily_changes`` grades
what the deltas changed; this module turns one day's GLEIF rows into a file
of BODS statements a reader can load without running a single lookup:

- **Mirror-derived GLEIF BODS, never a lookup.** Each LEI GLEIF changed that
  day is mapped from the Golden Copy mirror the way a lookup's anchor is
  (``GleifAdapter._snapshot_bundle`` + ``map_gleif``, inside
  ``mapping_provenance``), as it stands after the day's deltas.
- **Every band**, new LEIs and note-only changes included: the page lists
  the larger changes, the file carries them all.
- **Children are left out.** Each child publishes its own parent edge; a
  parent's entity statement rides along so the file's references resolve
  inside it. Statements are deduplicated by ``statementId``.
- **OpenSanctions is not in it.** Its changes are references only (CC BY-NC);
  they are in the day's JSON and CSV, never as statements.
- **Stored on the persistent disk** beside the store
  (``<db dir>/changes/<day>.jsonl.gz``), rebuilt whole after each GLEIF
  delta of that day and written atomically, pruned with the store.

Nothing here raises into the mirror refresh: a failure is logged and noted.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Iterable

from .secret_scrub import describe_exception

log = logging.getLogger(__name__)

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SUFFIX = ".jsonl.gz"

#: The CSV's columns, in order. One row per change the store holds.
CSV_COLUMNS = (
    "day",
    "source",
    "band",
    "lei",
    "name",
    "jurisdiction",
    "reasons",
    "fields_changed",
    "opensanctions_id",
    "as_of",
)


def files_dir() -> Path | None:
    """Where the day files live: a ``changes`` folder beside the store, or
    ``None`` when the feature is off."""
    from .config import get_settings

    path = get_settings().daily_changes_db_file
    if not path:
        return None
    return Path(path).parent / "changes"


def day_path(day: str) -> Path | None:
    base = files_dir()
    if base is None or not DAY_RE.match(day):
        return None
    return base / f"{day}{SUFFIX}"


def held_days() -> list[str]:
    """The days with a file, newest first."""
    base = files_dir()
    if base is None or not base.is_dir():
        return []
    days = [
        p.name[: -len(SUFFIX)]
        for p in base.iterdir()
        if p.name.endswith(SUFFIX) and DAY_RE.match(p.name[: -len(SUFFIX)])
    ]
    return sorted(days, reverse=True)


def _lei_of(stmt: dict[str, Any]) -> str | None:
    for ident in (stmt.get("recordDetails") or {}).get("identifiers") or []:
        if ident.get("scheme") == "XI-LEI" and ident.get("id"):
            return str(ident["id"]).upper()
    return None


def statements_for(
    lei: str, removed: Iterable[tuple[str, str]] = ()
) -> list[dict[str, Any]]:
    """What one LEI publishes of itself in the file: its entity statement,
    its parents and the edges to them, its reporting exceptions — stamped
    as its own declared network (the lookup's rule).

    ``removed`` names the parents the day's deltas took away, as
    ``(kind, parent LEI)``: each is published as a **closed** relationship
    under the recordId its open statement carried, with no ``endDate``
    (GLEIF's delta says that the edge went, not when it ended), and the
    former parent's entity statement beside it so the reference resolves."""
    from . import provenance
    from .bods import map_gleif
    from .bods.declaration import stamp_bundle
    from .bods.statements import _stable_id, make_relationship_statement
    from .sources.gleif import GleifAdapter

    with provenance.recording() as recorder:
        bundle = GleifAdapter()._snapshot_bundle(lei, reason="mirror", children=False)
    if bundle is None:
        return []
    bundle["direct_children"] = []
    bundle.pop("direct_child_relationships", None)
    with provenance.mapping_provenance(recorder.resolve(is_stub=False)):
        statements = list(map_gleif(bundle).statements)
        subject = next(
            (s for s in statements if s.get("recordType") == "entity" and _lei_of(s) == lei),
            None,
        )
        open_records = {s.get("recordId") for s in statements}
        for kind, parent_lei in removed:
            if subject is None:
                break
            local_id = f"{lei}:{kind}-parent:{parent_lei}"
            if _stable_id("gleif", "relationship-record", local_id) in open_records:
                continue  # the parent came back later the same day
            parent = next(
                (
                    s
                    for s in statements_for(parent_lei)
                    if s.get("recordType") == "entity" and _lei_of(s) == parent_lei
                ),
                None,
            )
            if parent is not None:
                statements.append(parent)
            statements.append(
                make_relationship_statement(
                    source_id="gleif",
                    local_id=local_id,
                    subject_statement_id=subject["recordId"],
                    interested_party_statement_id=(
                        parent["recordId"]
                        if parent is not None
                        else _stable_id("gleif", "entity", parent_lei)
                    ),
                    interested_party_type="entity",
                    interests=[
                        {
                            "type": "otherInfluenceOrControl",
                            "directOrIndirect": "direct" if kind == "direct" else "indirect",
                            "beneficialOwnershipOrControl": False,
                            "details": f"GLEIF Level 2 {kind}-parent (accounting consolidation)",
                        }
                    ],
                    source_url=f"https://www.gleif.org/lei/{lei}",
                    record_status="closed",
                )
            )
    return stamp_bundle(statements, source_id="gleif", ref=lei, record=False)


#: The two parent fields a delta can take a parent away from.
_PARENT_FIELDS = {"direct_parent_lei": "direct", "ultimate_parent_lei": "ultimate"}


def removed_parents(rows: Iterable[dict[str, Any]]) -> dict[str, list[tuple[str, str]]]:
    """Per LEI, the ``(kind, parent LEI)`` pairs the day's GLEIF rows took
    away, in the order they were recorded."""
    out: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        for change in row.get("changes") or []:
            kind = _PARENT_FIELDS.get(str(change.get("field")))
            old = str(change.get("old") or "").upper()
            if kind and old and old != str(change.get("new") or "").upper():
                pair = (kind, old)
                held = out.setdefault(row["lei"], [])
                if pair not in held:
                    held.append(pair)
    return out


def _ordered(statements: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicated by ``statementId``, parties before relationships, so a
    reader streaming the file meets every party before an edge names it."""
    seen: set[str] = set()
    parties: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for stmt in statements:
        sid = stmt.get("statementId")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        (edges if stmt.get("recordType") == "relationship" else parties).append(stmt)
    return parties + edges


def write_atomic(path: Path, statements: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    # mtime=0: the same statements give the same bytes.
    with open(tmp, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
        for stmt in statements:
            gz.write(json.dumps(stmt, ensure_ascii=False, sort_keys=True).encode("utf-8"))
            gz.write(b"\n")
    os.replace(tmp, path)


def build_day(
    day: str,
    leis: Iterable[str],
    removed: dict[str, list[tuple[str, str]]] | None = None,
) -> dict[str, int]:
    """Map every LEI and write the day's file. Returns counts; an LEI that
    cannot be mapped is counted, never fatal."""
    path = day_path(day)
    result = {"leis": 0, "statements": 0, "unmapped": 0, "closed": 0}
    if path is None:
        return result
    collected: list[dict[str, Any]] = []
    for lei in sorted(set(leis)):
        result["leis"] += 1
        try:
            stmts = statements_for(lei, (removed or {}).get(lei, ()))
        except Exception as exc:  # noqa: BLE001
            log.warning("daily changes file: %s not mapped: %s", lei, describe_exception(exc))
            stmts = []
        if not stmts:
            result["unmapped"] += 1
        collected.extend(stmts)
    ordered = _ordered(collected)
    write_atomic(path, ordered)
    result["statements"] = len(ordered)
    result["closed"] = sum(1 for stmt in ordered if stmt.get("recordStatus") == "closed")
    return result


def build_from_store(store: Any, day: str) -> dict[str, int] | None:
    """Rebuild ``day``'s file from every GLEIF row the store holds for it.
    Never raises."""
    from . import daily_changes

    try:
        rows = store.day(day, daily_changes.BANDS)
        gleif = [r for r in rows if r["source"] == daily_changes.SOURCE_GLEIF]
        result = build_day(day, [r["lei"] for r in gleif], removed_parents(gleif))
        store.set_meta(f"file:{day}", json.dumps(result, sort_keys=True))
        log.info(
            "daily changes file %s: %d LEIs, %d statements, %d not mapped",
            day, result["leis"], result["statements"], result["unmapped"],
        )
        return result
    except Exception as exc:  # noqa: BLE001
        log.warning("daily changes file %s failed: %s", day, describe_exception(exc))
        return None


def prune(keep: Iterable[str]) -> int:
    """Remove every day file whose day the store no longer holds."""
    base = files_dir()
    if base is None or not base.is_dir():
        return 0
    held = set(keep)
    removed = 0
    for day in held_days():
        if day not in held:
            path = day_path(day)
            if path is not None:
                path.unlink(missing_ok=True)
                removed += 1
    return removed


def csv_bytes(rows: list[dict[str, Any]], day: str) -> bytes:
    """The day's list as CSV — every band, both sources. OpenSanctions rows
    carry the entity id and topic codes only (no names: CC BY-NC)."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for r in rows:
        # A GLEIF change names its field; an OpenSanctions one is keyed by
        # what moved (topics_added, datasets_added …).
        fields = sorted(
            {
                str(key)
                for c in r.get("changes") or []
                for key in ([c["field"]] if c.get("field") else list(c))
            }
        )
        writer.writerow(
            [
                day,
                r.get("source") or "",
                r.get("band") or "",
                r.get("lei") or "",
                r.get("name") or "",
                r.get("jurisdiction") or "",
                ";".join(r.get("reasons") or []),
                ";".join(fields),
                (r.get("ref") or {}).get("opensanctions_id") or "",
                r.get("as_of") or "",
            ]
        )
    return buf.getvalue().encode("utf-8")
