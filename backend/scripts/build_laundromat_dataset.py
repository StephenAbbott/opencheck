"""Build the Azerbaijani Laundromat dataset release (Phase 288).

The entities named in the OCCRP *Azerbaijani Laundromat* wire transfers —
as curated by Paco Nathan in ``DerwenAI/azeri_laverie`` for the Connected
Data London 2026 masterclass — run through OpenCheck's own pipeline, and the
result is written as one versioned, provenance-stamped bundle in every
format OpenCheck exports: BODS v0.4, BODS RDF (NQuads), FollowTheMoney,
Senzing entity records and Neo4j CSV. Participants download the release;
nobody has to hit the live server from a conference room.

Two subcommands, run from ``backend/``:

    # 1. Derive the seed list (identifiers + names only) from the thesaurus.
    uv run python scripts/build_laundromat_dataset.py seed \\
        --thesaurus /path/to/azeri_laverie/data/thesaurus.json \\
        --thesaurus-commit <git sha> \\
        --out ../data/laundromat/seed.json

    # 2. Run the pipeline over the seed and write the release artefacts.
    OPENCHECK_ALLOW_LIVE=true uv run python scripts/build_laundromat_dataset.py build \\
        --seed ../data/laundromat/seed.json --out /tmp/laundromat

The seed is committed under ``data/laundromat/`` so the build is reproducible
without Paco's repository. It carries identifiers, names and his ``class``
label per entity — nothing else of his thesaurus.

What the build does
-------------------
* Every **LEI** subject runs the full lookup pipeline in-process
  (``routers.lookup._lookup_impl``, the same call the MCP ``opencheck_lookup``
  tool and ``/export`` make), then its GLEIF subsidiary network
  (``subsidiaries.assemble_subsidiaries(include_bods=True)``) is folded in,
  deduplicated by ``statementId`` exactly as ``GET /export?subsidiaries=true``
  does.
* Every **register** subject (``GB-COH`` company numbers, in this dataset)
  runs the Phase 182 register hop (``routers.expand._register_one_layer``):
  the one register that owns the scheme, its PSC / officer / related-company
  walk, and the sanctions / PEP name screen over everything it brought
  back. A subject that also carries an LEI is hopped *onto* its GLEIF node,
  as "+1 layer" in FullCheck does.
* The build runs in three paced passes — LEI lookups one at a time, then
  their subsidiary networks, then register hops — because everything that
  touches GLEIF shares one 50-calls-a-minute throttle and a bank's
  subsidiary network costs one call per child. A lookup refused as momentary
  (429/503) is retried after ``--retry-wait`` seconds.
* Raw results are cached per subject under ``<out>/raw/`` so an interrupted
  run resumes where it stopped and a rate-capped register can be retried with
  ``--retry-degraded`` without re-running the rest.
* A subsidiary network GLEIF did not fully answer (a list refused, or the
  fetch errored) is flagged ``subsidiaries_partial`` in the subjects table and
  counted as degraded — ``0 children`` with the flag false means GLEIF lists
  none; with it true, the network was not obtained. ``--retry-subsidiaries``
  refetches only those networks (Phase 293).
* The assembled bundle is deduplicated by ``statementId`` across subjects
  (two banks in one group share the parent's entity statement), converted to
  each format, and described by ``manifest.json`` (counts, checksums, which
  subjects failed or degraded, licence verdict) and ``LICENSES.md`` (the same
  notes ``/batch-export`` writes). A subject that failed contributes nothing
  and is named in the manifest — the bundle never reads as complete for a
  list it only partly covered.

Work started by this script has no HTTP client, so it is never charged to
the Phase 234 lookup budget; the process-wide GLEIF throttle and each
adapter's outbound-rate scope still apply, which is why register hops are
paced (``--register-pause``) rather than fired at once.

Exit codes: 0 = success, 1 = bad arguments or nothing built.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

log = logging.getLogger("build_laundromat_dataset")

THESAURUS_REPO = "https://github.com/DerwenAI/azeri_laverie"
THESAURUS_PATH = "data/thesaurus.json"
DATASET_SLUG = "azerbaijani-laundromat"
_LEI_RE = re.compile(r"^[A-Z0-9]{18}[0-9]{2}$")
_FORMATS = ("bods", "rdf", "ftm", "senzing", "neo4j")


# ----------------------------------------------------------------------
# seed — derive identifiers from the thesaurus
# ----------------------------------------------------------------------


def _first(record: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        v = record.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def seed_from_thesaurus(
    records: list[dict[str, Any]], *, commit: str | None = None, retrieved: str | None = None
) -> dict[str, Any]:
    """Reduce the thesaurus to what the build needs: one subject per entity
    that carries an identifier OpenCheck can act on.

    An entity with an LEI becomes an ``lei`` subject; one with a register
    number in a scheme OpenCheck can hop on becomes a ``register`` subject;
    one with both becomes both — the LEI subject runs the full pipeline and
    the register subject is hopped onto its GLEIF node, so the register's
    record stitches onto the same graph node. Entities with neither
    (Russian tax ids, Azerbaijani names) are counted in ``skipped`` so the
    release notes can say how much of the thesaurus the bundle covers.
    """
    subjects: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    seen: set[str] = set()
    for r in records:
        name = (r.get("bods:fullName") or "").strip()
        klass = _first(r, "lavie:class", "lavid:class")
        lei_block = r.get("lavie:lei") or {}
        lei = ""
        if isinstance(lei_block, dict):
            lei = (lei_block.get("bods:idString") or "").strip().upper()
        scheme = (r.get("bods:scheme") or "").strip()
        ident = (r.get("bods:idString") or "").strip()
        if lei and not _LEI_RE.match(lei):
            skipped["malformed lei"] += 1
            lei = ""
        added = False
        if lei:
            key = f"lei:{lei}"
            if key not in seen:
                seen.add(key)
                subjects.append(
                    {"key": key, "kind": "lei", "lei": lei, "name": name, "class": klass}
                )
            else:
                skipped["duplicate"] += 1
            added = True
        if scheme == "GB-COH" and ident:
            key = f"GB-COH:{ident.upper()}"
            if key not in seen:
                seen.add(key)
                subj: dict[str, Any] = {
                    "key": key, "kind": "register", "scheme": "GB-COH",
                    "id": ident.upper(), "name": name, "class": klass,
                }
                if lei:
                    subj["lei"] = lei
                subjects.append(subj)
            else:
                skipped["duplicate"] += 1
            added = True
        if not added:
            skipped[scheme or "no identifier"] += 1
    return {
        "dataset": DATASET_SLUG,
        "source": {
            "repo": THESAURUS_REPO,
            "file": THESAURUS_PATH,
            "commit": commit,
            "retrieved": retrieved or datetime.now(UTC).strftime("%Y-%m-%d"),
            "records": len(records),
        },
        "subjects": subjects,
        "skipped": dict(sorted(skipped.items())),
    }


def cmd_seed(args: argparse.Namespace) -> int:
    records = json.loads(Path(args.thesaurus).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        log.error("thesaurus is not a JSON array")
        return 1
    seed = seed_from_thesaurus(records, commit=args.thesaurus_commit)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(seed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    kinds = Counter(s["kind"] for s in seed["subjects"])
    log.info(
        "seed: %d subjects (%s) from %d records; skipped %s → %s",
        len(seed["subjects"]), dict(kinds), len(records), seed["skipped"], out,
    )
    return 0


# ----------------------------------------------------------------------
# build — run the pipeline, cache raw results per subject
# ----------------------------------------------------------------------


def _safe(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", key)


def _raw_path(out: Path, key: str) -> Path:
    return out / "raw" / f"{_safe(key)}.json"


def _is_degraded(raw: dict[str, Any]) -> bool:
    """A result worth refetching: it failed, nothing came back, or a check degraded."""
    return raw.get("status") in ("failed", "stub") or bool(raw.get("degraded_sources"))


def subsidiaries_partial(raw: dict[str, Any]) -> tuple[bool, str | None]:
    """``(partial, note)`` for a cached result's subsidiary network.

    Partial when the fetch errored or GLEIF refused the direct or the ultimate
    children list (``direct_available`` / ``ultimate_available`` false). A
    result with no ``subsidiaries`` block (a register subject, a failed
    lookup, ``--skip-subsidiaries``) has no network to be partial about. A
    list the Golden Copy snapshot stood in for counts as available: its rows
    are real, and ``snapshot_date`` says they are not live.
    """
    sub = raw.get("subsidiaries")
    if not isinstance(sub, dict):
        return False, None
    if sub.get("error"):
        return True, str(sub["error"])
    direct_missing = sub.get("direct_available") is False
    ultimate_missing = sub.get("ultimate_available") is False
    if direct_missing and ultimate_missing:
        return True, "GLEIF direct and ultimate lists unavailable"
    if direct_missing:
        return True, "GLEIF direct list unavailable"
    if ultimate_missing:
        return True, "GLEIF ultimate list unavailable"
    return False, None


async def _run_lei(
    subject: dict[str, Any], *, deepen_top: int, retries: int = 2, retry_wait: float = 70.0
) -> dict[str, Any]:
    """One LEI through the lookup pipeline.

    A momentary refusal (GLEIF rate-limited the shared connection, 503; the
    lookup budget, 429) is retried after ``retry_wait`` seconds, up to
    ``retries`` times, because in a batch the only thing wrong is timing.
    The subsidiary network is NOT fetched here: that fan-out costs one GLEIF
    call per child, and done inline it starves the next lookup's anchor —
    see :func:`_run_subsidiaries`, which runs after every lookup is cached.
    """
    from fastapi import HTTPException

    from opencheck.mcp.shaping import shape_batch_row
    from opencheck.routers.lookup import _lookup_impl

    lei = subject["lei"]
    attempt = 0
    while True:
        try:
            resp = await _lookup_impl(lei, deepen_top=deepen_top, refresh=True)
            break
        except HTTPException as exc:
            retryable = exc.status_code in (429, 503)
            if retryable and attempt < retries:
                attempt += 1
                log.warning(
                    "lei %s refused (%s) — retry %d/%d in %.0fs",
                    lei, exc.status_code, attempt, retries, retry_wait,
                )
                await asyncio.sleep(retry_wait)
                continue
            return {
                "key": subject["key"], "status": "failed", "http_status": exc.status_code,
                "reason": str(exc.detail), "retryable": retryable, "attempts": attempt + 1,
            }
    row = shape_batch_row(resp)
    return {
        "key": subject["key"],
        "status": "done",
        "attempts": attempt + 1,
        "row": row,
        "bods": list(resp.bods or []),
        "risk_signals": list(resp.risk_signals or []),
        "degraded_sources": list(resp.degraded_sources or []),
        "license_notices": list(resp.license_notices or []),
        "possibly_same_entities": list(resp.possibly_same_entities or []),
        "contributing_ids": sorted({h.source_id for h in (resp.hits or []) if not h.is_stub}),
        "source_liveness": dict(resp.source_liveness or {}),
    }


async def _run_subsidiaries(raw: dict[str, Any], lei: str) -> dict[str, Any]:
    """Fold the GLEIF subsidiary network into a cached lookup result.

    Deduplicated by ``statementId`` as ``GET /export?subsidiaries=true`` does.
    Best effort: a failure or a throttled GLEIF leaves the lookup intact and
    is recorded under ``subsidiaries`` — ``direct_available`` /
    ``ultimate_available`` false means the list is partial, not empty.
    """
    from opencheck.subsidiaries import assemble_subsidiaries

    # A retry (``--retry-subsidiaries``) refetches onto a result that already
    # holds the statements an earlier partial network added: they stay, and
    # the count is cumulative, so the row does not under-report them.
    previous = raw.get("subsidiaries") if isinstance(raw.get("subsidiaries"), dict) else {}
    prior_statements = int(previous.get("statements") or 0)
    try:
        data = await assemble_subsidiaries(lei, include_bods=True)
    except Exception as exc:  # noqa: BLE001 — best effort, as /export does
        raw["subsidiaries"] = {
            "statements": prior_statements,
            "children": int(previous.get("children") or 0),
            "error": f"{type(exc).__name__}: {exc}",
        }
        return raw
    sub = (data or {}).get("bods") or []
    existing = {s.get("statementId") for s in raw.get("bods") or []}
    added = [s for s in sub if s.get("statementId") not in existing]
    raw.setdefault("bods", []).extend(added)
    raw["subsidiaries"] = {
        "statements": prior_statements + len(added),
        "children": len((data or {}).get("children") or []),
        "direct_available": (data or {}).get("direct_available"),
        "ultimate_available": (data or {}).get("ultimate_available"),
        "snapshot_date": (data or {}).get("snapshot_date"),
    }
    return raw


async def _run_register(subject: dict[str, Any]) -> dict[str, Any]:
    """One register number through the Phase 182 hop."""
    from opencheck.bods.statements import _stable_id
    from opencheck.routers.expand import _register_one_layer

    scheme, ident = subject["scheme"], subject["id"]
    lei = subject.get("lei")
    # Anchor on the GLEIF node when the entity has an LEI — the hop then
    # stitches the register record onto it as FullCheck's "+1 layer" would.
    # Otherwise the register's own entity statement is the anchor.
    anchor = _stable_id("gleif", "entity", lei) if lei else _stable_id(
        "companies_house", "entity", ident
    )
    try:
        bods, signals, degraded = await _register_one_layer(
            scheme, ident, anchor, name=subject.get("name") or None
        )
    except Exception as exc:  # noqa: BLE001 — a subject, never a build abort
        return {
            "key": subject["key"], "status": "failed", "reason": f"{type(exc).__name__}: {exc}",
            "retryable": True,
        }
    # Phase 283: a register that refused says so in the scope's records; an
    # unknown number simply yields nothing. Both are "no record", and the
    # degradation list tells them apart.
    status = "stub" if not bods else "done"
    sources = {"companies_house"} if scheme == "GB-COH" else set()
    for s in signals:
        for sid in s.get("sources") or []:
            sources.add(sid)
    return {
        "key": subject["key"],
        "status": status,
        "anchor": anchor,
        "bods": bods,
        "risk_signals": signals,
        "degraded_sources": degraded,
        "contributing_ids": sorted(sources) if bods else [],
    }


async def _build_raw(
    subjects: list[dict[str, Any]],
    *,
    out: Path,
    deepen_top: int,
    lei_concurrency: int,
    lei_pause: float,
    register_pause: float,
    retries: int,
    retry_wait: float,
    force: bool,
    retry_degraded: bool,
    skip_subsidiaries: bool,
    retry_subsidiaries: bool = False,
    subsidiary_concurrency: int = 1,
) -> None:
    """Fetch every subject not already cached under ``out/raw``, in three passes:
    LEI lookups, then their subsidiary networks, then register hops.

    Everything that touches GLEIF is paced: the process-wide throttle allows
    50 calls a minute and GLEIF itself 60 per address, a lookup's anchor costs
    about eight, and a bank's subsidiary network one per child. The first
    keyed run (5 Oct 2026) fetched subsidiaries inline, two lookups at a
    time, and 21 of 26 LEI subjects failed on the resulting 429s while every
    register hop succeeded — hence the passes, the default concurrency of
    one, and the retries.
    """
    (out / "raw").mkdir(parents=True, exist_ok=True)

    def _needs(subject: dict[str, Any]) -> bool:
        p = _raw_path(out, subject["key"])
        if force or not p.exists():
            return True
        if retry_degraded:
            try:
                return _is_degraded(json.loads(p.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                return True
        return False

    todo = [s for s in subjects if _needs(s)]
    log.info(
        "%d of %d subjects to fetch (%d cached)",
        len(todo), len(subjects), len(subjects) - len(todo),
    )

    leis = [s for s in todo if s["kind"] == "lei"]
    regs = [s for s in todo if s["kind"] == "register"]

    # Pass 1 — LEI lookups.
    sem = asyncio.Semaphore(max(1, lei_concurrency))

    async def _one_lei(i: int, s: dict[str, Any]) -> None:
        async with sem:
            started = time.monotonic()
            raw = await _run_lei(s, deepen_top=deepen_top, retries=retries, retry_wait=retry_wait)
            raw["elapsed_s"] = round(time.monotonic() - started, 1)
            _write_json(_raw_path(out, s["key"]), raw)
            log.info(
                "[lei %d/%d] %s %s — %s in %.1fs (%d statements%s)",
                i, len(leis), s["lei"], s.get("name", "")[:40], raw["status"], raw["elapsed_s"],
                len(raw.get("bods") or []),
                ", degraded" if raw.get("degraded_sources") else "",
            )
            if lei_pause > 0 and i < len(leis):
                await asyncio.sleep(lei_pause)

    if leis:
        await asyncio.gather(*(_one_lei(i, s) for i, s in enumerate(leis, 1)))

    # Pass 2 — subsidiary networks for every cached, successful LEI lookup
    # that does not have one yet (a re-run picks up where it stopped), plus,
    # with --retry-subsidiaries or --retry-degraded, every cached network that
    # came back partial or errored.
    if not skip_subsidiaries:
        # Script-side pacing (Phase 293): the per-child direct-parent calls
        # inside a network fan out four at a time, and the GLEIF throttle's
        # bounded wait is what turns a large network into a partial one. The
        # build sets this module constant for its own process only, so the
        # Subsidiaries tab keeps its concurrency.
        import opencheck.subsidiaries as _subsidiaries

        _subsidiaries._PARENT_LOOKUP_CONCURRENCY = max(1, subsidiary_concurrency)
        retry_partial = retry_subsidiaries or retry_degraded
        pending = select_subsidiary_fetches(
            subjects, _load_raw(out, subjects), force=force, retry_partial=retry_partial
        )
        for i, (s, raw, retry) in enumerate(pending, 1):
            started = time.monotonic()
            raw = await _run_subsidiaries(raw, s["lei"])
            _write_json(_raw_path(out, s["key"]), raw)
            sub = raw.get("subsidiaries") or {}
            partial, note = subsidiaries_partial(raw)
            log.info(
                "[subsidiaries%s %d/%d] %s %s — %d children, %d statements%s in %.1fs",
                " retry" if retry else "", i, len(pending), s["lei"], s.get("name", "")[:40],
                sub.get("children", 0), sub.get("statements", 0),
                f" (partial: {note})" if partial else "",
                time.monotonic() - started,
            )
            if lei_pause > 0 and i < len(pending):
                await asyncio.sleep(lei_pause)

    # Pass 3 — register hops, one at a time with a pause: Companies House
    # allows 600 calls per 5 minutes per key and a hop costs about four, plus
    # the PSC walk. The outbound-rate scope degrades a capped register rather
    # than failing, and --retry-degraded picks those up on the next run.
    for i, s in enumerate(regs, 1):
        started = time.monotonic()
        raw = await _run_register(s)
        raw["elapsed_s"] = round(time.monotonic() - started, 1)
        _write_json(_raw_path(out, s["key"]), raw)
        log.info(
            "[register %d/%d] %s %s — %s in %.1fs (%d statements%s)",
            i, len(regs), s["key"], (s.get("name") or "")[:40], raw["status"], raw["elapsed_s"],
            len(raw.get("bods") or []),
            ", degraded" if raw.get("degraded_sources") else "",
        )
        if register_pause > 0 and i < len(regs):
            await asyncio.sleep(register_pause)


def select_subsidiary_fetches(
    subjects: list[dict[str, Any]],
    raws: dict[str, dict[str, Any]],
    *,
    force: bool = False,
    retry_partial: bool = False,
) -> list[tuple[dict[str, Any], dict[str, Any], bool]]:
    """``(subject, raw, is_retry)`` for every LEI network pass 2 should fetch.

    A successful lookup with no network yet is always fetched; ``force``
    refetches every one; ``retry_partial`` refetches those whose network is
    partial or errored (:func:`subsidiaries_partial`) and leaves complete
    ones alone. A failed or missing lookup is pass 1's business.
    """
    out: list[tuple[dict[str, Any], dict[str, Any], bool]] = []
    for s in subjects:
        if s["kind"] != "lei":
            continue
        raw = raws.get(s["key"])
        if not raw or raw.get("status") != "done":
            continue
        if force or "subsidiaries" not in raw:
            out.append((s, raw, False))
        elif retry_partial and subsidiaries_partial(raw)[0]:
            out.append((s, raw, True))
    return out


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)


# ----------------------------------------------------------------------
# assemble — merge the raw results into one bundle
# ----------------------------------------------------------------------


def _load_raw(out: Path, subjects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    raws: dict[str, dict[str, Any]] = {}
    for s in subjects:
        p = _raw_path(out, s["key"])
        if p.exists():
            raws[s["key"]] = json.loads(p.read_text(encoding="utf-8"))
    return raws


def _status_text(status: Any) -> str | None:
    """Flatten a profile status dict to ``ACTIVE (gleif, live)``."""
    if not isinstance(status, dict):
        return status if isinstance(status, str) else None
    raw = status.get("raw") or status.get("label") or ""
    bits = [b for b in (status.get("source_id"), status.get("liveness")) if b]
    return f"{raw} ({', '.join(bits)})" if bits else (raw or None)


def assemble(subjects: list[dict[str, Any]], raws: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Merge every subject's result, deduplicated by ``statementId`` in seed order.

    Returns ``statements``, per-subject ``rows``, the union of ``signals``,
    ``contributing_ids``, ``license_notices`` and the counts the manifest
    needs. Pure: no I/O, so it is what the tests exercise.
    """
    statements: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicates = 0
    rows: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    sig_seen: set[str] = set()
    sources: set[str] = set()
    notices: dict[str, dict[str, Any]] = {}
    networks = {"fetched": 0, "partial": 0, "errored": 0, "children_total": 0}
    # LEI subjects first: a register subject anchored on an LEI stitches its
    # record onto the GLEIF node, so that node's entity statement must already
    # be in the bundle when the register's relationships reference it (BODS
    # orders a referenced statement before the statement that references it).
    ordered = [s for s in subjects if s["kind"] == "lei"] + [
        s for s in subjects if s["kind"] != "lei"
    ]
    for s in ordered:
        raw = raws.get(s["key"])
        row: dict[str, Any] = {
            "key": s["key"], "kind": s["kind"], "lei": s.get("lei"), "scheme": s.get("scheme"),
            "id": s.get("id"), "name": s.get("name"), "class": s.get("class"),
        }
        if raw is None:
            row.update({"status": "missing", "statements": 0, "degraded": True})
            rows.append(row)
            continue
        status = raw.get("status")
        row["status"] = status
        if status == "failed":
            row.update({"reason": raw.get("reason"), "statements": 0, "degraded": True})
            rows.append(row)
            continue
        n_before = len(statements)
        for st in raw.get("bods") or []:
            sid = st.get("statementId")
            if sid and sid in seen:
                duplicates += 1
                continue
            if sid:
                seen.add(sid)
            statements.append(st)
        row["statements"] = len(statements) - n_before
        for sig in raw.get("risk_signals") or []:
            k = json.dumps(
                {"code": sig.get("code"), "ids": sorted(sig.get("statement_ids") or []),
                 "summary": sig.get("summary")},
                sort_keys=True,
            )
            if k in sig_seen:
                continue
            sig_seen.add(k)
            signals.append({"subject": s["key"], **sig})
        sources.update(raw.get("contributing_ids") or [])
        for n in raw.get("license_notices") or []:
            notices.setdefault(json.dumps(n, sort_keys=True, default=str), n)
        degraded = raw.get("degraded_sources") or []
        degraded_checks = {f"{d.get('check')}:{d.get('reason')}" for d in degraded}
        # A partial network is a check that did not fully run: without the
        # flag, "0 children" reads as "GLEIF lists none" (HSBC and Deutsche
        # Bank in the first keyed run, 5 Oct 2026).
        partial, note = subsidiaries_partial(raw)
        if partial:
            degraded_checks.add("subsidiaries:partial")
        row["degraded"] = bool(degraded) or partial
        row["degraded_checks"] = sorted(degraded_checks)
        if raw.get("row"):
            r = raw["row"]
            row.update({
                "legal_name": r.get("legal_name"), "jurisdiction": r.get("jurisdiction"),
                "register_status": _status_text(r.get("register_status")),
                "verdict": r.get("verdict"),
                "risk_codes": r.get("risk_codes") or [],
                "context_codes": r.get("context_codes") or [],
                "sources_with_data": (r.get("coverage") or {}).get("with_data_ids") or [],
            })
        else:
            sigs = raw.get("risk_signals") or []
            row.update({
                "risk_codes": sorted(
                    {x["code"] for x in sigs if x.get("code") and x.get("kind") != "context"}
                ),
                "context_codes": sorted(
                    {x["code"] for x in sigs if x.get("code") and x.get("kind") == "context"}
                ),
                "sources_with_data": raw.get("contributing_ids") or [],
            })
        sub = raw.get("subsidiaries") or {}
        if sub:
            row["subsidiary_statements"] = sub.get("statements", 0)
            row["subsidiary_children"] = sub.get("children", 0)
            row["subsidiaries_partial"] = partial
            row["subsidiaries_note"] = note
            networks["fetched"] += 1
            networks["children_total"] += int(sub.get("children") or 0)
            if sub.get("error"):
                networks["errored"] += 1
            elif partial:
                networks["partial"] += 1
        rows.append(row)

    counts: Counter[str] = Counter(st.get("recordType") or "?" for st in statements)
    status_counts = Counter(r["status"] for r in rows)
    return {
        "statements": statements,
        "rows": rows,
        "signals": signals,
        "contributing_ids": sorted(sources),
        "license_notices": list(notices.values()),
        "duplicate_statements_collapsed": duplicates,
        "node_counts": dict(counts),
        "subject_status": dict(status_counts),
        "degraded_subjects": sum(1 for r in rows if r.get("degraded")),
        "networks": networks,
    }


# ----------------------------------------------------------------------
# write — every format, the manifest and the licence notes
# ----------------------------------------------------------------------


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _gz_write_text(path: Path, text: str) -> None:
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=9) as fh:
        fh.write(text)


def _neo4j_zip(jsonl: Path, zip_path: Path) -> bool:
    """Run ``bods-neo4j to-csv`` and zip the CSVs; False when the CLI is absent."""
    venv_bin = Path(__file__).resolve().parents[1] / ".venv" / "bin" / "bods-neo4j"
    cmd = str(venv_bin) if venv_bin.exists() else shutil.which("bods-neo4j")
    if cmd is None:
        log.warning(
            "bods-neo4j not found — skipping the Neo4j CSV zip. Install with: "
            "pip install git+https://github.com/StephenAbbott/bods-neo4j.git"
        )
        return False
    with tempfile.TemporaryDirectory() as td:
        csv_dir = Path(td) / "neo4j"
        result = subprocess.run(
            [cmd, "to-csv", str(jsonl), "-o", str(csv_dir)], capture_output=True, text=True
        )
        if result.returncode != 0:
            log.error("bods-neo4j to-csv failed (%d): %s", result.returncode, result.stderr[-2000:])
            return False
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(csv_dir.rglob("*")):
                if p.is_file():
                    zf.write(p, arcname=f"{zip_path.stem}/{p.relative_to(csv_dir)}")
    return True


def write_release(
    assembled: dict[str, Any], *, out: Path, seed: dict[str, Any], stamp: str,
    formats: tuple[str, ...] = _FORMATS, run_date: str | None = None,
) -> dict[str, Any]:
    """Write the artefacts for ``assembled`` under ``out`` and return the manifest."""
    from opencheck import __version__
    from opencheck.bods import to_ftm_jsonl, to_rdf, to_senzing_jsonl, validate_shape
    from opencheck.licensing import assess as assess_licensing
    from opencheck.routers.export import _build_licenses_md
    from opencheck.sources import SearchKind

    out.mkdir(parents=True, exist_ok=True)
    base = f"{DATASET_SLUG}-{stamp}"
    statements = assembled["statements"]
    artefacts: dict[str, dict[str, Any]] = {}

    def _record(name: str, path: Path, description: str) -> None:
        artefacts[name] = {
            "file": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path),
            "description": description,
        }

    jsonl_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in statements) + "\n"
    bods_path = out / f"{base}.bods.jsonl.gz"
    if "bods" in formats:
        _gz_write_text(bods_path, jsonl_text)
        _record("bods", bods_path, "BODS v0.4 statements, newline-delimited JSON, gzipped")

    if "rdf" in formats:
        nq = to_rdf(
            statements, fmt="nquads", run_date=run_date or stamp[:10],
            risk_signals=[
                {k: v for k, v in s.items() if k != "subject"} for s in assembled["signals"]
            ],
        )
        p = out / f"{base}.nq.gz"
        _gz_write_text(p, nq)
        _record(
            "rdf", p,
            "BODS RDF — NQuads (one named graph per statement; OpenCheck's risk signals as "
            "bods:Annotation in a separate graph), gzipped",
        )
        artefacts["rdf"]["quads"] = sum(1 for line in nq.splitlines() if line.strip())

    if "ftm" in formats:
        p = out / f"{base}.ftm.jsonl.gz"
        _gz_write_text(p, to_ftm_jsonl(statements))
        _record("ftm", p, "FollowTheMoney entities (bods-ftm), newline-delimited JSON, gzipped")

    if "senzing" in formats:
        p = out / f"{base}.senzing.jsonl.gz"
        _gz_write_text(p, to_senzing_jsonl(statements))
        _record("senzing", p, "Senzing JSON entity records, newline-delimited, gzipped")

    if "neo4j" in formats:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as fh:
            fh.write(jsonl_text)
            tmp_jsonl = Path(fh.name)
        try:
            p = out / f"{base}.neo4j.zip"
            if _neo4j_zip(tmp_jsonl, p):
                _record("neo4j", p, "Neo4j CSV files + import.cypher (bods-neo4j to-csv)")
        finally:
            tmp_jsonl.unlink(missing_ok=True)

    # Subject table — what each entity got, and whether it was fully checked.
    rows = assembled["rows"]
    subjects_path = out / f"{base}.subjects.csv"
    cols = [
        "key", "kind", "lei", "scheme", "id", "name", "class", "status", "legal_name",
        "jurisdiction", "register_status", "verdict", "risk_codes", "context_codes",
        "statements", "subsidiary_children", "subsidiary_statements", "subsidiaries_partial",
        "subsidiaries_note", "sources_with_data",
        "degraded", "degraded_checks", "reason",
    ]
    with subjects_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([
                "; ".join(v) if isinstance(v, list) else ("" if v is None else v)
                for v in (r.get(c) for c in cols)
            ])
    _record(
        "subjects", subjects_path,
        "One row per seed subject: what the pipeline returned and whether every check fully ran",
    )

    signals_path = out / f"{base}.signals.jsonl"
    signals_path.write_text(
        "".join(
            json.dumps(s, ensure_ascii=False, default=str) + "\n" for s in assembled["signals"]
        ),
        encoding="utf-8",
    )
    _record(
        "signals", signals_path,
        "OpenCheck risk and context signals, one per line, with the subject key that raised them",
    )

    licensing = assess_licensing(assembled["contributing_ids"])
    licenses_md = _build_licenses_md(
        contributing_ids=assembled["contributing_ids"],
        license_notices=assembled["license_notices"],
        licensing=licensing,
        query=f"{DATASET_SLUG} ({len(rows)} subjects)",
        kind=SearchKind.ENTITY,
    )
    (out / "LICENSES.md").write_text(licenses_md, encoding="utf-8")

    manifest = {
        "dataset": DATASET_SLUG,
        "stamp": stamp,
        "opencheck_version": __version__,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "seed": seed.get("source"),
        "subjects": {
            "total": len(rows),
            "by_kind": dict(Counter(r["kind"] for r in rows)),
            "by_status": assembled["subject_status"],
            "degraded": assembled["degraded_subjects"],
            "networks": assembled["networks"],
            "partial_networks": [
                {"key": r["key"], "note": r.get("subsidiaries_note")}
                for r in rows
                if r.get("subsidiaries_partial")
            ],
            "failed": [
                {"key": r["key"], "reason": r.get("reason")}
                for r in rows
                if r["status"] in ("failed", "missing")
            ],
        },
        "seed_skipped": seed.get("skipped"),
        "bods_statement_count": len(statements),
        "node_counts": assembled["node_counts"],
        "duplicate_statements_collapsed": assembled["duplicate_statements_collapsed"],
        "signal_count": len(assembled["signals"]),
        "signal_codes": dict(Counter(s.get("code") for s in assembled["signals"])),
        "contributing_source_ids": assembled["contributing_ids"],
        "bods_validation_issues": validate_shape(statements),
        "licensing": licensing.model_dump(),
        "artefacts": artefacts,
        "note": (
            "A subject with status failed or missing was NOT checked and contributes no "
            "statements; one with degraded=true had a screening check that did not fully run "
            "(see degraded_checks in the subjects file). Neither is a clean result. A "
            "subsidiaries_partial=true row did not obtain its whole GLEIF subsidiary network, "
            "so its subsidiary_children is a floor, not a count. Register "
            "subjects (GB-COH) carry the register's record and the sanctions/PEP name screen "
            "over it, not the full source fan-out an LEI subject gets."
        ),
    }
    (out / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (out / "RELEASE_NOTES.md").write_text(release_notes(manifest, licensing), encoding="utf-8")
    return manifest


def release_notes(manifest: dict[str, Any], licensing: Any) -> str:
    """A release description with the numbers filled in; the prose is for Stephen to edit."""
    m = manifest
    subj = m["subjects"]
    lines = [
        f"# OpenCheck dataset — Azerbaijani Laundromat ({m['stamp']})",
        "",
        "The companies and banks named in the OCCRP *Azerbaijani Laundromat* wire transfers "
        "(Danske Bank Estonia, 2012–2014), as curated for the Connected Data London 2026 "
        f"masterclass in [DerwenAI/azeri_laverie]({THESAURUS_REPO}), run through OpenCheck's "
        "due-diligence pipeline and published in every format OpenCheck exports.",
        "",
        (
            "**Non-commercial.** The bundle includes CC-BY-NC sources (OpenSanctions, "
            "EveryPolitician), so the combined dataset is non-commercial by construction. "
            if str(licensing.commercial_use) == "no"
            else ""
        )
        + f"Licence verdict: **{licensing.headline}** — see `LICENSES.md` for the per-source "
        "matrix and the attribution each source requires.",
        "",
        "## What's inside",
        "",
        "| File | What it is | Size | SHA-256 |",
        "|---|---|---|---|",
    ]
    for a in m["artefacts"].values():
        lines.append(f"| `{a['file']}` | {a['description']} | {a['bytes']:,} B | `{a['sha256']}` |")
    lines += [
        "| `manifest.json` | Counts, checksums, subject status, licence verdict | | |",
        "| `LICENSES.md` | Licence notes over the union of sources | | |",
        "",
        "## Coverage",
        "",
        f"- Seed: {m['seed'].get('records')} thesaurus records at commit "
        f"`{m['seed'].get('commit')}`; {subj['total']} became subjects ({subj['by_kind']}); "
        f"skipped: {m.get('seed_skipped')}.",
        f"- Subject status: {subj['by_status']}; {subj['degraded']} with at least one check "
        "that did not fully run.",
        f"- GLEIF subsidiary networks: {subj['networks']['fetched']} fetched, "
        f"{subj['networks']['children_total']:,} children; "
        f"{subj['networks']['partial']} partial and {subj['networks']['errored']} errored"
        + (
            " (" + ", ".join(p["key"] for p in subj["partial_networks"]) + ")."
            if subj["partial_networks"]
            else "."
        ),
        f"- {m['bods_statement_count']:,} BODS statements ({m['node_counts']}); "
        f"{m['duplicate_statements_collapsed']} duplicates collapsed across subjects.",
        f"- {m['signal_count']} signals: {m['signal_codes']}.",
        f"- Sources contributing: {', '.join(m['contributing_source_ids'])}.",
        "",
        "LEI subjects carry the full OpenCheck fan-out plus their GLEIF subsidiary network; "
        "`GB-COH` subjects carry the Companies House record (profile, officers, PSCs, related "
        "companies) and the sanctions/PEP name screen over it. Russian tax ids and Azerbaijani "
        "names in the thesaurus have no register OpenCheck reads, so they are not in this bundle.",
        "",
        "## Reproduce",
        "",
        "```",
        "cd backend",
        "OPENCHECK_ALLOW_LIVE=true uv run python scripts/build_laundromat_dataset.py build \\",
        "    --seed ../data/laundromat/seed.json --out /tmp/laundromat",
        "```",
        "",
        f"Built with OpenCheck {m['opencheck_version']} on {m['generated_at'][:10]}. "
        "Every statement carries its source, retrieval time and liveness; the RDF carries a "
        "licence per statement.",
        "",
    ]
    return "\n".join(lines)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def cmd_build(args: argparse.Namespace) -> int:
    seed = json.loads(Path(args.seed).read_text(encoding="utf-8"))
    subjects: list[dict[str, Any]] = list(seed.get("subjects") or [])
    if args.only:
        subjects = [s for s in subjects if s["kind"] == args.only]
    if args.limit:
        subjects = subjects[: args.limit]
    if not subjects:
        log.error("no subjects selected")
        return 1
    out = Path(args.out)
    if not args.assemble_only:
        os.environ.setdefault("OPENCHECK_ALLOW_LIVE", "true")
        from opencheck.config import get_settings

        if not get_settings().allow_live:
            log.error("OPENCHECK_ALLOW_LIVE must be true to fetch (settings say it is not)")
            return 1
        asyncio.run(
            _build_raw(
                subjects, out=out, deepen_top=args.deepen_top,
                lei_concurrency=args.lei_concurrency, lei_pause=args.lei_pause,
                register_pause=args.register_pause, retries=args.retries,
                retry_wait=args.retry_wait, force=args.force,
                retry_degraded=args.retry_degraded, skip_subsidiaries=args.skip_subsidiaries,
                retry_subsidiaries=args.retry_subsidiaries,
                subsidiary_concurrency=args.subsidiary_concurrency,
            )
        )
    raws = _load_raw(out, subjects)
    if not raws:
        log.error("no raw results under %s", out / "raw")
        return 1
    assembled = assemble(subjects, raws)
    stamp = args.stamp or datetime.now(UTC).strftime("%Y-%m-%d")
    formats = tuple(f for f in _FORMATS if f not in set(args.skip_format or []))
    manifest = write_release(assembled, out=out, seed=seed, stamp=stamp, formats=formats)
    log.info(
        "wrote %d artefacts to %s — %d statements, %d signals, subjects %s, licence: %s",
        len(manifest["artefacts"]), out, manifest["bods_statement_count"], manifest["signal_count"],
        manifest["subjects"]["by_status"], manifest["licensing"]["headline"],
    )
    issues = manifest["bods_validation_issues"]
    if issues:
        log.warning("%d BODS shape issues (first 5): %s", len(issues), issues[:5])
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed", help="derive the seed list from the thesaurus")
    s.add_argument("--thesaurus", required=True, help="path to azeri_laverie data/thesaurus.json")
    s.add_argument(
        "--thesaurus-commit", default=None, help="git commit of the thesaurus, for provenance"
    )
    s.add_argument("--out", default="../data/laundromat/seed.json")
    s.set_defaults(func=cmd_seed)

    b = sub.add_parser(
        "build", help="run the pipeline over the seed and write the release artefacts"
    )
    b.add_argument("--seed", default="../data/laundromat/seed.json")
    b.add_argument("--out", required=True, help="output directory (raw/ cache lives here too)")
    b.add_argument("--only", choices=["lei", "register"], default=None)
    b.add_argument("--limit", type=int, default=0, help="first N subjects only (smoke runs)")
    b.add_argument("--deepen-top", type=int, default=5)
    b.add_argument(
        "--lei-concurrency", type=int, default=1,
        help="LEI lookups in flight (default 1: an anchor costs ~8 GLEIF calls of the 50/min)",
    )
    b.add_argument(
        "--lei-pause", type=float, default=5.0,
        help="seconds between LEI lookups and between subsidiary fetches",
    )
    b.add_argument(
        "--retries", type=int, default=2,
        help="retries of a lookup refused as momentary (GLEIF 429/503, lookup budget)",
    )
    b.add_argument("--retry-wait", type=float, default=70.0, help="seconds before a retry")
    b.add_argument(
        "--skip-subsidiaries", action="store_true",
        help="do not fetch GLEIF subsidiary networks (the costliest GLEIF calls)",
    )
    b.add_argument(
        "--register-pause", type=float, default=3.0, help="seconds between register hops"
    )
    b.add_argument(
        "--force", action="store_true", help="refetch subjects already cached under raw/"
    )
    b.add_argument(
        "--retry-degraded", action="store_true",
        help=(
            "refetch cached subjects that failed or degraded, and every partial or errored "
            "subsidiary network"
        ),
    )
    b.add_argument(
        "--retry-subsidiaries", action="store_true",
        help="refetch only the cached subsidiary networks that came back partial or errored",
    )
    b.add_argument(
        "--subsidiary-concurrency", type=int, default=1,
        help=(
            "direct-parent calls in flight inside one subsidiary network (default 1; the "
            "Subsidiaries tab uses 4) — lower keeps a large network under the GLEIF throttle"
        ),
    )
    b.add_argument("--assemble-only", action="store_true", help="skip fetching; assemble from raw/")
    b.add_argument("--stamp", default=None, help="release stamp (default: today, YYYY-MM-DD)")
    b.add_argument("--skip-format", action="append", choices=list(_FORMATS), default=None)
    b.set_defaults(func=cmd_build)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
