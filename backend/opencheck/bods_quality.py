"""Weekly BODS quality sweep: is what production publishes valid, honest BODS? (Phase 316)

The source-health sweep (Phase 121) asks whether every adapter answers, and the
findings regression (Phase 277) what the risk engine concludes. Neither reads
the BODS statements themselves, and the dates audit of 9 Oct 2026 showed what
that costs: the statements of 41 sources had been dated today with no
``retrievedAt`` on every deepen (Phase 314), and PRH and CRO answered HTTP 500
on ``/deepen`` for days (Phase 315) while both sweeps were green. This is the
third sibling, on the same weekly workflow shape, asserting the data.

What it reads
-------------

Production, as a reader gets it:

* ``GET /lookup?refresh=true`` for every LEI in the findings golden set
  (``backend/findings_golden/``) — the statements of every deepened source,
  with ``source_liveness`` beside them, so the date rules that depend on how
  a source was read can be checked;
* ``GET /deepen`` for every source-health probe subject that ``/deepen`` can
  address (``probes.PROBES``: a single-argument ``fetch`` or ``fetch_by_lei``),
  so each register is exercised even where no golden LEI reaches it. These
  carry no liveness, so the probe's ``expect_liveness`` stands in for it.

What it checks, by class
------------------------

Every finding names its class, its source and the statement it is about.

``fail`` — the statement is invalid or asserts something false:

``schema``
    A JSON Schema error from lib-cove-bods against BODS v0.4.
``bods_additional``
    A lib-cove-bods additional check, other than the unknown-identifier-scheme
    advisory (the same exclusion ``test_bods_libcovebods.py`` makes).
``date_in_future``
    ``statementDate``, ``foundingDate`` / ``dissolutionDate`` or an interest
    start/end date after the day of the run.
``statement_after_publication`` / ``retrieved_after_publication``
    The claim, or OpenCheck's download, dated after the statement was
    published — impossible, and the shape of a field mix-up.
``start_after_end``
    An interest that ends before it starts.
``bulk_dated_today``
    A statement from a ``snapshot`` or ``curated`` source dated the day of the
    run: a bulk dataset that declared neither its cut nor its build (the
    Phase 314 conftest rule, applied to production).
``deepen_failed``
    ``/deepen`` returned an HTTP error for a probe subject that should map —
    the PRH / CRO 500 class.

``warn`` — honest but weaker than it should be, or a known backlog:

``cut_as_retrieval``
    ``retrievedAt`` at exactly midnight on the statement's own date for a
    snapshot source that declared no separate ``source_as_of``: the
    register's cut published as OpenCheck's download, the pre-Phase-314
    conflation. Lookups only — a deepen carries no liveness detail.
``no_retrieved_at``
    A statement from a source that was read (not a stub) with no
    ``source.retrievedAt``.
``no_source_id``
    A ``source`` block without ``opencheckSourceId`` (Phase 267's licence
    lookup falls through on it).
``source_type``
    ``source.type`` disagrees with ``OFFICIAL_REGISTER_SOURCES``.
``ended_not_closed``
    A relationship whose every interest has ended, still ``recordStatus``
    other than ``closed`` — the Phase 317 lifecycle rule, which the factory
    now applies.

The warnings are checked only on statements OpenCheck publishes
(``publicationDetails.publisher.name`` "OpenCheck"): another publisher's
statement handed on verbatim — MEIP's, Open Ownership's stored bundles —
keeps its own source block, since a BODS statement is immutable (Phase 317).

Per source, the report also carries the shape of its dating — how many
statements, how many carry ``retrievedAt``, how many are dated by the
retrieval day, how many distinct ``statementDate`` values — because "every
statement dated the day it was read" is the signature this whole line of work
removes, and it is a number to watch rather than a rule to fail.

Week over week
--------------

The diff is against the last **published** report (the ``bods-quality-latest``
release), per ``(check, source)``: new, resolved and changed counts. A missing
baseline is reported as "no comparison", never as "nothing changed".
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

FAIL = "fail"
WARN = "warn"

DEFAULT_PACE_S = 60.0
#: Seconds between ``/deepen`` calls: each is one cached or live source read.
DEFAULT_DEEPEN_PACE_S = 3.0
MAX_RETRIES = 1
_MAX_RETRY_WAIT_S = 120.0

#: Sources whose statements are a publisher's own BODS, handed on verbatim
#: (Phase 208): their ``source`` block is the publisher's, not OpenCheck's.
PUBLISHER_VERBATIM: frozenset[str] = frozenset({"meip"})

#: lib-cove-bods' advisory that OpenCheck tracks but does not fail on.
_ADVISORY_ADDITIONAL = frozenset({"entity_identifiers_not_known_scheme"})

#: Liveness values describing a bulk or committed dataset.
_BULK = frozenset({"snapshot", "curated"})

Sleep = Callable[[float], None]


# --- findings -------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    check: str
    severity: str
    source_id: str
    message: str
    statement_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out = {
            "check": self.check,
            "severity": self.severity,
            "source_id": self.source_id,
            "message": self.message,
        }
        if self.statement_id:
            out["statement_id"] = self.statement_id
        return out


def _source_id(statement: Mapping[str, Any]) -> str:
    src = statement.get("source") or {}
    return str(src.get("opencheckSourceId") or src.get("description") or "unknown")


def _day(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        date.fromisoformat(text[:10])
    except ValueError:
        return None
    return text[:10]


def _published_by_opencheck(statement: Mapping[str, Any]) -> bool:
    publisher = (statement.get("publicationDetails") or {}).get("publisher") or {}
    return str(publisher.get("name") or "").startswith("OpenCheck")


def _dates_in(statement: Mapping[str, Any]) -> list[tuple[str, str]]:
    """``(label, day)`` for every date a statement asserts about the world."""
    out: list[tuple[str, str]] = []
    details = statement.get("recordDetails") or {}
    for key in ("foundingDate", "dissolutionDate", "birthDate", "deathDate"):
        day = _day(details.get(key))
        if day:
            out.append((key, day))
    for i, interest in enumerate(details.get("interests") or []):
        if not isinstance(interest, dict):
            continue
        for key in ("startDate", "endDate"):
            day = _day(interest.get(key))
            if day:
                out.append((f"interests[{i}].{key}", day))
    return out


def check_statements(
    statements: Iterable[Mapping[str, Any]],
    *,
    today: str,
    liveness: Mapping[str, str] | None = None,
    no_cut: frozenset[str] = frozenset(),
    official_registers: frozenset[str] = frozenset(),
) -> list[Finding]:
    """The OpenCheck date and provenance rules, statement by statement.

    ``liveness`` maps a source id to how it was read (``source_liveness``
    from a lookup, or a probe's expectation); a source absent from it is
    checked only by the rules that need no liveness. ``no_cut`` names the
    sources whose ``source_liveness`` declared no ``source_as_of`` — the only
    ones where a midnight ``retrievedAt`` on the statement's own day is the
    cut-as-retrieval signature rather than a date-only build stamp beside a
    separately declared cut.
    """
    liveness = liveness or {}
    findings: list[Finding] = []
    for st in statements:
        sid = _source_id(st)
        stmt_id = str(st.get("statementId") or "") or None
        src = st.get("source") or {}
        statement_day = _day(st.get("statementDate"))
        published = _day((st.get("publicationDetails") or {}).get("publicationDate"))
        retrieved_raw = str(src.get("retrievedAt") or "")
        retrieved = _day(retrieved_raw)
        read_as = liveness.get(sid)

        def add(check: str, severity: str, message: str) -> None:
            findings.append(Finding(check, severity, sid, message, stmt_id))

        if statement_day and statement_day > today:
            add("date_in_future", FAIL, f"statementDate {statement_day} is after the run ({today})")
        for label, day in _dates_in(st):
            if day > today:
                add("date_in_future", FAIL, f"{label} {day} is after the run ({today})")
        if statement_day and published and statement_day > published:
            add(
                "statement_after_publication", FAIL,
                f"statementDate {statement_day} is after publicationDate {published}",
            )
        if retrieved and published and retrieved > published:
            add(
                "retrieved_after_publication", FAIL,
                f"retrievedAt {retrieved} is after publicationDate {published}",
            )
        for interest in (st.get("recordDetails") or {}).get("interests") or []:
            if not isinstance(interest, dict):
                continue
            start, end = _day(interest.get("startDate")), _day(interest.get("endDate"))
            if start and end and start > end:
                add("start_after_end", FAIL, f"interest starts {start} and ends {end}")
        if read_as in _BULK and statement_day == today:
            add(
                "bulk_dated_today", FAIL,
                f"a {read_as} source dated today — it declared neither its cut nor its build",
            )

        # The provenance rules below are about what OpenCheck publishes. A
        # statement another publisher wrote and OpenCheck hands on verbatim
        # (the OECD's MEIP file, Open Ownership's stored GLEIF and UK PSC
        # bundles) keeps its publisher's source block: BODS statements are
        # immutable, so its gaps are the publisher's, not ours to patch
        # (Phase 317).
        if sid in PUBLISHER_VERBATIM or not _published_by_opencheck(st):
            continue
        if (
            read_as == "snapshot"
            and sid in no_cut
            and retrieved_raw.endswith("T00:00:00Z")
            and retrieved == statement_day
        ):
            add(
                "cut_as_retrieval", WARN,
                f"retrievedAt {retrieved_raw} is midnight on the statement's own date — "
                "the register's cut published as OpenCheck's download",
            )
        if src and read_as not in (None, "stub") and not retrieved_raw:
            add("no_retrieved_at", WARN, f"read as {read_as} but no source.retrievedAt")
        if src and not src.get("opencheckSourceId"):
            add("no_source_id", WARN, "source block has no opencheckSourceId")
        types = src.get("type") or []
        if isinstance(types, str):
            types = [types]
        if official_registers and src.get("opencheckSourceId"):
            official = "officialRegister" in types
            if official != (sid in official_registers):
                add(
                    "source_type", WARN,
                    f"source.type {types} disagrees with OFFICIAL_REGISTER_SOURCES",
                )
        if st.get("recordType") == "relationship":
            interests = [
                i for i in (st.get("recordDetails") or {}).get("interests") or []
                if isinstance(i, dict)
            ]
            ended = [i for i in interests if (_day(i.get("endDate")) or "9999") <= today]
            if interests and len(ended) == len(interests) and st.get("recordStatus") != "closed":
                add(
                    "ended_not_closed", WARN,
                    f"every interest has ended but recordStatus is {st.get('recordStatus')!r}",
                )
    return findings


# --- lib-cove-bods --------------------------------------------------------------


def validate_schema(statements: Sequence[Mapping[str, Any]]) -> list[Finding]:
    """lib-cove-bods JSON Schema errors and non-advisory additional checks.

    Imported lazily: lib-cove-bods is a dev dependency, present in the sweep's
    environment (``uv sync``) but not in the production image. If it is
    missing the sweep says so as one finding rather than passing silently.
    """
    if not statements:
        return []
    try:
        import libcovebods.run_tasks
        from libcovebods.config import LibCoveBODSConfig
        from libcovebods.data_reader import DataReader
        from libcovebods.jsonschemavalidate import JSONSchemaValidator
        from libcovebods.schema import SchemaBODS
    except ImportError:
        return [Finding("schema", FAIL, "sweep", "lib-cove-bods is not installed; nothing validated")]

    by_id = {str(s.get("statementId")): s for s in statements}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(list(statements), fh)
        path = fh.name
    findings: list[Finding] = []
    try:
        config = LibCoveBODSConfig()
        schema = SchemaBODS(data_reader=DataReader(path), lib_cove_bods_config=config)
        for err in JSONSchemaValidator(schema).validate(DataReader(path)):
            data = err.json()
            sid = _schema_error_source(data, statements)
            findings.append(Finding(
                "schema", FAIL, sid,
                f"{data.get('message')} at {'/'.join(str(p) for p in data.get('path') or [])}",
            ))
        extra = libcovebods.run_tasks.process_additional_checks(DataReader(path), config, schema)
        for item in extra.get("additional_checks") or []:
            kind = str(item.get("type") or "")
            if kind in _ADVISORY_ADDITIONAL:
                continue
            stmt_id = item.get("statement")
            owner = by_id.get(str(stmt_id)) if stmt_id else None
            detail = {k: v for k, v in item.items() if k not in ("type", "statement")}
            findings.append(Finding(
                "bods_additional", FAIL,
                _source_id(owner) if owner else "unknown",
                f"{kind}: {json.dumps(detail, ensure_ascii=False, default=str)[:300]}",
                str(stmt_id) if stmt_id else None,
            ))
    finally:
        os.unlink(path)
    return findings


def _schema_error_source(error: Mapping[str, Any], statements: Sequence[Mapping[str, Any]]) -> str:
    path = error.get("path") or []
    if path and isinstance(path[0], int) and 0 <= path[0] < len(statements):
        return _source_id(statements[path[0]])
    return "unknown"


# --- per-source dating shape ----------------------------------------------------


def dating_profile(statements: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    """Per source: statements, with ``retrievedAt``, dated by the retrieval
    day, and distinct ``statementDate`` values."""
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    distinct: dict[str, set[str]] = defaultdict(set)
    for st in statements:
        sid = _source_id(st)
        src = st.get("source") or {}
        c = counts[sid]
        c["statements"] += 1
        day = _day(st.get("statementDate"))
        if day:
            distinct[sid].add(day)
        retrieved = _day(src.get("retrievedAt"))
        if retrieved:
            c["with_retrieved_at"] += 1
            if retrieved == day:
                c["dated_by_retrieval"] += 1
    return {
        sid: {
            "statements": c["statements"],
            "with_retrieved_at": c["with_retrieved_at"],
            "dated_by_retrieval": c["dated_by_retrieval"],
            "distinct_statement_dates": len(distinct[sid]),
        }
        for sid, c in sorted(counts.items())
    }


def merge_profiles(profiles: Iterable[Mapping[str, Mapping[str, int]]]) -> dict[str, dict[str, int]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for profile in profiles:
        for sid, row in profile.items():
            out[sid].update(row)
    return {sid: dict(c) for sid, c in sorted(out.items())}


# --- fetching -------------------------------------------------------------------


@dataclass
class Fetched:
    payload: Any = None
    error: str | None = None
    status: int | None = None
    attempts: int = 0
    seconds: float = 0.0
    waits: list[float] = field(default_factory=list)


def fetch_json(
    client: Any,
    url: str,
    params: Mapping[str, str],
    *,
    sleep: Sleep = time.sleep,
    retries: int = MAX_RETRIES,
) -> Fetched:
    """GET honouring ``Retry-After`` on 429/503; one retry on a transport error."""
    result = Fetched()
    started = time.monotonic()
    for attempt in range(retries + 1):
        result.attempts = attempt + 1
        try:
            resp = client.get(url, params=dict(params))
        except Exception as exc:  # noqa: BLE001 — report, never crash the run
            result.error = type(exc).__name__
            if attempt < retries:
                result.waits.append(30.0)
                sleep(30.0)
                continue
            break
        result.status = resp.status_code
        if resp.status_code in (429, 503) and attempt < retries:
            try:
                wait = float(resp.headers.get("Retry-After", "30"))
            except ValueError:
                wait = 30.0
            wait = min(max(wait, 1.0), _MAX_RETRY_WAIT_S)
            result.waits.append(wait)
            sleep(wait)
            continue
        if resp.status_code != 200:
            result.error = f"HTTP {resp.status_code}"
            try:
                detail = (resp.json() or {}).get("detail")
                if detail:
                    result.error += f": {str(detail)[:200]}"
            except Exception:  # noqa: BLE001
                pass
            break
        try:
            result.payload = resp.json()
            result.error = None
        except ValueError:
            result.error = "response was not JSON"
        break
    result.seconds = round(time.monotonic() - started, 1)
    return result


# --- subjects -------------------------------------------------------------------


@dataclass(frozen=True)
class DeepenSubject:
    source_id: str
    hit_id: str
    label: str
    expect_liveness: frozenset[str]


#: LEI-keyed probes whose adapter ``fetch`` takes something other than the
#: LEI, so ``/deepen`` cannot address them by it. OpenAleph's takes an entity
#: id; the golden-set lookups exercise it instead.
_NOT_DEEPENABLE_BY_LEI: frozenset[str] = frozenset({"openaleph"})


def deepen_subjects(probes: Mapping[str, Any]) -> list[DeepenSubject]:
    """Every probe ``/deepen`` can address: a one-argument ``fetch``, or
    ``fetch_by_lei`` (an LEI-keyed source's ``fetch`` takes the LEI). Probes of
    an ``inactive`` source are skipped — nothing is configured to answer."""
    out: list[DeepenSubject] = []
    for sid, probe in sorted(probes.items()):
        if getattr(probe, "tier", "") == "inactive":
            continue
        args = tuple(getattr(probe, "args", ()) or ())
        method = getattr(probe, "method", "fetch")
        if method == "fetch_by_lei" and sid in _NOT_DEEPENABLE_BY_LEI:
            continue
        if len(args) == 1 and method in ("fetch", "fetch_by_lei") and isinstance(args[0], str):
            out.append(DeepenSubject(
                sid, args[0], str(getattr(probe, "subject", "") or args[0]),
                frozenset(getattr(probe, "expect_liveness", frozenset()) or ()),
            ))
    return out


# --- the run --------------------------------------------------------------------


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _liveness_map(payload: Mapping[str, Any]) -> dict[str, str]:
    raw = payload.get("source_liveness") or {}
    return {
        sid: str((row or {}).get("liveness"))
        for sid, row in raw.items()
        if isinstance(row, dict) and row.get("liveness")
    }


def run(
    *,
    client: Any,
    base_url: str,
    lookup_subjects: list[Mapping[str, str]],
    deepen: list[DeepenSubject],
    official_registers: frozenset[str],
    pace_s: float = DEFAULT_PACE_S,
    deepen_pace_s: float = DEFAULT_DEEPEN_PACE_S,
    sleep: Sleep = time.sleep,
    previous: Mapping[str, Any] | None = None,
    commit: str | None = None,
    today: str | None = None,
    now: Callable[[], str] = _now,
) -> dict[str, Any]:
    """Read every subject from production and build the report."""
    today = today or _today()
    base = base_url.rstrip("/")
    subjects: list[dict[str, Any]] = []
    profiles: list[dict[str, dict[str, int]]] = []

    for i, subj in enumerate(lookup_subjects):
        if i and pace_s:
            sleep(pace_s)
        fetched = fetch_json(
            client, f"{base}/lookup", {"lei": subj["lei"], "refresh": "true"}, sleep=sleep
        )
        entry: dict[str, Any] = {
            "kind": "lookup", "id": subj["lei"], "name": subj.get("name") or subj["lei"],
            "seconds": fetched.seconds,
        }
        if fetched.error or not isinstance(fetched.payload, dict):
            entry["error"] = fetched.error or "no payload"
            entry["findings"] = [Finding(
                "lookup_failed", FAIL, "lookup", f"/lookup failed: {entry['error']}"
            ).to_dict()]
            subjects.append(entry)
            continue
        statements = [s for s in fetched.payload.get("bods") or [] if isinstance(s, dict)]
        live = _liveness_map(fetched.payload)
        no_cut = frozenset(
            sid for sid, row in (fetched.payload.get("source_liveness") or {}).items()
            if isinstance(row, dict) and not row.get("source_as_of")
        )
        found = validate_schema(statements) + check_statements(
            statements, today=today, liveness=live, no_cut=no_cut,
            official_registers=official_registers,
        )
        entry["statements"] = len(statements)
        entry["liveness"] = live
        entry["findings"] = [f.to_dict() for f in found]
        profiles.append(dating_profile(statements))
        subjects.append(entry)

    for i, ds in enumerate(deepen):
        if i and deepen_pace_s:
            sleep(deepen_pace_s)
        fetched = fetch_json(
            client, f"{base}/deepen", {"source": ds.source_id, "hit_id": ds.hit_id}, sleep=sleep
        )
        entry = {
            "kind": "deepen", "id": f"{ds.source_id}:{ds.hit_id}", "name": ds.label,
            "source_id": ds.source_id, "seconds": fetched.seconds,
        }
        if fetched.error or not isinstance(fetched.payload, dict):
            entry["error"] = fetched.error or "no payload"
            entry["findings"] = [Finding(
                "deepen_failed", FAIL, ds.source_id, f"/deepen failed: {entry['error']}"
            ).to_dict()]
            subjects.append(entry)
            continue
        statements = [s for s in fetched.payload.get("bods") or [] if isinstance(s, dict)]
        # A probe's expectation stands in for liveness only when it names one
        # state; "live or cached" says nothing a date rule can use.
        expected = next(iter(ds.expect_liveness)) if len(ds.expect_liveness) == 1 else None
        live = {ds.source_id: expected} if expected else {}
        found = validate_schema(statements) + check_statements(
            statements, today=today, liveness=live, official_registers=official_registers
        )
        entry["statements"] = len(statements)
        entry["findings"] = [f.to_dict() for f in found]
        profiles.append(dating_profile(statements))
        subjects.append(entry)

    report: dict[str, Any] = {
        "generated_at": now(),
        "base_url": base,
        "commit": commit,
        "today": today,
        "subjects": subjects,
        "dating": merge_profiles(profiles),
    }
    report["totals"] = totals(report)
    report["diff"] = diff(report, previous)
    return report


def totals(report: Mapping[str, Any]) -> dict[str, Any]:
    by_check: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    by_check_source: Counter[str] = Counter()
    for s in report.get("subjects", []):
        for f in s.get("findings", []):
            by_check[f["check"]] += 1
            by_severity[f["severity"]] += 1
            by_check_source[f"{f['check']}|{f['source_id']}"] += 1
    return {
        "subjects": len(report.get("subjects", [])),
        "statements": sum(int(s.get("statements") or 0) for s in report.get("subjects", [])),
        "fail": by_severity.get(FAIL, 0),
        "warn": by_severity.get(WARN, 0),
        "by_check": dict(sorted(by_check.items())),
        "by_check_source": dict(sorted(by_check_source.items())),
    }


def diff(report: Mapping[str, Any], previous: Mapping[str, Any] | None) -> dict[str, Any]:
    """Per ``check|source`` against the last published report."""
    if not previous:
        return {"available": False}
    now = (report.get("totals") or totals(report))["by_check_source"]
    before = (previous.get("totals") or {}).get("by_check_source") or {}
    keys = sorted(set(now) | set(before))
    return {
        "available": True,
        "compared_against": previous.get("generated_at"),
        "new": {k: now[k] for k in keys if k in now and k not in before},
        "resolved": {k: before[k] for k in keys if k in before and k not in now},
        "changed": {k: [before[k], now[k]] for k in keys if k in now and k in before and now[k] != before[k]},
    }


def failed(report: Mapping[str, Any]) -> bool:
    return bool((report.get("totals") or totals(report))["fail"])


# --- markdown -------------------------------------------------------------------


def render_markdown(report: Mapping[str, Any], *, examples_per_check: int = 3) -> str:
    t = report.get("totals") or totals(report)
    lines = [
        "# BODS quality sweep",
        "",
        f"Run {report['generated_at']} against `{report['base_url']}`"
        + (f" (code at `{report['commit']}`)" if report.get("commit") else "")
        + f". {t['subjects']} subjects, {t['statements']} statements: "
        + f"**{t['fail']} failing findings, {t['warn']} warnings**.",
        "",
        "Checks and their classes are documented in `backend/opencheck/bods_quality.py` "
        "and `docs/bods-quality.md`.",
        "",
    ]
    d = report.get("diff") or {}
    lines.append("## Since the last published run")
    lines.append("")
    if not d.get("available"):
        lines.append("No comparison available (no previous published report).")
    else:
        lines.append(f"Compared against {d.get('compared_against')}.")
        for label, key in (("New", "new"), ("Resolved", "resolved")):
            items = d.get(key) or {}
            lines.append(f"- {label}: " + (", ".join(f"`{k}` ×{v}" for k, v in items.items()) or "none"))
        changed = d.get("changed") or {}
        lines.append(
            "- Changed: "
            + (", ".join(f"`{k}` {a}→{b}" for k, (a, b) in changed.items()) or "none")
        )
    lines.append("")

    lines += ["## Findings by class and source", ""]
    if not t["by_check_source"]:
        lines.append("None.")
    else:
        examples: dict[str, list[str]] = defaultdict(list)
        severity: dict[str, str] = {}
        for s in report.get("subjects", []):
            for f in s.get("findings", []):
                key = f"{f['check']}|{f['source_id']}"
                severity[key] = f["severity"]
                if len(examples[key]) < examples_per_check:
                    where = f" (`{f['statement_id']}`)" if f.get("statement_id") else ""
                    examples[key].append(f"{s['name']}: {f['message']}{where}")
        lines += ["| | Check | Source | Count | Examples |", "|---|---|---|---|---|"]
        for key, n in sorted(t["by_check_source"].items(), key=lambda kv: (severity[kv[0]] != FAIL, kv[0])):
            check, sid = key.split("|", 1)
            mark = "❌" if severity[key] == FAIL else "⚠️"
            ex = "<br>".join(e.replace("|", "\\|") for e in examples[key])
            lines.append(f"| {mark} | `{check}` | `{sid}` | {n} | {ex} |")
    lines.append("")

    lines += [
        "## How each source dates its statements",
        "",
        "| Source | Statements | With retrievedAt | Dated by the retrieval day | Distinct statementDates |",
        "|---|---|---|---|---|",
    ]
    for sid, row in (report.get("dating") or {}).items():
        lines.append(
            f"| `{sid}` | {row.get('statements', 0)} | {row.get('with_retrieved_at', 0)} | "
            f"{row.get('dated_by_retrieval', 0)} | {row.get('distinct_statement_dates', 0)} |"
        )
    lines.append("")

    errors = [s for s in report.get("subjects", []) if s.get("error")]
    if errors:
        lines += ["## Subjects that could not be read", ""]
        for s in errors:
            lines.append(f"- {s['name']} (`{s['id']}`): {s['error']}")
        lines.append("")
    return "\n".join(lines)
