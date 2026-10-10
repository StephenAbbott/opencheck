"""watchlist — watch an LEI, be told when something about it changes.

Phase 215. A watchlist is a saved batch that re-runs on a delta. The design
problem was never the UI; it was the polling budget. "Re-run every watched
lookup daily" reproduces the crawl wave of 29 August 2026 — every watched
LEI a cold anchor of four to six GLEIF calls and ten upstream fetches, several
rate-limited and one CC-BY-NC at volume. So nothing here polls a company.

Two delta feeds, one shape
--------------------------

* **Tier 1 — GLEIF.** The Golden Copy delta that ``mirror_refresh`` already
  streams and applies in-process every hour is intersected with the
  watchlist. ``goldencopy.gleif.org`` is a different host from
  ``api.gleif.org``: the download costs nothing against the throttled
  window, regardless of watchlist size. A watched LEI in the delta is then
  read back from the mirror and its **material** fields — name, statuses,
  jurisdiction, legal form, parents, reporting exceptions, successors, expiry,
  the register identifier, the two address countries, category and
  conformity flag (Phase 300), legal entity events (Phase 301) and the
  standing direct subsidiaries (Phase 302) — are digested. A row whose only change is
  ``NextRenewalDate`` or
  ``LastUpdateDate`` has the same digest, so renewal churn (most of the
  16,000 rows a day) triggers nothing. Only a changed digest queues a re-run.
* **Tier 2 — OpenSanctions.** They publish an entity-level delta per
  version (``artifacts/default/<version>/entities.delta.json``, JSON-lines
  of ``{"op": ADD|MOD|DEL, "entity": {…}}``), about four a day. Every
  version since the last one seen is streamed and each organisation entity
  in it is compared with the watched entities' **own legal names** (the
  0.88 gate the rest of OpenCheck uses) and LEIs. Zero API calls, no
  re-screening, and the same public bulk file the licence already covers.
  Person screening deltas would put UBO names into this store; that waits
  for the GDPR ticket, as does email.

There is no third tier. National registers are re-fetched only as a
consequence of a Tier 1 or Tier 2 hit: a hit re-runs the whole lookup
pipeline (``refresh=True``, so the replay cache cannot answer for it), which
fetches them anyway. Freshness is a consequence of an observed change rather
than a clock, so "OpenCheck does not continuously monitor the registers" is
a description of the mechanism, not a disclaimer. The Phase 146 rule comes
for free: a source degraded during the re-run reports "could not check",
never "clean" — see :func:`diff_snapshots`.

Where state lives
-----------------

One SQLite file on the persistent disk that already holds the GLEIF mirror
(``OPENCHECK_WATCHLIST_DB_FILE``). A list is addressed by a capability token
— random, shown once, stored only as its SHA-256 — so holding the file never
yields a feed URL. There are no accounts, no credentials and no personal
data: LEIs, the legal names GLEIF publishes for them, digests, and the
diffs. Caps (``OPENCHECK_WATCHLIST_MAX_TOTAL`` / ``_MAX_PER_LIST``) are
cheap insurance, not what keeps the budget safe — at 0.47 % of LEI records
changing a day, two hundred watched LEIs cost under one re-run a day.

What a "change" is
------------------

:func:`snapshot_from_response` reduces a ``LookupResponse`` to the facts a
feed reader needs — the same helpers the batch row and the report use
(``subject_profile`` for register status, founding date and legal form;
``consistency.one_per_entity_identifiers`` for identifiers; the risk-signal
codes with the source that produced each; the Phase 156 coverage figures).
:func:`diff_snapshots` then names each difference with a closed vocabulary
(:data:`CHANGE_KINDS`). Two rules shape it:

* A signal code that was present and is now absent is **retired** only when
  the source that produced it answered this time. If that source is in
  ``degraded_sources``, the change is ``signal_unchecked`` — the code could
  not be re-assessed — never ``signal_retired``.
* Coverage that fell because sources were degraded is ``coverage_unchecked``,
  not ``coverage_changed``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import identifiers, sqlite_schema
from .bods import gleif_events as _gleif_events
from .names import name_similarity
from .secret_scrub import describe_exception
from .verdict import VERDICT_TEMPLATE

#: Mirrors ``risk.RETIRED_SIGNAL_CODES``. Not imported from ``risk`` to keep
#: this module's import graph light; ``test_watchlist`` pins the two equal.
RETIRED_SIGNAL_CODES: frozenset[str] = frozenset({"COMPLEX_CORPORATE_STRUCTURE"})

#: Phase 273 — the risk RULES version a snapshot was taken under, mirroring
#: Phase 245's ``verdict_template``. Bump it whenever a rule change moves which
#: codes fire for an unchanged company, and record the codes it moved below.
#: A snapshot written before Phase 273 carries no number: it was version 1.
SIGNAL_RULES = 3

#: version -> the codes whose firing that rules version changed. Comparing a
#: baseline from an older version, appearances and disappearances of these
#: codes are the rule's doing, not the company's, and are not reported; the
#: baseline then moves on, so the next re-run compares like with like.
#:
#: 2 = Phase 273: the FATF / EU list signals stopped reading subsidiaries and
#: side branches, and subsidiaries moved to SUBSIDIARY_LISTED_JURISDICTION.
#: 3 = Phase 319: EU_TAX_NON_COOPERATIVE is new, and the subsidiary note
#: reads Annex I of the EU tax list too, so it can appear for a company with
#: a subsidiary in, say, Anguilla and nothing else listed.
SIGNAL_RULES_CHANGED: dict[int, frozenset[str]] = {
    2: frozenset(
        {
            "FATF_BLACK_LIST",
            "FATF_GREY_LIST",
            "EU_HIGH_RISK_THIRD_COUNTRY",
            "SUBSIDIARY_LISTED_JURISDICTION",
        }
    ),
    3: frozenset({"EU_TAX_NON_COOPERATIVE", "SUBSIDIARY_LISTED_JURISDICTION"}),
}


#: Phase 305 — how a snapshot's ``dissolution_date`` was read. Version 2 reads
#: GLEIF's Legal Entity Events, which date an INACTIVE entity that GLEIF's
#: empty ``expiration`` left undated. A snapshot written before it carries no
#: number: it was version 1.
DISSOLUTION_READING = 2


def _dissolution_newly_read(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """True when ``after``'s dissolution date is one the old reading could
    not see rather than news: the baseline predates :data:`DISSOLUTION_READING`,
    had no date, and the new date is on or before the day the baseline was
    taken (its latest source retrieval; no retrieval recorded counts as
    before). A dissolution dated after the baseline is still reported, and
    once the baseline moves on every later change is compared as usual. The
    Phase 291 pattern: a reclassification, not a change in the company."""
    if int(before.get("dissolution_reading") or 1) >= DISSOLUTION_READING:
        return False
    if before.get("dissolution_date") or not after.get("dissolution_date"):
        return False
    taken = max(
        (str(c.get("retrieved_at") or "")[:10] for c in before.get("checked") or [] if isinstance(c, dict)),
        default="",
    )
    return not taken or str(after["dissolution_date"]) <= taken


def _rule_moved_codes(before: dict[str, Any], after: dict[str, Any]) -> frozenset[str]:
    """Codes a rules change between the two snapshots could have moved."""
    old = int(before.get("signal_rules") or 1)
    new = int(after.get("signal_rules") or 1)
    moved: set[str] = set()
    for version in range(old + 1, new + 1):
        moved |= SIGNAL_RULES_CHANGED.get(version, frozenset())
    return frozenset(moved)

log = logging.getLogger("opencheck.watchlist")

#: Tiers, as recorded on an entry. Closed vocabulary.
TIER_GLEIF = "gleif"
TIER_OPENSANCTIONS = "opensanctions"
TIER_MANUAL = "manual"
#: Phase 260: a re-run of every watched entity because OpenSanctions versions
#: could no longer be read (aged out of ``versions.json``, or their delta file
#: is gone). Writes an entry only when the re-run found a difference — unlike
#: a Tier 2 hit, the catch-up itself is not news about the company.
TIER_CATCHUP = "catch_up"
TIERS = (TIER_GLEIF, TIER_OPENSANCTIONS, TIER_MANUAL, TIER_CATCHUP)

#: The kinds a diff can contain. Closed vocabulary; the frontend words them
#: (``lib/watchlist.ts``) and a test pins that every kind has words.
CHANGE_KINDS: tuple[str, ...] = (
    "gleif_field",  # a material GLEIF record field (Tier 1 facts)
    "legal_name",
    "jurisdiction",
    "register_status",
    "founding_date",
    "legal_form",
    "dissolution_date",
    "identifier",
    "signal_new",
    "signal_retired",
    "signal_unchecked",
    "context_new",
    "context_retired",
    "context_unchecked",
    "coverage_changed",
    "coverage_unchecked",
    "verdict",
)

#: The name-match gate — the one concept product-wide (``names.name_similarity``).
NAME_MATCH_THRESHOLD = 0.88

#: OpenSanctions schemata that are organisations. v1 matches the watched
#: entity's own names only, so Person and its kin are never read.
_OS_ORG_SCHEMATA = frozenset({"Company", "LegalEntity", "Organization", "PublicBody"})

OS_VERSIONS_URL = "https://data.opensanctions.org/artifacts/default/versions.json"
OS_DELTA_URL = "https://data.opensanctions.org/artifacts/default/{version}/entities.delta.json"

#: How many OpenSanctions versions one tick will catch up on. Four a day are
#: published; after a week down this bounds the work to ~130 MB. The backlog
#: is drained OLDEST first (Phase 260): the watermark only ever moves past a
#: version that was read, or one recorded as a gap, so a longer outage takes
#: several ticks instead of silently skipping everything but the newest 12.
OS_MAX_VERSIONS_PER_TICK = 12

#: How many recorded gaps the store keeps (``meta.opensanctions_gaps``).
OS_GAPS_KEPT = 20


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def token_hash(token: str) -> str:
    return hashlib.sha256(token.strip().encode("utf-8")).hexdigest()


def new_token() -> str:
    return secrets.token_urlsafe(24)


# ---------------------------------------------------------------------------
# Tier 1 facts: the material GLEIF fields
# ---------------------------------------------------------------------------

#: The GLEIF fields whose change is material. ``Registration.NextRenewalDate``
#: and ``Registration.LastUpdateDate`` are deliberately absent: a row whose
#: only change is one of those is renewal churn and must trigger nothing.
#:
#: Phase 300 widened the set after reading GLEIF's own "what changed" tooling
#: (the 7 Oct 2026 "Metric in Motion" post and its DataAlerts notebook)
#: against the 7 Oct LastDay delta: the register identifier
#: (``registered_at`` / ``registered_as`` / ``validated_at``) is the key every
#: identifier-dispatched adapter hangs off, so a change to it changes every
#: downstream fetch; ``successors`` carries the names GLEIF publishes without
#: an LEI (36 of 61 M&A rows that day); the two address *countries* catch a
#: headquarters moving jurisdiction without any street-level noise (Birtley's
#: annual Wimborne/Cranleigh ping-pong is exactly what must stay out);
#: ``category`` / ``sub_category`` and ``conformity_flag`` are published facts
#: about how the record should be read. Nothing below country level, and no
#: event, address line, managing LOU or validation source.
GLEIF_MATERIAL_FIELDS: tuple[str, ...] = (
    "legal_name",
    "entity_status",
    "registration_status",
    "jurisdiction",
    "legal_form",
    "successors",
    "direct_parent_lei",
    "ultimate_parent_lei",
    "direct_exception",
    "ultimate_exception",
    "creation_date",
    "expiration_date",
    "expiration_reason",
    "registered_at",
    "registered_as",
    "validated_at",
    "legal_address_country",
    "hq_address_country",
    "category",
    "sub_category",
    "conformity_flag",
    "corporate_events",
    "direct_children",
)

#: Legal Entity Event types that are *not* material (Phase 301). Measured on
#: GLEIF's 7 Oct 2026 LastMonth delta (386,447 rows): 72,427 legal-address and
#: 71,181 headquarters-address events and 1,660 other-name events — the same
#: renewal-time re-keying the address fields are kept out for. Every other
#: type is material, including types GLEIF adds later: liquidation,
#: dissolution, bankruptcy, insolvency, voluntary arrangement, mergers and
#: acquisitions, absorption, demerger, breakup, spin-off, the fund
#: transformations, and the legal name and legal form changes (which carry
#: the effective date the plain ``legal_name`` / ``legal_form`` fields lack).
#: Phase 305: the one list, shared with the BODS event annotations.
CORPORATE_EVENT_EXCLUDED: frozenset[str] = _gleif_events.EXCLUDED_TYPES


def _corporate_events(detail: dict[str, Any]) -> list[dict[str, Any]] | None:
    """The material Legal Entity Events on a mirror row, as
    ``{"type", "status", "effective", "recorded"}`` dicts ordered by recorded
    date. A status moving (IN_PROGRESS → COMPLETED) or a new event changes
    the list, and so the digest. ``None`` when GLEIF lists none."""
    out: list[dict[str, Any]] = []
    for e in detail.get("events") or []:
        etype = str(e.get("type") or "").upper()
        if not etype or etype in CORPORATE_EVENT_EXCLUDED:
            continue
        out.append({
            "type": etype,
            "status": e.get("status") or None,
            "effective": e.get("effectiveDate") or None,
            "recorded": e.get("recordedDate") or None,
        })
    out.sort(key=lambda x: (x["recorded"] or "", x["type"], x["effective"] or "", x["status"] or ""))
    return out or None


def _parse_utc(value: Any) -> datetime | None:
    """A mirror watermark (``2026-09-16 00:00:00``) or a CDF timestamp
    (``2026-10-06T09:12:00Z``) as an aware UTC datetime."""
    if not value:
        return None
    raw = str(value).strip().replace("Z", "+00:00")
    if "T" not in raw and " " in raw:
        raw = raw.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _successor_labels(detail: dict[str, Any], row_successor: str | None) -> list[str] | None:
    """GLEIF's successor entities as ``"LEI — name"`` / ``"LEI"`` / ``"name"``
    labels, in file order. The detail carries up to five with their names;
    a v1 mirror row (no detail) still has the first successor LEI as a
    column. ``None`` when GLEIF names no successor."""
    out: list[str] = []
    for s in detail.get("successorEntities") or []:
        lei, name = (s.get("lei") or "").strip(), (s.get("name") or "").strip()
        if lei and name:
            out.append(f"{lei} — {name}")
        elif lei or name:
            out.append(lei or name)
    if not out and row_successor:
        out.append(row_successor)
    return out or None


def _authority_id(block: Any) -> str | None:
    """``registeredAt`` / ``validatedAt`` as one string: the RA code, or the
    free-text ``other`` when GLEIF has no code for the register."""
    if not isinstance(block, dict):
        return None
    return block.get("id") or block.get("other") or None


def gleif_facts(store: Any, lei: str) -> dict[str, Any] | None:
    """The material fields of ``lei``'s mirror row, or ``None`` when the
    mirror does not hold it. ``store`` is an ``entity_pages.EntityStore``."""
    row = store.get(lei)
    if row is None:
        return None
    detail = row.detail or {}
    expiration = detail.get("expiration") or {}
    exceptions = {}
    try:
        exceptions = store.exceptions(lei) or {}
    except Exception:  # noqa: BLE001 — a v1 file has no exceptions table
        exceptions = {}

    def _reason(kind: str) -> str | None:
        ex = exceptions.get(kind)
        return getattr(ex, "reason", None) if ex is not None else None

    legal_addr = detail.get("legalAddress") or {}
    hq_addr = detail.get("headquartersAddress") or {}
    return {
        "legal_name": row.name or None,
        "entity_status": row.entity_status or None,
        "registration_status": row.registration_status or None,
        "jurisdiction": row.jurisdiction or None,
        "legal_form": row.legal_form or None,
        "successors": _successor_labels(detail, row.successor_lei or None),
        "direct_parent_lei": row.direct_parent_lei or None,
        "ultimate_parent_lei": row.ultimate_parent_lei or None,
        "direct_exception": _reason("direct"),
        "ultimate_exception": _reason("ultimate"),
        "creation_date": detail.get("creationDate") or None,
        "expiration_date": expiration.get("date") or None,
        "expiration_reason": expiration.get("reason") or None,
        # Phase 300 — the register identifier, the address countries and the
        # record's own classification. All read off the detail the mirror
        # already keeps; nothing new is fetched.
        "registered_at": _authority_id(detail.get("registeredAt")),
        "registered_as": detail.get("registeredAs") or None,
        "validated_at": _authority_id(detail.get("validatedAt")),
        "legal_address_country": legal_addr.get("country") or row.country or None,
        "hq_address_country": hq_addr.get("country") or None,
        "category": detail.get("category") or None,
        "sub_category": detail.get("subCategory") or None,
        "conformity_flag": detail.get("conformityFlag") or None,
        # Phase 301 — only from a mirror whose full build carried events. On
        # an older file the key is absent rather than None: "the mirror does
        # not hold events" must never read as "GLEIF lists none", or the
        # rebuild would look like a new event on every watched LEI.
        **(
            {"corporate_events": _corporate_events(detail)}
            if getattr(store, "carries_events", False)
            else {}
        ),
        # Phase 302 — the standing direct subsidiaries, as sorted LEIs. LEIs
        # only: a child renaming itself is not a change to the parent. Absent
        # (not empty) on a file without the relationships table.
        **(
            {"direct_children": store.direct_child_leis(lei)}
            if getattr(store, "has_relationships", False)
            else {}
        ),
    }


def digest(obj: Any) -> str:
    """A stable digest of a JSON-able value."""
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def diff_gleif_facts(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    *,
    since: str | None = None,
) -> list[dict[str, Any]]:
    """The material GLEIF fields that differ, as ``gleif_field`` changes.

    Only a field **both** sides carry is compared. A baseline stored before a
    field joined :data:`GLEIF_MATERIAL_FIELDS` (Phase 300 added nine) has no
    value for it, and "OpenCheck started watching this field" is not a
    change to the company — the same rule as the verdict template and the
    signal-rules version on the lookup snapshot. :func:`on_gleif_delta`
    upgrades such a baseline to the current shape without an entry.
    """
    if before is None or after is None:
        return []
    out = []
    for name in GLEIF_MATERIAL_FIELDS:
        if name not in before or name not in after:
            if name == "corporate_events" and name in after:
                change = _events_since(after.get(name), since)
                if change:
                    out.append(change)
            continue
        a, b = before.get(name), after.get(name)
        if a != b:
            out.append({"kind": "gleif_field", "field": name, "old": a, "new": b})
    return out


def _events_since(events: list[dict[str, Any]] | None, since: str | None) -> dict[str, Any] | None:
    """Phase 301's one exception to the both-sides rule. A baseline taken
    before the mirror carried events has no ``corporate_events``, but it does
    have the watermark it was read at, and an event GLEIF *recorded* after
    that watermark happened while the company was being watched. Those are
    reported as new (old = none); anything recorded earlier was already true
    when the watch began."""
    start = _parse_utc(since)
    if start is None or not events:
        return None
    new = [e for e in events if (_parse_utc(e.get("recorded")) or start) > start]
    if not new:
        return None
    return {"kind": "gleif_field", "field": "corporate_events", "old": None, "new": new}


def facts_on_baseline_shape(facts: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    """``facts`` projected onto the keys ``baseline`` knows, so a digest
    comparison asks "did anything the baseline recorded change?" rather than
    "does the baseline have today's shape?"."""
    return {k: facts.get(k) for k in baseline}


# ---------------------------------------------------------------------------
# The lookup snapshot and its diff
# ---------------------------------------------------------------------------


def _subject_statements(lei: str, bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .subject_profile import subject_statements

    return subject_statements(lei, bods)


def snapshot_from_response(resp: Any) -> dict[str, Any]:
    """Reduce a ``LookupResponse`` to the facts the feed compares.

    Every figure comes from the helpers the report and the batch row use, so
    the watchlist cannot disagree with either about what a company's status
    or findings are.
    """
    from .consistency import one_per_entity_identifiers
    from .mcp.shaping import shape_batch_row

    row = shape_batch_row(resp)
    profile = getattr(resp, "subject_profile", None) or {}
    bods = list(getattr(resp, "bods", None) or [])
    stmts = _subject_statements(resp.lei, bods)

    identifiers_: dict[str, str] = {}
    dissolution: str | None = None
    for stmt in stmts:
        for scheme, value in one_per_entity_identifiers(stmt).items():
            identifiers_.setdefault(scheme, value)
        rd = stmt.get("recordDetails") or {}
        d = rd.get("dissolutionDate")
        if isinstance(d, str) and d and not dissolution:
            dissolution = d

    signals: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for s in getattr(resp, "risk_signals", None) or []:
        code = str(s.get("code") or "")
        if not code:
            continue
        key = (code, str(s.get("source_id") or ""), str(s.get("kind") or "risk"))
        if key in seen:
            continue
        seen.add(key)
        signals.append({"code": key[0], "source_id": key[1], "kind": key[2]})

    degraded = []
    for d in getattr(resp, "degraded_sources", None) or []:
        degraded.append(
            {
                "source_id": str(d.get("source_id") or ""),
                "check": str(d.get("check") or ""),
                "affected_signals": list(d.get("affected_signals") or []),
            }
        )

    def _value(key: str) -> str | None:
        v = profile.get(key) or {}
        return v.get("value") if isinstance(v, dict) else None

    return {
        "legal_name": resp.legal_name,
        "jurisdiction": resp.jurisdiction,
        "register_status": row["register_status"],
        "founding_date": _value("founding_date"),
        "legal_form": _value("legal_form"),
        "dissolution_date": dissolution,
        "identifiers": dict(sorted(identifiers_.items())),
        "signals": sorted(signals, key=lambda s: (s["kind"], s["code"], s["source_id"])),
        "coverage": row["coverage"],
        "degraded_sources": degraded,
        "verdict": row["verdict"],
        # Phase 245: which wording produced ``verdict``, so a template change
        # is not reported as a change in the company (see diff_snapshots).
        "verdict_template": VERDICT_TEMPLATE,
        # Phase 273: which risk-rules version produced ``signals`` (see
        # SIGNAL_RULES), so a rule change is not reported as a company change.
        "signal_rules": SIGNAL_RULES,
        # Phase 305: which reading produced ``dissolution_date``.
        "dissolution_reading": DISSOLUTION_READING,
        # Which sources were actually reached, and when (Phase 99/100: the
        # retrieval clock, per source). The feed says "these sources were
        # checked on that date as a result".
        "checked": _checked(getattr(resp, "source_liveness", None) or {}),
    }


def _checked(source_liveness: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for sid, prov in sorted(source_liveness.items()):
        if not isinstance(prov, dict):
            continue
        out.append(
            {
                "source_id": sid,
                "liveness": prov.get("liveness"),
                "retrieved_at": prov.get("retrieved_at"),
            }
        )
    return out


def _codes(snapshot: dict[str, Any], kind: str) -> dict[str, set[str]]:
    """``code → {source_ids}`` for the signals of one kind."""
    out: dict[str, set[str]] = {}
    for s in snapshot.get("signals") or []:
        if s.get("kind", "risk") == kind:
            out.setdefault(s["code"], set()).add(s.get("source_id") or "")
    return out


def _degraded_index(snapshot: dict[str, Any]) -> tuple[set[str], set[str]]:
    """``(degraded source ids, codes those degradations affect)``."""
    ids: set[str] = set()
    codes: set[str] = set()
    for d in snapshot.get("degraded_sources") or []:
        if d.get("source_id"):
            ids.add(d["source_id"])
        codes.update(d.get("affected_signals") or [])
    return ids, codes


def _coverage_view(cov: dict[str, Any] | None) -> dict[str, Any]:
    """A stored ``coverage`` block read the Phase 241 way.

    A block with ``with_data`` is current: ``answered`` counts every source
    that replied. A block without it was stored earlier, when ``answered``
    counted only sources with a record — so that figure is its ``with_data``
    and its ``answered`` (in the new sense) is unknown.
    """
    cov = cov or {}
    if "with_data" in cov:
        return {
            "applicable": cov.get("applicable"),
            "answered": cov.get("answered"),
            "with_data": cov.get("with_data"),
            "applicable_ids": list(cov.get("applicable_ids") or []),
            "answered_ids": list(cov.get("answered_ids") or []),
            "with_data_ids": list(cov.get("with_data_ids") or []),
        }
    return {
        "applicable": cov.get("applicable"),
        "answered": None,
        "with_data": cov.get("answered"),
        "applicable_ids": list(cov.get("applicable_ids") or []),
        "answered_ids": [],
        "with_data_ids": list(cov.get("answered_ids") or []),
    }


def _status_class(liveness: Any) -> Any:
    """The register-status class a change is judged on.

    Phase 291 split GLEIF's unmaintained ACTIVE out of ``live`` as
    ``declared``. The entity status is the same claim either way; what moved
    is the LEI registration, which ``gleif_field`` already reports. Without
    this every watched lapsed LEI would log a "register status changed" entry
    on its first re-check after the deploy — a reclassification, not news.
    """
    return "live" if liveness == "declared" else liveness


def diff_snapshots(before: dict[str, Any] | None, after: dict[str, Any]) -> list[dict[str, Any]]:
    """Name every difference between two snapshots. Empty when nothing the
    feed reports about has changed. ``before`` may be ``None`` (first run):
    then there is nothing to compare and the list is empty."""
    if not before:
        return []
    changes: list[dict[str, Any]] = []

    def _scalar(kind: str, key: str) -> None:
        a, b = before.get(key), after.get(key)
        if (a or None) != (b or None):
            changes.append({"kind": kind, "old": a, "new": b})

    _scalar("legal_name", "legal_name")
    _scalar("jurisdiction", "jurisdiction")
    _scalar("founding_date", "founding_date")
    _scalar("legal_form", "legal_form")
    if not _dissolution_newly_read(before, after):
        _scalar("dissolution_date", "dissolution_date")

    rs_a = _status_class((before.get("register_status") or {}).get("liveness"))
    rs_b = _status_class((after.get("register_status") or {}).get("liveness"))
    if rs_a != rs_b:
        changes.append(
            {
                "kind": "register_status",
                "old": before.get("register_status"),
                "new": after.get("register_status"),
            }
        )

    ids_a = before.get("identifiers") or {}
    ids_b = after.get("identifiers") or {}
    for scheme in sorted(set(ids_a) | set(ids_b)):
        if scheme in ids_a and scheme in ids_b and ids_a[scheme] != ids_b[scheme]:
            changes.append(
                {"kind": "identifier", "scheme": scheme, "old": ids_a[scheme], "new": ids_b[scheme]}
            )

    degraded_ids, degraded_codes = _degraded_index(after)
    rule_moved = _rule_moved_codes(before, after)
    for kind, new_kind, retired_kind, unchecked_kind in (
        ("risk", "signal_new", "signal_retired", "signal_unchecked"),
        ("context", "context_new", "context_retired", "context_unchecked"),
    ):
        ca, cb = _codes(before, kind), _codes(after, kind)
        for code in sorted(set(cb) - set(ca)):
            if code in rule_moved:
                continue
            changes.append({"kind": new_kind, "code": code, "sources": sorted(cb[code])})
        for code in sorted(set(ca) - set(cb)):
            # Phase 272: a code the engine stopped emitting on purpose (the
            # rule was withdrawn) vanishing from a stored baseline is not a
            # change in the company. Reporting it as "retired" would read as
            # an improvement nobody observed.
            if code in RETIRED_SIGNAL_CODES or code in rule_moved:
                continue
            producers = ca[code]
            # The Phase 146 rule: absence is a finding only when the source
            # that produced the code answered. A degraded producer, or a
            # degradation that names this code, means "could not check".
            if (producers & degraded_ids) or code in degraded_codes:
                changes.append(
                    {
                        "kind": unchecked_kind,
                        "code": code,
                        "sources": sorted(producers),
                        "degraded": sorted(producers & degraded_ids),
                    }
                )
            else:
                changes.append({"kind": retired_kind, "code": code, "sources": sorted(producers)})

    cov_a = _coverage_view(before.get("coverage"))
    cov_b = _coverage_view(after.get("coverage"))
    # Phase 241: ``answered`` now counts a source that replied with no record,
    # and ``with_data`` holds what ``answered`` meant before. A baseline stored
    # earlier carries only the old field, so it is compared on ``with_data``
    # alone — comparing its ``answered`` with a new one would report every
    # watched company's coverage as changed on the first re-run after deploy.
    answered_known = cov_a["answered"] is not None and cov_b["answered"] is not None
    changed = cov_a["applicable"] != cov_b["applicable"] or cov_a["with_data"] != cov_b["with_data"]
    if answered_known and cov_a["answered"] != cov_b["answered"]:
        changed = True
    if changed:
        missing = sorted(
            set(cov_b["applicable_ids"])
            - set(cov_b["answered_ids"] if cov_b["answered"] is not None else cov_b["with_data_ids"])
        )
        fell = (
            (cov_b["answered"] or 0) < (cov_a["answered"] or 0)
            if answered_known
            else (cov_b["with_data"] or 0) < (cov_a["with_data"] or 0)
        )
        unchecked = bool(degraded_ids & set(missing)) and fell
        changes.append(
            {
                "kind": "coverage_unchecked" if unchecked else "coverage_changed",
                "old": {k: cov_a[k] for k in ("applicable", "answered", "with_data")},
                "new": {k: cov_b[k] for k in ("applicable", "answered", "with_data")},
                "missing": missing,
            }
        )

    # A snapshot written before Phase 245 carries no template number: it was
    # template 1. Two sentences from different templates differ in wording
    # whatever happened to the company, so they are not compared.
    same_template = (before.get("verdict_template") or 1) == (after.get("verdict_template") or 1)
    # Phase 273: a verdict built under different risk rules is not compared
    # either — the sentence is built from the signals the rules produced.
    same_template = same_template and not rule_moved
    if same_template and (before.get("verdict") or None) != (after.get("verdict") or None) and not any(
        c["kind"] in ("register_status", "signal_new", "signal_retired", "signal_unchecked")
        for c in changes
    ):
        # The verdict sentence is built from the signals and the degraded
        # list, so it usually changes *with* one of the kinds above. On its
        # own it is still worth a line (a degraded source recovering, say).
        changes.append({"kind": "verdict", "old": before.get("verdict"), "new": after.get("verdict")})
    return changes


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS lists (
    token_hash TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS watches (
    token_hash TEXT NOT NULL,
    lei TEXT NOT NULL,
    added_at TEXT NOT NULL,
    legal_name TEXT,
    jurisdiction TEXT,
    gleif_facts_json TEXT,
    gleif_digest TEXT,
    gleif_watermark TEXT,
    snapshot_json TEXT,
    snapshot_at TEXT,
    last_checked_at TEXT,
    PRIMARY KEY (token_hash, lei)
);
CREATE INDEX IF NOT EXISTS watches_lei ON watches (lei);
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash TEXT NOT NULL,
    lei TEXT NOT NULL,
    created_at TEXT NOT NULL,
    tier TEXT NOT NULL,
    trigger_json TEXT NOT NULL,
    changes_json TEXT NOT NULL,
    checked_json TEXT NOT NULL,
    degraded_json TEXT NOT NULL,
    legal_name TEXT
);
CREATE INDEX IF NOT EXISTS entries_list ON entries (token_hash, id);
CREATE TABLE IF NOT EXISTS pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lei TEXT NOT NULL,
    tier TEXT NOT NULL,
    trigger_json TEXT NOT NULL,
    queued_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


#: Phase 260: the file's ``PRAGMA user_version`` history. Version 1 is the
#: schema as Phase 215 shipped it, so a file written before 260 (version 0)
#: is stamped 1 and nothing else changes. Append; never edit a shipped step.
MIGRATIONS: tuple[sqlite_schema.Migration, ...] = (
    sqlite_schema.Migration(1, "Phase 215 schema", sqlite_schema.statements(SCHEMA)),
    # Phase 303: GLEIF's own field-modification log — on an entry, the lines
    # since that list's baseline; on a watch, the 30 days before it began.
    # Both nullable: a NULL is "not fetched", distinct from a fetched log
    # that GLEIF could not serve ({"available": false}).
    sqlite_schema.Migration(
        2,
        "Phase 303 GLEIF modification log",
        (
            "ALTER TABLE entries ADD COLUMN gleif_log_json TEXT",
            "ALTER TABLE watches ADD COLUMN prewatch_log_json TEXT",
        ),
    ),
)


@dataclass
class Caps:
    max_total: int
    max_per_list: int


class WatchlistStore:
    """The SQLite file. Every method opens and closes its own short
    connection: the mirror-refresh thread, the worker task and request
    handlers all write, and short transactions are what keeps that safe."""

    def __init__(self, path: Path, caps: Caps):
        self.path = Path(path)
        self.caps = caps
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._conn()
        try:
            self.schema_version = sqlite_schema.migrate(conn, self.path, MIGRATIONS)
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    # -- lists -------------------------------------------------------------

    def create_list(self) -> str:
        """Create a list and return its token (the only time it is seen)."""
        token = new_token()
        now = _now_iso()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lists (token_hash, created_at, last_seen_at) VALUES (?, ?, ?)",
                (token_hash(token), now, now),
            )
        return token

    def list_exists(self, th: str) -> bool:
        with self._conn() as conn:
            return conn.execute("SELECT 1 FROM lists WHERE token_hash = ?", (th,)).fetchone() is not None

    def touch(self, th: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE lists SET last_seen_at = ? WHERE token_hash = ?", (_now_iso(), th))

    # -- watches -----------------------------------------------------------

    def counts(self, th: str | None = None) -> dict[str, int]:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
            distinct = conn.execute("SELECT COUNT(DISTINCT lei) FROM watches").fetchone()[0]
            mine = (
                conn.execute("SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)).fetchone()[0]
                if th
                else 0
            )
        return {"total": total, "distinct_leis": distinct, "in_list": mine}

    def check_capacity(self, th: str | None, lei: str) -> None:
        """Raise :class:`CapExceededError` if adding ``lei`` to the list
        ``th`` (``None`` = a list not yet created) would break either cap.

        Phase 234: asked *before* the baseline lookup, so a full instance
        refuses without running a lookup it cannot keep. :meth:`add_watch`
        asks again inside its transaction, which is what makes it exact."""
        with self._conn() as conn:
            if th is not None:
                exists = conn.execute(
                    "SELECT 1 FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
                ).fetchone()
                if exists:
                    return
                mine = conn.execute(
                    "SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)
                ).fetchone()[0]
                if mine >= self.caps.max_per_list:
                    raise CapExceededError("per_list", self.caps.max_per_list)
            total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
            if total >= self.caps.max_total:
                raise CapExceededError("total", self.caps.max_total)

    def add_watch(
        self,
        th: str,
        lei: str,
        *,
        legal_name: str | None,
        jurisdiction: str | None,
        gleif_facts: dict[str, Any] | None,
        gleif_watermark: str | None,
        snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Add ``lei`` to the list. Raises ``CapExceeded`` on either cap.
        Re-adding a watched LEI refreshes its baseline and is not a second
        row."""
        now = _now_iso()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                exists = conn.execute(
                    "SELECT 1 FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
                ).fetchone()
                if not exists:
                    mine = conn.execute(
                        "SELECT COUNT(*) FROM watches WHERE token_hash = ?", (th,)
                    ).fetchone()[0]
                    if mine >= self.caps.max_per_list:
                        raise CapExceededError("per_list", self.caps.max_per_list)
                    total = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
                    if total >= self.caps.max_total:
                        raise CapExceededError("total", self.caps.max_total)
                conn.execute(
                    """
                    INSERT INTO watches (token_hash, lei, added_at, legal_name, jurisdiction,
                        gleif_facts_json, gleif_digest, gleif_watermark, snapshot_json, snapshot_at,
                        last_checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(token_hash, lei) DO UPDATE SET
                        legal_name=excluded.legal_name, jurisdiction=excluded.jurisdiction,
                        gleif_facts_json=excluded.gleif_facts_json, gleif_digest=excluded.gleif_digest,
                        gleif_watermark=excluded.gleif_watermark, snapshot_json=excluded.snapshot_json,
                        snapshot_at=excluded.snapshot_at, last_checked_at=excluded.last_checked_at
                    """,
                    (
                        th,
                        lei,
                        now,
                        legal_name,
                        jurisdiction,
                        json.dumps(gleif_facts) if gleif_facts is not None else None,
                        digest(gleif_facts) if gleif_facts is not None else None,
                        gleif_watermark,
                        json.dumps(snapshot) if snapshot is not None else None,
                        now if snapshot is not None else None,
                        now if snapshot is not None else None,
                    ),
                )
                conn.execute("UPDATE lists SET last_seen_at = ? WHERE token_hash = ?", (now, th))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return self.get_watch(th, lei) or {}

    def remove_watch(self, th: str, lei: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM watches WHERE token_hash = ? AND lei = ?", (th, lei))
            return cur.rowcount > 0

    def get_watch(self, th: str, lei: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
            ).fetchone()
        return _watch_dict(row) if row else None

    def watches(self, th: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM watches WHERE token_hash = ? ORDER BY added_at, lei", (th,)
            ).fetchall()
        return [_watch_dict(r) for r in rows]

    def watched_leis(self) -> set[str]:
        with self._conn() as conn:
            return {r[0] for r in conn.execute("SELECT DISTINCT lei FROM watches")}

    def watched_names(self) -> dict[str, list[str]]:
        """``lei → [legal names]`` across every list — what Tier 2 matches on."""
        out: dict[str, list[str]] = {}
        with self._conn() as conn:
            for lei, name, facts in conn.execute(
                "SELECT lei, legal_name, gleif_facts_json FROM watches"
            ):
                names = out.setdefault(lei, [])
                for candidate in (name, (json.loads(facts) if facts else {}).get("legal_name")):
                    if candidate and candidate not in names:
                        names.append(candidate)
        return out

    def rows_for_lei(self, lei: str, *, with_hash: bool = False) -> list[dict[str, Any]]:
        """Every list's watch of ``lei``. ``with_hash`` adds the list's
        ``token_hash`` — for the re-run only; never serialise it."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM watches WHERE lei = ?", (lei,)).fetchall()
        out = []
        for r in rows:
            d = _watch_dict(r)
            if with_hash:
                d["token_hash"] = r["token_hash"]
            out.append(d)
        return out

    def upgrade_facts(
        self, th: str, lei: str, facts: dict[str, Any], watermark: str | None
    ) -> None:
        """Rewrite a watch's GLEIF facts to the current shape without
        touching ``last_checked_at`` (Phase 300: a churn row whose baseline
        predates a material field). Nothing is logged — the company did not
        change, OpenCheck's field set did."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE watches SET gleif_facts_json = ?, gleif_digest = ?, gleif_watermark = ? "
                "WHERE token_hash = ? AND lei = ?",
                (json.dumps(facts), digest(facts), watermark, th, lei),
            )

    def prewatch_log(self, th: str, lei: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT prewatch_log_json FROM watches WHERE token_hash = ? AND lei = ?", (th, lei)
            ).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def set_prewatch_log(self, th: str, lei: str, log_: dict[str, Any]) -> None:
        """Store what GLEIF's log showed for the days before this list
        started watching ``lei`` (Phase 303). Written once, when the watch is
        new; re-adding a watched LEI keeps it."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE watches SET prewatch_log_json = ? WHERE token_hash = ? AND lei = ?",
                (json.dumps(log_), th, lei),
            )

    def update_baseline(
        self,
        th: str,
        lei: str,
        *,
        gleif_facts: dict[str, Any] | None = None,
        gleif_watermark: str | None = None,
        snapshot: dict[str, Any] | None = None,
        legal_name: str | None = None,
        jurisdiction: str | None = None,
    ) -> None:
        now = _now_iso()
        sets = ["last_checked_at = ?"]
        args: list[Any] = [now]
        if gleif_facts is not None:
            sets += ["gleif_facts_json = ?", "gleif_digest = ?", "gleif_watermark = ?"]
            args += [json.dumps(gleif_facts), digest(gleif_facts), gleif_watermark]
        if snapshot is not None:
            sets += ["snapshot_json = ?", "snapshot_at = ?"]
            args += [json.dumps(snapshot), now]
        if legal_name:
            sets.append("legal_name = ?")
            args.append(legal_name)
        if jurisdiction:
            sets.append("jurisdiction = ?")
            args.append(jurisdiction)
        args += [th, lei]
        with self._conn() as conn:
            conn.execute(f"UPDATE watches SET {', '.join(sets)} WHERE token_hash = ? AND lei = ?", args)

    # -- entries -----------------------------------------------------------

    def add_entry(
        self,
        th: str,
        lei: str,
        *,
        tier: str,
        trigger: dict[str, Any],
        changes: list[dict[str, Any]],
        checked: list[dict[str, Any]],
        degraded: list[dict[str, Any]],
        legal_name: str | None,
        gleif_log: dict[str, Any] | None = None,
    ) -> int:
        assert tier in TIERS, tier
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO entries (token_hash, lei, created_at, tier, trigger_json, changes_json,
                    checked_json, degraded_json, legal_name, gleif_log_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    th,
                    lei,
                    _now_iso(),
                    tier,
                    json.dumps(trigger),
                    json.dumps(changes),
                    json.dumps(checked),
                    json.dumps(degraded),
                    legal_name,
                    json.dumps(gleif_log) if gleif_log is not None else None,
                ),
            )
            return int(cur.lastrowid or 0)

    def entries(self, th: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM entries WHERE token_hash = ? ORDER BY id DESC LIMIT ?", (th, limit)
            ).fetchall()
        return [_entry_dict(r) for r in rows]

    # -- pending re-runs ---------------------------------------------------

    def enqueue(self, lei: str, tier: str, trigger: dict[str, Any]) -> bool:
        """Queue a re-run of ``lei``. One pending row per LEI: a second
        trigger before the first ran is folded into it — except that a
        catch-up row (Phase 260) gives way to a real trigger, whose entry is
        news in itself, so a delta naming the company is never folded into a
        catch-up that writes nothing when the re-run finds no difference."""
        with self._conn() as conn:
            existing = conn.execute("SELECT id, tier FROM pending WHERE lei = ?", (lei,)).fetchone()
            if existing:
                if existing["tier"] == TIER_CATCHUP and tier != TIER_CATCHUP:
                    conn.execute(
                        "UPDATE pending SET tier = ?, trigger_json = ? WHERE id = ?",
                        (tier, json.dumps(trigger), existing["id"]),
                    )
                    return True
                return False
            conn.execute(
                "INSERT INTO pending (lei, tier, trigger_json, queued_at) VALUES (?, ?, ?, ?)",
                (lei, tier, json.dumps(trigger), _now_iso()),
            )
            return True

    def take_pending(self, limit: int) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pending ORDER BY id LIMIT ?", (limit,)
            ).fetchall()
            out = [
                {
                    "id": r["id"],
                    "lei": r["lei"],
                    "tier": r["tier"],
                    "trigger": json.loads(r["trigger_json"]),
                    "queued_at": r["queued_at"],
                }
                for r in rows
            ]
            if out:
                conn.executemany("DELETE FROM pending WHERE id = ?", [(r["id"],) for r in out])
        return out

    def pending_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    # -- meta / housekeeping -------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def prune_empty_lists(self, *, older_than_s: float = 86400.0) -> int:
        """Drop lists that hold nothing and have not been opened for a day —
        a token minted by a visitor who never watched anything."""
        cutoff = datetime.fromtimestamp(time.time() - older_than_s, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._conn() as conn:
            cur = conn.execute(
                """
                DELETE FROM lists WHERE last_seen_at < ?
                  AND token_hash NOT IN (SELECT DISTINCT token_hash FROM watches)
                  AND token_hash NOT IN (SELECT DISTINCT token_hash FROM entries)
                """,
                (cutoff,),
            )
            return cur.rowcount


    def prune_stale_lists(self, *, older_than_days: int) -> int:
        """Phase 234: delete lists nobody has opened for ``older_than_days``
        — the page or the Atom feed; both touch ``last_seen_at`` — with their
        watches, entries and nothing else. Without it an abandoned list held
        its share of the instance cap, and was re-run on every delta, forever.
        Returns the number of lists deleted."""
        if older_than_days <= 0:
            return 0
        cutoff = datetime.fromtimestamp(
            time.time() - older_than_days * 86400.0, UTC
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                stale = [
                    r[0]
                    for r in conn.execute(
                        "SELECT token_hash FROM lists WHERE last_seen_at < ?", (cutoff,)
                    ).fetchall()
                ]
                for th in stale:
                    conn.execute("DELETE FROM watches WHERE token_hash = ?", (th,))
                    conn.execute("DELETE FROM entries WHERE token_hash = ?", (th,))
                    conn.execute("DELETE FROM lists WHERE token_hash = ?", (th,))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return len(stale)


class CapExceededError(Exception):
    def __init__(self, which: str, cap: int):
        super().__init__(f"{which} cap of {cap} reached")
        self.which = which
        self.cap = cap


def _watch_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "lei": row["lei"],
        "added_at": row["added_at"],
        "legal_name": row["legal_name"],
        "jurisdiction": row["jurisdiction"],
        "gleif_facts": json.loads(row["gleif_facts_json"]) if row["gleif_facts_json"] else None,
        "gleif_watermark": row["gleif_watermark"],
        "snapshot": json.loads(row["snapshot_json"]) if row["snapshot_json"] else None,
        "snapshot_at": row["snapshot_at"],
        "last_checked_at": row["last_checked_at"],
        "gleif_history": _json_col(row, "prewatch_log_json"),
    }


def _json_col(row: sqlite3.Row, name: str) -> Any:
    """A nullable JSON column, or ``None`` when the row predates it."""
    if name not in row.keys() or not row[name]:
        return None
    return json.loads(row[name])


def _entry_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "lei": row["lei"],
        "legal_name": row["legal_name"],
        "created_at": row["created_at"],
        "tier": row["tier"],
        "trigger": json.loads(row["trigger_json"]),
        "changes": json.loads(row["changes_json"]),
        "checked": json.loads(row["checked_json"]),
        "degraded": json.loads(row["degraded_json"]),
        "gleif_log": _json_col(row, "gleif_log_json"),
    }


# ---------------------------------------------------------------------------
# The configured store
# ---------------------------------------------------------------------------

_store: WatchlistStore | None = None
_store_lock = threading.Lock()


def get_store() -> WatchlistStore | None:
    """The configured store, or ``None`` when ``OPENCHECK_WATCHLIST_DB_FILE``
    is unset — in which case the feature is off and its routes say so."""
    global _store
    from .config import get_settings

    path = get_settings().watchlist_db_file
    if not path:
        return None
    with _store_lock:
        if _store is None or _store.path != Path(path):
            s = get_settings()
            _store = WatchlistStore(
                Path(path), Caps(s.watchlist_max_total, s.watchlist_max_per_list)
            )
        return _store


def reset_for_tests() -> None:
    global _store, _state
    with _store_lock:
        _store = None
    with _state_lock:
        _state = WatcherState()


# ---------------------------------------------------------------------------
# Tier 1 — the GLEIF delta hook
# ---------------------------------------------------------------------------


@dataclass
class WatcherState:
    """What ``/watchstats`` reports. Aggregate only — no LEI, no name."""

    enabled: bool = False
    gleif_deltas_seen: int = 0
    gleif_touched: int = 0  # watched LEIs present in a delta
    gleif_churn: int = 0  # …whose material digest did not change
    gleif_queued: int = 0
    gleif_rebaselined: int = 0  # …churn rows whose baseline predated a field and was upgraded (Phase 300)
    gleif_resyncs: int = 0  # full re-reads of every watch after a mirror rebuild (Phase 301)
    gleif_log_calls: int = 0  # GLEIF field-modification log reads (Phase 303)
    gleif_log_unavailable: int = 0  # …that came back without a log
    gleif_last_publish: str | None = None
    os_versions_seen: int = 0
    os_last_version: str | None = None
    os_queued: int = 0
    os_last_checked_at: str | None = None
    os_last_error: str | None = None
    os_backlog: int = 0  # listed versions newer than the watermark, not yet read
    os_gaps: int = 0  # versions recorded as unreadable since boot
    os_last_gap_at: str | None = None
    reruns: int = 0
    rerun_failures: int = 0
    entries_written: int = 0
    last_tick_at: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


_state = WatcherState()
_state_lock = threading.Lock()


def state() -> dict[str, Any]:
    with _state_lock:
        return _state.to_dict()


def _bump(**fields: Any) -> None:
    with _state_lock:
        for k, v in fields.items():
            if isinstance(v, bool) or not isinstance(v, int):
                setattr(_state, k, v)
            else:
                setattr(_state, k, getattr(_state, k) + v)


def on_gleif_delta(
    changed_leis: Iterable[str], publish_label: str, *, resync: bool = False
) -> dict[str, int]:
    """Called by ``mirror_refresh.apply_delta`` after a delta landed, with
    the LEIs the delta's three files named. Runs on the refresh thread.

    Intersects with the watchlist, re-reads each touched LEI from the mirror
    and queues a re-run only where the material digest changed. Never
    raises: the mirror refresh must not fail because the watcher did.
    """
    try:
        store = get_store()
        if store is None:
            return {"touched": 0, "queued": 0, "churn": 0}
        from . import entity_pages as ep

        mirror = ep.get_store()
        watched = store.watched_leis()
        touched = sorted(watched & {str(x).strip().upper() for x in changed_leis})
        queued = churn = rebaselined = 0
        watermark: str | None = None
        if mirror is not None:
            wm = mirror.watermark()
            watermark = wm.strftime("%Y-%m-%d %H:%M:%S") if wm else None
        for lei in touched:
            facts = gleif_facts(mirror, lei) if mirror is not None else None
            rows = store.rows_for_lei(lei, with_hash=True)
            changed_fields: list[dict[str, Any]] = []
            material = False
            for row in rows:
                baseline = row.get("gleif_facts")
                if facts is None or baseline is None:
                    if resync:
                        # A rebuild resync re-reads every watch; one with no
                        # baseline yet is not news about the company.
                        continue
                    # No baseline (or no mirror row) — the delta named it, so
                    # a re-run is the honest response; the re-run stores one.
                    material = True
                    continue
                # Compare on the material fields both sides carry (Phase 300),
                # plus events recorded since the baseline's watermark when the
                # baseline predates them (Phase 301). Phase 301 also decides
                # on the diff rather than a digest of the projection, so a key
                # retired from the set (``successor_lei``) cannot queue a
                # re-run that names no field.
                diff = diff_gleif_facts(baseline, facts, since=row.get("gleif_watermark"))
                if diff:
                    material = True
                    changed_fields = diff
                elif set(baseline) != set(facts):
                    # Churn, but the baseline predates the current field set:
                    # move it on silently so the next delta compares like
                    # with like. Not a check, so ``last_checked_at`` stays.
                    store.upgrade_facts(row["token_hash"], lei, facts, watermark)
                    rebaselined += 1
            if material:
                trigger: dict[str, Any] = {
                    "tier": TIER_GLEIF,
                    "publish": publish_label,
                    "fields": [c["field"] for c in changed_fields],
                }
                if resync:
                    trigger["resync"] = True
                if store.enqueue(lei, TIER_GLEIF, trigger):
                    queued += 1
            else:
                churn += 1
        if resync:
            _bump(
                gleif_resyncs=1,
                gleif_queued=queued,
                gleif_rebaselined=rebaselined,
            )
        else:
            _bump(
                gleif_deltas_seen=1,
                gleif_touched=len(touched),
                gleif_churn=churn,
                gleif_queued=queued,
                gleif_rebaselined=rebaselined,
                gleif_last_publish=publish_label,
            )
        if touched:
            log.info(
                "watchlist: GLEIF delta %s named %d watched LEI(s) — %d queued, %d renewal churn",
                publish_label, len(touched), queued, churn,
            )
        return {"touched": len(touched), "queued": queued, "churn": churn}
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist: GLEIF delta hook failed: %s", exc)
        _bump(last_error=f"gleif: {type(exc).__name__}: {exc}")
        return {"touched": 0, "queued": 0, "churn": 0}


# ---------------------------------------------------------------------------
# Tier 2 — the OpenSanctions entity delta
# ---------------------------------------------------------------------------


def _os_entity_names(entity: dict[str, Any]) -> list[str]:
    props = entity.get("properties") or {}
    names: list[str] = []
    for key in ("name", "alias", "previousName"):
        for v in props.get(key) or []:
            if isinstance(v, str) and v and v not in names:
                names.append(v)
    caption = entity.get("caption")
    if isinstance(caption, str) and caption and caption not in names:
        names.append(caption)
    return names


def match_os_entity(
    entity: dict[str, Any], watched: dict[str, list[str]], *, threshold: float = NAME_MATCH_THRESHOLD
) -> list[dict[str, Any]]:
    """The watched LEIs an OpenSanctions entity matches: by ``leiCode``
    exactly, else by name at the 0.88 gate. Organisation schemata only."""
    if entity.get("schema") not in _OS_ORG_SCHEMATA:
        return []
    props = entity.get("properties") or {}
    hits: list[dict[str, Any]] = []
    leis = {str(v).strip().upper() for v in props.get("leiCode") or [] if v}
    for lei in sorted(leis & set(watched)):
        hits.append({"lei": lei, "matched_on": "lei", "score": 1.0})
    matched = {h["lei"] for h in hits}
    names = _os_entity_names(entity)
    if not names:
        return hits
    for lei, own in watched.items():
        if lei in matched:
            continue
        best = 0.0
        for a in own:
            for b in names:
                best = max(best, name_similarity(a, b))
                if best >= 1.0:
                    break
        if best >= threshold:
            hits.append({"lei": lei, "matched_on": "name", "score": round(best, 3)})
    return hits


def _os_versions(client: Any) -> list[str]:
    r = client.get(OS_VERSIONS_URL, timeout=60.0)
    r.raise_for_status()
    items = r.json().get("items") or []
    return [str(v) for v in items]


def scan_os_delta(lines: Iterable[str], watched: dict[str, list[str]], version: str) -> list[dict[str, Any]]:
    """Every (LEI, trigger) a delta file yields, one per LEI (first match
    wins; the trigger names the entity and the op)."""
    triggers: dict[str, dict[str, Any]] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue
        entity = rec.get("entity") or {}
        for hit in match_os_entity(entity, watched):
            triggers.setdefault(
                hit["lei"],
                {
                    "tier": TIER_OPENSANCTIONS,
                    "version": version,
                    "op": rec.get("op"),
                    "entity_id": entity.get("id"),
                    "caption": entity.get("caption"),
                    "schema": entity.get("schema"),
                    "datasets": list(entity.get("datasets") or [])[:8],
                    "topics": list((entity.get("properties") or {}).get("topics") or [])[:8],
                    "matched_on": hit["matched_on"],
                    "score": hit["score"],
                },
            )
    return [{"lei": lei, "trigger": t} for lei, t in triggers.items()]


class _DeltaGone(Exception):
    """A listed version whose delta file answers 404 — it will not come back."""


def _read_os_delta(client: Any, store: "WatchlistStore", watched: dict[str, list[str]], version: str) -> int:
    queued = 0
    with client.stream("GET", OS_DELTA_URL.format(version=version), timeout=600.0) as r:
        if r.status_code == 404:
            raise _DeltaGone(version)
        r.raise_for_status()
        for item in scan_os_delta(r.iter_lines(), watched, version):
            if store.enqueue(item["lei"], TIER_OPENSANCTIONS, item["trigger"]):
                queued += 1
    return queued


def os_gaps(store: "WatchlistStore") -> list[dict[str, Any]]:
    """The OpenSanctions versions the watcher could not read, newest last."""
    raw = store.get_meta("opensanctions_gaps")
    try:
        gaps = json.loads(raw) if raw else []
    except json.JSONDecodeError:
        gaps = []
    return gaps if isinstance(gaps, list) else []


def _record_os_gap(store: "WatchlistStore", gap: dict[str, Any]) -> int:
    """Record versions that could not be read and queue a catch-up re-run of
    every watched entity, so a company named only in a lost delta is still
    re-checked. Returns how many re-runs were queued."""
    gap = {**gap, "detected_at": _now_iso()}
    gaps = (os_gaps(store) + [gap])[-OS_GAPS_KEPT:]
    store.set_meta("opensanctions_gaps", json.dumps(gaps))
    queued = 0
    trigger = {"tier": TIER_CATCHUP, "reason": gap["reason"], "after": gap.get("after"), "before": gap.get("before")}
    for lei in sorted(store.watched_leis()):
        if store.enqueue(lei, TIER_CATCHUP, trigger):
            queued += 1
    log.warning(
        "watchlist: OpenSanctions versions could not be read (%s, after %s, before %s); "
        "queued %d catch-up re-run%s",
        gap["reason"], gap.get("after"), gap.get("before"), queued, "" if queued == 1 else "s",
    )
    with _state_lock:
        _state.os_gaps += 1
        _state.os_last_gap_at = gap["detected_at"]
    return queued


def _set_backlog(n: int) -> None:
    with _state_lock:
        _state.os_backlog = n


def opensanctions_tick(client: Any | None = None) -> dict[str, Any]:
    """Catch up on OpenSanctions versions since the last one seen, oldest
    first and at most :data:`OS_MAX_VERSIONS_PER_TICK` per call, and queue a
    re-run for each watched entity a delta named. Runs on a thread.
    Downloads nothing when nothing is watched.

    The watermark (``meta.opensanctions_version``) moves past a version only
    once its delta was read, or once it is recorded as a *gap*: versions that
    aged out of ``versions.json`` (it lists about the last 100 — some 25
    days) before they were read, or a listed version whose delta answers
    404. A gap queues a catch-up re-run of every watched entity
    (:data:`TIER_CATCHUP`) — without it a company named only in a lost delta
    would never be re-checked. Any other failure stops the tick with the
    watermark where it was, so the next tick retries that same version.
    """
    import httpx

    store = get_store()
    if store is None:
        return {"versions": 0, "queued": 0}
    watched = store.watched_names()
    result: dict[str, Any] = {"versions": 0, "queued": 0}
    _bump(os_last_checked_at=_now_iso())
    if not watched:
        return result
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, headers={"User-Agent": "OpenCheck watchlist"})
    try:
        versions = sorted(set(_os_versions(client)))
        if not versions:
            return result
        last = store.get_meta("opensanctions_version")
        if last is None:
            # First run: mark the current version and start from the next.
            store.set_meta("opensanctions_version", versions[-1])
            _bump(os_last_version=versions[-1])
            _set_backlog(0)
            return result
        pending = [v for v in versions if v > last]
        if pending and last not in versions and last < versions[0]:
            # Versions between ``last`` and the oldest still listed were
            # published and dropped from the list while we were not reading.
            result["catch_up_queued"] = _record_os_gap(
                store, {"reason": "aged_out", "after": last, "before": versions[0]}
            )
            result["gaps"] = 1
        todo = pending[:OS_MAX_VERSIONS_PER_TICK]
        _set_backlog(len(pending))
        for i, version in enumerate(todo):
            try:
                queued = _read_os_delta(client, store, watched, version)
            except _DeltaGone:
                result["catch_up_queued"] = result.get("catch_up_queued", 0) + _record_os_gap(
                    store, {"reason": "delta_missing", "after": version, "before": version}
                )
                result["gaps"] = result.get("gaps", 0) + 1
                queued = 0
            store.set_meta("opensanctions_version", version)
            _set_backlog(len(pending) - i - 1)
            result["versions"] += 1
            result["queued"] += queued
            _bump(os_versions_seen=1, os_queued=queued, os_last_version=version, os_last_error=None)
            if queued:
                log.info("watchlist: OpenSanctions %s named %d watched entit%s", version, queued, "y" if queued == 1 else "ies")
        result["backlog"] = len(pending) - len(todo)
        if result["backlog"]:
            log.info("watchlist: %d OpenSanctions version%s still to read", result["backlog"], "" if result["backlog"] == 1 else "s")
        return result
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist: OpenSanctions delta failed: %s", describe_exception(exc))
        _bump(os_last_error=describe_exception(exc))
        return result
    finally:
        if own_client:
            client.close()


# ---------------------------------------------------------------------------
# The re-run
# ---------------------------------------------------------------------------


async def _lookup(lei: str) -> Any:
    from .routers.lookup import _lookup_impl

    return await _lookup_impl(lei, refresh=True)


async def rerun(lei: str, tier: str, trigger: dict[str, Any], *, only_token_hash: str | None = None) -> dict[str, Any]:
    """Re-run the lookup for ``lei`` and write an entry to every list that
    watches it (or just ``only_token_hash``'s). Returns what happened, for
    the manual re-check to show.

    An entry is written when the tier that fired reported a material GLEIF
    change, when the lookup snapshot differs, or when OpenSanctions named
    the entity — a Tier 2 hit is itself the news even if the screen came
    back the same. A manual re-check that finds nothing writes no entry.
    """
    # Phase 260: every store and mirror read below runs on a worker thread —
    # this coroutine runs on the event loop (the worker's tick, or the
    # manual re-check route), and the SQLite calls can wait up to 30 s on a
    # write lock the mirror refresh holds.
    store = await asyncio.to_thread(get_store)
    if store is None:
        return {"lei": lei, "entries": 0, "changes": []}
    rows = await asyncio.to_thread(store.rows_for_lei, lei, with_hash=True)
    if only_token_hash:
        rows = [r for r in rows if r["token_hash"] == only_token_hash]
    if not rows:
        return {"lei": lei, "entries": 0, "changes": []}

    try:
        resp = await _lookup(lei)
    except Exception as exc:  # noqa: BLE001 — a failed re-run is counted, the baseline stays
        log.warning("watchlist: re-run of a watched LEI failed: %s: %s", type(exc).__name__, exc)
        _bump(rerun_failures=1)
        return {"lei": lei, "entries": 0, "changes": [], "error": f"{type(exc).__name__}"}
    _bump(reruns=1)

    facts, watermark = await asyncio.to_thread(_mirror_facts, lei)
    snapshot = snapshot_from_response(resp)
    # Phase 303: on a GLEIF-triggered re-run, GLEIF's own log since the
    # oldest baseline among these lists — one call for all of them.
    glog = await fetch_gleif_log(lei, rows) if tier == TIER_GLEIF else None
    written, all_changes = await asyncio.to_thread(
        _write_rerun, store, rows, lei, tier, trigger, resp, facts, watermark, snapshot, glog
    )
    _bump(entries_written=written)
    return {
        "lei": lei,
        "legal_name": resp.legal_name,
        "entries": written,
        "changes": all_changes,
        "checked": snapshot["checked"],
        "degraded": snapshot["degraded_sources"],
        "snapshot": snapshot,
    }


def _mirror_facts(lei: str) -> tuple[dict[str, Any] | None, str | None]:
    """The mirror's material GLEIF facts for ``lei`` and its watermark."""
    from . import entity_pages as ep

    mirror = ep.get_store()
    if mirror is None:
        return None, None
    facts = gleif_facts(mirror, lei)
    wm = mirror.watermark()
    return facts, (wm.strftime("%Y-%m-%d %H:%M:%S") if wm else None)


def name_children(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach the names of the subsidiaries a ``direct_children`` change
    added or removed, read from the mirror at entry-write time (Phase 302),
    so the entry can say who joined or left without the fact carrying names
    a rename would disturb."""
    from . import entity_pages as ep

    for c in changes:
        if c.get("kind") != "gleif_field" or c.get("field") != "direct_children":
            continue
        old, new = set(c.get("old") or []), set(c.get("new") or [])
        moved = sorted(old ^ new)
        mirror = ep.get_store()
        names: dict[str, str] = {}
        if mirror is not None and moved:
            for lei, row in mirror.get_many(moved).items():
                if row.name:
                    names[lei] = row.name
        c["names"] = names
    return changes


def baseline_cutoff(row: dict[str, Any]) -> datetime | None:
    """When a list's baseline for this LEI was taken, for GLEIF's log: the
    mirror watermark it was read at (a log line dated at that publish is
    already in it), else when it was last checked or added."""
    for key in ("gleif_watermark", "last_checked_at", "added_at"):
        when = _parse_utc(row.get(key))
        if when is not None:
            return when
    return None


async def fetch_gleif_log(lei: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """GLEIF's field-modification log since the oldest of ``rows``'
    baselines (Phase 303). Counted; never raises."""
    from . import gleif_log

    cutoffs = [c for c in (baseline_cutoff(r) for r in rows) if c is not None]
    if not cutoffs:
        return None
    result = await gleif_log.fetch_since(lei, min(cutoffs))
    _bump(gleif_log_calls=1, gleif_log_unavailable=0 if result.get("available") else 1)
    return result


def _write_rerun(
    store: "WatchlistStore",
    rows: list[dict[str, Any]],
    lei: str,
    tier: str,
    trigger: dict[str, Any],
    resp: Any,
    facts: dict[str, Any] | None,
    watermark: str | None,
    snapshot: dict[str, Any],
    glog: dict[str, Any] | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Move each list's baseline on and write its entry. Runs on a thread."""
    written = 0
    all_changes: list[dict[str, Any]] = []
    from . import gleif_log

    for row in rows:
        th = row["token_hash"]
        row_log = gleif_log.after(glog, baseline_cutoff(row))
        gleif_changes = name_children(
            diff_gleif_facts(row.get("gleif_facts"), facts, since=row.get("gleif_watermark"))
        )
        changes = gleif_changes + diff_snapshots(row.get("snapshot"), snapshot)
        all_changes = changes
        store.update_baseline(
            th,
            lei,
            gleif_facts=facts,
            gleif_watermark=watermark,
            snapshot=snapshot,
            legal_name=resp.legal_name,
            jurisdiction=resp.jurisdiction,
        )
        # A Tier 2 hit is itself the news; a catch-up or a GLEIF re-run that
        # found nothing is not.
        if changes or tier == TIER_OPENSANCTIONS:
            store.add_entry(
                th,
                lei,
                tier=tier,
                trigger=trigger,
                changes=changes,
                checked=snapshot["checked"],
                degraded=snapshot["degraded_sources"],
                legal_name=resp.legal_name,
                gleif_log=row_log,
            )
            written += 1
    return written, all_changes


# ---------------------------------------------------------------------------
# The worker task
# ---------------------------------------------------------------------------


#: ``meta`` key holding the ``built_at`` of the last mirror file every watch
#: was re-read against (Phase 301).
RESYNC_META_KEY = "gleif_resync_built_at"


def resync_after_rebuild() -> dict[str, int] | None:
    """Re-read every watch once against a newly built mirror (Phase 301).

    A rebuilt file arrives by asset replacement at boot, never through
    :func:`on_gleif_delta`, so a company whose liquidation GLEIF recorded
    last week — read as churn before events were material — would otherwise
    wait until GLEIF next touched its record. Runs only on a file that
    carries events, once per ``built_at``; on a file with nothing new it is
    renewal churn and baseline upgrades. Returns the hook's counts, or
    ``None`` when there was nothing to do."""
    from . import entity_pages as ep

    store = get_store()
    mirror = ep.get_store()
    if store is None or mirror is None or not getattr(mirror, "carries_events", False):
        return None
    built = mirror.meta().get("built_at") or ""
    if not built or store.get_meta(RESYNC_META_KEY) == built:
        return None
    watched = store.watched_leis()
    result: dict[str, int] = {"touched": 0, "queued": 0, "churn": 0}
    if watched:
        wm = mirror.watermark()
        label = wm.strftime("%Y-%m-%d %H:%M:%S") if wm else built
        result = on_gleif_delta(watched, label, resync=True)
    store.set_meta(RESYNC_META_KEY, built)
    return result


async def record_prewatch_log(th: str, lei: str) -> dict[str, Any] | None:
    """When a list starts watching ``lei``, keep what GLEIF's log showed in
    the :data:`gleif_log.PREWATCH_DAYS` before (Phase 303). Only for a watch
    with no history yet, so re-adding an LEI costs no call. Never raises."""
    from datetime import timedelta

    from . import gleif_log

    store = await asyncio.to_thread(get_store)
    if store is None:
        return None
    if await asyncio.to_thread(store.prewatch_log, th, lei) is not None:
        return None
    since = datetime.now(UTC) - timedelta(days=gleif_log.PREWATCH_DAYS)
    result = await gleif_log.fetch_since(lei, since)
    _bump(gleif_log_calls=1, gleif_log_unavailable=0 if result.get("available") else 1)
    await asyncio.to_thread(store.set_prewatch_log, th, lei, result)
    return result


async def tick() -> dict[str, Any]:
    """One pass: drain queued re-runs (bounded), catch up on OpenSanctions
    when due, prune empty lists."""
    from .config import get_settings

    settings = get_settings()
    store = await asyncio.to_thread(get_store)
    out: dict[str, Any] = {"reruns": 0, "os": None, "pruned": 0, "expired": 0}
    if store is None:
        return out
    _bump(last_tick_at=_now_iso())
    try:
        out["resync"] = await asyncio.to_thread(resync_after_rebuild)
        for item in await asyncio.to_thread(store.take_pending, settings.watchlist_reruns_per_tick):
            await rerun(item["lei"], item["tier"], item["trigger"])
            out["reruns"] += 1
        interval = settings.watchlist_opensanctions_interval_s
        if interval > 0:
            last = await asyncio.to_thread(store.get_meta, "opensanctions_checked_at")
            due = last is None or (
                datetime.now(UTC) - datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            ).total_seconds() >= interval
            # Phase 260: a backlog left by the last catch-up is read on the
            # next tick, not after another full interval.
            if due or state().get("os_backlog", 0) > 0:
                out["os"] = await asyncio.to_thread(opensanctions_tick)
                await asyncio.to_thread(store.set_meta, "opensanctions_checked_at", _now_iso())
                # Anything Tier 2 queued runs next tick, within the same bound.
        out["pruned"] = await asyncio.to_thread(store.prune_empty_lists)
        out["expired"] = await asyncio.to_thread(
            store.prune_stale_lists, older_than_days=settings.watchlist_stale_days
        )
    except Exception as exc:  # noqa: BLE001 — the loop outlives any one tick
        log.exception("watchlist: tick failed")
        _bump(last_error=f"tick: {type(exc).__name__}: {exc}")
    return out


async def watch_loop(interval_s: float) -> None:
    """Run :func:`tick` every ``interval_s`` seconds until cancelled. The
    first tick is delayed by one interval — boot is busy enough."""
    with _state_lock:
        _state.enabled = True
    try:
        while True:
            await asyncio.sleep(interval_s)
            try:
                await tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("watchlist: unexpected error")
    finally:
        with _state_lock:
            _state.enabled = False


# ---------------------------------------------------------------------------
# Adding a watch: the baseline
# ---------------------------------------------------------------------------


async def baseline(lei: str) -> tuple[Any, dict[str, Any] | None, str | None]:
    """The lookup response (usually replayed — the reader is on the report
    page), the mirror facts and the mirror watermark for a new watch."""
    from .routers.lookup import _lookup_impl

    resp = await _lookup_impl(lei)
    facts, watermark = await asyncio.to_thread(_mirror_facts, lei)
    return resp, facts, watermark


def new_list_quota() -> Any:
    """Per-client quota on new lists (``OPENCHECK_WATCHLIST_NEW_LISTS_PER_IP``)."""
    from .config import get_settings
    from .lookup_budget import Quota

    return Quota("watch-lists", lambda: get_settings().watchlist_new_lists_per_ip)


def valid_lei(value: str) -> str | None:
    lei = identifiers.normalise_lei(value)
    return lei if identifiers.is_valid_lei(lei) else None
