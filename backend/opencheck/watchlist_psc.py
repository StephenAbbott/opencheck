"""watchlist_psc — the Companies House PSC stream as a watchlist tier (pKYC stage E, Route 1).

Moody's names a change of owner or controller as the first example of a
monitoring trigger. Until this module the watchlist re-ran a watched company
only when GLEIF's Golden Copy, OpenSanctions' entity delta or the Estonian
index diff named it, so a UK PSC filing — the register's own record of an
ownership change — reached the watchlist only as a side-effect of some other
delta. The PSC graph has been live-fed from the register's stream since
Phases 184–189; this module is the hook that turns a batch of its events into
re-runs of the watched companies they touch.

Stephen's decisions on the stage E scoping page (18 Oct 2026):

- **Route 1 first, as its own ticket**: a push tier, GB only, the one route
  that adds a *trigger*. A statement-level ownership diff is Route 2.
- **Count, never name** (Q2). The trigger carries counts by kind and outcome
  and the PSC item ids (the key the graph draws the party on since the item-id
  fix) — never a name, a date of birth or an address. A person-level change
  is counted before the GDPR ticket decides whether it may ever be named.
- **A watch with no Companies House number says so** (Q5): "Register changes:
  not followed — GLEIF files no Companies House number", the knowability voice.
- **Never poll.** The stream pushes; nothing here fetches on a clock.

The measured cost (the scoping page): ~15–17k PSC events a day across ~5.5m
UK companies, ~4 % of which hold an LEI, so a watched UK company expects about
one PSC-triggered re-run a year. Cost is not the constraint; the hook and the
counts-only trigger are the work.

Shape: ``psc_stream.apply_events`` returns what each event did
(:class:`psc_stream.Touched`); ``_flush`` hands the committed batch to
:func:`on_psc_events`, which maps company numbers to watched LEIs through
the mirror's ``registeredAs`` under the three Companies House RA codes and
queues a re-run on :data:`watchlist.TIER_PSC`, folding a day's filings into
one pending row. A live resume of the stream (events skipped) is a **gap**:
:func:`on_psc_gap` queues a catch-up of every watched UK company, Phase 260's
shape. Words shared with the page are :data:`PSC_KIND_WORDS` /
:data:`PSC_OUTCOME_WORDS` / :data:`REGISTER_NOT_FOLLOWED_WORDS`, mirrored in
``frontend/src/lib/watchlist.ts`` and pinned by ``tests/test_watchlist_psc.py``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from .identifiers import normalise_ch_company_number

if TYPE_CHECKING:
    from .psc_stream import Touched
    from .watchlist import WatchlistStore

log = logging.getLogger("opencheck.watchlist")

#: GLEIF's registration authorities for Companies House: England & Wales,
#: Northern Ireland, Scotland (``ra_codes.SUB_REGISTRIES``; Phase 140).
COMPANIES_HOUSE_RAS: frozenset[str] = frozenset({"RA000585", "RA000586", "RA000587"})
#: GLEIF's registration authority for the Estonian Commercial Register.
ARIREGISTER_RA = "RA000181"
#: The register name on a trigger and a watch's ``register_tier`` block.
REGISTER_COMPANIES_HOUSE = "companies_house"
REGISTER_ARIREGISTER = "ariregister"

#: How many PSC item ids a trigger keeps. The counts are complete; the ids
#: are for Route 2 to say which rows moved, and fifty is more than any one
#: company files in a day.
PSC_IDS_KEPT = 50
#: How long the company-number index over the watches is trusted before the
#: store is re-read. A batch arrives every few seconds; the stamp query that
#: decides whether anything changed is one row.
PSC_INDEX_TTL_S = 60.0
#: ``meta`` keys: the last stream gap answered, and the gaps recorded.
PSC_GAP_KEY = "psc_gap_since"
PSC_GAPS_KEY = "psc_gaps"
PSC_GAPS_KEPT = 20

#: The words the feed and the page use for a kind of PSC item and for what
#: happened to it. Closed vocabularies — ``psc_stream.PSC_KINDS`` /
#: ``PSC_OUTCOMES`` — mirrored in ``lib/watchlist.ts`` and pinned by a test.
PSC_KIND_WORDS: dict[str, str] = {
    "individual": "person",
    "corporate": "corporate entity",
    "legal_person": "legal person",
    "withheld": "person whose details are withheld",
    "statement": "PSC statement",
    "unknown": "PSC item",
}
PSC_KIND_PLURALS: dict[str, str] = {
    "individual": "persons",
    "corporate": "corporate entities",
    "legal_person": "legal persons",
    "withheld": "persons whose details are withheld",
    "statement": "PSC statements",
    "unknown": "PSC items",
}
PSC_OUTCOME_WORDS: dict[str, str] = {
    "added": "added",
    "changed": "changed",
    "ceased": "ceased",
    "deleted": "removed",
}

#: Why a watch's register changes are not followed (Q5). Closed vocabulary;
#: the page carries the same words.
REGISTER_NOT_FOLLOWED_WORDS: dict[str, str] = {
    "no_register_number": "GLEIF files no register number for this company",
    "register_not_covered": "OpenCheck follows no change feed for this company's register",
}
#: What is followed, per register, for the watch row.
REGISTER_FOLLOWED_WORDS: dict[str, str] = {
    REGISTER_COMPANIES_HOUSE: "Companies House PSC stream, live",
    REGISTER_ARIREGISTER: "e-Business Register open data, daily",
}


# ---------------------------------------------------------------------------
# Which watches the stream can reach
# ---------------------------------------------------------------------------


def watch_ch_number(row: dict[str, Any]) -> str | None:
    """A watch's Companies House number: GLEIF's ``registeredAs`` where
    ``registeredAt`` is one of the three Companies House authorities (the
    mirror facts), else the ``GB-COH`` identifier the baseline lookup read —
    the Estonia tier's rule, with the number normalised on both sides so the
    stream's canonical eight characters match a filer's ``2999029``."""
    facts = row.get("gleif_facts") or {}
    if facts.get("registered_at") in COMPANIES_HOUSE_RAS:
        number = normalise_ch_company_number(facts.get("registered_as"))
        if number:
            return number
    identifiers_ = (row.get("snapshot") or {}).get("identifiers") or {}
    return normalise_ch_company_number(identifiers_.get("GB-COH"))


def register_tier_of(row: dict[str, Any]) -> dict[str, Any]:
    """What the watch row says about register changes (Q5): which register
    feed follows this company, or why none does. ``followed`` is a statement
    about OpenCheck's feeds, never about the company."""
    from .watchlist import watch_register_code

    if watch_ch_number(row):
        return {
            "followed": True,
            "register": REGISTER_COMPANIES_HOUSE,
            "reason": None,
            "words": REGISTER_FOLLOWED_WORDS[REGISTER_COMPANIES_HOUSE],
        }
    if watch_register_code(row):
        return {
            "followed": True,
            "register": REGISTER_ARIREGISTER,
            "reason": None,
            "words": REGISTER_FOLLOWED_WORDS[REGISTER_ARIREGISTER],
        }
    facts = row.get("gleif_facts") or {}
    identifiers_ = (row.get("snapshot") or {}).get("identifiers") or {}
    has_number = bool(facts.get("registered_as")) or any(k for k in identifiers_ if k != "XI-LEI")
    reason = "register_not_covered" if has_number else "no_register_number"
    return {
        "followed": False,
        "register": None,
        "reason": reason,
        "words": REGISTER_NOT_FOLLOWED_WORDS[reason],
    }


_index_lock = threading.Lock()
_index: dict[str, list[str]] = {}
_index_stamp: tuple[Any, ...] | None = None
_index_at: float = 0.0


def _stamp(store: WatchlistStore) -> tuple[Any, ...]:
    with store._conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*), MAX(added_at), MAX(snapshot_at) FROM watches"
        ).fetchone()
    return tuple(row)


def watched_ch_numbers(store: WatchlistStore, *, now: float | None = None) -> dict[str, list[str]]:
    """``company number → [LEIs]`` across every list — the index a batch's
    company numbers are looked up in. Rebuilt when the watches changed (the
    count or the newest add / baseline moved) or after
    :data:`PSC_INDEX_TTL_S`; between, one set lookup per company number."""
    global _index, _index_stamp, _index_at
    now = time.monotonic() if now is None else now
    with _index_lock:
        fresh = _index_stamp is not None and now - _index_at < PSC_INDEX_TTL_S
        if fresh:
            return _index
        stamp = _stamp(store)
        if stamp == _index_stamp:
            _index_at = now
            return _index
        out: dict[str, list[str]] = {}
        with store._conn() as conn:
            rows = conn.execute(
                "SELECT lei, gleif_facts_json, snapshot_json FROM watches"
            ).fetchall()
        for r in rows:
            number = watch_ch_number(
                {
                    "gleif_facts": json.loads(r["gleif_facts_json"])
                    if r["gleif_facts_json"]
                    else None,
                    "snapshot": json.loads(r["snapshot_json"]) if r["snapshot_json"] else None,
                }
            )
            if number and r["lei"] not in out.setdefault(number, []):
                out[number].append(r["lei"])
        _index, _index_stamp, _index_at = out, stamp, now
        return out


def reset_index_for_tests() -> None:
    global _index, _index_stamp, _index_at
    with _index_lock:
        _index, _index_stamp, _index_at = {}, None, 0.0


# ---------------------------------------------------------------------------
# The trigger
# ---------------------------------------------------------------------------


def build_trigger(
    company_number: str, touched: list[Touched], published_at: str | None
) -> dict[str, Any]:
    """One company's filings in a batch as a ``psc`` trigger: counts by kind
    and outcome, the item ids, the publication time — nothing else."""
    kinds: dict[str, dict[str, int]] = {}
    ids: list[str] = []
    for t in touched:
        bucket = kinds.setdefault(t.kind, {})
        bucket[t.outcome] = bucket.get(t.outcome, 0) + 1
        if t.psc_id not in ids:
            ids.append(t.psc_id)
    return {
        "tier": "psc",
        "register": REGISTER_COMPANIES_HOUSE,
        "company_number": company_number,
        "events": len(touched),
        "kinds": kinds,
        "psc_ids": ids[:PSC_IDS_KEPT],
        "first_at": published_at,
        "last_at": published_at,
    }


def fold_triggers(existing: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Two ``psc`` triggers for one company as one: counts added, ids
    united (capped), the span of publication times kept."""
    kinds: dict[str, dict[str, int]] = {}
    for trig in (existing, new):
        for kind, outcomes in (trig.get("kinds") or {}).items():
            bucket = kinds.setdefault(kind, {})
            for outcome, n in (outcomes or {}).items():
                bucket[outcome] = bucket.get(outcome, 0) + int(n or 0)
    ids = list(existing.get("psc_ids") or [])
    for i in new.get("psc_ids") or []:
        if i not in ids:
            ids.append(i)
    return {
        **existing,
        "events": int(existing.get("events") or 0) + int(new.get("events") or 0),
        "kinds": kinds,
        "psc_ids": ids[:PSC_IDS_KEPT],
        "first_at": existing.get("first_at") or new.get("first_at"),
        "last_at": new.get("last_at") or existing.get("last_at"),
        "coalesced": int(existing.get("coalesced") or 0) + 1,
    }


def psc_change_lines(trigger: dict[str, Any]) -> list[str]:
    """One line per kind of item the filing touched, counts only: "2 persons
    with significant control: 1 added, 1 ceased." Kinds in the order the
    stream vocabulary lists them; outcomes likewise."""
    from .psc_stream import PSC_KINDS, PSC_OUTCOMES

    lines: list[str] = []
    kinds = trigger.get("kinds") or {}
    for kind in PSC_KINDS:
        outcomes = kinds.get(kind)
        if not isinstance(outcomes, dict) or not outcomes:
            continue
        total = sum(int(v or 0) for v in outcomes.values())
        noun = PSC_KIND_WORDS[kind] if total == 1 else PSC_KIND_PLURALS[kind]
        parts = [f"{outcomes[o]} {PSC_OUTCOME_WORDS[o]}" for o in PSC_OUTCOMES if outcomes.get(o)]
        lines.append(f"{total} {noun}: {', '.join(parts)}.")
    return lines


def psc_sentence(trigger: dict[str, Any]) -> str:
    """Why a ``psc`` entry exists, in the feed's words."""
    n = int(trigger.get("events") or 0)
    number = trigger.get("company_number")
    day = str(trigger.get("last_at") or "")[:10]
    return (
        f"Companies House recorded {n} change{'' if n == 1 else 's'} to this company's people with significant control"
        + (f" on {day}" if day else "")
        + (f" (company number {number})" if number else "")
        + "."
    )


# ---------------------------------------------------------------------------
# The hooks
# ---------------------------------------------------------------------------


def on_psc_events(touched: list[Touched], published_at: str | None = None) -> dict[str, int]:
    """Called by ``psc_stream`` after a batch is committed, with what each
    event did. Runs on a worker thread. Never raises: the stream must not
    fail because the watcher did.

    Groups the batch by company number, intersects with the watched UK
    companies, and queues one re-run per watched LEI with a counts-only
    trigger — folding into a pending ``psc`` row when one is already queued,
    so a company that files three times in an afternoon is re-run once with
    all three counted."""
    from . import watchlist as wl

    out = {"events": len(touched), "matched": 0, "queued": 0, "coalesced": 0}
    try:
        store = wl.get_store()
        if store is None:
            return out
        wl._bump(psc_events_seen=len(touched), psc_last_event_at=wl._now_iso())
        index = watched_ch_numbers(store)
        if not index:
            return out
        by_company: dict[str, list[Touched]] = {}
        for t in touched:
            if t.company_number and t.company_number in index:
                by_company.setdefault(t.company_number, []).append(t)
        if not by_company:
            return out
        for number, items in sorted(by_company.items()):
            trigger = build_trigger(number, items, published_at)
            for lei in index[number]:
                out["matched"] += 1
                if store.enqueue(lei, wl.TIER_PSC, trigger, fold=fold_triggers):
                    out["queued"] += 1
                else:
                    out["coalesced"] += 1
        wl._bump(
            psc_matched=out["matched"],
            psc_queued=out["queued"],
            psc_coalesced=out["coalesced"],
            psc_last_match_at=wl._now_iso(),
        )
        if out["queued"]:
            log.info(
                "watchlist: Companies House PSC filings on %d watched compan%s",
                out["queued"],
                "y" if out["queued"] == 1 else "ies",
            )
    except Exception:  # noqa: BLE001 — never fail the stream
        log.exception("watchlist: the PSC hook failed")
    return out


def psc_gaps(store: WatchlistStore) -> list[dict[str, Any]]:
    raw = store.get_meta(PSC_GAPS_KEY)
    try:
        gaps = json.loads(raw) if raw else []
    except json.JSONDecodeError:
        gaps = []
    return gaps if isinstance(gaps, list) else []


def on_psc_gap(since: str, timepoint: int | None = None) -> dict[str, int]:
    """Called when the stream resumes live — the register published events
    the consumer did not see. Records the gap and queues a catch-up re-run of
    every watched UK company (``reason: register_gap``, Phase 260's shape; an
    entry only where the re-run finds a difference). One gap is answered
    once: a reconnect loop that resumes live repeatedly does not re-queue.
    Never raises."""
    from . import watchlist as wl

    out = {"queued": 0}
    try:
        store = wl.get_store()
        if store is None:
            return out
        if store.get_meta(PSC_GAP_KEY) == since:
            return out
        store.set_meta(PSC_GAP_KEY, since)
        index = watched_ch_numbers(store)
        gap = {
            "since": since,
            "timepoint": timepoint,
            "detected_at": wl._now_iso(),
            "watched": sum(len(v) for v in index.values()),
        }
        store.set_meta(PSC_GAPS_KEY, json.dumps((psc_gaps(store) + [gap])[-PSC_GAPS_KEPT:]))
        trigger = {
            "tier": wl.TIER_CATCHUP,
            "reason": "register_gap",
            "register": REGISTER_COMPANIES_HOUSE,
            "after": None,
            "before": since,
        }
        for lei in sorted({lei for leis in index.values() for lei in leis}):
            if store.enqueue(lei, wl.TIER_CATCHUP, trigger):
                out["queued"] += 1
        wl._bump(psc_gaps=1, psc_last_gap_at=gap["detected_at"])
        log.warning(
            "watchlist: the PSC stream resumed live at %s, so its changes before that were not seen; "
            "queued %d catch-up re-run%s of watched UK companies",
            since,
            out["queued"],
            "" if out["queued"] == 1 else "s",
        )
    except Exception:  # noqa: BLE001
        log.exception("watchlist: the PSC gap hook failed")
    return out


# ---------------------------------------------------------------------------
# Counts for /watchstats
# ---------------------------------------------------------------------------


def watched_by_registration_authority(store: WatchlistStore) -> dict[str, int]:
    """Watched LEIs by the registration authority GLEIF files them under —
    the count measurement 1 on the stage E scoping page could not take.
    Counts only; ``none`` for a watch whose record names no authority."""
    out: dict[str, int] = {}
    with store._conn() as conn:
        rows = conn.execute("SELECT DISTINCT lei, gleif_facts_json FROM watches").fetchall()
    seen: set[str] = set()
    for r in rows:
        if r["lei"] in seen:
            continue
        seen.add(r["lei"])
        facts = json.loads(r["gleif_facts_json"]) if r["gleif_facts_json"] else {}
        ra = str(facts.get("registered_at") or "").strip() or "none"
        out[ra] = out.get(ra, 0) + 1
    return dict(sorted(out.items()))
