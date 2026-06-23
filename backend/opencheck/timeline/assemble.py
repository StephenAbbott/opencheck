"""Time Machine assembler — merge per-source changes into one timeline.

Takes already-fetched raw change data (GLEIF LEI + RR field-modifications and,
where known, Companies House filing history), runs the per-source emitters, and
produces a single :class:`Timeline`:

- ``events`` — every classified change, raw and chronological, **including
  Tier-3 noise** (the full audit trail; the renderer suppresses by tier).
- ``notable`` — the rendered view: Tier-1/2 events grouped into
  :class:`TimelineEntry` rows, with cross-source **entity** changes de-duplicated
  and corroboration recorded.

This module is pure (no network). Fetching the raw data belongs to the service /
endpoint layer built on top.

De-dup policy (deliberately conservative for the draft):

- **Entity-identity changes** (name, form, status, jurisdiction, address) that
  share a ``change_type`` and fall within a time window are clustered into one
  entry — GLEIF's *recorded* date lags Companies House's *effective* date (the
  Morrisons name change lagged by months), so the window is generous and the
  most authoritative date (effective > recorded) becomes the entry's date.
- **Ownership changes** (Tier 1, relationships) are **not** merged across
  sources: GLEIF reports corporate parents (by LEI) and Companies House reports
  PSCs (often individuals, no LEI), so there is no reliable cross-source key.
  Each is its own entry. Cross-source owner matching is a future enhancement.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

from .companies_house import classify_companies_house_filing
from .gleif import classify_gleif_modification, relationship_interest_dates
from .model import (
    CHANGE_TYPES,
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

# Window for clustering cross-source ENTITY changes. Wide enough to absorb
# GLEIF's recording lag versus Companies House effective dates.
_DEFAULT_WINDOW_DAYS = 400

_BASIS_RANK = {
    DateBasis.EFFECTIVE: 3,
    DateBasis.RECORDED: 2,
    DateBasis.SNAPSHOT_WINDOW: 1,
}
_CONFIDENCE_RANK = {
    DateConfidence.HIGH: 3,
    DateConfidence.MEDIUM: 2,
    DateConfidence.LOW: 1,
}


@dataclass
class TimelineEntry:
    """One rendered row of the timeline — a notable change, possibly
    corroborated by more than one source."""

    change_type: ChangeType
    tier: Tier
    record_type: RecordType
    label: str
    date: str | None
    date_basis: DateBasis
    date_confidence: DateConfidence
    sources: list[str]
    primary: ChangeEvent
    corroborating: list[ChangeEvent] = field(default_factory=list)
    interest_start_date: str | None = None
    interest_end_date: str | None = None
    counterparty: str | None = None
    boosted: bool = False


@dataclass
class Timeline:
    subject_lei: str
    company_number: str | None
    events: list[ChangeEvent]  # ALL classified changes, chronological (incl. noise)
    notable: list[TimelineEntry]  # deduped, Tier-1/2 view for rendering


# --------------------------------------------------------------------------- #
# Date helpers
# --------------------------------------------------------------------------- #

def _parse(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _chrono_key(ev_date: str | None) -> tuple[bool, date]:
    """Ascending (oldest first); undated sorts last."""
    d = _parse(ev_date)
    return (d is None, d or date.max)


# --------------------------------------------------------------------------- #
# GLEIF relationship classification with period-date attachment
# --------------------------------------------------------------------------- #

def _classify_gleif_relationships(rr_mods: list[dict]) -> list[ChangeEvent]:
    """Classify RR modifications grouped by parent (context.endNode), attaching
    that relationship's economic period dates to its ownership events."""
    by_parent: dict[str | None, list[dict]] = defaultdict(list)
    for mod in rr_mods:
        context = mod.get("context") or {}
        by_parent[context.get("endNode")].append(mod)

    events: list[ChangeEvent] = []
    for mods in by_parent.values():
        start, end = relationship_interest_dates(mods)
        for mod in mods:
            ev = classify_gleif_modification(mod)
            if ev.change_type in (ChangeType.OWNER_ADDED, ChangeType.OWNER_REMOVED):
                ev.interest_start_date = start
                ev.interest_end_date = end
            events.append(ev)
    return events


# --------------------------------------------------------------------------- #
# Clustering + entry construction
# --------------------------------------------------------------------------- #

def _primary_sort_key(ev: ChangeEvent):
    # Most authoritative date first: effective > recorded, high > medium > low,
    # then earliest date (the real event tends to be the earliest record).
    return (
        -_BASIS_RANK[ev.date_basis],
        -_CONFIDENCE_RANK[ev.date_confidence],
        _chrono_key(ev.event_date),
    )


def _make_entry(cluster: list[ChangeEvent]) -> TimelineEntry:
    ordered = sorted(cluster, key=_primary_sort_key)
    primary = ordered[0]
    corroborating = ordered[1:]
    spec = CHANGE_TYPES[primary.change_type]  # notable ⇒ change_type set

    def _first(attr: str):
        for ev in ordered:
            val = getattr(ev, attr)
            if val:
                return val
        return None

    return TimelineEntry(
        change_type=primary.change_type,
        tier=primary.tier,
        record_type=primary.record_type,
        label=spec.label,
        date=primary.event_date,
        date_basis=primary.date_basis,
        date_confidence=primary.date_confidence,
        sources=sorted({ev.source_id for ev in cluster}),
        primary=primary,
        corroborating=corroborating,
        interest_start_date=_first("interest_start_date"),
        interest_end_date=_first("interest_end_date"),
        counterparty=_first("counterparty"),
        boosted=any(ev.boosted for ev in cluster),
    )


def _cluster_entity_events(
    events: list[ChangeEvent], window_days: int
) -> list[TimelineEntry]:
    """Cluster entity-identity events sharing a change_type within a time window.
    Undated events are never merged."""
    groups: dict[tuple, list[ChangeEvent]] = defaultdict(list)
    for ev in events:
        groups[(ev.change_type, ev.record_type)].append(ev)

    entries: list[TimelineEntry] = []
    for evs in groups.values():
        clusters: list[list[ChangeEvent]] = []
        for ev in sorted(evs, key=lambda e: _chrono_key(e.event_date)):
            d = _parse(ev.event_date)
            placed = False
            if d is not None:
                for cl in clusters:
                    ref = _parse(cl[0].event_date)
                    if ref is not None and abs((d - ref).days) <= window_days:
                        cl.append(ev)
                        placed = True
                        break
            if not placed:
                clusters.append([ev])
        entries.extend(_make_entry(cl) for cl in clusters)
    return entries


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def assemble_timeline(
    *,
    lei: str,
    company_number: str | None = None,
    gleif_lei_mods: list[dict] | None = None,
    gleif_rr_mods: list[dict] | None = None,
    ch_filings: list[dict] | None = None,
    extra_events: list[ChangeEvent] | None = None,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> Timeline:
    """Classify and merge raw per-source change data into one timeline.

    ``extra_events`` are already-classified ChangeEvents from emitters that
    produce them directly (e.g. the NZ emitter reconstructs events from dated
    records rather than classifying a raw stream)."""
    all_events: list[ChangeEvent] = []
    all_events += [classify_gleif_modification(m) for m in (gleif_lei_mods or [])]
    all_events += _classify_gleif_relationships(gleif_rr_mods or [])
    all_events += [
        classify_companies_house_filing(f, company_id=company_number or "")
        for f in (ch_filings or [])
    ]
    all_events += list(extra_events or [])

    notable = [ev for ev in all_events if ev.is_notable]
    entity_events = [ev for ev in notable if ev.record_type is RecordType.ENTITY]
    rel_events = [ev for ev in notable if ev.record_type is RecordType.RELATIONSHIP]

    entries = _cluster_entity_events(entity_events, window_days)
    # Ownership changes: one entry each, no cross-source merge (see module doc).
    entries += [_make_entry([ev]) for ev in rel_events]
    entries.sort(key=lambda e: (_chrono_key(e.date), e.tier.value))

    all_events.sort(key=lambda e: _chrono_key(e.event_date))
    return Timeline(
        subject_lei=lei,
        company_number=company_number,
        events=all_events,
        notable=entries,
    )


__all__ = ["Timeline", "TimelineEntry", "assemble_timeline"]
