"""Screen related-party names against OpenAleph via text-based percolation.

Why this exists
---------------

``cross_check.py`` screens every related party in the BODS bundle against
OpenSanctions + EveryPolitician (one HTTP search per name per source), and
``icij_check.py`` against the ICIJ Offshore Leaks reconciliation API. This
module adds a third pass over the same targets using OpenAleph's text-based
percolation endpoint (``POST /api/2/beta/percolate``, OpenAleph 5.3.1 — the
feature OpenCheck requested in openaleph/openaleph#105): **all** related-party
names are joined into one text and screened in **two** HTTP calls against
every percolator-indexed collection on the instance — national sanctions
lists OpenSanctions doesn't mirror, disqualified-directors registers, leak
archives, court records.

The direction is the inverse of a search: OpenAleph stores one name-percolator
query per entity, and percolation returns the stored entities whose names
appear in *our* text. Each hit carries ``surface_forms`` — the exact phrase of
ours that fired — which is what lets a match be attributed back to the BODS
statement (and so the graph node) that bears that name.

Design decisions (2026-08-13, from the percolation implementation plan):

* **Persons are screened broadly** (``filter:schema=Person``, no topic
  filter): measured live, person-name percolation is high-precision — a
  sanctioned oligarch lights up across eight watchlist collections while
  non-notable executives return nothing.
* **Entities are screened topic-scoped** (``_WATCHLIST_TOPICS``): measured
  live, unfiltered entity percolation over well-known company names drowns
  in near-duplicate registry records (Shell + BP → 43 hits, the top 15 all
  Companies House-PSC/CorpWatch copies). The topic filter also keeps
  percolation latency in the ~10 ms band vs ~1.8 s unfiltered.
* **Signals reuse the cross_check machinery**: the same topic ladder
  (direct sanction > debarment > sanction-linked > PEP), the same 0.88
  name-similarity threshold, the same single-token person-name guard and
  birth-year compatibility check. A percolator match proves *a* name of the
  hit appears in our text; the similarity gate proves it is the right
  *degree* of match for the specific target.
* **Sub-signal matches are informational, never discarded**: a match whose
  topics map to no ``RELATED_*`` code (``poi``, ``corp.disqual``, a leak or
  court collection with no topics at all) is collected into the
  ``screening`` out-parameter and surfaced to the UI as an informational
  block. Name-derived — never identifier corroboration, and never an entry
  in any ``identifiers`` dict.
* **Duplicates across screens are kept**: an OpenSanctions signal and an
  OpenAleph signal for the same person on the same underlying list both
  survive (dedupe keys include ``source_id``), consistent with how the
  OS + EveryPolitician probes coexist in ``cross_check``.

Requires ``OPENALEPH_API_KEY`` (the flagship edge 405s anonymous POSTs to
the percolate path). Without it — or on any percolation failure — the screen
records a :class:`DegradedSource` so an empty result never passes for a
clean screen, mirroring issue #50's contract in ``cross_check``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import names
from .config import get_settings
from .cross_check import (
    _DEBARMENT_TOPICS,
    _KIND_PERSON,
    _PEP_TOPICS,
    RELATED_DEBARMENT,
    RELATED_PEP,
    RELATED_SANCTIONED,
    RELATED_SANCTIONS_CONTROLLED,
    RELATED_SANCTIONS_LINKED,
    _birth_year_compatible,
    _collect_targets,
    _dedupe,
    _extract_topics,
    _topic_blurb,
)
from .matching import is_matchable_name
from .risk import (
    DEGRADED_NOT_CONFIGURED,
    DEGRADED_UPSTREAM_ERROR,
    DegradedSource,
    RiskSignal,
    classify_sanction_topics,
)
from .sources import REGISTRY, SearchKind

_LOG = logging.getLogger(__name__)

#: Name of this derived check in ``DegradedSource.check`` records.
CHECK_NAME = "openaleph_percolation"

#: Signals this screen can contribute — the codes whose absence becomes
#: unreliable when the percolation calls fail (issue #50).
_AFFECTED_SIGNALS = [
    RELATED_SANCTIONED,
    RELATED_SANCTIONS_CONTROLLED,
    RELATED_SANCTIONS_LINKED,
    RELATED_DEBARMENT,
    RELATED_PEP,
]

#: Topic scope for the ENTITY percolation call — exactly the topics the
#: cross_check signal ladder can act on. Entities are never PEPs, so the
#: role.* topics are deliberately absent. (The person call is unfiltered by
#: topic: person percolation is precise enough without it, and the broad
#: call is what lets sub-signal material — poi, corp.disqual, leak
#: collections — reach the informational block.)
_WATCHLIST_TOPICS: tuple[str, ...] = (
    "sanction",
    "sanction.counter",
    "sanction.control",
    "sanction.linked",
    "debarment",
)

#: Result cap per percolation call.
_RESULT_LIMIT = 50


async def assess_openaleph_names(
    bods: list[dict[str, Any]],
    *,
    min_score: float = 0.88,
    degraded: list[DegradedSource] | None = None,
    screening: list[dict[str, Any]] | None = None,
) -> list[RiskSignal]:
    """Return scoped ``RELATED_*`` signals for related parties in the BODS
    bundle whose names percolate against a watchlist-topic OpenAleph record.

    ``degraded`` is the issue-#50 out-collector: when a percolation call
    could not run (no ``OPENALEPH_API_KEY`` in live mode, pre-5.3.1
    instance, HTTP failure) a :class:`DegradedSource` is appended so an
    empty result never reads as a clean screen. Records carry counts only,
    never the related-party names being screened.

    ``screening`` is an optional out-collector for **informational**
    matches: attributed, similarity-gated hits whose topics map to no
    ``RELATED_*`` code (leak/court collections, ``poi``, ``corp.disqual``).
    Each entry carries ``statement_id`` so the UI can place it next to the
    right graph node. Name-derived — never identifier corroboration.

    No-op (returns ``[]``) when live mode is off, the bundle has no
    screenable statements, or the OpenAleph adapter is not registered.
    """
    if not bods:
        return []

    settings = get_settings()
    if not settings.allow_live:
        return []

    targets = _collect_targets(bods)
    if not targets:
        return []

    adapter = REGISTRY.get("openaleph")
    if adapter is None or not hasattr(adapter, "percolate_text"):
        return []

    if not settings.openaleph_api_key:
        # Live mode but no key: percolation cannot run at all (the edge
        # 405s anonymous POSTs) and an empty result would look identical
        # to a clean screen.
        _LOG.warning(
            "OpenAleph percolation screening disabled: OPENALEPH_API_KEY is "
            "not set while live mode is on. RELATED_* signals from OpenAleph "
            "will be absent for every lookup — this is NOT a clean screen."
        )
        if degraded is not None:
            degraded.append(
                DegradedSource(
                    source_id="openaleph",
                    check=CHECK_NAME,
                    affected_signals=list(_AFFECTED_SIGNALS),
                    detail=(
                        "OPENALEPH_API_KEY is not configured while live mode "
                        f"is on; {len(targets)} related-party name(s) were "
                        "not screened via percolation."
                    ),
                    reason=DEGRADED_NOT_CONFIGURED,
                )
            )
        return []

    person_targets = [t for t in targets if t["kind"] == _KIND_PERSON]
    entity_targets = [t for t in targets if t["kind"] != _KIND_PERSON]
    person_text = _screening_text(person_targets)
    entity_text = _screening_text(entity_targets)

    person_results, entity_results = await asyncio.gather(
        adapter.percolate_text(
            person_text, schema="Person", limit=_RESULT_LIMIT
        )
        if person_text
        else _none(),
        adapter.percolate_text(
            entity_text,
            schema="LegalEntity",
            topics=_WATCHLIST_TOPICS,
            limit=_RESULT_LIMIT,
        )
        if entity_text
        else _none(),
    )

    signals: list[RiskSignal] = []
    for results, call_targets, call_text, call_label in (
        (person_results, person_targets, person_text, "person"),
        (entity_results, entity_targets, entity_text, "entity"),
    ):
        if not call_targets or not call_text:
            continue
        if results is None:
            # The call could not run (HTTP failure / pre-5.3.1 404 — the
            # no-key case was handled above). Counts only, never names.
            _LOG.warning(
                "OpenAleph percolation screening degraded: the %s call did "
                "not run; %d related-party name(s) were not screened — an "
                "empty result here is not a clean screen.",
                call_label,
                len(call_targets),
            )
            if degraded is not None:
                degraded.append(
                    DegradedSource(
                        source_id="openaleph",
                        check=CHECK_NAME,
                        affected_signals=list(_AFFECTED_SIGNALS),
                        detail=(
                            f"The percolation {call_label} call failed; "
                            f"{len(call_targets)} of {len(targets)} "
                            "related-party name(s) were not screened."
                        ),
                        reason=DEGRADED_UPSTREAM_ERROR,
                    )
                )
            continue
        signals.extend(
            _process_results(
                results,
                call_targets,
                min_score=min_score,
                screening=screening,
            )
        )

    return _dedupe(signals)


async def _none() -> None:
    """Awaitable None placeholder for skipped percolation calls."""
    return None


# ---------------------------------------------------------------------
# Text construction
# ---------------------------------------------------------------------


def _screening_text(targets: list[dict[str, Any]]) -> str:
    """Newline-joined unique target names — the percolation input text.

    Percolator queries are ``match_phrase`` with slop 2, so one name per
    line keeps phrases from bleeding across two adjacent names.
    """
    seen: set[str] = set()
    lines: list[str] = []
    for t in targets:
        name = (t.get("name") or "").strip()
        key = names.normalise_name(name)
        if not name or not key or key in seen:
            continue
        seen.add(key)
        lines.append(name)
    return "\n".join(lines)


# ---------------------------------------------------------------------
# Attribution + signal construction
# ---------------------------------------------------------------------


def _process_results(
    results: list[dict[str, Any]],
    targets: list[dict[str, Any]],
    *,
    min_score: float,
    screening: list[dict[str, Any]] | None,
) -> list[RiskSignal]:
    """Attribute percolation hits back to targets and build signals.

    For each hit: its ``surface_forms`` (the exact phrases of ours that
    fired) are matched against the targets whose normalised name contains
    the normalised surface form; each candidate pair then passes the same
    gates as ``cross_check`` (single-token person guard, ≥ ``min_score``
    name similarity against the hit's own FtM names, birth-year
    compatibility) before the hit's topics run through the signal ladder.
    Gated matches with no signal-mapping topic go to ``screening``.
    """
    signals: list[RiskSignal] = []
    seen_screening: set[tuple[str, str]] = set()
    for item in results:
        surface_forms = [
            s for s in (item.get("surface_forms") or []) if isinstance(s, str)
        ]
        if not surface_forms:
            continue
        for target in targets:
            target_norm = names.normalise_name(target["name"])
            if not target_norm:
                continue
            matched_form = next(
                (
                    sf
                    for sf in surface_forms
                    if (sf_norm := names.normalise_name(sf))
                    and sf_norm in target_norm
                ),
                None,
            )
            if matched_form is None:
                continue
            best_name, score = _best_name_score(target["name"], item)
            if score < min_score:
                continue
            if target["kind"] == _KIND_PERSON and not (
                is_matchable_name(target_norm)
                and is_matchable_name(names.normalise_name(best_name))
            ):
                # Single-token person names ("Fernández") are too generic
                # to base a related-party match on — same guard as
                # cross_check / ftmg.
                continue
            hit = _as_hit(item, target)
            if not _birth_year_compatible(target.get("birth_year"), hit):
                continue
            signal = _signal_from_percolate(
                item, target, best_name, matched_form, score
            )
            if signal is not None:
                signals.append(signal)
            elif screening is not None:
                key = (target["statement_id"], str(item.get("id") or ""))
                if key not in seen_screening:
                    seen_screening.add(key)
                    screening.append(
                        _screening_entry(
                            item, target, best_name, matched_form, score
                        )
                    )
    return signals


def _best_name_score(
    target_name: str, item: dict[str, Any]
) -> tuple[str, float]:
    """Best (name, similarity) over the hit's FtM names group + caption.

    ``cross_check`` scores against the display name only, but a percolator
    match may have fired on an alias or previousName (``percolator_match:
    ["other_name"]``) — the honest comparison is against whichever of the
    hit's own names is closest to the target.
    """
    props = item.get("properties") or {}
    candidates: list[str] = []
    for prop in ("name", "alias", "previousName"):
        values = props.get(prop) or []
        if isinstance(values, str):
            values = [values]
        candidates.extend(str(v) for v in values if v)
    caption = item.get("caption")
    if caption:
        candidates.append(str(caption))
    best_name, best_score = "", 0.0
    for cand in candidates:
        s = names.name_similarity(target_name, cand)
        if s > best_score:
            best_name, best_score = cand, s
    return best_name, best_score


def _as_hit(item: dict[str, Any], target: dict[str, Any]) -> Any:
    """A SourceHit view of a percolation item, for the shared gates.

    ``cross_check._birth_year_compatible`` reads ``hit.raw.properties``;
    the adapter's ``_hit`` factory gives exactly that shape.
    """
    from .sources.openaleph import OpenAlephAdapter

    kind = (
        SearchKind.PERSON if target["kind"] == _KIND_PERSON else SearchKind.ENTITY
    )
    return OpenAlephAdapter._hit(item, kind)


def _collection_label(item: dict[str, Any]) -> str:
    collection = item.get("collection") or {}
    return str(collection.get("label") or collection.get("foreign_id") or "")


def _ui_url(item: dict[str, Any]) -> str:
    return str((item.get("links") or {}).get("ui") or "")


def _signal_from_percolate(
    item: dict[str, Any],
    target: dict[str, Any],
    matched_name: str,
    surface_form: str,
    score: float,
) -> RiskSignal | None:
    """Run the hit's topics through the cross_check signal ladder.

    Same priority order as ``cross_check._signal_from_os``: a direct
    sanctions listing outranks a debarment, which outranks a sanctions
    link, which outranks PEP status. Returns ``None`` when no topic maps —
    the caller sends those to the informational screening block instead.
    """
    topics = _extract_topics(item)
    sanctions = classify_sanction_topics(topics)
    direct_sanction = bool(sanctions.direct)
    controlled = bool(sanctions.control)
    linked_sanction = bool(sanctions.linked or sanctions.unknown)
    is_debarred = any(t in _DEBARMENT_TOPICS for t in topics)
    is_pep = any(t in _PEP_TOPICS for t in topics)

    collection = _collection_label(item)
    coll_note = f" via '{collection}'" if collection else ""

    if direct_sanction:
        code, extra = RELATED_SANCTIONED, f"sanctioned{coll_note}"
    elif controlled:
        code, extra = (
            RELATED_SANCTIONS_CONTROLLED,
            f"inside a sanctioned party's ownership chain{coll_note}",
        )
    elif is_debarred:
        code, extra = (
            RELATED_DEBARMENT,
            f"debarred from public contracts{coll_note}",
        )
    elif linked_sanction:
        code, extra = (
            RELATED_SANCTIONS_LINKED,
            f"linked to sanctioned entities{coll_note}",
        )
    elif is_pep and target["kind"] == _KIND_PERSON:
        # Entities can never be PEPs — only natural persons hold political
        # office (same rule as cross_check).
        code, extra = RELATED_PEP, f"PEP{coll_note}"
    else:
        return None

    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
    return RiskSignal(
        code=code,
        confidence="high" if score >= 0.95 else "medium",
        summary=(
            f"{relation} '{target['name']}' matches a record on openaleph: "
            f"{extra} ({_topic_blurb(topics)})."
        ),
        source_id="openaleph",
        hit_id=str(item.get("id") or ""),
        evidence={
            "subject_statement_id": target["statement_id"],
            "matched_name": matched_name,
            "search_name": target["name"],
            "surface_form": surface_form,
            "percolator_match": list(item.get("percolator_match") or []),
            "score": round(score, 3),
            "kind": target["kind"],
            "collection": _collection_label(item),
            "collection_url": _ui_url(item),
            "topics": topics,
        },
    )


def _screening_entry(
    item: dict[str, Any],
    target: dict[str, Any],
    matched_name: str,
    surface_form: str,
    score: float,
) -> dict[str, Any]:
    """Informational match record for the ``screening`` out-collector."""
    return {
        "statement_id": target["statement_id"],
        "search_name": target["name"],
        "kind": target["kind"],
        "matched_name": matched_name,
        "entity_id": str(item.get("id") or ""),
        "collection": _collection_label(item),
        "url": _ui_url(item),
        "topics": _extract_topics(item),
        "surface_form": surface_form,
        "percolator_match": list(item.get("percolator_match") or []),
        "score": round(score, 3),
    }
