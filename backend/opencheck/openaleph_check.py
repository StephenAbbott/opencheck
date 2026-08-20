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
* **Per-collection copies within THIS screen collapse** (Phase 119): one
  designation-class fact about one related party surfaces once per
  percolator collection carrying it, which read as N distinct findings.
  ``_collapse_collections`` keeps one signal per ``(code,
  subject_statement_id)`` with every copy preserved in
  ``evidence.collections`` — see its docstring. The cross-source rule
  above is unaffected: the collapse never crosses ``source_id``.
* **Boilerplate-only names never match** (Phase 119): for entity targets,
  both the target's name and the matched record's best-scoring name must
  keep a non-empty residue once legal-form tokens are stripped
  (``names.org_name_residue``). A Canadian-list record named only
  «Общество с ограниченной ответственностью» matched nineteen distinct
  subsidiaries at 0.88+ on legal-form similarity alone.

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
    RELATED_COUNTER_SANCTIONED,
    RELATED_DEBARMENT,
    RELATED_EXPORT_CONTROL_LINKED,
    RELATED_EXPORT_CONTROLLED,
    RELATED_EXPORT_RISK,
    RELATED_PEP,
    RELATED_SANCTIONED,
    RELATED_SANCTIONS_CONTROLLED,
    RELATED_SANCTIONS_LINKED,
    _birth_year_compatible,
    _collect_targets,
    corroborating_attributes,
    match_confidence,
    match_summary,
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
    classify_export_topics,
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
    RELATED_COUNTER_SANCTIONED,
    RELATED_SANCTIONS_CONTROLLED,
    RELATED_SANCTIONS_LINKED,
    RELATED_DEBARMENT,
    RELATED_EXPORT_CONTROLLED,
    RELATED_EXPORT_CONTROL_LINKED,
    RELATED_EXPORT_RISK,
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
    "export.control",
    "export.control.linked",
    "export.risk",
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

    return _collapse_collections(_dedupe(signals))


def _collapse_collections(signals: list[RiskSignal]) -> list[RiskSignal]:
    """One finding per related party per code, however many collections carry it.

    OpenAleph percolates against every percolator-indexed collection on the
    instance, so one designation-class fact about one related party surfaces
    once per collection that carries it — and occasionally more than once
    within a single collection (duplicate records: Syzran Refinery appeared
    twice in Swiss SECO). OpenSanctions returns one canonical,
    upstream-deduplicated entity for the same fact. Keeping the copies as
    separate signals made volume read as breadth: a reader seeing four
    "Related sanctioned" chips for Igor Sechin infers four distinct
    findings — the Phase 105/106 class of error (a thing misreported as
    what it isn't), expressed as count rather than classification.

    Collapse per ``(code, evidence.subject_statement_id)``. The survivor is
    the highest-(confidence, score) copy; every copy is preserved in
    ``evidence.collections``; ``evidence.collection_count`` counts distinct
    collections; ``corroboration`` becomes the union across copies (one
    list may publish the birth year another lacks) and ``name_match_only``
    is recomputed from it. A multi-collection summary says "listed across N
    OpenAleph collections" — deliberately never "corroborated": per the
    SubjectCard rule, aggregation copies inside one instance are not
    independent sources agreeing about the world.

    Cross-SOURCE duplicates (an OpenSanctions signal and an OpenAleph
    signal for the same person) are untouched — the collapse never crosses
    ``source_id``, and the docstring rule at the top of this module still
    holds. Because ``/signalstats`` counts post-dedup inside
    ``_merge_signals``, the counter reports the collapsed number
    automatically: display and instrument agree by construction.
    """
    groups: dict[tuple[str, str], list[RiskSignal]] = {}
    for sig in signals:
        sub = str((sig.evidence or {}).get("subject_statement_id") or "")
        groups.setdefault((sig.code, sub), []).append(sig)

    rank = {"high": 3, "medium": 2, "low": 1}
    out: list[RiskSignal] = []
    for copies in groups.values():
        if len(copies) == 1:
            out.append(copies[0])
            continue
        survivor = max(
            copies,
            key=lambda s: (
                rank.get(s.confidence, 0),
                float((s.evidence or {}).get("score") or 0.0),
            ),
        )
        evidence = dict(survivor.evidence)
        collections = [
            {
                "collection": (s.evidence or {}).get("collection") or "",
                "collection_url": (s.evidence or {}).get("collection_url") or "",
                "matched_name": (s.evidence or {}).get("matched_name") or "",
                "hit_id": s.hit_id,
                "score": (s.evidence or {}).get("score"),
            }
            for s in copies
        ]
        labels = sorted({c["collection"] for c in collections if c["collection"]})
        corroboration = sorted(
            {a for s in copies for a in ((s.evidence or {}).get("corroboration") or ())}
        )
        evidence["collections"] = collections
        evidence["collection_count"] = len(labels)
        evidence["corroboration"] = corroboration
        if evidence.get("kind") == _KIND_PERSON:
            evidence["name_match_only"] = not corroboration
        summary = survivor.summary
        if len(labels) > 1:
            # The survivor's summary was built with a single-collection
            # ``via '<label>'`` note (owned by ``_signals_from_percolate``,
            # so the format is this module's to rely on). State the spread
            # instead of one arbitrary member of it.
            listed = f" — listed across {len(labels)} OpenAleph collections"
            coll_note = f" via '{evidence.get('collection') or ''}'"
            if evidence.get("collection") and coll_note in summary:
                summary = summary.replace(coll_note, listed, 1)
            else:  # pragma: no cover - defensive: no via-note to rewrite
                summary = f"{summary} Listed across {len(labels)} OpenAleph collections."
        out.append(
            RiskSignal(
                code=survivor.code,
                confidence=survivor.confidence,
                summary=summary,
                source_id=survivor.source_id,
                hit_id=survivor.hit_id,
                evidence=evidence,
                kind=survivor.kind,
            )
        )
    return out


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
            if target["kind"] != _KIND_PERSON and not (
                names.org_name_residue(target["name"])
                and names.org_name_residue(best_name)
            ):
                # Entity-side analogue of the guard above: a name that is
                # nothing but legal-form boilerplate once org types are
                # stripped cannot be meaningfully matched. Found in
                # production (Phase 119): a Canadian-list record named only
                # «Общество с ограниченной ответственностью» — "Limited
                # Liability Company" in Russian — "matched" nineteen
                # distinct subsidiaries at 0.88+, all of it legal-form
                # similarity. Applied to both sides, mirroring
                # ``icij_check._screenable`` on the query side.
                continue
            hit = _as_hit(item, target)
            if not _birth_year_compatible(target.get("birth_year"), hit):
                continue
            hit_signals = _signals_from_percolate(
                item, target, best_name, matched_form, score
            )
            if hit_signals:
                signals.extend(hit_signals)
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


def _signals_from_percolate(
    item: dict[str, Any],
    target: dict[str, Any],
    matched_name: str,
    surface_form: str,
    score: float,
) -> list[RiskSignal]:
    """Every fact this percolation hit asserts about the related party.

    Shares the classification and the reporting rule with
    ``cross_check._signals_from_os``: each rung fires on its own merit, and
    the only suppression is ``sanction.linked`` when ``sanction.control``
    fires (upstream declares linked a superset of control). Returns ``[]``
    when no topic maps to a signal — the caller sends those to the
    informational screening block instead.

    Order is most-severe-first.
    """
    topics = _extract_topics(item)
    sanctions = classify_sanction_topics(topics)
    exports = classify_export_topics(topics)
    controlled = bool(sanctions.control)

    collection = _collection_label(item)
    coll_note = f" via '{collection}'" if collection else ""
    # Same corroboration rule as cross_check: percolation matches on names,
    # so an uncorroborated person hit is a name collision candidate here for
    # exactly the same reason. One rule, two screens.
    corroboration = corroborating_attributes(target, item)

    out: list[RiskSignal] = []

    def add(code: str, extra: str) -> None:
        out.append(
            RiskSignal(
                code=code,
                confidence=match_confidence(target, score, corroboration),
                summary=match_summary(
                    target=target,
                    source_id="openaleph",
                    summary_extra=f"{extra} ({_topic_blurb(topics)})",
                    corroboration=corroboration,
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
                    "collection": collection,
                    "collection_url": _ui_url(item),
                    "topics": topics,
                    "corroboration": list(corroboration),
                    "name_match_only": bool(
                        target["kind"] == _KIND_PERSON and not corroboration
                    ),
                },
            )
        )

    if sanctions.direct:
        add(RELATED_SANCTIONED, f"sanctioned{coll_note}")
    if controlled:
        add(
            RELATED_SANCTIONS_CONTROLLED,
            f"inside a sanctioned party's ownership chain{coll_note}",
        )
    if any(t in _DEBARMENT_TOPICS for t in topics):
        add(RELATED_DEBARMENT, f"debarred from public contracts{coll_note}")
    # Export-control listing outranks plain sanction adjacency; no
    # suppression within the export family (same rule as cross_check).
    if exports.control:
        add(
            RELATED_EXPORT_CONTROLLED,
            f"subject to export-control restrictions{coll_note}",
        )
    if not controlled and (sanctions.linked or sanctions.unknown):
        add(RELATED_SANCTIONS_LINKED, f"linked to sanctioned entities{coll_note}")
    if exports.linked or exports.unknown:
        add(
            RELATED_EXPORT_CONTROL_LINKED,
            f"linked to an export-controlled party{coll_note}",
        )
    if exports.risk:
        add(RELATED_EXPORT_RISK, f"flagged for trade risk{coll_note}")
    # Entities can never be PEPs — only natural persons hold political
    # office (same rule as cross_check).
    if target["kind"] == _KIND_PERSON and any(t in _PEP_TOPICS for t in topics):
        add(RELATED_PEP, f"PEP{coll_note}")
    # Last rung — same reasoning as cross_check._signals_from_os.
    if sanctions.counter:
        add(
            RELATED_COUNTER_SANCTIONED,
            "counter-sanctioned — designated by a state with weak democratic "
            f"institutions, not by a mainstream sanctions authority{coll_note}",
        )
    return out


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
