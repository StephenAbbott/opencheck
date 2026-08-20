"""Cross-check related-party names against OpenSanctions and EveryPolitician.

Why this exists
---------------

``/lookup`` today is *identifier-driven*: it bridges GLEIF / Companies
House / Wikidata via LEI / GB-COH / Q-ID. Once we have the BODS bundle
for a subject (which, post-Open-Ownership-override, includes its full
PSC / officer / parent chain), the *names* of the related parties
themselves are still un-checked. That means a sanctioned PSC behind a
clean shell company, or a PEP officer of an otherwise innocuous
holding, would not surface in the risk panel.

This module closes that gap. After the BODS bundle is assembled, every
``personStatement`` and every ``entityStatement`` has its name
(and where available its birth-year) searched against OpenSanctions
and EveryPolitician. Fuzzy matches above a similarity threshold
become scoped risk signals — ``RELATED_PEP`` or ``RELATED_SANCTIONED``
— attached to the related party's statement id, so the UI can place
them next to the right node in the graph rather than at the subject
level.

Scope notes
-----------

* OS and EP only for now. Wikidata SPARQL-by-name is too noisy for a
  deterministic check; revisit when the RDF/Oxigraph backbone lands.
* Bounded by ``max_targets`` to keep the request volume sane on
  large PSC chains. Targets are chosen by walking the bundle in
  insertion order — for a typical UK PSC bundle that means
  subject → PSCs → parents, which is exactly the prioritisation we
  want.
* Persons get checked for both ``RELATED_PEP`` and
  ``RELATED_SANCTIONED``. Entities only for ``RELATED_SANCTIONED``
  (entities can't be PEPs).
* Live mode + an OpenSanctions API key are required. Without them
  the module is a no-op — there's no useful "stub" cross-check to
  fabricate.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from . import names
from .config import get_settings
from .matching import is_matchable_name
from .risk import (
    _DEBARMENT_TOPICS,
    _EXPORT_TOPIC_PREFIX,
    _PEP_TOPICS,
    _SANCTION_TOPIC_PREFIX,
    DEGRADED_NOT_CONFIGURED,
    DegradedSource,
    RiskSignal,
    classify_degradation_reason,
    classify_export_topics,
    classify_sanction_topics,
    pick_degradation_reason,
)
from .sources import REGISTRY, SearchKind, SourceHit

_LOG = logging.getLogger(__name__)


# Risk code names — match strings used by the frontend's
# RISK_PRESENTATION map.
RELATED_PEP = "RELATED_PEP"
RELATED_SANCTIONED = "RELATED_SANCTIONED"
RELATED_COUNTER_SANCTIONED = "RELATED_COUNTER_SANCTIONED"
RELATED_SANCTIONS_CONTROLLED = "RELATED_SANCTIONS_CONTROLLED"
RELATED_SANCTIONS_LINKED = "RELATED_SANCTIONS_LINKED"
RELATED_DEBARMENT = "RELATED_DEBARMENT"
RELATED_EXPORT_CONTROLLED = "RELATED_EXPORT_CONTROLLED"
RELATED_EXPORT_CONTROL_LINKED = "RELATED_EXPORT_CONTROL_LINKED"
RELATED_EXPORT_RISK = "RELATED_EXPORT_RISK"

#: Name of this derived check in ``DegradedSource.check`` records.
CHECK_NAME = "cross_source_names"

#: Which signals each upstream source contributes — the codes whose absence
#: becomes unreliable when that source's probes fail (issue #50).
_AFFECTED_BY_SOURCE: dict[str, list[str]] = {
    "opensanctions": [
        RELATED_SANCTIONED,
        RELATED_COUNTER_SANCTIONED,
        RELATED_SANCTIONS_CONTROLLED,
        RELATED_SANCTIONS_LINKED,
        RELATED_DEBARMENT,
        RELATED_EXPORT_CONTROLLED,
        RELATED_EXPORT_CONTROL_LINKED,
        RELATED_EXPORT_RISK,
        RELATED_PEP,
    ],
    "everypolitician": [RELATED_PEP],
}


# OpenSanctions topic taxonomy — imported from ``risk.py``, which owns it,
# and re-exported here because ``openaleph_check`` reuses this module's
# ladder. These sets used to be a second hand-maintained copy "so the
# cross-check can be reasoned about in isolation"; in practice that meant
# ``sanction.control`` was mis-classified identically in every copy. The
# sanction family is now split by ``classify_sanction_topics`` — never
# hand-roll a ``startswith("sanction")`` test again.


# ---------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------


async def assess_cross_source_names(
    bods: list[dict[str, Any]],
    *,
    max_targets: int = 25,
    min_score: float = 0.88,
    degraded: list[DegradedSource] | None = None,
) -> list[RiskSignal]:
    """Return scoped ``RELATED_*`` risk signals for related parties in
    the BODS bundle that match an OpenSanctions / EveryPolitician
    record by name.

    ``degraded`` is an optional out-collector (issue #50): when the screen
    could not fully run — missing API key in live mode, upstream errors,
    timeouts — a :class:`DegradedSource` record per affected source is
    appended so callers can surface "this is not a clean screen" instead
    of letting the empty result pass for one. Records carry counts only,
    never the related-party names being screened.

    No-op (returns ``[]``) when:

    * Live mode is off — offline/demo mode is expected, not a degradation.
    * The bundle has no person/entity statements (nothing to screen, so
      nothing degraded either).
    * No OpenSanctions API key is configured in live mode — ``[]`` with
      ``not_configured`` degradation records.
    """
    if not bods:
        return []

    settings = get_settings()
    if not settings.allow_live:
        # Offline/demo mode — expected, not a degradation. Debug only.
        _LOG.debug("Cross-source name screening skipped: live mode is off.")
        return []

    targets = _collect_targets(bods)[:max_targets]
    if not targets:
        return []

    if not settings.opensanctions_api_key:
        # Live mode but no key: the screen genuinely cannot run, and an
        # empty result would otherwise look identical to a clean screen.
        _LOG.warning(
            "Cross-source name screening disabled: OPENSANCTIONS_API_KEY is not "
            "set while live mode is on. RELATED_SANCTIONED / RELATED_PEP signals "
            "will be absent for every lookup — this is NOT a clean screen."
        )
        if degraded is not None:
            for source_id, affected in _AFFECTED_BY_SOURCE.items():
                degraded.append(
                    DegradedSource(
                        source_id=source_id,
                        check=CHECK_NAME,
                        affected_signals=list(affected),
                        detail=(
                            "OPENSANCTIONS_API_KEY is not configured while live "
                            f"mode is on; {len(targets)} related-party name(s) "
                            "were not screened."
                        ),
                        reason=DEGRADED_NOT_CONFIGURED,
                    )
                )
        return []

    # Run the OS + EP probes concurrently — both adapters are cheap
    # and each name yields at most ~10 hits to score.
    tasks: list[asyncio.Task] = []
    for target in targets:
        tasks.append(asyncio.create_task(_check_target(target, min_score=min_score)))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[RiskSignal] = []
    # Failure bookkeeping. An adapter error must not poison the rest of
    # the bundle, but it must not pass for "screened, nothing found"
    # either — a silently-dropped sanctions probe is a false negative.
    # Counts only: the target names are people/companies from the
    # subject's ownership graph and must not reach hosted logs or the
    # DegradedSource records built from these tallies.
    failed_by_source: dict[str, dict[str, int]] = {}
    target_errors = 0
    target_error_reasons: dict[str, int] = {}
    for r in results:
        if isinstance(r, BaseException):
            target_errors += 1
            reason = classify_degradation_reason(r)
            target_error_reasons[reason] = target_error_reasons.get(reason, 0) + 1
            _LOG.warning(
                "Cross-source screening: probe for one related party raised "
                "%s: %s (that party was not screened).",
                type(r).__name__,
                r,
            )
            continue
        target_signals, failures = r
        signals.extend(target_signals)
        for source_id, reason in failures.items():
            by_reason = failed_by_source.setdefault(source_id, {})
            by_reason[reason] = by_reason.get(reason, 0) + 1

    if failed_by_source or target_errors:
        detail = ", ".join(
            f"{source_id} failed for {sum(reasons.values())} of {len(targets)} name(s)"
            for source_id, reasons in sorted(failed_by_source.items())
        )
        if target_errors:
            extra = f"{target_errors} of {len(targets)} name(s) errored outright"
            detail = f"{detail}; {extra}" if detail else extra
        _LOG.warning(
            "Cross-source screening degraded for this lookup (%s): "
            "RELATED_SANCTIONED / RELATED_PEP signals may be incomplete — "
            "an empty result here is not a clean screen.",
            detail,
        )
        if degraded is not None:
            for source_id, reasons in sorted(failed_by_source.items()):
                count = sum(reasons.values())
                degraded.append(
                    DegradedSource(
                        source_id=source_id,
                        check=CHECK_NAME,
                        affected_signals=list(
                            _AFFECTED_BY_SOURCE.get(source_id, [])
                        ),
                        detail=(
                            f"Search failed for {count} of {len(targets)} "
                            "related-party name(s); those parties were not "
                            "screened against this source."
                        ),
                        reason=pick_degradation_reason(reasons),
                    )
                )
            if target_errors:
                # The probe died before reaching either upstream, so no
                # single source can be blamed — every RELATED_* code is
                # potentially incomplete.
                degraded.append(
                    DegradedSource(
                        source_id="opencheck",
                        check=CHECK_NAME,
                        affected_signals=list(
                            _AFFECTED_BY_SOURCE["opensanctions"]
                        ),
                        detail=(
                            f"Screening errored for {target_errors} of "
                            f"{len(targets)} related-party name(s) before "
                            "reaching the upstream sources."
                        ),
                        reason=pick_degradation_reason(target_error_reasons),
                    )
                )
    return _dedupe(signals)


# ---------------------------------------------------------------------
# Target extraction
# ---------------------------------------------------------------------


_KIND_PERSON = "person"
_KIND_ENTITY = "entity"


def _collect_targets(bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pull ``{kind, statement_id, name, birth_year}`` records out of
    every person and entity statement in the bundle.

    Skips placeholder shapes (``unknownPerson`` / ``anonymousEntity``)
    — they have no checkable name. Skips records with empty names.
    """
    out: list[dict[str, Any]] = []
    for stmt in bods:
        record_type = stmt.get("recordType") or stmt.get("statementType", "").replace(
            "Statement", ""
        )
        rd = stmt.get("recordDetails") or {}
        sid = stmt.get("statementId") or ""
        if not sid:
            continue
        if record_type == "person":
            person_type = rd.get("personType") or stmt.get("personType")
            if person_type and person_type != "knownPerson":
                continue
            name = _person_full_name(rd)
            if not name:
                continue
            out.append(
                {
                    "kind": _KIND_PERSON,
                    "statement_id": sid,
                    "name": name,
                    "birth_year": _person_birth_year(rd),
                    "nationalities": _person_nationalities(rd),
                }
            )
        elif record_type == "entity":
            entity_type = (
                (rd.get("entityType") or {}).get("type")
                if isinstance(rd.get("entityType"), dict)
                else stmt.get("entityType")
            )
            if entity_type in {"anonymousEntity", "unknownEntity"}:
                continue
            name = rd.get("name") or stmt.get("name")
            if not name:
                continue
            out.append(
                {
                    "kind": _KIND_ENTITY,
                    "statement_id": sid,
                    "name": name.strip(),
                    "birth_year": None,
                    "nationalities": (),
                }
            )
    return out


def _person_full_name(rd: dict[str, Any]) -> str:
    """Pick a usable display name from the BODS person statement.

    BODS stores ``names`` as a list of ``{type, fullName, ...}``; we
    prefer the entry whose ``type == "individual"`` if present, else
    the first one with a ``fullName``.
    """
    names = rd.get("names") or []
    if not isinstance(names, list):
        return ""
    individual = next(
        (n for n in names if isinstance(n, dict) and n.get("type") == "individual"),
        None,
    )
    pick = individual or next(
        (n for n in names if isinstance(n, dict) and n.get("fullName")),
        None,
    )
    if pick is None:
        return ""
    full = pick.get("fullName") or ""
    if full:
        return full.strip()
    given = pick.get("givenName") or ""
    family = pick.get("familyName") or ""
    return f"{given} {family}".strip()


def _person_birth_year(rd: dict[str, Any]) -> int | None:
    bd = rd.get("birthDate") or ""
    m = re.match(r"(\d{4})", bd)
    return int(m.group(1)) if m else None


def _person_nationalities(rd: dict[str, Any]) -> tuple[str, ...]:
    """ISO codes from the BODS person statement's ``nationalities``.

    BODS models these as ``[{"name": …, "code": "GB"}, …]``; only the code
    is usable for comparison, since the name is free text in the
    register's own language.
    """
    out: list[str] = []
    for nat in rd.get("nationalities") or ():
        if isinstance(nat, dict):
            code = nat.get("code")
            if isinstance(code, str) and code.strip():
                out.append(code.strip())
    return tuple(out)


# ---------------------------------------------------------------------
# Per-target probe
# ---------------------------------------------------------------------


async def _check_target(
    target: dict[str, Any], *, min_score: float
) -> tuple[list[RiskSignal], dict[str, str]]:
    """Run OS (+ EP for persons) searches for one target and score the
    matches.

    Returns ``(signals, failed_sources)`` where ``failed_sources`` maps a
    source id to a closed-vocabulary degradation reason. It lets the
    caller distinguish "screened, nothing found" from "the screen never
    ran" — see the aggregated warning in ``assess_cross_source_names``.
    """
    name = target["name"]
    kind = SearchKind.PERSON if target["kind"] == _KIND_PERSON else SearchKind.ENTITY

    os_adapter = REGISTRY.get("opensanctions")
    ep_adapter = (
        REGISTRY.get("everypolitician") if target["kind"] == _KIND_PERSON else None
    )

    tasks: list[asyncio.Task[list[SourceHit]]] = []
    if os_adapter is not None:
        tasks.append(asyncio.create_task(os_adapter.search(name, kind)))
    if ep_adapter is not None:
        tasks.append(asyncio.create_task(ep_adapter.search(name, SearchKind.PERSON)))
    if not tasks:
        return [], {}

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[RiskSignal] = []
    failed: dict[str, str] = {}
    # OpenSanctions hits — derive RELATED_PEP / RELATED_SANCTIONED from
    # the topics on the underlying record.
    if os_adapter is not None:
        os_raw = raw_results[0]
        if isinstance(os_raw, BaseException):
            failed["opensanctions"] = classify_degradation_reason(os_raw)
            os_hits: list[SourceHit] = []
        else:
            os_hits = os_raw
        for hit in os_hits:
            signals.extend(_signals_from_os(hit, target, min_score=min_score))
    # EveryPolitician hits — every hit is by construction a PEP.
    if ep_adapter is not None:
        ep_index = 0 if os_adapter is None else 1
        ep_raw = raw_results[ep_index]
        if isinstance(ep_raw, BaseException):
            failed["everypolitician"] = classify_degradation_reason(ep_raw)
            ep_hits: list[SourceHit] = []
        else:
            ep_hits = ep_raw
        for hit in ep_hits:
            sig = _signal_from_ep(hit, target, min_score=min_score)
            if sig is not None:
                signals.append(sig)
    return signals, failed


def _signals_from_os(
    hit: SourceHit, target: dict[str, Any], *, min_score: float
) -> list[RiskSignal]:
    """Every fact this OpenSanctions hit asserts about the related party.

    Previously returned at most one signal, chosen by a priority ladder. That
    made a related party who is *both* designated and inside another
    designated party's ownership chain report only as sanctioned — losing a
    fact that ``risk.py`` reports for the subject. The two paths now agree:
    each rung fires on its own merit, and the only suppression is
    ``sanction.linked`` when ``sanction.control`` fires, because upstream
    declares linked a superset of control — the same fact stated less
    precisely, not an additional one.

    Order is preserved most-severe-first so a caller taking ``[0]`` still
    gets the headline finding — which is why ``RELATED_COUNTER_SANCTIONED``
    is emitted last despite being, structurally, a direct listing.
    """
    if hit.is_stub:
        return []
    # Single-token person names ("Fernández", "Ivanov") are too generic to
    # base a related-party match on — a bare surname collides across unrelated
    # people (ftmg drops single-token names from matching). Entities keep
    # single-token names, which are distinctive ("Gazprom").
    if target["kind"] == _KIND_PERSON and not (
        is_matchable_name(_normalise(target["name"]))
        and is_matchable_name(_normalise(hit.name))
    ):
        return []
    score = _name_score(target["name"], hit.name)
    if score < min_score:
        return []
    if not _birth_year_compatible(target.get("birth_year"), hit):
        return []
    topics = _extract_topics(hit.raw or {})
    blurb = _topic_blurb(topics)
    sanctions = classify_sanction_topics(topics)
    exports = classify_export_topics(topics)
    controlled = bool(sanctions.control)

    out: list[RiskSignal] = []

    def add(code: str, extra: str) -> None:
        out.append(
            _make_signal(
                code=code, target=target, hit=hit, score=score, summary_extra=extra
            )
        )

    if sanctions.direct:
        add(RELATED_SANCTIONED, f"sanctioned per OpenSanctions ({blurb})")
    if controlled:
        add(
            RELATED_SANCTIONS_CONTROLLED,
            f"inside a sanctioned party's ownership chain per OpenSanctions ({blurb})",
        )
    if any(t in _DEBARMENT_TOPICS for t in topics):
        add(
            RELATED_DEBARMENT,
            f"debarred from public contracts per OpenSanctions ({blurb})",
        )
    # Export-control listing outranks plain sanction adjacency: it is a
    # restriction on the related party itself. No suppression anywhere in the
    # export family — upstream declares no superset relationship among the
    # export topics (see risk.py).
    if exports.control:
        add(
            RELATED_EXPORT_CONTROLLED,
            f"subject to export-control restrictions per OpenSanctions ({blurb})",
        )
    # Suppressed when control fires — see the docstring.
    if not controlled and (sanctions.linked or sanctions.unknown):
        add(
            RELATED_SANCTIONS_LINKED,
            f"linked to sanctioned entities per OpenSanctions ({blurb})",
        )
    if exports.linked or exports.unknown:
        add(
            RELATED_EXPORT_CONTROL_LINKED,
            f"linked to an export-controlled party per OpenSanctions ({blurb})",
        )
    if exports.risk:
        add(
            RELATED_EXPORT_RISK,
            f"flagged for trade risk per OpenSanctions ({blurb})",
        )
    # Entities can never be PEPs by definition — only natural persons
    # hold political office. Skip the RELATED_PEP path for entity
    # targets even when OpenSanctions tags an entity record with a
    # ``role.pep`` topic (which it sometimes does for legal vehicles
    # owned by a PEP).
    if target["kind"] == _KIND_PERSON and any(t in _PEP_TOPICS for t in topics):
        add(RELATED_PEP, f"PEP per OpenSanctions ({blurb})")
    # Last rung deliberately. A counter-sanction is a direct listing of the
    # related party, but by a regime the reader almost certainly owes no
    # obligation to — so it must never be the headline finding a caller gets
    # from ``[0]``, and must never read as "related party sanctioned".
    if sanctions.counter:
        add(
            RELATED_COUNTER_SANCTIONED,
            "counter-sanctioned per OpenSanctions — designated by a state with "
            "weak democratic institutions, not by a mainstream sanctions "
            f"authority ({blurb})",
        )
    return out


def _signal_from_ep(
    hit: SourceHit, target: dict[str, Any], *, min_score: float
) -> RiskSignal | None:
    if hit.is_stub:
        return None
    # EveryPolitician hits are always persons — apply the same single-token
    # guard as the OpenSanctions path.
    if not (
        is_matchable_name(_normalise(target["name"]))
        and is_matchable_name(_normalise(hit.name))
    ):
        return None
    score = _name_score(target["name"], hit.name)
    if score < min_score:
        return None
    if not _birth_year_compatible(target.get("birth_year"), hit):
        return None
    return _make_signal(
        code=RELATED_PEP,
        target=target,
        hit=hit,
        score=score,
        summary_extra="political office-holder per EveryPolitician",
    )


def match_confidence(
    target: dict[str, Any], score: float, corroboration: tuple[str, ...]
) -> str:
    """Confidence for a name-derived related-party signal.

    Entities keep the historical score-only ladder: an organisation name
    is distinctive enough that "Gazprom PJSC" matching "Gazprom" is a
    finding on its own. **Persons are capped one rung down** when nothing
    but the name agrees — never ``high``, since "high confidence this is
    the same person" is precisely the claim a name alone cannot support.

    The cap is two-tier rather than a flat ``low``, and the distinction
    matters. Sanctions screening *is* name-based: an exact match against a
    designated person is something a reviewer must adjudicate, and burying
    every such hit at ``low`` would misreport standard practice as a weak
    lead. The fuzzy band is different — that is where "Michael Gordon" vs
    "Michael R. Gordon" (0.9333) lives, alongside "Michael R." vs
    "Michael E." (0.9375, definitively different people). So:

    * exact-ish (``>= 0.95``), uncorroborated → ``medium``
    * threshold band, uncorroborated        → ``low``

    Either way the summary says "possible name match only", so the *prose*
    never asserts identity regardless of which rung it lands on.
    """
    if target["kind"] == _KIND_PERSON and not corroboration:
        return "medium" if score >= 0.95 else "low"
    return "high" if score >= 0.95 else "medium"


def match_summary(
    *,
    target: dict[str, Any],
    source_id: str,
    summary_extra: str,
    corroboration: tuple[str, ...],
) -> str:
    """Signal prose. An uncorroborated person match must not read as an
    assertion that the related party *is* the listed record."""
    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
    if target["kind"] == _KIND_PERSON and not corroboration:
        return (
            f"Possible name match only: {relation.lower()} '{target['name']}' "
            f"shares a name with a record on {source_id}, with no birth date "
            f"or nationality in common to confirm they are the same person "
            f"— {summary_extra}."
        )
    via = f" (confirmed on {', '.join(corroboration)})" if corroboration else ""
    return (
        f"{relation} '{target['name']}' matches a record "
        f"on {source_id}{via}: {summary_extra}."
    )


def _make_signal(
    *,
    code: str,
    target: dict[str, Any],
    hit: SourceHit,
    score: float,
    summary_extra: str,
) -> RiskSignal:
    corroboration = corroborating_attributes(target, hit.raw or {})
    return RiskSignal(
        code=code,
        confidence=match_confidence(target, score, corroboration),
        summary=match_summary(
            target=target,
            source_id=hit.source_id,
            summary_extra=summary_extra,
            corroboration=corroboration,
        ),
        source_id=hit.source_id,
        hit_id=hit.hit_id,
        evidence={
            "subject_statement_id": target["statement_id"],
            "matched_name": hit.name,
            "search_name": target["name"],
            "score": round(score, 3),
            "kind": target["kind"],
            "corroboration": list(corroboration),
            "name_match_only": bool(
                target["kind"] == _KIND_PERSON and not corroboration
            ),
        },
    )


# ---------------------------------------------------------------------
# Helpers — name normalisation, scoring, topic extraction, dedupe.
# ---------------------------------------------------------------------


def _normalise(name: str) -> str:
    """Shared comparable form (Phase B, rigour adoption): see
    ``opencheck/names.py``. Identical output to the old local normaliser for
    Latin-script names (its fold table is a subset of the shared one); adds
    Cyrillic/Greek transliteration so native vs transliterated forms of the
    same person can finally score as similar instead of ~0."""
    return names.normalise_name(name)


def _name_score(a: str, b: str) -> float:
    """Similarity in [0.0, 1.0] for two names — the shared Phase-D scorer
    (see ``names.name_similarity``): the historical difflib ratio plus
    token-sort order invariance and, with rigour installed, its
    edit-budgeted Levenshtein. Also reused by BackgroundCheck person
    screening (routers/person_check.py) at the same 0.88 threshold."""
    return names.name_similarity(a, b)


def _extract_topics(raw: dict[str, Any]) -> list[str]:
    topics = raw.get("topics")
    if not topics:
        topics = (raw.get("properties") or {}).get("topics") or []
    if isinstance(topics, str):
        topics = [topics]
    return [t for t in topics if isinstance(t, str)]


def _topic_blurb(topics: list[str]) -> str:
    """Compact ``role.pep, sanction`` summary for the chip tooltip."""
    keep = [
        t
        for t in topics
        if t.startswith(_SANCTION_TOPIC_PREFIX)
        or t.startswith(_EXPORT_TOPIC_PREFIX)
        or t in _PEP_TOPICS
        or t in _DEBARMENT_TOPICS
    ]
    return ", ".join(sorted(set(keep))[:3]) if keep else "no topic"


def _prop_values(raw: dict[str, Any], *keys: str) -> list[str]:
    """Collect string values for ``keys`` from an FtM-shaped payload.

    Properties may sit at the top level or under ``properties``, and each
    may be a bare string or a list. Shared by the birth-year and
    nationality readers so the two cannot drift.
    """
    out: list[str] = []
    props = raw.get("properties") if isinstance(raw.get("properties"), dict) else {}
    for container in (props, raw):
        if not isinstance(container, dict):
            continue
        for key in keys:
            v = container.get(key)
            if isinstance(v, list):
                out.extend(str(x) for x in v if x)
            elif isinstance(v, str) and v:
                out.append(v)
    return out


def _hit_birth_years(raw: dict[str, Any]) -> set[int]:
    """Every four-digit year the hit's record offers as a birth date."""
    years: set[int] = set()
    for cand in _prop_values(raw, "birthDate", "birthDates"):
        m = re.match(r"(\d{4})", cand)
        if m:
            years.add(int(m.group(1)))
    return years


def _hit_countries(raw: dict[str, Any]) -> set[str]:
    """Country-ish codes on the hit: nationality, citizenship, country."""
    return {
        v.strip().lower()
        for v in _prop_values(raw, "nationality", "citizenship", "country")
        if v.strip()
    }


def corroborating_attributes(
    target: dict[str, Any], raw: dict[str, Any]
) -> tuple[str, ...]:
    """Attributes beyond the name on which the target and the hit *agree*.

    A name match on its own is weak evidence about a person: "Michael
    Gordon" collides across unrelated people, and the scorer cannot help —
    a middle initial is two characters, so ``Michael R. Gordon`` and
    ``Michael E. Gordon``, who are definitively different people, score
    **higher** (0.9375) than a genuine abbreviation pair (0.9333).
    Tightening the threshold cannot fix that; it drops true positives
    first. So the discrimination has to come from a second attribute.

    Returns the names of the attributes that agree — empty means the match
    rests on the name alone. Note the asymmetry this deliberately does not
    paper over: agreement requires **both** sides to supply the attribute.
    A hit with no birth date is not corroborated by our knowing one; it is
    simply unchecked, and reporting silence as agreement is how the
    Liverpool FC false positive read as a finding.
    """
    found: list[str] = []
    year = target.get("birth_year")
    if year is not None:
        hit_years = _hit_birth_years(raw)
        if hit_years and any(abs(y - year) <= 1 for y in hit_years):
            found.append("birth_year")
    ours = {c.lower() for c in (target.get("nationalities") or ()) if c}
    if ours:
        theirs = _hit_countries(raw)
        if theirs and (ours & theirs):
            found.append("nationality")
    return tuple(found)


def _birth_year_compatible(year: int | None, hit: SourceHit) -> bool:
    """Reject a match whose birth years actively *conflict*.

    Distinct from :func:`corroborating_attributes`: this is the hard
    filter (drop the match), that is the confidence input (keep it, but
    say what it rests on). When either side is missing we keep the match
    — name alone is still worth surfacing — but it will now be reported
    as a name match only rather than as an assertion.
    """
    if year is None:
        return True
    hit_years = _hit_birth_years(hit.raw or {})
    if not hit_years:
        return True  # hit has no DOB — keep, but uncorroborated
    return any(abs(y - year) <= 1 for y in hit_years)


def _dedupe(signals: list[RiskSignal]) -> list[RiskSignal]:
    """Two probes (OS + EP) on the same target may both flag the same
    upstream record id. Keep the highest-confidence single instance per
    ``(code, source_id, hit_id, subject_statement_id)``."""
    rank = {"high": 3, "medium": 2, "low": 1}
    keyed: dict[tuple, RiskSignal] = {}
    for sig in signals:
        sub = sig.evidence.get("subject_statement_id", "")
        key = (sig.code, sig.source_id, sig.hit_id, sub)
        existing = keyed.get(key)
        if existing is None or rank.get(sig.confidence, 0) > rank.get(
            existing.confidence, 0
        ):
            keyed[key] = sig
    return list(keyed.values())
