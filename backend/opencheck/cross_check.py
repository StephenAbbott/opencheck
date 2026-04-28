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
import difflib
import re
import unicodedata
from typing import Any

from .config import get_settings
from .risk import RiskSignal
from .sources import REGISTRY, SearchKind, SourceHit


# Risk code names — match strings used by the frontend's
# RISK_PRESENTATION map.
RELATED_PEP = "RELATED_PEP"
RELATED_SANCTIONED = "RELATED_SANCTIONED"


# OpenSanctions topic taxonomy. Same shape as the regular ``risk.py``
# rules; duplicated here so the cross-check can be reasoned about
# in isolation.
_PEP_TOPICS = {"role.pep", "role.rca", "role.spouse", "role.family"}
_SANCTION_TOPIC_PREFIX = "sanction"


# ---------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------


async def assess_cross_source_names(
    bods: list[dict[str, Any]],
    *,
    max_targets: int = 25,
    min_score: float = 0.88,
) -> list[RiskSignal]:
    """Return scoped ``RELATED_*`` risk signals for related parties in
    the BODS bundle that match an OpenSanctions / EveryPolitician
    record by name.

    No-op (returns ``[]``) when:

    * Live mode is off or no OpenSanctions API key is configured —
      we can't ask the upstream sources.
    * The bundle has no person/entity statements.
    """
    if not bods:
        return []

    settings = get_settings()
    if not settings.opensanctions_api_key or not settings.allow_live:
        return []

    targets = _collect_targets(bods)[:max_targets]
    if not targets:
        return []

    # Run the OS + EP probes concurrently — both adapters are cheap
    # and each name yields at most ~10 hits to score.
    tasks: list[asyncio.Task] = []
    for target in targets:
        tasks.append(asyncio.create_task(_check_target(target, min_score=min_score)))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[RiskSignal] = []
    for r in results:
        if isinstance(r, Exception):
            # An adapter error shouldn't poison the rest of the bundle.
            # The caller will still see plain risk signals; cross-check
            # silently skips the target that errored.
            continue
        signals.extend(r)
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


# ---------------------------------------------------------------------
# Per-target probe
# ---------------------------------------------------------------------


async def _check_target(
    target: dict[str, Any], *, min_score: float
) -> list[RiskSignal]:
    """Run OS (+ EP for persons) searches for one target and score the
    matches."""
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
        return []

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[RiskSignal] = []
    # OpenSanctions hits — derive RELATED_PEP / RELATED_SANCTIONED from
    # the topics on the underlying record.
    if os_adapter is not None:
        os_hits = raw_results[0] if not isinstance(raw_results[0], Exception) else []
        for hit in os_hits:
            sig = _signal_from_os(hit, target, min_score=min_score)
            if sig is not None:
                signals.append(sig)
    # EveryPolitician hits — every hit is by construction a PEP.
    if ep_adapter is not None:
        ep_index = 0 if os_adapter is None else 1
        ep_hits = (
            raw_results[ep_index]
            if not isinstance(raw_results[ep_index], Exception)
            else []
        )
        for hit in ep_hits:
            sig = _signal_from_ep(hit, target, min_score=min_score)
            if sig is not None:
                signals.append(sig)
    return signals


def _signal_from_os(
    hit: SourceHit, target: dict[str, Any], *, min_score: float
) -> RiskSignal | None:
    if hit.is_stub:
        return None
    score = _name_score(target["name"], hit.name)
    if score < min_score:
        return None
    if not _birth_year_compatible(target.get("birth_year"), hit):
        return None
    topics = _extract_topics(hit.raw or {})
    sanctioned = any(t.startswith(_SANCTION_TOPIC_PREFIX) for t in topics)
    is_pep = any(t in _PEP_TOPICS for t in topics)
    if sanctioned:
        return _make_signal(
            code=RELATED_SANCTIONED,
            target=target,
            hit=hit,
            score=score,
            summary_extra=f"sanctioned per OpenSanctions ({_topic_blurb(topics)})",
        )
    # Entities can never be PEPs by definition — only natural persons
    # hold political office. Skip the RELATED_PEP path for entity
    # targets even when OpenSanctions tags an entity record with a
    # ``role.pep`` topic (which it sometimes does for legal vehicles
    # owned by a PEP).
    if is_pep and target["kind"] == _KIND_PERSON:
        return _make_signal(
            code=RELATED_PEP,
            target=target,
            hit=hit,
            score=score,
            summary_extra=f"PEP per OpenSanctions ({_topic_blurb(topics)})",
        )
    return None


def _signal_from_ep(
    hit: SourceHit, target: dict[str, Any], *, min_score: float
) -> RiskSignal | None:
    if hit.is_stub:
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


def _make_signal(
    *,
    code: str,
    target: dict[str, Any],
    hit: SourceHit,
    score: float,
    summary_extra: str,
) -> RiskSignal:
    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
    return RiskSignal(
        code=code,
        # High confidence on near-exact matches; medium on the
        # threshold band.
        confidence="high" if score >= 0.95 else "medium",
        summary=(
            f"{relation} '{target['name']}' matches a record "
            f"on {hit.source_id}: {summary_extra}."
        ),
        source_id=hit.source_id,
        hit_id=hit.hit_id,
        evidence={
            "subject_statement_id": target["statement_id"],
            "matched_name": hit.name,
            "search_name": target["name"],
            "score": round(score, 3),
            "kind": target["kind"],
        },
    )


# ---------------------------------------------------------------------
# Helpers — name normalisation, scoring, topic extraction, dedupe.
# ---------------------------------------------------------------------


# Stand-alone non-ASCII letters that NFKD does NOT decompose. Real
# OpenSanctions / EveryPolitician records sometimes carry the
# transliterated form, sometimes the native one — we fold them so a
# Polish ``ł`` matches an English ``l`` and similar.
_NON_DECOMPOSABLE_FOLDS = {
    "ł": "l", "Ł": "L",
    "ø": "o", "Ø": "O",
    "æ": "ae", "Æ": "Ae",
    "œ": "oe", "Œ": "Oe",
    "ð": "d", "Ð": "D",
    "þ": "th", "Þ": "Th",
    "ß": "ss",
}


def _normalise(name: str) -> str:
    """Lower-case, strip diacritics, drop punctuation, collapse spaces.

    Beyond the standard NFKD pass we also fold a small set of
    stand-alone non-ASCII letters (``ł``, ``ø``, ``æ`` etc.) that NFKD
    leaves in place — they're common in central / northern European
    person names and we want them to match the transliterated form.
    """
    if not name:
        return ""
    folded = "".join(_NON_DECOMPOSABLE_FOLDS.get(c, c) for c in name)
    decomposed = unicodedata.normalize("NFKD", folded)
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_only.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _name_score(a: str, b: str) -> float:
    """Similarity in [0.0, 1.0] for two names."""
    na, nb = _normalise(a), _normalise(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return difflib.SequenceMatcher(a=na, b=nb).ratio()


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
        if t.startswith(_SANCTION_TOPIC_PREFIX) or t in _PEP_TOPICS
    ]
    return ", ".join(sorted(set(keep))[:3]) if keep else "no topic"


def _birth_year_compatible(year: int | None, hit: SourceHit) -> bool:
    """If we know the target's birth year and the hit's record carries
    one, require that they agree (off by ≤1 to allow for date precision
    differences). When either side is missing we leave the match in
    place — name alone is a useful signal."""
    if year is None:
        return True
    raw = hit.raw or {}
    candidates: list[str] = []
    props = raw.get("properties") if isinstance(raw.get("properties"), dict) else {}
    if isinstance(props, dict):
        for key in ("birthDate", "birthDates"):
            v = props.get(key)
            if isinstance(v, list):
                candidates.extend(str(x) for x in v if x)
            elif isinstance(v, str):
                candidates.append(v)
    if not candidates:
        return True  # hit has no DOB — keep
    for cand in candidates:
        m = re.match(r"(\d{4})", cand)
        if m and abs(int(m.group(1)) - year) <= 1:
            return True
    return False


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
