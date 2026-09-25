"""One PEP chip per person per upstream record, and PEPs by this company's own role.

Phase 247 — the Opus 5.5 check (DQ-8). Equinor's lookup carried 24
``RELATED_PEP`` signals for about a dozen people, for two reasons this module
takes apart:

1. **The same upstream record, counted once per mirror.** EveryPolitician is
   OpenSanctions' ``peps`` dataset, read through the same API, and it hands
   back OpenSanctions' own entity id. OpenAleph re-indexes OpenSanctions'
   collections and signs every id with its collection namespace
   (``Q28047991.d63cd770…``: the FtM id, a dot, 40 hex characters). So
   "Anders Opedal is a PEP" arrived three times — OpenSanctions, EveryPolitician
   and OpenAleph — and twice more because two person statements in the bundle
   carry his name, one with a birth date and one without. Five chips, one
   record: ``Q28047991``.

   :func:`merge_derived_pep` groups ``RELATED_PEP`` signals on the upstream
   record they resolve to (:func:`upstream_record_id`) and the related party's
   normalised name, and keeps one: the highest confidence, ties going to the
   source the others are derived from (``sources.lineage``). The rest are
   listed on ``evidence.also_reported_by`` and named in the summary, and every
   person statement the group matched stays on ``evidence.subject_statement_ids``
   so each graph node keeps its badge. Lineage is consulted here to decide
   whether two signals are *one observation*, never whether a signal fires —
   the rule ``sources/lineage.py`` sets for itself.

2. **PEP by virtue of this role.** Most of those people are PEPs in
   OpenSanctions only through its ``no_brreg`` dataset, *Norway State-Owned
   Enterprises Leadership*: their one position is "Chairman, EQUINOR ASA",
   "Managing Director, EQUINOR ASA" (topic ``gov.soe``). Screening a state
   company's board and reporting that its board is politically exposed because
   it is the board of a state company is circular. It is still true, and the
   EU regime does treat SOE leadership as PEPs, so the signal is kept — as
   :data:`~opencheck.cross_check.RELATED_PEP_SUBJECT_ROLE`, ``kind="context"``,
   saying which role (Stephen, 25 Sept 2026). A PEP whose record also holds a
   position elsewhere (Haakon Bruun-Hanssen, Chief of Defence of Norway 2013–20)
   stays a ``RELATED_PEP`` finding, with the role at this company noted.

   The positions come from OpenSanctions' entity record, nested
   (``positionOccupancies[].post[]``), read through the adapter's cached
   ``fetch`` — one call per distinct upstream record, only for PEP matches.
   A position is at the subject when its ``organization`` is one of the
   subject's own OpenSanctions entities, or when the organisation the
   position's name carries — after the comma in Norway's "Chairman, EQUINOR
   ASA", before it in Denmark's "Ørsted A/S, board of directors" — is one of
   the subject's names. OpenSanctions holds Equinor twice (a ``Company`` with
   the LEI and an ``Organization`` without, and the positions point at the
   second), so the name test is the one that does the work.

   A record that cannot be read changes nothing: the signal stays what it was,
   with ``pep_role_checked: false``. This is a refinement of a finding that
   already stands, so a failed fetch is not a degraded screen.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Iterable

from . import names
from .cross_check import RELATED_PEP, RELATED_PEP_SUBJECT_ROLE
from .sources import REGISTRY, source_display_name
from .sources.lineage import derived_from

_LOG = logging.getLogger(__name__)

#: The upstream every PEP-capable screen reads. EveryPolitician and OpenAleph
#: declare it in ``derived_from``; anything that does not is left alone.
_UPSTREAM = "opensanctions"

#: OpenAleph signs every entity id with its collection namespace:
#: ``<ftm id>.<sha1 hex>``. The FtM id before the dot is the publisher's own.
_ALEPH_SIGNATURE = re.compile(r"^(?P<id>.+)\.[0-9a-f]{40}$")

_RANK = {"high": 3, "medium": 2, "low": 1}

#: At most this many record reads per lookup, and this many at once. Equinor
#: needs about a dozen; the cap is the screen's own target cap.
_MAX_ROLE_CHECKS = 25
_ROLE_CHECK_CONCURRENCY = 6


def upstream_record_id(signal: dict[str, Any]) -> str | None:
    """The OpenSanctions entity id this signal's record is a copy of, if any.

    OpenSanctions' own hit id is the record; a source that declares
    OpenSanctions as its upstream hands back the same id, OpenAleph with its
    collection signature appended. ``None`` for anything else.
    """
    source_id = str(signal.get("source_id") or "")
    hit_id = str(signal.get("hit_id") or "").strip()
    if not hit_id:
        return None
    if source_id == _UPSTREAM:
        return hit_id
    if _UPSTREAM not in derived_from(source_id):
        return None
    m = _ALEPH_SIGNATURE.match(hit_id)
    return m.group("id") if m else hit_id


def _upstreamness(source_id: str, group_sources: Iterable[str]) -> int:
    """How many sources in the group republish ``source_id`` — higher is
    further upstream, so it wins a confidence tie."""
    return sum(1 for other in group_sources if source_id in derived_from(other))


def _source_list(ids: list[str]) -> str:
    labels = [source_display_name(i) for i in ids]
    if len(labels) == 1:
        return labels[0]
    return f"{', '.join(labels[:-1])} and {labels[-1]}"


def merge_derived_pep(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One ``RELATED_PEP`` per (upstream record, related party's name).

    Order is preserved: a merged signal takes the place of the first member of
    its group. Signals that resolve to no upstream record pass through.
    """
    groups: dict[tuple[str, str], list[int]] = {}
    for i, sig in enumerate(signals):
        if sig.get("code") != RELATED_PEP:
            continue
        upstream = upstream_record_id(sig)
        name = names.normalise_name(str((sig.get("evidence") or {}).get("search_name") or ""))
        if not upstream or not name:
            continue
        groups.setdefault((upstream, name), []).append(i)

    replaced: dict[int, dict[str, Any] | None] = {}
    for (upstream, _name), idxs in groups.items():
        members = [signals[i] for i in idxs]
        sources = [str(m.get("source_id") or "") for m in members]
        ranked = sorted(
            members,
            key=lambda m: (
                -_RANK.get(str(m.get("confidence")), 0),
                -_upstreamness(str(m.get("source_id") or ""), sources),
            ),
        )
        winner = ranked[0]
        evidence = dict(winner.get("evidence") or {})
        evidence["upstream_record_id"] = upstream
        statement_ids = sorted(
            {
                str((m.get("evidence") or {}).get("subject_statement_id") or "")
                for m in members
            }
            - {""}
        )
        others: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for m in ranked[1:]:
            ev = m.get("evidence") or {}
            key = (
                str(m.get("source_id") or ""),
                str(m.get("hit_id") or ""),
                str(ev.get("subject_statement_id") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            others.append(
                {
                    "source_id": key[0],
                    "hit_id": key[1],
                    "subject_statement_id": key[2],
                    "confidence": m.get("confidence"),
                }
            )
        merged = {**winner, "evidence": evidence}
        if len(statement_ids) > 1:
            evidence["subject_statement_ids"] = statement_ids
        if others:
            evidence["also_reported_by"] = others
            also = [
                s for s in dict.fromkeys(o["source_id"] for o in others)
                if s != winner.get("source_id")
            ]
            if also:
                merged["summary"] = (
                    f"{str(winner.get('summary') or '').rstrip()} "
                    f"The same record is also reported by {_source_list(also)}."
                )
        replaced[idxs[0]] = merged
        for i in idxs[1:]:
            replaced[i] = None

    out: list[dict[str, Any]] = []
    for i, sig in enumerate(signals):
        if i in replaced:
            if replaced[i] is not None:
                out.append(replaced[i])  # type: ignore[arg-type]
            continue
        out.append(sig)
    return out


# ---------------------------------------------------------------------------
# PEP by virtue of this role
# ---------------------------------------------------------------------------


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def record_positions(entity: dict[str, Any]) -> list[dict[str, Any]]:
    """Every position an OpenSanctions person record holds, as
    ``{"name", "organizations", "topics"}``.

    Reads the nested occupancies (``positionOccupancies[].post[]``), where the
    organisation id and the position's topics are, and the flat ``position``
    strings the search result carries; the flat ones are dropped when an
    occupancy already names the same position.
    """
    props = entity.get("properties") or {}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for occ in _as_list(props.get("positionOccupancies")):
        if not isinstance(occ, dict):
            continue
        for post in _as_list((occ.get("properties") or {}).get("post")):
            if not isinstance(post, dict):
                continue
            pp = post.get("properties") or {}
            name = str(_first(pp.get("name")) or post.get("caption") or "").strip()
            if not name:
                continue
            orgs = [
                str(o.get("id") if isinstance(o, dict) else o)
                for o in _as_list(pp.get("organization"))
                if o
            ]
            out.append(
                {
                    "name": name,
                    "organizations": orgs,
                    "topics": [str(t) for t in _as_list(pp.get("topics"))],
                }
            )
            seen.add(names.normalise_name(name))
    for raw in _as_list(props.get("position")):
        text = str(raw or "").strip()
        # "Chief of Defence of Norway (2013-2020)" — the flat form carries the
        # span; the occupancy's name does not.
        bare = re.sub(r"\s*\([^()]*\)\s*$", "", text)
        if not text or names.normalise_name(bare) in seen:
            continue
        seen.add(names.normalise_name(bare))
        out.append({"name": text, "organizations": [], "topics": []})
    return out


def _orgs_named_in(position_name: str) -> list[str]:
    """The organisation a position's name may carry, either side of a comma.

    OpenSanctions' datasets disagree on the order: Norway's ``no_brreg`` writes
    "Chairman, EQUINOR ASA" (role first), Denmark's ``dk_pep`` writes
    "Ørsted A/S, board of directors (vice chairman)" (organisation first). Both
    ends are offered, with a trailing parenthesis dropped; a name with no comma
    offers nothing, so "Chief of Defence of Norway" is never read as an
    organisation.
    """
    text = re.sub(r"\s*\([^()]*\)\s*$", "", position_name or "").strip()
    if "," not in text:
        return []
    head = text.split(",", 1)[0].strip()
    tail = text.rsplit(",", 1)[1].strip()
    return [o for o in dict.fromkeys((tail, head)) if o]


def position_at_subject(
    position: dict[str, Any], subject_names: Iterable[str], subject_record_ids: Iterable[str]
) -> bool:
    """Is this position held at the looked-up company itself?"""
    ids = set(subject_record_ids)
    if ids and any(o in ids for o in position.get("organizations") or ()):
        return True
    for org in _orgs_named_in(str(position.get("name") or "")):
        target = names.normalise_name(org)
        if not target:
            continue
        for name in subject_names:
            norm = names.normalise_name(name)
            if norm and (norm == target or names.name_similarity(norm, target) >= 0.95):
                return True
    return False


def _party_label(evidence: dict[str, Any]) -> str:
    return "Former related party" if evidence.get("former") else "Related party"


def _as_subject_role(signal: dict[str, Any], at_subject: list[str]) -> dict[str, Any]:
    evidence = dict(signal.get("evidence") or {})
    source = source_display_name(str(signal.get("source_id") or ""))
    roles = "; ".join(at_subject)
    also = evidence.get("also_reported_by") or []
    also_ids = [
        s for s in dict.fromkeys(str(o.get("source_id") or "") for o in also)
        if s and s != signal.get("source_id")
    ]
    tail = f" Also reported by {_source_list(also_ids)}." if also_ids else ""
    summary = (
        f"{_party_label(evidence)} '{evidence.get('search_name') or ''}' is "
        f"listed as politically exposed on {source} only by virtue of a role "
        f"at this company ({roles}). Context about who runs the company, not "
        f"a finding against it.{tail}"
    )
    return {
        **signal,
        "code": RELATED_PEP_SUBJECT_ROLE,
        "kind": "context",
        "summary": summary,
        "evidence": evidence,
    }


async def _read_record(upstream_id: str) -> dict[str, Any] | None:
    adapter = REGISTRY.get(_UPSTREAM)
    if adapter is None:
        return None
    try:
        bundle = await adapter.fetch(upstream_id)
    except Exception as exc:  # noqa: BLE001 — a refinement; never fail the lookup
        _LOG.info("PEP role check: could not read %s (%s)", upstream_id, type(exc).__name__)
        return None
    if not isinstance(bundle, dict) or bundle.get("is_stub"):
        return None
    entity = bundle.get("entity")
    return entity if isinstance(entity, dict) else None


async def label_subject_role_peps(
    signals: list[dict[str, Any]],
    *,
    subject_names: Iterable[str],
    subject_record_ids: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Mark each ``RELATED_PEP`` whose only positions are at the subject.

    Adds ``pep_role_checked``, ``pep_positions``, ``subject_role_positions`` and
    ``pep_by_subject_role`` to the evidence of every ``RELATED_PEP`` it could
    check, and turns the circular ones into ``RELATED_PEP_SUBJECT_ROLE``
    context signals. Signals it could not check keep their shape, with
    ``pep_role_checked: false``.
    """
    subject_names = [n for n in dict.fromkeys(subject_names) if n]
    subject_record_ids = [i for i in dict.fromkeys(subject_record_ids) if i]
    targets: dict[str, list[int]] = {}
    for i, sig in enumerate(signals):
        if sig.get("code") != RELATED_PEP:
            continue
        upstream = upstream_record_id(sig)
        if upstream:
            targets.setdefault(upstream, []).append(i)
    if not targets or not subject_names:
        return signals

    upstream_ids = list(targets)[:_MAX_ROLE_CHECKS]
    gate = asyncio.Semaphore(_ROLE_CHECK_CONCURRENCY)

    async def read(uid: str) -> dict[str, Any] | None:
        async with gate:
            return await _read_record(uid)

    records = await asyncio.gather(*(read(u) for u in upstream_ids))
    by_id = dict(zip(upstream_ids, records))

    out = list(signals)
    for upstream, idxs in targets.items():
        entity = by_id.get(upstream)
        for i in idxs:
            sig = out[i]
            evidence = dict(sig.get("evidence") or {})
            if entity is None:
                evidence["pep_role_checked"] = False
                out[i] = {**sig, "evidence": evidence}
                continue
            positions = record_positions(entity)
            at_subject = [
                p["name"]
                for p in positions
                if position_at_subject(p, subject_names, subject_record_ids)
            ]
            circular = bool(positions) and len(at_subject) == len(positions)
            evidence["pep_role_checked"] = True
            evidence["pep_positions"] = [p["name"] for p in positions]
            evidence["pep_by_subject_role"] = circular
            if at_subject:
                evidence["subject_role_positions"] = at_subject
            updated = {**sig, "evidence": evidence}
            if circular:
                out[i] = _as_subject_role(updated, at_subject)
            elif at_subject:
                updated["summary"] = (
                    f"{str(sig.get('summary') or '').rstrip()} The record also "
                    f"lists a role at this company ({'; '.join(at_subject)})."
                )
                out[i] = updated
            else:
                out[i] = updated
    return out


def subject_names_from(bods: list[dict[str, Any]], statement_ids: Iterable[str], *extra: str) -> list[str]:
    """Every name the subject's own statements carry, plus ``extra``."""
    wanted = set(statement_ids)
    found: list[str] = [e for e in extra if e]
    for stmt in bods:
        if stmt.get("statementId") not in wanted:
            continue
        rd = stmt.get("recordDetails") or {}
        if rd.get("name"):
            found.append(str(rd["name"]))
        for alt in rd.get("alternateNames") or ():
            if isinstance(alt, str) and alt:
                found.append(alt)
    return list(dict.fromkeys(found))


async def consolidate_pep_signals(
    signals: list[dict[str, Any]],
    *,
    subject_names: Iterable[str],
    subject_record_ids: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Merge, then label. The one entry point the pipeline calls."""
    merged = merge_derived_pep(signals)
    return await label_subject_role_peps(
        merged, subject_names=subject_names, subject_record_ids=subject_record_ids
    )


__all__ = [
    "consolidate_pep_signals",
    "label_subject_role_peps",
    "merge_derived_pep",
    "position_at_subject",
    "record_positions",
    "subject_names_from",
    "upstream_record_id",
]
