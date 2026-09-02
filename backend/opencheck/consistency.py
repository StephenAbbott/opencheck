"""Record consistency — do independent sources agree about the same entity?

Phase 152 (shadow mode). Phase C of the record-consistency plan (Notion:
*Semantic Discrepancy & Conflict Flagging*).

What this is, and what it is not
--------------------------------

Nothing here is a risk signal, and in this phase nothing here reaches the
results page at all. This module computes, for every entity that more than
one source described, whether the sources *agree* on a short list of facts
where agreement is expected — and ``consistencystats`` counts the outcomes in
production so the next phase can decide, from measured base rates rather than
guesswork, which comparisons are informative enough to show.

Three rules bound every comparison, and they are why the generic
"Source A ≠ Source B" chip the original idea sketched would have been noise:

1. **Same referent first.** Statements are grouped by shared *strong*
   identifier (an LEI, or a register number within one scheme — the same
   ``identKeys`` rule the FullCheck network merges on). A name match is not
   a referent; two same-named companies disagreeing on a founding date are
   two companies, not a conflict.
2. **Same concept only.** A comparison exists only where both sources assert
   the same thing. ``foundingDate`` from a register and from Wikidata fails
   this — Wikidata's *inception* is the business (Novo Nordisk 1923), the
   register's is the incorporation of the legal person (1931) — so Wikidata
   is excluded from that comparison by allowlist, and the test that pins it
   is named for Novo Nordisk. OpenCorporates ids are excluded from the
   identifier-clash comparison because one entity legitimately holds several
   (Shell plc: ``gb/04366849`` and ``nl/34179503``).
3. **Independent lineage.** A difference between a source and its own
   upstream (OpenCorporates behind Companies House) is *staleness of the
   copy*, never a fact conflict — recorded as ``stale``, counted, and by
   decision never shown. Agreement between them is ``mirror``, not
   corroboration. ``sources/lineage.py`` decides independence.

Relations
---------

For each (field, statement A, statement B) pair within a referent group:

``agree``        both stated a value and they match (independent sources)
``disagree``     both stated a value and they differ (independent sources)
``mirror``       both stated, match, but one republishes the other
``stale``        both stated, differ, but one republishes the other
``one_missing``  exactly one side stated a value — says nothing about the
                 entity, but the count shows how often a comparison *could*
                 run, which is the denominator the base-rate gate needs

Pairs where neither side stated a value, and pairs excluded by a
comparison's allowlist, produce no item at all.

Everything fails soft: this runs inside the lookup pipeline and must never
slow or break a lookup, so ``assess_consistency`` swallows its own errors and
returns an empty result.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from . import identifiers as _identifiers
from .bods import liveness as _liveness
from .bods.mapper import SOURCE_NAMES
from .reconcile import _entity_jurisdiction, _identifier_keys
from .sources import lineage

log = logging.getLogger("opencheck.consistency")

# ---------------------------------------------------------------------
# Relations and items
# ---------------------------------------------------------------------

AGREE = "agree"
DISAGREE = "disagree"
MIRROR = "mirror"
STALE = "stale"
ONE_MISSING = "one_missing"

RELATIONS: tuple[str, ...] = (AGREE, DISAGREE, MIRROR, STALE, ONE_MISSING)


@dataclass
class Item:
    """One comparison outcome between two statements about one referent."""

    field: str
    relation: str
    statement_ids: tuple[str, str]
    sources: tuple[str, str]  # adapter ids (or the description when unknown)
    values: tuple[Any, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "relation": self.relation,
            "statement_ids": list(self.statement_ids),
            "sources": list(self.sources),
            "values": list(self.values),
        }


@dataclass
class ConsistencyResult:
    items: list[Item] = field(default_factory=list)
    #: Referent groups that had ≥2 statements, as lists of statementIds.
    groups: list[list[str]] = field(default_factory=list)

    def by_relation(self, relation: str) -> list[Item]:
        return [i for i in self.items if i.relation == relation]

    def to_dict(self) -> dict[str, Any]:
        return {"items": [i.to_dict() for i in self.items], "groups": self.groups}


# ---------------------------------------------------------------------
# Source identity
# ---------------------------------------------------------------------

_ID_BY_DESCRIPTION: dict[str, str] = {v: k for k, v in SOURCE_NAMES.items()}


def source_id_of(stmt: dict[str, Any]) -> str:
    """Adapter id for a statement, from the mapper's ``source.description``.

    Falls back to the description itself for a label the mapper does not
    own (an Open Ownership bundle, say), which lineage treats as original.
    """
    desc = str(((stmt.get("source") or {}).get("description")) or "").strip()
    return _ID_BY_DESCRIPTION.get(desc, desc)


# ---------------------------------------------------------------------
# Identifier schemes that are one-per-entity
# ---------------------------------------------------------------------

#: Scheme segments marking a NON-register identifier (tax, securities,
#: classification). Mirror of ``NON_REGISTER_SEGMENTS`` in
#: ``frontend/src/lib/reconcile.ts`` — a test asserts the two sets match.
NON_REGISTER_SEGMENTS: frozenset[str] = frozenset(
    {
        "VAT", "UID", "TVA", "MOMS", "MWST", "KMKR", "DIC", "NIP", "OIB", "BN",
        "EIN", "FEIN", "UTR", "TAX", "CIK", "ISIN", "CUSIP", "NACE", "SIC", "TOL",
    }
)

#: Scheme labels whose values are per-*registration*, not per-entity — one
#: legal person may legitimately carry several, so a difference is not a
#: clash. OpenCorporates ids are the canonical case (Shell plc has gb/… and
#: nl/…). Aggregator-internal ids likewise.
_PER_RECORD_SCHEMES: frozenset[str] = frozenset(
    {"OPENCORPORATES", "OPENSANCTIONS", "OPENALEPH", "WIKIDATA", "QCC CODE", "S&P CIQ COMPANY ID", "ISO-9362"}
)


def _is_register_scheme(scheme: str) -> bool:
    up = scheme.strip().upper()
    if not up or up in _PER_RECORD_SCHEMES:
        return False
    # Every segment is checked (the frontend checks all but the jurisdiction
    # prefix): a bare "ISIN" or "CUSIP" scheme has no prefix to skip.
    return not any(seg in NON_REGISTER_SEGMENTS for seg in re.split(r"[-_]", up))


def one_per_entity_identifiers(stmt: dict[str, Any]) -> dict[str, str]:
    """``{scheme: value}`` for the identifiers where one entity has one value.

    The LEI (any scheme label, recognised by shape) under the key ``LEI``,
    and each register-like scheme under its own label. Values are
    canonicalised the same way the merge keys are (``_identifier_keys``), so
    ``556056-6258`` and ``5560566258`` compare equal.
    """
    from .matching import canonical_identifier

    out: dict[str, str] = {}
    rd = stmt.get("recordDetails") or {}
    jur = _entity_jurisdiction(rd)
    for ident in rd.get("identifiers") or []:
        raw = str(ident.get("id") or "").strip().upper()
        if not raw:
            continue
        if _identifiers.classify_lei(raw):
            out.setdefault("LEI", raw)
            continue
        scheme = str(ident.get("scheme") or "").strip().upper()
        if "/" in raw:
            continue
        value = canonical_identifier(raw, min_len=0) or raw
        if scheme and _is_register_scheme(scheme):
            out.setdefault(scheme, value)
        # Sources label the same national register differently (GLEIF: no
        # scheme; OpenCorporates: DK-COA; CVR: DK-CVR), so the register
        # number is also compared under a jurisdiction key — the same rule
        # the merge uses. Two statements bridged by LEI whose register
        # numbers differ is the clash worth finding.
        if jur and (scheme == "" or (scheme.startswith(f"{jur}-") and _is_register_scheme(scheme))):
            out.setdefault(f"REGISTER:{jur}", value)
    return out


# ---------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------

Extractor = Callable[[dict[str, Any]], Any]
Comparator = Callable[[Any, Any], bool]


@dataclass(frozen=True)
class Comparison:
    """One aligned field.

    ``extract`` returns the value a statement asserts, or ``None`` when it
    asserts nothing. ``same`` decides agreement. ``exclude_sources`` names
    adapter ids whose value for this field is a *different concept* and must
    never be compared (rule 2) — an allowlist by exclusion, because the
    default for a new register is "same concept" and the exceptions are the
    ones worth writing down.
    """

    field: str
    extract: Extractor
    same: Comparator
    exclude_sources: frozenset[str] = frozenset()


def _extract_liveness(stmt: dict[str, Any]) -> str | None:
    status = _liveness.read_register_status(stmt)
    return status["liveness"] if status else None


def _extract_jurisdiction(stmt: dict[str, Any]) -> str | None:
    return _entity_jurisdiction(stmt.get("recordDetails") or {}) or None


def _extract_founding(stmt: dict[str, Any]) -> str | None:
    raw = str((stmt.get("recordDetails") or {}).get("foundingDate") or "").strip()
    return raw or None


def _dates_same(a: str, b: str) -> bool:
    """Compare at the coarser precision of the two (``2002`` vs ``2002-02-05``
    agree; ``2002-02`` vs ``2002-03`` do not)."""
    n = min(len(a), len(b), 10)
    return a[:n] == b[:n]


def _eq(a: Any, b: Any) -> bool:
    return a == b


COMPARABLE: tuple[Comparison, ...] = (
    # Liveness: the register's class, via the Phase 151 grammar. A
    # dissolved company with an ACTIVE LEI is the case this whole effort
    # was started for.
    Comparison("liveness", _extract_liveness, _eq),
    # Jurisdiction: almost never differs; when it does, the referent merge
    # was wrong, which is worth knowing.
    Comparison("jurisdiction", _extract_jurisdiction, _eq),
    # Founding date: registers and GLEIF record incorporation of the legal
    # person. Wikidata's P571 "inception" is the founding of the BUSINESS —
    # Novo Nordisk 1923 vs 1931, Shell 1890 vs 2002 — and is never compared.
    Comparison(
        "founding_date",
        _extract_founding,
        _dates_same,
        exclude_sources=frozenset({"wikidata"}),
    ),
)

#: Field name for the identifier-clash comparison (handled separately
#: because it is per scheme rather than per statement value).
IDENTIFIER_CLASH = "identifier_clash"


# ---------------------------------------------------------------------
# Referent groups
# ---------------------------------------------------------------------


def referent_groups(bods: Iterable[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group entity statements that share a strong identifier key.

    Union-find over ``_identifier_keys`` — the same rule the FullCheck
    network merges on, so "same referent" here means what it means there.
    Only groups with two or more statements are returned.
    """
    ents = [
        s for s in bods
        if s.get("recordType") == "entity" and s.get("statementId")
    ]
    parent: dict[str, str] = {s["statementId"]: s["statementId"] for s in ents}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    owner_of_key: dict[str, str] = {}
    for s in ents:
        sid = s["statementId"]
        for key in _identifier_keys(s):
            if key in owner_of_key:
                ra, rb = find(owner_of_key[key]), find(sid)
                if ra != rb:
                    parent[rb] = ra
            else:
                owner_of_key[key] = sid
    groups: dict[str, list[dict[str, Any]]] = {}
    for s in ents:
        groups.setdefault(find(s["statementId"]), []).append(s)
    return [g for g in groups.values() if len(g) >= 2]


# ---------------------------------------------------------------------
# Assessment
# ---------------------------------------------------------------------


def _relation(independent: bool, same: bool) -> str:
    if independent:
        return AGREE if same else DISAGREE
    return MIRROR if same else STALE


def _compare_pair(a: dict[str, Any], b: dict[str, Any]) -> list[Item]:
    sa, sb = source_id_of(a), source_id_of(b)
    if sa == sb:
        # Two statements from one source about one referent (OpenAleph's
        # several PSC-derived records, say) are that source repeating
        # itself; nothing to learn about the entity from it.
        return []
    independent = lineage.independent(sa, sb)
    ids = (a["statementId"], b["statementId"])
    srcs = (sa, sb)
    items: list[Item] = []

    for cmp in COMPARABLE:
        if sa in cmp.exclude_sources or sb in cmp.exclude_sources:
            continue
        va, vb = cmp.extract(a), cmp.extract(b)
        if va is None and vb is None:
            continue
        if va is None or vb is None:
            items.append(Item(cmp.field, ONE_MISSING, ids, srcs, (va, vb)))
            continue
        items.append(Item(cmp.field, _relation(independent, cmp.same(va, vb)), ids, srcs, (va, vb)))

    ida, idb = one_per_entity_identifiers(a), one_per_entity_identifiers(b)
    for scheme in sorted(set(ida) | set(idb)):
        va, vb = ida.get(scheme), idb.get(scheme)
        if va is None or vb is None:
            # One side not carrying a scheme is the normal case (each source
            # carries its own register's number) and is not even worth a
            # one_missing row — it would swamp the counters with nothing.
            continue
        items.append(
            Item(
                IDENTIFIER_CLASH,
                _relation(independent, va == vb),
                ids,
                srcs,
                (f"{scheme}:{va}", f"{scheme}:{vb}"),
            )
        )
    return items


def assess_consistency(bods: Iterable[dict[str, Any]]) -> ConsistencyResult:
    """Compare every pair of statements within every referent group.

    Pure and deterministic over the bundle. Fails soft: any exception is
    logged and an empty result returned, because this runs inside the lookup
    pipeline and instrumentation must never break a lookup.
    """
    result = ConsistencyResult()
    try:
        for group in referent_groups(list(bods)):
            ordered = sorted(group, key=lambda s: (source_id_of(s), s["statementId"]))
            result.groups.append([s["statementId"] for s in ordered])
            for i in range(len(ordered)):
                for j in range(i + 1, len(ordered)):
                    result.items.extend(_compare_pair(ordered[i], ordered[j]))
    except Exception as exc:  # noqa: BLE001
        log.warning("assess_consistency failed, returning empty: %s", exc)
        return ConsistencyResult()
    return result
