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

Phase 268 (after the 17 Sept and 1 Oct 2026 readings of ``/consistencystats``)
tightened four things the first production counts showed: an identifier with
no ``scheme`` key is not a register number; the identifier that *bridged*
two statements into one group is not counted as agreement; two registers in
one jurisdiction (ABN/ACN, CUI/J-number) are never compared with each other;
``pending`` liveness is not a side of the liveness comparison; GLEIF and
ANAF are excluded from ``founding_date`` and MEIP from ``jurisdiction`` as
different concepts, with the measured reasons on each ``Comparison``. Every
independent ``disagree`` is logged with its values, so the next reading can
be explained rather than hypothesised.

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
from .bods.annotations import dropped_partial_date
from .bods.mapper import SOURCE_NAMES
from .ra_codes import is_ra_scheme
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
    """Adapter id for a statement: ``source.opencheckSourceId`` (Phase 267),
    else the mapper's ``source.description`` mapped back to its id.

    Falls back to the description itself for a label the mapper does not
    own (an Open Ownership bundle, say), which lineage treats as original.
    """
    src = stmt.get("source") or {}
    stamped = src.get("opencheckSourceId")
    if isinstance(stamped, str) and stamped in SOURCE_NAMES:
        return stamped
    desc = str(src.get("description") or "").strip()
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


def _register_key(jur: str) -> str:
    return f"REGISTER:{jur}"


def _split_identifiers(stmt: dict[str, Any]) -> tuple[dict[str, str], dict[str, set[str]]]:
    """``(labelled, by_jurisdiction)`` for a statement's one-per-entity ids.

    ``labelled`` is ``{scheme: value}``: the LEI (any scheme label, recognised
    by shape) under ``LEI``, each register-like scheme under its own label,
    and — only for a number whose issuing register is *not* named by an
    org-id scheme (GLEIF's ``registeredAs`` with a present-and-empty
    ``scheme``, or a bare RA code since Phase 239) — the jurisdiction key
    ``REGISTER:<jur>``. ``by_jurisdiction`` is every *labelled* register
    number per jurisdiction, which is what an unlabelled number is compared
    against.

    Two shapes are deliberately not register numbers (Phase 268): an
    identifier with **no** ``scheme`` key at all (``schemeName`` only — a
    PermID, a DUNS, an S&P Capital IQ id on a passthrough statement), and an
    OpenCorporates-style ``jur/number`` value. Before Phase 268 the absent
    key read as the empty scheme and the first such id became the entity's
    "register number", which is where every MEIP identifier clash came from.

    Values are canonicalised the same way the merge keys are
    (``_identifier_keys``), so ``556056-6258`` and ``5560566258`` compare
    equal.
    """
    from .matching import canonical_identifier

    labelled: dict[str, str] = {}
    by_jur: dict[str, set[str]] = {}
    rd = stmt.get("recordDetails") or {}
    jur = _entity_jurisdiction(rd)
    for ident in rd.get("identifiers") or []:
        if not isinstance(ident, dict):
            continue
        raw = str(ident.get("id") or "").strip().upper()
        if not raw:
            continue
        if _identifiers.classify_lei(raw):
            labelled.setdefault("LEI", raw)
            continue
        if "scheme" not in ident:
            # schemeName only: a commercial or aggregator id in prose. Not a
            # register number, whatever the jurisdiction.
            continue
        scheme = str(ident.get("scheme") or "").strip().upper()
        if "/" in raw:
            continue
        value = canonical_identifier(raw, min_len=0) or raw
        if jur and (scheme == "" or is_ra_scheme(scheme)):
            # GLEIF's number with its register unnamed (empty scheme before
            # Phase 239) or named only by RA code: compared under the
            # jurisdiction against whichever labelled register carries it.
            labelled.setdefault(_register_key(jur), value)
        elif scheme and _is_register_scheme(scheme):
            labelled.setdefault(scheme, value)
            # A labelled national number is what an unlabelled copy of the
            # same number (GLEIF before Phase 239, or an RA code with no
            # org-id entry) is compared against. Two registers in one
            # jurisdiction (ABN and ACN; CUI and the ONRC J-number) each keep
            # their own label and are never compared with each other.
            if jur and scheme.startswith(f"{jur}-"):
                by_jur.setdefault(jur, set()).add(value)
    return labelled, by_jur


def one_per_entity_identifiers(stmt: dict[str, Any]) -> dict[str, str]:
    """``{scheme: value}`` for the identifiers where one entity has one value
    — see ``_split_identifiers``; this is its ``labelled`` half."""
    return _split_identifiers(stmt)[0]


def _identifier_pairs(a: dict[str, Any], b: dict[str, Any]) -> list[tuple[str, str, str, bool]]:
    """``(scheme, value_a, value_b, same)`` for every one-per-entity scheme
    both statements carry, plus an unlabelled register number on one side
    against the labelled numbers of the same jurisdiction on the other.

    A number carried under both its own label and the jurisdiction key is
    one identifier, so the same ``(value_a, value_b)`` is reported once.
    """
    la, ja = _split_identifiers(a)
    lb, jb = _split_identifiers(b)
    out: list[tuple[str, str, str, bool]] = []
    seen: set[tuple[str, str]] = set()

    def add(scheme: str, va: str, vb: str, same: bool) -> None:
        if (va, vb) in seen:
            return
        seen.add((va, vb))
        out.append((scheme, va, vb, same))

    shared = sorted(set(la) & set(lb), key=lambda s: (s != "LEI", s))
    for scheme in shared:
        add(scheme, la[scheme], lb[scheme], la[scheme] == lb[scheme])
    # Unlabelled on one side, labelled on the other: the unlabelled number
    # agrees if any labelled register of that jurisdiction carries it.
    for jur, values in jb.items():
        key = _register_key(jur)
        if key in la and key not in lb:
            va = la[key]
            match = va if va in values else sorted(values)[0]
            add(key, va, match, va in values)
    for jur, values in ja.items():
        key = _register_key(jur)
        if key in lb and key not in la:
            vb = lb[key]
            match = vb if vb in values else sorted(values)[0]
            add(key, match, vb, vb in values)
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
    """``live`` or ``terminal``, or ``None`` when the statement did not decide.

    ``pending`` (a liquidation under way) is not a side of this comparison
    (Phase 268): a company in liquidation is ACTIVE in GLEIF until it is
    dissolved, so pending-vs-live is the two registers describing one
    situation at different stages, not a disagreement. The Phase C plan said
    terminal-vs-live only; the shipped comparator compared all four classes.
    """
    status = _liveness.read_register_status(stmt)
    if not status:
        return None
    cls = status["liveness"]
    return cls if cls in (_liveness.LIVE, _liveness.TERMINAL) else None


def _extract_jurisdiction(stmt: dict[str, Any]) -> str | None:
    return _entity_jurisdiction(stmt.get("recordDetails") or {}) or None


def _extract_founding(stmt: dict[str, Any]) -> str | None:
    raw = str((stmt.get("recordDetails") or {}).get("foundingDate") or "").strip()
    # Phase 255: a year-only date is not a BODS foundingDate, so it travels in
    # a transformation annotation; compared at its own precision as before.
    return raw or dropped_partial_date(stmt, "foundingDate")


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
    # was wrong, which is worth knowing. MEIP is excluded (Phase 268): the
    # OECD's country column is the economy its group register files the
    # entity under, and on 1 Oct 2026 it differed from GLEIF's legal
    # jurisdiction on 7 of 58 pairs while every other pair ran at 1,327/10.
    Comparison(
        "jurisdiction",
        _extract_jurisdiction,
        _eq,
        exclude_sources=frozenset({"meip"}),
    ),
    # Founding date: registers record incorporation of the legal person.
    # Wikidata's P571 "inception" is the founding of the BUSINESS — Novo
    # Nordisk 1923 vs 1931, Shell 1890 vs 2002 — and is never compared.
    # GLEIF's ``entity.creationDate`` is registrant-supplied and, measured on
    # 1 Oct 2026 across seven registers, differed from the register's date
    # on 13–100 % of pairs (OpenCorporates 97 of 173, ARES 57 of 65, KRS 12
    # of 12), so it is a different concept outside the UK and is excluded
    # too. ANAF's date is the fiscal registration, not the ONRC
    # incorporation (14 of 23 differ), so register-vs-register for Romania
    # keeps ONRC only.
    Comparison(
        "founding_date",
        _extract_founding,
        _dates_same,
        exclude_sources=frozenset({"wikidata", "gleif", "anaf_romania"}),
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

    # One side not carrying a scheme is the normal case (each source carries
    # its own register's number) and is not even worth a one_missing row —
    # it would swamp the counters with nothing. Only schemes both carry.
    pairs = _identifier_pairs(a, b)
    # The bridge is not agreement (Phase 268). Two statements are in one
    # referent group *because* they share an identifier, so the first
    # identifier they agree on is the one that put them there and says
    # nothing about the entity. It is dropped; an ``agree`` that survives
    # means a SECOND identifier matched, which is the corroboration worth
    # counting, and a ``disagree`` is a clash on a one-per-entity scheme
    # between records the bridge said were the same thing.
    bridge_dropped = False
    for scheme, va, vb, same in pairs:
        if same and not bridge_dropped:
            bridge_dropped = True
            continue
        items.append(
            Item(
                IDENTIFIER_CLASH,
                _relation(independent, same),
                ids,
                srcs,
                (f"{scheme}:{va}", f"{scheme}:{vb}"),
            )
        )
    return items


def _log_disagreements(result: ConsistencyResult) -> None:
    """One INFO line per independent ``disagree`` (Phase 268).

    The counters never carry a value, by design — but that means a row above
    the 10 % gate cannot be diagnosed from the endpoint at all. The server log
    is private, so the two values go there: ``grep "consistency disagree
    field=founding_date"`` is how a semantic mismatch gets explained rather
    than hypothesised. ``stale`` is not logged (the copy lagging its upstream
    is known and uninteresting).
    """
    for item in result.items:
        if item.relation != DISAGREE:
            continue
        log.info(
            "consistency disagree field=%s sources=%s/%s statements=%s/%s values=%r/%r",
            item.field,
            item.sources[0],
            item.sources[1],
            item.statement_ids[0],
            item.statement_ids[1],
            item.values[0],
            item.values[1],
        )


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
    try:
        _log_disagreements(result)
    except Exception:  # noqa: BLE001
        pass
    return result
