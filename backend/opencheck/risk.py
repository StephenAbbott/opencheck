"""Deterministic risk-signal rules.

OpenCheck never invents risk — it surfaces what the open data already
asserts. Every rule is keyed off either a raw source payload (topics,
collections, positions) or BODS v0.4 statements assembled by the
mapper, with the explicit goal of mirroring the AMLA CDD RTS (currently
under EU consultation).

Source-derived signals
======================

* ``PEP`` — politically exposed person.
  Fires when:
    - OpenSanctions hit has a ``role.pep`` family topic
    - EveryPolitician hit (the dataset is, by construction, PEPs only)
    - Wikidata person bundle (``/deepen``) has at least one position
      with no end date — i.e. a *currently held* office

* ``SANCTIONED`` — currently or historically sanctioned.
  Fires when an OpenSanctions hit/bundle has a ``sanction`` topic.

* ``OFFSHORE_LEAKS`` — appears in an ICIJ-style leak.
  Fires for OpenAleph hits whose collection is one of the known leak
  collections (Panama / Paradise / Pandora / Bahamas / Offshore Leaks).

* ``OPAQUE_OWNERSHIP`` — ownership chain leads to an unknown person or
  anonymous entity. Fires when a BODS bundle contains a
  ``personStatement`` with ``personType == "unknownPerson"`` or an
  ``entityStatement`` whose ``entityType == "anonymousEntity"``.

AMLA CDD RTS signals (BODS v0.4 derived)
========================================

Mirror of the objective conditions in AMLA's draft CDD RTS for
"complex corporate structures". Each fires independently so a UI can
show them as discrete chips, and a composite ``COMPLEX_CORPORATE_STRUCTURE``
fires when the AMLA "≥3 layers + ≥1 of {trust, non-EU, nominee}"
threshold is met.

* ``TRUST_OR_ARRANGEMENT`` — any ``entityStatement`` whose entityType is
  ``arrangement``, or whose ``legalForm``/``entitySubtype``/``details``
  mentions ``trust``, ``foundation``, ``stiftung`` or ``anstalt``.
  Maps to AMLA condition (a).
* ``NON_EU_JURISDICTION`` — any ``entityStatement.incorporatedInJurisdiction.code``
  outside the EU+EEA set. Maps to AMLA condition (b).
* ``NOMINEE`` — any ``relationshipStatement`` with an interest type or
  ``details`` field mentioning ``nominee``, or a ``personStatement``
  whose names/details mention nominee. Maps to AMLA condition (c).
* ``COMPLEX_OWNERSHIP_LAYERS`` — the longest chain of entity nodes in
  the BODS relationship graph has ≥3 corporate layers.
* ``COMPLEX_CORPORATE_STRUCTURE`` — composite, fires when
  ``COMPLEX_OWNERSHIP_LAYERS`` and ≥1 of
  {``TRUST_OR_ARRANGEMENT``, ``NON_EU_JURISDICTION``, ``NOMINEE``} have
  fired.
* ``POSSIBLE_OBFUSCATION`` — advisory mirror of AMLA's subjective
  condition ("structure obfuscates or diminishes transparency of
  ownership with no legitimate economic rationale"). Cannot be judged
  from data alone — fires ``low`` when ``OPAQUE_OWNERSHIP`` plus
  non-EU layer or nominee are present, with the summary explicitly
  noting that a human should confirm legitimate rationale.

Each signal is intentionally explained — confidence + a one-line
``summary`` + the ``evidence`` dict — because users want to be told
*why* something is flagged, not just see a red dot.

Confidence ladder
-----------------

* ``high`` — the source asserts it directly (e.g. ``topic == sanction``,
  ``entityType == arrangement``).
* ``medium`` — strong proxy (e.g. ICIJ leak collection, BODS chain
  meeting the layer threshold).
* ``low`` — advisory inference, requires human review (e.g.
  ``POSSIBLE_OBFUSCATION``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .config import get_settings
from .sources import SearchKind, SourceHit


# Codes — source-derived
PEP = "PEP"
SANCTIONED = "SANCTIONED"
OFFSHORE_LEAKS = "OFFSHORE_LEAKS"
OPAQUE_OWNERSHIP = "OPAQUE_OWNERSHIP"

# Codes — AMLA CDD RTS (BODS-derived)
TRUST_OR_ARRANGEMENT = "TRUST_OR_ARRANGEMENT"
NON_EU_JURISDICTION = "NON_EU_JURISDICTION"
NOMINEE = "NOMINEE"
COMPLEX_OWNERSHIP_LAYERS = "COMPLEX_OWNERSHIP_LAYERS"
COMPLEX_CORPORATE_STRUCTURE = "COMPLEX_CORPORATE_STRUCTURE"
POSSIBLE_OBFUSCATION = "POSSIBLE_OBFUSCATION"

# Codes — FATF jurisdiction lists (BODS-derived)
FATF_BLACK_LIST = "FATF_BLACK_LIST"
FATF_GREY_LIST = "FATF_GREY_LIST"


# Default EU + EEA member states (ISO 3166-1 alpha-2). The AMLA RTS
# scopes "outside the European Union" — we extend with EEA (NO/IS/LI)
# because they share AML supervisory frameworks under the EU's
# third-country regime, which most practitioners include here. Keep this
# list visible rather than buried so reviewers can audit it.
#
# Operators can adjust this at runtime via two env vars:
#
# * ``OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS`` — comma-separated codes
#   ADDED to the default set (e.g. ``GB,CH`` for UK + Swiss equivalence).
# * ``OPENCHECK_AMLA_EU_EEA_OVERRIDE`` — when set, REPLACES the default
#   set entirely. Use only when you want strict AMLA EU-only or a fully
#   custom basis.
DEFAULT_EU_EEA_COUNTRY_CODES: frozenset[str] = frozenset(
    {
        # EU-27
        "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
        "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
        "PL", "PT", "RO", "SK", "SI", "ES", "SE",
        # EEA non-EU
        "IS", "LI", "NO",
    }
)

# Back-compat alias — older code (and external callers) imported the
# original constant. Keep the name pointing at the defaults; the rule
# itself now resolves at call-time via ``_eu_eea_codes()``.
EU_EEA_COUNTRY_CODES = DEFAULT_EU_EEA_COUNTRY_CODES


def _split_codes(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {part.strip().upper() for part in raw.split(",") if part.strip()}


def _eu_eea_codes() -> frozenset[str]:
    """Resolve the active EU+EEA-equivalent jurisdiction set.

    Reads settings every call (settings is itself ``lru_cache``'d, so
    this is cheap and stays in sync if the cache is cleared in tests).
    """
    settings = get_settings()
    if settings.amla_eu_eea_override is not None:
        return frozenset(_split_codes(settings.amla_eu_eea_override))
    extras = _split_codes(settings.amla_equivalent_jurisdictions)
    if not extras:
        return DEFAULT_EU_EEA_COUNTRY_CODES
    return frozenset(DEFAULT_EU_EEA_COUNTRY_CODES | extras)

# FATF High-Risk Jurisdictions subject to a Call for Action ("black list")
# as of February 2026 — Democratic People's Republic of Korea, Iran, Myanmar.
# Source: https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html
FATF_BLACK_LIST_CODES: frozenset[str] = frozenset({"KP", "IR", "MM"})

# FATF Jurisdictions under Increased Monitoring ("grey list") as of
# February 2026.  Note that Bulgaria (BG) is an EU member-state — if
# NON_EU_JURISDICTION is suppressed via OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS,
# FATF_GREY_LIST will still fire for it.
# Source: https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html
FATF_GREY_LIST_CODES: frozenset[str] = frozenset(
    {
        "DZ",  # Algeria
        "AO",  # Angola
        "BO",  # Bolivia
        "BG",  # Bulgaria
        "CM",  # Cameroon
        "CI",  # Côte d'Ivoire
        "CD",  # Democratic Republic of Congo
        "HT",  # Haiti
        "KE",  # Kenya
        "KW",  # Kuwait
        "LA",  # Laos (Lao PDR)
        "LB",  # Lebanon
        "MC",  # Monaco
        "NA",  # Namibia
        "NP",  # Nepal
        "PG",  # Papua New Guinea
        "SS",  # South Sudan
        "SY",  # Syria
        "VE",  # Venezuela
        "VN",  # Vietnam
        "VG",  # British Virgin Islands
        "YE",  # Yemen
    }
)

# Free-text fragments that signal a trust / non-corporate arrangement
# in legal-form / details fields. Lower-cased.
_TRUST_LEGAL_FORM_FRAGMENTS = (
    "trust",
    "foundation",
    "stiftung",
    "anstalt",
    "fideicomiso",  # ES/LATAM trust
    "treuhand",     # German trust-equivalent
)

# "Nominee" terms across English / common European legal-vocabulary.
_NOMINEE_FRAGMENTS = (
    "nominee",
    "nomineeshareholder",
    "nominee shareholder",
    "nomineedirector",
    "nominee director",
    "prête-nom",
    "prete-nom",
    "fiduciaire",
)


# OpenSanctions topic taxonomy. Anything in the "role.pep" family — pep,
# rca (relative or close associate), spouse, family — is treated as a
# PEP signal. Sanction-flavoured topics all start with "sanction".
_PEP_TOPICS = {"role.pep", "role.rca", "role.spouse", "role.family"}
_SANCTION_TOPIC_PREFIX = "sanction"

# Known ICIJ leak collections on OpenAleph. Match on either the
# collection foreign_id (preferred) or a fragment of the label.
_LEAK_FOREIGN_ID_PREFIXES = (
    "icij",
    "panama_papers",
    "paradise_papers",
    "pandora_papers",
    "bahamas_leaks",
    "offshore_leaks",
)
_LEAK_LABEL_FRAGMENTS = (
    "icij",
    "panama papers",
    "paradise papers",
    "pandora papers",
    "bahamas leaks",
    "offshore leaks",
)


@dataclass
class RiskSignal:
    """One risk assertion about a hit, with explanation.

    ``evidence`` carries the raw bits the rule keyed off (topic name,
    collection foreign_id, position label) so the UI can show a tooltip
    without re-running the rule.
    """

    code: str
    confidence: str
    summary: str
    source_id: str
    hit_id: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "confidence": self.confidence,
            "summary": self.summary,
            "source_id": self.source_id,
            "hit_id": self.hit_id,
            "evidence": self.evidence,
        }


# ----------------------------------------------------------------------
# Rules over SourceHit (search-time data)
# ----------------------------------------------------------------------


def assess_hit(hit: SourceHit) -> list[RiskSignal]:
    """Risk signals derivable from a single search-time hit.

    Stub hits never produce signals — the raw payload is fictional.
    """
    if hit.is_stub:
        return []

    signals: list[RiskSignal] = []

    if hit.source_id == "opensanctions":
        signals.extend(_opensanctions_topic_signals(hit, hit.raw))
    elif hit.source_id == "everypolitician" and hit.kind == SearchKind.PERSON:
        # The EveryPolitician dataset (now sourced from OpenSanctions
        # peps) is, by construction, persons-with-political-positions.
        signals.append(
            RiskSignal(
                code=PEP,
                confidence="high",
                summary="Listed in the EveryPolitician / OpenSanctions PEPs dataset.",
                source_id=hit.source_id,
                hit_id=hit.hit_id,
                evidence={"dataset": "peps"},
            )
        )

    return signals


def assess_hits(hits: Iterable[SourceHit]) -> list[RiskSignal]:
    """Risk signals across an entire fan-out result.

    Deduplicates by (code, source_id, hit_id) — a hit that is both PEP
    and sanctioned still produces both signals, but the same PEP signal
    isn't emitted twice for one hit.
    """
    seen: set[tuple[str, str, str]] = set()
    out: list[RiskSignal] = []
    for hit in hits:
        for signal in assess_hit(hit):
            key = (signal.code, signal.source_id, signal.hit_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(signal)
    return out


# ----------------------------------------------------------------------
# Rules over deepen bundles (post-fetch data)
# ----------------------------------------------------------------------


def assess_bundle(
    source_id: str, raw: dict[str, Any], bods: list[dict[str, Any]] | None = None
) -> list[RiskSignal]:
    """Risk signals derivable from a ``/deepen`` payload.

    Fed both the raw source-shaped bundle and the BODS statements so it
    can reason over either layer. ``bods`` may be empty/None for sources
    we haven't mapped yet — rules that need BODS will simply not fire.
    """
    signals: list[RiskSignal] = []

    if raw.get("is_stub"):
        return signals

    if source_id == "opensanctions":
        entity = raw.get("entity") or {}
        hit_id = raw.get("entity_id") or entity.get("id") or ""
        signals.extend(_opensanctions_topic_signals_from_entity(hit_id, entity))

    elif source_id == "everypolitician":
        entity = raw.get("entity") or {}
        hit_id = raw.get("entity_id") or entity.get("id") or ""
        if hit_id:
            signals.append(
                RiskSignal(
                    code=PEP,
                    confidence="high",
                    summary="Listed in the EveryPolitician / OpenSanctions PEPs dataset.",
                    source_id=source_id,
                    hit_id=hit_id,
                    evidence={"dataset": "peps"},
                )
            )
            # Some PEP records also carry sanction topics — surface both.
            signals.extend(
                _opensanctions_topic_signals_from_entity(
                    hit_id, entity, source_id=source_id
                )
            )

    elif source_id == "openaleph":
        signals.extend(_openaleph_leak_signals(raw))

    elif source_id == "wikidata":
        signals.extend(_wikidata_position_signals(raw))

    if bods:
        signals.extend(_opaque_ownership_signals(source_id, raw, bods))
        signals.extend(assess_amla(source_id, raw, bods))

    # Subjective AMLA "obfuscation" signal looks at the assembled
    # signal set (after every other rule has fired) — last to run.
    obfuscation = _possible_obfuscation_signal(
        source_id, raw.get("entity_id") or raw.get("hit_id") or "", signals
    )
    if obfuscation is not None:
        signals.append(obfuscation)

    return signals


# ----------------------------------------------------------------------
# Per-source rule helpers
# ----------------------------------------------------------------------


def _opensanctions_topic_signals(
    hit: SourceHit, raw: dict[str, Any]
) -> list[RiskSignal]:
    """OpenSanctions search-card topics → PEP / SANCTIONED."""
    return _opensanctions_topic_signals_from_entity(hit.hit_id, raw, source_id=hit.source_id)


def _opensanctions_topic_signals_from_entity(
    hit_id: str, entity: dict[str, Any], *, source_id: str = "opensanctions"
) -> list[RiskSignal]:
    topics = _extract_topics(entity)
    out: list[RiskSignal] = []
    if any(t in _PEP_TOPICS for t in topics):
        matched = sorted(t for t in topics if t in _PEP_TOPICS)
        out.append(
            RiskSignal(
                code=PEP,
                confidence="high",
                summary=f"OpenSanctions tags this record as {', '.join(matched)}.",
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": matched},
            )
        )
    sanction_topics = sorted(
        t for t in topics if t.startswith(_SANCTION_TOPIC_PREFIX)
    )
    if sanction_topics:
        out.append(
            RiskSignal(
                code=SANCTIONED,
                confidence="high",
                summary=(
                    "OpenSanctions tags this record as sanctioned"
                    f" ({', '.join(sanction_topics)})."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": sanction_topics},
            )
        )
    return out


def _extract_topics(payload: dict[str, Any]) -> list[str]:
    """Topics may live at the top level or under ``properties``."""
    topics = payload.get("topics")
    if not topics:
        topics = (payload.get("properties") or {}).get("topics") or []
    if isinstance(topics, str):
        topics = [topics]
    return [t for t in topics if isinstance(t, str)]


def _openaleph_leak_signals(raw: dict[str, Any]) -> list[RiskSignal]:
    """OpenAleph bundle → OFFSHORE_LEAKS when the collection is a leak."""
    collection = raw.get("collection") or {}
    inline = (raw.get("entity") or {}).get("collection") or {}
    # Some hosts return the collection block inline on the entity.
    foreign_id = (
        collection.get("foreign_id")
        or inline.get("foreign_id")
        or ""
    ).lower()
    label = (collection.get("label") or inline.get("label") or "").lower()

    matched_via: dict[str, str] | None = None
    if any(foreign_id.startswith(prefix) for prefix in _LEAK_FOREIGN_ID_PREFIXES):
        matched_via = {"foreign_id": foreign_id}
    elif any(frag in label for frag in _LEAK_LABEL_FRAGMENTS):
        matched_via = {"label": label}

    if not matched_via:
        return []

    hit_id = raw.get("entity_id") or (raw.get("entity") or {}).get("id") or ""
    return [
        RiskSignal(
            code=OFFSHORE_LEAKS,
            confidence="medium",
            summary=(
                "Mentioned in the "
                f"{collection.get('label') or inline.get('label') or foreign_id} "
                "leak collection on OpenAleph."
            ),
            source_id="openaleph",
            hit_id=hit_id,
            evidence={
                "collection": collection.get("label")
                or inline.get("label")
                or foreign_id,
                "match": matched_via,
            },
        )
    ]


def _wikidata_position_signals(raw: dict[str, Any]) -> list[RiskSignal]:
    """Wikidata person with a current position → PEP."""
    if not raw.get("is_person"):
        return []
    positions = raw.get("positions") or []
    current = [p for p in positions if not p.get("end")]
    if not current:
        return []
    labels = [p.get("label") for p in current if p.get("label")]
    qid = raw.get("qid") or ""
    return [
        RiskSignal(
            code=PEP,
            confidence="medium",
            summary=(
                "Wikidata records a currently-held political or public"
                f" position ({', '.join(labels) or 'unspecified'})."
            ),
            source_id="wikidata",
            hit_id=qid,
            evidence={"positions": labels},
        )
    ]


def _opaque_ownership_signals(
    source_id: str, raw: dict[str, Any], bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """BODS bundle with unknown persons or anonymous entities."""
    hit_id = raw.get("entity_id") or raw.get("hit_id") or ""
    findings: list[str] = []
    for stmt in bods:
        if _stmt_kind(stmt) == "person" and _person_type(stmt) == "unknownPerson":
            findings.append("unknown person in ownership chain")
        elif _stmt_kind(stmt) == "entity" and _entity_type(stmt) == "anonymousEntity":
            findings.append("anonymous entity in ownership chain")
    if not findings:
        return []
    # Dedupe but keep order.
    deduped: list[str] = []
    for f in findings:
        if f not in deduped:
            deduped.append(f)
    return [
        RiskSignal(
            code=OPAQUE_OWNERSHIP,
            confidence="medium",
            summary="Ownership chain contains: " + "; ".join(deduped) + ".",
            source_id=source_id,
            hit_id=hit_id,
            evidence={"findings": deduped},
        )
    ]


# ----------------------------------------------------------------------
# BODS shape readers (tolerate v0.4 nested + flat fixtures)
# ----------------------------------------------------------------------


def _stmt_kind(stmt: dict[str, Any]) -> str:
    """Return ``"entity"``, ``"person"``, ``"relationship"`` or ``""``.

    v0.4 puts the kind under ``recordType``. Older flat fixtures may
    use ``statementType: "entityStatement"`` etc.
    """
    rt = stmt.get("recordType")
    if rt:
        return rt
    st = stmt.get("statementType", "")
    return st.replace("Statement", "") if st else ""


def _record_details(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails")
    return rd if isinstance(rd, dict) else {}


def _entity_type(stmt: dict[str, Any]) -> str:
    rd = _record_details(stmt)
    et = rd.get("entityType")
    if isinstance(et, dict):
        return et.get("type", "")
    if isinstance(et, str):
        return et
    return stmt.get("entityType", "") or ""


def _person_type(stmt: dict[str, Any]) -> str:
    rd = _record_details(stmt)
    return rd.get("personType") or stmt.get("personType", "") or ""


def _entity_jurisdiction(stmt: dict[str, Any]) -> dict[str, str] | None:
    rd = _record_details(stmt)
    j = rd.get("incorporatedInJurisdiction") or stmt.get("incorporatedInJurisdiction")
    if isinstance(j, dict):
        return j
    return None


def _entity_legal_form_blob(stmt: dict[str, Any]) -> str:
    """Flatten any legal-form / subtype / details fields to one string for matching."""
    rd = _record_details(stmt)
    parts: list[str] = []
    for key in ("legalForm", "entitySubtype", "name", "details"):
        v = rd.get(key)
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, dict):
            for sub in v.values():
                if isinstance(sub, str):
                    parts.append(sub)
    return " ".join(parts).lower()


def _statement_id(stmt: dict[str, Any]) -> str:
    return stmt.get("statementId") or stmt.get("statement_id") or ""


def _relationship_endpoints(stmt: dict[str, Any]) -> tuple[str, str, str]:
    """Return (subject_id, ip_id, ip_kind) for a relationship statement.

    ``ip_kind`` is ``"entity"``, ``"person"`` or ``""`` (unknown).
    """
    rd = _record_details(stmt)
    subj = (rd.get("subject") or {}).get("describedByEntityStatement") or ""
    ip = rd.get("interestedParty") or {}
    if "describedByEntityStatement" in ip:
        return subj, ip["describedByEntityStatement"], "entity"
    if "describedByPersonStatement" in ip:
        return subj, ip["describedByPersonStatement"], "person"
    if "describedByAnonymousEntityStatement" in ip:
        # Some BODS implementations carry this as a distinct key —
        # treat it as an entity for chain-counting purposes.
        return subj, ip["describedByAnonymousEntityStatement"], "entity"
    return subj, "", ""


def _interests(stmt: dict[str, Any]) -> list[dict[str, Any]]:
    rd = _record_details(stmt)
    interests = rd.get("interests")
    return interests if isinstance(interests, list) else []


# ----------------------------------------------------------------------
# AMLA CDD RTS rules
# ----------------------------------------------------------------------


def assess_amla(
    source_id: str, raw: dict[str, Any], bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """Run all AMLA-aligned rules over a BODS bundle.

    Called from ``assess_bundle``; broken out so callers (CLI, tests,
    a future export pipeline) can invoke it directly on a hand-built
    BODS bundle without going through a deepen response.
    """
    if not bods:
        return []
    hit_id = raw.get("entity_id") or raw.get("hit_id") or ""

    trust_signal = _trust_or_arrangement_signal(source_id, hit_id, bods)
    non_eu_signal = _non_eu_jurisdiction_signal(source_id, hit_id, bods)
    nominee_signal = _nominee_signal(source_id, hit_id, bods)
    layers_signal = _layers_signal(source_id, hit_id, bods)

    out: list[RiskSignal] = []
    for sig in (trust_signal, non_eu_signal, nominee_signal, layers_signal):
        if sig is not None:
            out.append(sig)

    # FATF jurisdiction signals — independent of the AMLA composite rule.
    out.extend(_fatf_jurisdiction_signals(source_id, hit_id, bods))

    # AMLA "complex corporate structure" = ≥3 layers AND ≥1 of
    # {trust/arrangement, non-EU, nominee}.
    if layers_signal is not None and (
        trust_signal is not None
        or non_eu_signal is not None
        or nominee_signal is not None
    ):
        triggers = []
        if trust_signal is not None:
            triggers.append("trust/arrangement")
        if non_eu_signal is not None:
            triggers.append("non-EU jurisdiction")
        if nominee_signal is not None:
            triggers.append("nominee")
        out.append(
            RiskSignal(
                code=COMPLEX_CORPORATE_STRUCTURE,
                confidence="high",
                summary=(
                    "Meets AMLA CDD RTS threshold for a complex corporate "
                    f"structure: {layers_signal.evidence['layers']} layers "
                    "of ownership combined with " + ", ".join(triggers) + "."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={
                    "layers": layers_signal.evidence["layers"],
                    "triggers": triggers,
                },
            )
        )

    return out


def _trust_or_arrangement_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    matches: list[dict[str, str]] = []
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        et = _entity_type(stmt)
        if et == "arrangement":
            matches.append(
                {
                    "statement_id": _statement_id(stmt),
                    "match": "entityType=arrangement",
                }
            )
            continue
        blob = _entity_legal_form_blob(stmt)
        for frag in _TRUST_LEGAL_FORM_FRAGMENTS:
            if frag in blob:
                matches.append(
                    {
                        "statement_id": _statement_id(stmt),
                        "match": f"legalForm contains '{frag}'",
                    }
                )
                break
    if not matches:
        return None
    return RiskSignal(
        code=TRUST_OR_ARRANGEMENT,
        confidence="high",
        summary=(
            "Ownership chain includes a trust or non-corporate "
            f"arrangement ({len(matches)} entity statement(s)). "
            "AMLA CDD RTS condition (a)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"matches": matches},
    )


def _non_eu_jurisdiction_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    """Fires when the chain has any entity outside the EU+EEA set.

    The "EU+EEA set" is resolved at call time from settings — see
    ``_eu_eea_codes()`` and the ``OPENCHECK_AMLA_*`` env vars.
    """
    eu_eea = _eu_eea_codes()
    non_eu: list[dict[str, str]] = []
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        name = j.get("name") or ""
        if code and code not in eu_eea:
            non_eu.append(
                {
                    "statement_id": _statement_id(stmt),
                    "code": code,
                    "name": name,
                }
            )
    if not non_eu:
        return None
    # Pull a short, deduped list of country codes for the summary.
    codes = sorted({m["code"] for m in non_eu})
    return RiskSignal(
        code=NON_EU_JURISDICTION,
        confidence="high",
        summary=(
            "Ownership chain reaches into jurisdictions outside the EU/EEA: "
            + ", ".join(codes)
            + ". AMLA CDD RTS condition (b)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"jurisdictions": non_eu},
    )


def _nominee_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    matches: list[dict[str, str]] = []
    for stmt in bods:
        kind = _stmt_kind(stmt)
        if kind == "relationship":
            for interest in _interests(stmt):
                blob = " ".join(
                    str(v).lower()
                    for k, v in interest.items()
                    if k in ("type", "details") and isinstance(v, str)
                )
                if any(frag in blob for frag in _NOMINEE_FRAGMENTS):
                    matches.append(
                        {
                            "statement_id": _statement_id(stmt),
                            "match": f"interest mentions nominee ({interest.get('type', '')})",
                        }
                    )
                    break
        elif kind == "person":
            blob_parts: list[str] = []
            rd = _record_details(stmt)
            for name in rd.get("names") or []:
                if isinstance(name, dict):
                    blob_parts.extend(
                        str(v) for v in name.values() if isinstance(v, str)
                    )
            for key in ("details", "publicationDetails"):
                v = rd.get(key)
                if isinstance(v, str):
                    blob_parts.append(v)
            blob = " ".join(blob_parts).lower()
            if any(frag in blob for frag in _NOMINEE_FRAGMENTS):
                matches.append(
                    {
                        "statement_id": _statement_id(stmt),
                        "match": "person record mentions nominee",
                    }
                )
    if not matches:
        return None
    return RiskSignal(
        code=NOMINEE,
        confidence="high",
        summary=(
            f"Ownership chain includes nominee shareholder/director "
            f"references ({len(matches)} statement(s)). "
            "AMLA CDD RTS condition (c)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"matches": matches},
    )


def _layers_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    """Longest entity-only chain in the BODS relationship graph.

    AMLA defines a complex corporate structure as having "three or more
    layers of ownership". We treat that as: there exists a chain of
    relationship edges through ≥3 distinct entity nodes.

    Edge direction: ``interestedParty --(owns)--> subject``. So walking
    from a leaf interestedParty up through subject_ids approximates the
    ownership-direction chain. We DFS over the entity-only subgraph and
    track the longest simple path (cycles guarded via per-path visited
    set).
    """
    # Map statementId -> entity type to filter to entity nodes.
    entity_ids: set[str] = set()
    for stmt in bods:
        if _stmt_kind(stmt) == "entity":
            sid = _statement_id(stmt)
            if sid:
                entity_ids.add(sid)

    # Build adjacency: ip -> {subject1, subject2, ...} restricted to
    # entity nodes (ignore person interestedParties because they end
    # the chain, not extend it).
    adj: dict[str, set[str]] = {}
    for stmt in bods:
        if _stmt_kind(stmt) != "relationship":
            continue
        subj, ip, ip_kind = _relationship_endpoints(stmt)
        if not subj or not ip or ip_kind != "entity":
            continue
        if subj not in entity_ids or ip not in entity_ids:
            continue
        adj.setdefault(ip, set()).add(subj)

    if not adj and len(entity_ids) < 3:
        return None

    longest = 0
    longest_path: list[str] = []

    def dfs(node: str, visited: list[str]) -> None:
        nonlocal longest, longest_path
        path_len = len(visited)
        if path_len > longest:
            longest = path_len
            longest_path = list(visited)
        for nxt in adj.get(node, ()):
            if nxt in visited:
                continue  # cycle guard
            visited.append(nxt)
            dfs(nxt, visited)
            visited.pop()

    # Start from every entity node — the graph may have multiple roots.
    for start in entity_ids:
        dfs(start, [start])

    if longest < 3:
        return None
    return RiskSignal(
        code=COMPLEX_OWNERSHIP_LAYERS,
        confidence="medium",
        summary=(
            f"Ownership chain has {longest} corporate layers "
            "(AMLA threshold: ≥3)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"layers": longest, "longest_path": longest_path},
    )


def _fatf_jurisdiction_signals(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """Fire FATF_BLACK_LIST / FATF_GREY_LIST when any entity in the BODS
    bundle is incorporated in a FATF-listed jurisdiction.

    Two separate signals — one per list — so the UI can present them with
    different severities.  Both can fire on the same bundle (e.g. an entity
    that is itself grey-listed but has an owner in a black-listed jurisdiction).

    Lists current as of February 2026. Update ``FATF_BLACK_LIST_CODES`` and
    ``FATF_GREY_LIST_CODES`` at each FATF plenary (typically February, June,
    October) when the lists are refreshed.
    """
    black_hits: list[dict[str, str]] = []
    grey_hits: list[dict[str, str]] = []

    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        name = j.get("name") or ""
        if not code:
            continue
        entry = {"statement_id": _statement_id(stmt), "code": code, "name": name}
        if code in FATF_BLACK_LIST_CODES:
            black_hits.append(entry)
        elif code in FATF_GREY_LIST_CODES:
            grey_hits.append(entry)

    out: list[RiskSignal] = []

    if black_hits:
        codes = sorted({h["code"] for h in black_hits})
        names = sorted({h["name"] for h in black_hits if h["name"]})
        label = ", ".join(names) if names else ", ".join(codes)
        out.append(
            RiskSignal(
                code=FATF_BLACK_LIST,
                confidence="high",
                summary=(
                    f"Ownership chain reaches into {label}, "
                    "a jurisdiction on the FATF High-Risk list "
                    "(Call for Action / black list, February 2026)."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"jurisdictions": black_hits, "list": "black"},
            )
        )

    if grey_hits:
        codes = sorted({h["code"] for h in grey_hits})
        names = sorted({h["name"] for h in grey_hits if h["name"]})
        label = ", ".join(names) if names else ", ".join(codes)
        out.append(
            RiskSignal(
                code=FATF_GREY_LIST,
                confidence="medium",
                summary=(
                    f"Ownership chain reaches into {label}, "
                    "a jurisdiction under FATF Increased Monitoring "
                    "(grey list, February 2026)."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"jurisdictions": grey_hits, "list": "grey"},
            )
        )

    return out


def _possible_obfuscation_signal(
    source_id: str, hit_id: str, signals: list[RiskSignal]
) -> RiskSignal | None:
    """Advisory mirror of AMLA's subjective condition.

    Cannot be judged from data alone — fires ``low`` when the bundle
    already has signals that, taken together, suggest a structure
    "obfuscating ownership". Always notes the human-judgment caveat.
    """
    codes = {s.code for s in signals}
    has_opacity = OPAQUE_OWNERSHIP in codes
    has_layered_concern = (
        COMPLEX_CORPORATE_STRUCTURE in codes
        or (COMPLEX_OWNERSHIP_LAYERS in codes and (NON_EU_JURISDICTION in codes or NOMINEE in codes))
    )
    if not (has_opacity and has_layered_concern):
        return None
    return RiskSignal(
        code=POSSIBLE_OBFUSCATION,
        confidence="low",
        summary=(
            "Advisory: structure combines opacity (unknown/anonymous "
            "parties) with complex layering. AMLA CDD RTS subjective "
            "condition — confirm whether there is a legitimate "
            "economic rationale before relying on this signal."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={
            "triggered_by": sorted(
                codes
                & {
                    OPAQUE_OWNERSHIP,
                    COMPLEX_CORPORATE_STRUCTURE,
                    COMPLEX_OWNERSHIP_LAYERS,
                    NON_EU_JURISDICTION,
                    NOMINEE,
                }
            )
        },
    )
