"""Build a compact, fully-evidenced ``EvidencePacket`` from an OpenCheck result.

The packet is the single source of truth handed to the LLM. Every fact is
atomic and already carries its provenance (which source, which BODS statement,
what confidence), so the model only ever rephrases evidence — it cannot add,
infer or retrieve anything. The same packet feeds the (future) PDF export.

Input is the serialised lookup/report result (``LookupResponse`` /
``ReportResponse`` as a dict), so the builder is decoupled from the routers and
trivially testable with fixtures.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

# Confidence is a small, ordered vocabulary used for both facts and risks.
Confidence = str  # "high" | "medium" | "low"


class Fact(BaseModel):
    """One atomic, evidenced statement about the subject or its network."""

    id: str  # stable within a packet, e.g. "f1"
    statement: str  # human-readable, e.g. "Jane Smith is a director (appointed 2018-06-01)."
    source_name: str  # the human source name, e.g. "UK Companies House"
    source_id: str | None = None  # OpenCheck adapter id when known
    source_url: str | None = None
    bods_statement_ids: list[str] = Field(default_factory=list)
    confidence: Confidence = "medium"


class RiskItem(BaseModel):
    """A structural risk signal, with how it was derived and how sure we are."""

    id: str  # e.g. "r1"
    code: str
    label: str
    confidence: Confidence
    rationale: str  # plain-English: how the rule fired
    source_name: str
    source_id: str | None = None  # OpenCheck adapter id, for UI source-card linking
    fact_ids: list[str] = Field(default_factory=list)  # supporting facts, if any


class SourceRef(BaseModel):
    source_id: str
    name: str
    license: str
    homepage: str | None = None


class Gap(BaseModel):
    """An absence relevant to CDD — itself citable evidence.

    A gap is a finding ("no beneficial owner was disclosed", "OpenSanctions could
    not be queried"), so the narrative must be able to *ground* a statement about
    it. Each gap therefore carries an id (``g1`` …) the model can cite.
    """

    id: str
    statement: str


class EvidencePacket(BaseModel):
    """Everything — and only what — the LLM is allowed to use."""

    subject_name: str
    lei: str | None = None
    jurisdiction: str | None = None
    # "identifier-confirmed" for LEI-anchored lookups; "name-matched" for the
    # free-text /report flow (the narrative MUST caveat name matches).
    subject_confidence: str = "identifier-confirmed"
    identifiers: dict[str, str] = Field(default_factory=dict)
    facts: list[Fact] = Field(default_factory=list)
    risks: list[RiskItem] = Field(default_factory=list)
    sources_consulted: list[SourceRef] = Field(default_factory=list)
    gaps: list[Gap] = Field(default_factory=list)  # absences relevant to CDD

    @field_validator("gaps", mode="before")
    @classmethod
    def _coerce_gaps(cls, v: Any) -> Any:
        """Accept a plain list of strings (fixtures) and assign stable g-ids."""
        if not isinstance(v, list):
            return v
        out = []
        for i, item in enumerate(v, start=1):
            if isinstance(item, str):
                out.append({"id": f"g{i}", "statement": item})
            else:
                out.append(item)
        return out

    def fact_ids(self) -> set[str]:
        return {f.id for f in self.facts}

    def gap_ids(self) -> set[str]:
        return {g.id for g in self.gaps}

    def risk_ids(self) -> set[str]:
        return {r.id for r in self.risks}

    def evidence_ids(self) -> set[str]:
        """Every id a claim may legitimately cite."""
        return self.fact_ids() | self.risk_ids() | self.gap_ids()


# --- helpers -----------------------------------------------------------------


def _source_name(stmt: dict[str, Any]) -> str:
    return ((stmt.get("source") or {}).get("description")) or "an OpenCheck source"


def _source_authority(stmt: dict[str, Any]) -> Confidence:
    """Official national registers are high-confidence; everything else medium."""
    types = (stmt.get("source") or {}).get("type") or []
    return "high" if "officialRegister" in types else "medium"


def _entity_name(stmt: dict[str, Any]) -> str:
    return (stmt.get("recordDetails") or {}).get("name") or "an entity"


def _person_name(stmt: dict[str, Any]) -> str:
    names = (stmt.get("recordDetails") or {}).get("names") or []
    if names and names[0].get("fullName"):
        return names[0]["fullName"]
    return "an unnamed person"


def _party_label(party: Any, by_id: dict[str, dict[str, Any]]) -> str:
    """Resolve a relationship subject/interestedParty to a display label."""
    if isinstance(party, dict):  # unspecified record {reason, description}
        reason = party.get("reason") or "unspecified"
        return f"an unspecified party ({reason})"
    stmt = by_id.get(party)
    if not stmt:
        return "a party"
    return _person_name(stmt) if stmt.get("recordType") == "person" else _entity_name(stmt)


def _interest_phrase(interest: dict[str, Any]) -> str:
    detail = interest.get("details") or interest.get("type") or "an interest"
    share = interest.get("share") or {}
    lo, hi = share.get("exclusiveMinimum"), share.get("maximum")
    band = f" ({lo}–{hi}%)" if lo is not None or hi is not None else ""
    dates = []
    if interest.get("startDate"):
        dates.append(f"from {interest['startDate']}")
    if interest.get("endDate"):
        dates.append(f"to {interest['endDate']}")
    date_str = f", {', '.join(dates)}" if dates else ""
    return f"{detail}{band}{date_str}"


_RISK_LABELS = {
    "SANCTIONED": "Sanctioned",
    "RELATED_SANCTIONED": "Related party sanctioned",
    "PEP": "Politically exposed person",
    "RELATED_PEP": "Related party politically exposed",
    "FATF_BLACK_LIST": "FATF black-list jurisdiction",
    "FATF_GREY_LIST": "FATF grey-list jurisdiction",
    "NON_EU_JURISDICTION": "Non-EU jurisdiction",
    "OFFSHORE_LEAKS": "Offshore Leaks match",
    "TRUST_OR_ARRANGEMENT": "Trust / arrangement",
    "NOMINEE": "Nominee arrangement",
    "OPAQUE_OWNERSHIP": "Opaque ownership (super-secure)",
    "COMPLEX_OWNERSHIP_LAYERS": "Complex ownership layers",
    "COMPLEX_CORPORATE_STRUCTURE": "Complex corporate structure",
    "STATE_CONTROLLED": "State-controlled (possible SOE)",
}


def build_evidence_packet(
    report: dict[str, Any],
    *,
    subject_confidence: str | None = None,
) -> EvidencePacket:
    """Distil an OpenCheck lookup/report result into an evidence packet."""
    from ..sources import REGISTRY  # lazy import to avoid cycles

    bods: list[dict[str, Any]] = report.get("bods") or []
    by_id = {s.get("statementId"): s for s in bods if s.get("statementId")}

    lei = report.get("lei")
    subject_name = report.get("legal_name") or report.get("query") or ""
    jurisdiction = report.get("jurisdiction")
    # subject confidence: LEI-anchored lookups are identifier-confirmed; the
    # free-text /report flow is only a name match unless told otherwise.
    if subject_confidence is None:
        subject_confidence = "identifier-confirmed" if lei else "name-matched"

    # Identify the subject entity statement (prefer one carrying the LEI).
    subject_stmt = None
    for s in bods:
        if s.get("recordType") != "entity":
            continue
        idents = (s.get("recordDetails") or {}).get("identifiers") or []
        if lei and any(i.get("id") == lei for i in idents):
            subject_stmt = s
            break
    if subject_stmt is None:
        subject_stmt = next((s for s in bods if s.get("recordType") == "entity"), None)
    if not subject_name and subject_stmt:
        subject_name = _entity_name(subject_stmt)

    # Reverse map the human source name back to the adapter id, so a citation
    # chip in the UI can link a fact to its source card reliably (matching on
    # display text alone is fragile).
    name_to_id = {a.info.name: sid for sid, a in REGISTRY.items()}

    facts: list[Fact] = []
    fid = 0

    def add_fact(statement: str, stmt: dict[str, Any], extra_ids: list[str] | None = None) -> str:
        nonlocal fid
        fid += 1
        source_name = _source_name(stmt)
        f = Fact(
            id=f"f{fid}",
            statement=statement,
            source_name=source_name,
            source_id=name_to_id.get(source_name),
            source_url=(stmt.get("source") or {}).get("url"),
            bods_statement_ids=[stmt.get("statementId")] + (extra_ids or []),
            confidence=_source_authority(stmt),
        )
        facts.append(f)
        return f.id

    # Relationship facts (the substance: directors, ownership, nominees, …).
    for s in bods:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        ip = _party_label(rd.get("interestedParty"), by_id)
        subj = _party_label(rd.get("subject"), by_id)
        interests = rd.get("interests") or []
        phrase = "; ".join(_interest_phrase(i) for i in interests) if interests else "an interest"
        closed = s.get("recordStatus") == "closed"
        verb = "previously held" if closed else "holds"
        add_fact(f"{ip} {verb} {phrase} in {subj}.", s)

    # Subject registration fact (name + jurisdiction + identifiers).
    if subject_stmt:
        rd = subject_stmt.get("recordDetails") or {}
        idents = ", ".join(
            f"{i.get('scheme', '')} {i.get('id', '')}".strip()
            for i in (rd.get("identifiers") or [])
        )
        jur = (rd.get("jurisdiction") or {}).get("name") or jurisdiction or ""
        bits = [f"{_entity_name(subject_stmt)} is a registered entity"]
        if jur:
            bits.append(f"in {jur}")
        sentence = " ".join(bits) + (f" ({idents})." if idents else ".")
        add_fact(sentence, subject_stmt)

    # Risk items.
    risks: list[RiskItem] = []
    for i, sig in enumerate(report.get("risk_signals") or [], start=1):
        src = REGISTRY.get(sig.get("source_id", ""))
        source_name = src.info.name if src else (sig.get("source_id") or "OpenCheck risk engine")
        code = sig.get("code", "")
        risks.append(
            RiskItem(
                id=f"r{i}",
                code=code,
                label=_RISK_LABELS.get(code, code.replace("_", " ").title()),
                confidence=sig.get("confidence", "medium"),
                rationale=sig.get("summary") or "Flagged by the OpenCheck risk engine.",
                source_name=source_name,
                source_id=sig.get("source_id") or None,
            )
        )

    # Absence of risk is itself an evidenced finding (the engine ran and raised
    # nothing) — give the model a fact to cite so it can say so without
    # fabricating a citation.
    if not risks:
        facts.append(
            Fact(
                id=f"f{len(facts) + 1}",
                statement=(
                    "The OpenCheck risk engine ran against the available data and identified "
                    "no structural or jurisdictional risk signals for this entity."
                ),
                source_name="OpenCheck risk engine",
                confidence="high",
            )
        )

    # Sources consulted (non-stub hits), with licence.
    sources: list[SourceRef] = []
    seen_src: set[str] = set()
    for h in report.get("hits") or []:
        sid = h.get("source_id")
        if not sid or sid in seen_src or h.get("is_stub"):
            continue
        seen_src.add(sid)
        adapter = REGISTRY.get(sid)
        if adapter is None:
            continue
        info = adapter.info
        sources.append(
            SourceRef(source_id=sid, name=info.name, license=info.license, homepage=info.homepage)
        )

    # Gaps — absences that matter for due diligence.
    gaps: list[str] = []
    def _is_person_rel(s: dict[str, Any]) -> bool:
        if s.get("recordType") != "relationship":
            return False
        ip = (s.get("recordDetails") or {}).get("interestedParty")
        return (by_id.get(ip) or {}).get("recordType") == "person"

    has_person_rel = any(_is_person_rel(s) for s in bods)
    if not has_person_rel:
        gaps.append(
            "No beneficial owner or controlling individual was disclosed in the available sources."
        )
    for sid, err in (report.get("errors") or {}).items():
        adapter = REGISTRY.get(sid)
        name = adapter.info.name if adapter else sid
        gaps.append(f"{name} could not be queried ({err}).")
    for notice in report.get("license_notices") or []:
        gaps.append(notice.get("notice", ""))

    return EvidencePacket(
        subject_name=subject_name or "Unknown entity",
        lei=lei,
        jurisdiction=jurisdiction,
        subject_confidence=subject_confidence,
        identifiers=report.get("derived_identifiers") or {},
        facts=facts,
        risks=risks,
        sources_consulted=sources,
        gaps=[g for g in gaps if g],
    )
