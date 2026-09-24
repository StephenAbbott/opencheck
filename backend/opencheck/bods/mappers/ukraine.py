"""Ukraine — ЄДР (Unified State Register of Legal Entities) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..annotations import annotate, pointer, transformation
from ..statements import (
    SOURCE_NAMES,
    _country_obj,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ---------------------------------------------------------------------------
# Ukraine — ЄДР (Unified State Register of Legal Entities), data.gov.ua
# ---------------------------------------------------------------------------
#
# The ЄДР is unusual among commercial registers: it files beneficial owners as
# a first-class element, alongside founders, heads/signatories and
# governing-body members. This mapper therefore emits five kinds of edge, and
# is careful to keep them apart — a founder is a shareholder, not an officer,
# and the register says so in different elements.
#
#   BENEFICIARIES → person + relationship, beneficialOwnershipOrControl TRUE
#                   (or, where absence is filed, an unspecified interestedParty
#                   carrying the register's own stated reason — BODS
#                   missing-information modelling, data-standard #389)
#   FOUNDERS      → person/entity + shareholding, NO BO claim
#   SIGNERS       → person + seniorManagingOfficial
#   MEMBERS       → person + boardChair / boardMember
#   EXECUTIVE_POWER → stateBody + controlByLegalFramework (SOE modelling)
#
# Everything the register withholds under martial law — addresses, precise
# locations, activity codes — stays withheld. Nothing here reconstructs a
# redacted field from another source.

_UA_JURISDICTION: tuple[str, str] = ("Ukraine", "UA")
_UA_SCHEME = "UA-EDR"
_UA_SCHEME_NAME = (
    "EDRPOU — Unified State Register of Legal Entities, Individual "
    "Entrepreneurs and Public Organisations (Ukraine)"
)

#: Founder holdings are filed in hryvnia, never as a percentage. A share can be
#: derived by dividing by the declared authorised capital, but only where the
#: holdings actually add up to it — measured on 120,000 active entities, they
#: reconcile within 1 % in 94.2 % of cases where both parse. Where they do not,
#: the set is not asserted at all: a percentage OpenCheck cannot reconcile is a
#: guess, and the register publishes no percentage to fall back on.
_UA_CAPITAL_TOLERANCE = 0.01

#: Role text marking a governing-body member as the chair rather than a member.
_UA_CHAIR_TOKENS = ("голова", "президент", "head", "chair")


def _ua_person_or_entity(name: str, code: str | None) -> str:
    """"entity" when the ЄДР gives the party its own EDRPOU, else "person".

    The register does not type founders explicitly; carrying a registration
    code is what distinguishes a legal person from a natural one.
    """
    return "entity" if code else "person"


def _ua_derived_shares(
    founders: list[dict[str, Any]], capital: float | None
) -> dict[int, float] | None:
    """Founder index → percentage, or None when the set does not reconcile.

    All-or-nothing on purpose: a partial set would put a percentage on some
    founders and not others of the same company, which reads as "the rest hold
    nothing" rather than "we could not tell".
    """
    if not capital or capital <= 0:
        return None
    amounts = {
        i: f["amount_uah"]
        for i, f in enumerate(founders)
        if f.get("amount_uah") is not None
    }
    if len(amounts) != len(founders) or not amounts:
        return None
    total = sum(amounts.values())
    if total <= 0 or abs(total - capital) / capital > _UA_CAPITAL_TOLERANCE:
        return None
    return {i: round(amount / capital * 100, 4) for i, amount in amounts.items()}


def _ua_bo_interests(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Interests for one declared UBO, from the register's own influence type.

    The ЄДР files the influence type ("Прямий вирішальний вплив" — direct
    decisive influence) and up to two percentages, one per direction. A record
    declaring both directions yields two interests rather than one averaged
    fiction.
    """
    influence = row.get("influence")
    out: list[dict[str, Any]] = []

    def _add(direction: str, pct: float | None) -> None:
        interest: dict[str, Any] = {
            "type": "shareholding" if pct is not None else "otherInfluenceOrControl",
            "directOrIndirect": direction,
        }
        if pct is not None:
            interest["share"] = {"exact": pct}
        if row.get("note"):
            interest["details"] = row["note"]
        set_beneficial_ownership(
            interest, "edr_ukraine", record_kind="beneficiary"
        )
        out.append(interest)

    pct_direct = row.get("pct_direct")
    pct_indirect = row.get("pct_indirect")
    if influence in ("direct", "both") or pct_direct is not None:
        _add("direct", pct_direct)
    if influence in ("indirect", "both") or pct_indirect is not None:
        _add("indirect", pct_indirect)
    if not out:
        # Influence type unreadable and no percentage: the register still says
        # this person is the UBO, so say that and nothing more.
        _add("unknown", None)
    return out


def map_edr_ukraine(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EdrUkraineAdapter fetch bundle to BODS v0.4 statements.

    Yields, in order: the company; its beneficial owners (or a statement of
    their filed absence); its founders; its signatories; its governing-body
    members; and, for a state enterprise, the executive authority above it.
    """
    # Local import to avoid a circular import at module load time.
    from ...sources.edr_ukraine import ABSENCE_BODS_REASON, classify_absence

    if not bundle or bundle.get("is_stub"):
        return

    edrpou = (bundle.get("edrpou") or "").strip()
    entity = bundle.get("entity") or {}
    if not edrpou or not entity:
        return

    name = entity.get("name") or bundle.get("name") or f"UA {edrpou}"
    source_url = bundle.get("link") or None

    identifiers = [
        {"id": edrpou, "scheme": _UA_SCHEME, "schemeName": _UA_SCHEME_NAME}
    ]
    alternate_names = [entity["short_name"]] if entity.get("short_name") else []

    company = make_entity_statement(
        source_id="edr_ukraine",
        local_id=edrpou,
        name=name,
        jurisdiction=_UA_JURISDICTION,
        identifiers=identifiers,
        alternate_names=alternate_names,
        entity_details=entity.get("opf") or None,
        source_url=source_url,
    )
    stan = (entity.get("stan") or "").strip()
    _liveness.apply_register_status(
        company,
        source_label=SOURCE_NAMES["edr_ukraine"],
        liveness=_liveness.classify(
            stan,
            live=("зареєстровано",),
            pending=(
                "в стані припинення",
                "порушено справу про банкрутство",
                "порушено справу про банкрутство (санація)",
            ),
            terminal=("припинено", "скасовано", "архівний"),
        ),
        raw=stan or None,
    )
    yield company
    company_id = company["statementId"]

    # ── Beneficial owners, and filed absences ─────────────────────────────
    for row in bundle.get("beneficiaries") or []:
        seq = row.get("seq", 0)
        kind = row.get("kind")

        if kind == "absence":
            reason_text = (row.get("reason_text") or "").strip()
            code = classify_absence(reason_text)
            bods_reason = ABSENCE_BODS_REASON.get(code, "unknown")
            # NO interest, on any absence record — deliberately unlike the
            # Companies House PSC statements, which attach an `unknownInterest`
            # asserting beneficial ownership when the code says a PSC exists
            # but is unidentified. No Ukrainian absence reason says that. They
            # say either "no natural person meets the definition" (no owner to
            # describe) or "no reason was supplied" (we cannot tell whether one
            # exists). An interest either way would assert a relationship the
            # register does not report. The unspecified party and its stated
            # reason are the whole claim.
            yield make_relationship_statement(
                source_id="edr_ukraine",
                local_id=f"{edrpou}:bo-absent:{seq}",
                subject_statement_id=company_id,
                interested_party_unspecified={
                    "reason": bods_reason,
                    # The register's own words, verbatim, alongside the
                    # codelist value. The code carries the KIND of absence —
                    # a finding of no owner, an exemption from filing, or an
                    # unexplained gap — and the description carries the
                    # Ukrainian the registrar actually wrote, which is finer
                    # grained than any codelist (a statutory exemption and a
                    # legal-form exemption share a code but not a reason).
                    "description": reason_text or "No reason recorded.",
                },
                source_url=source_url,
            )
            continue

        if kind != "named" or not (row.get("name") or "").strip():
            continue

        nationalities: list[dict[str, str]] = []
        citizenship = _country_obj(row.get("citizenship") or "")
        if citizenship:
            nationalities.append(citizenship)
        person = make_person_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:bo:{seq}",
            full_name=row["name"],
            nationalities=nationalities,
            source_url=source_url,
        )
        yield person
        yield make_relationship_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:bo-rel:{seq}",
            subject_statement_id=company_id,
            interested_party_statement_id=person["statementId"],
            interested_party_type="person",
            interests=_ua_bo_interests(row),
            source_url=source_url,
        )

    # ── Founders (equity holders — never officers) ────────────────────────
    founders = list(bundle.get("founders") or [])
    shares = _ua_derived_shares(founders, entity.get("capital"))
    for idx, row in enumerate(founders):
        party_name = (row.get("name") or "").strip()
        if not party_name:
            continue
        code = row.get("code")
        party_type = _ua_person_or_entity(party_name, code)
        seq = row.get("seq", idx)
        local = f"{edrpou}:founder:{seq}"

        if party_type == "entity":
            party = make_entity_statement(
                source_id="edr_ukraine",
                local_id=local,
                name=party_name,
                jurisdiction=_UA_JURISDICTION,
                identifiers=[
                    {"id": code, "scheme": _UA_SCHEME, "schemeName": _UA_SCHEME_NAME}
                ],
                source_url=source_url,
            )
        else:
            nationalities = []
            citizenship = _country_obj(row.get("citizenship") or "")
            if citizenship:
                nationalities.append(citizenship)
            party = make_person_statement(
                source_id="edr_ukraine",
                local_id=local,
                full_name=party_name,
                nationalities=nationalities,
                source_url=source_url,
            )
        yield party

        pct = (shares or {}).get(idx)
        interest: dict[str, Any] = {
            "type": "shareholding" if pct is not None else "unknownInterest",
            "directOrIndirect": "direct",
        }
        if pct is not None:
            interest["share"] = {"exact": pct}
            interest["details"] = (
                f"Holding filed as {row['amount_uah']:,.2f} UAH of "
                f"{entity['capital']:,.2f} UAH authorised capital."
            )
        elif row.get("amount_uah") is not None:
            # The amount is published but cannot become a percentage: the
            # declared capital is missing, zero, or the holdings do not add up
            # to it. Say the amount, claim no share.
            interest["details"] = (
                f"Holding filed as {row['amount_uah']:,.2f} UAH; the register "
                "publishes no percentage."
            )
        set_beneficial_ownership(
            interest,
            "edr_ukraine",
            record_kind="corporate_founder" if code else "founder",
        )
        rel = make_relationship_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:founder-rel:{seq}",
            subject_statement_id=company_id,
            interested_party_statement_id=party["statementId"],
            interested_party_type=party_type,
            interests=[interest],
            source_url=source_url,
        )
        if pct is not None:
            # The percentage is OpenCheck's arithmetic, not the register's
            # claim — the ЄДР publishes hryvnia and nothing else.
            annotate(
                rel,
                transformation(
                    "Share percentage derived by OpenCheck from the filed "
                    "hryvnia holding and the declared authorised capital; the "
                    "register publishes no percentage.",
                    pointer("/recordDetails/interests/0/share"),
                ),
            )
        yield rel

    # ── Signatories: the head of the entity and anyone acting for it ──────
    for idx, row in enumerate(bundle.get("signers") or []):
        party_name = (row.get("name") or "").strip()
        if not party_name:
            continue
        seq = row.get("seq", idx)
        person = make_person_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:signer:{seq}",
            full_name=party_name,
            source_url=source_url,
        )
        yield person
        interest = {"type": "seniorManagingOfficial", "directOrIndirect": "direct"}
        if row.get("role"):
            interest["details"] = row["role"]
        set_beneficial_ownership(interest, "edr_ukraine", record_kind="signer")
        yield make_relationship_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:signer-rel:{seq}",
            subject_statement_id=company_id,
            interested_party_statement_id=person["statementId"],
            interested_party_type="person",
            interests=[interest],
            source_url=source_url,
        )

    # ── Governing-body members ────────────────────────────────────────────
    for idx, row in enumerate(bundle.get("members") or []):
        party_name = (row.get("name") or "").strip()
        if not party_name:
            continue
        seq = row.get("seq", idx)
        role = (row.get("role") or "").lower()
        person = make_person_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:member:{seq}",
            full_name=party_name,
            source_url=source_url,
        )
        yield person
        interest = {
            "type": (
                "boardChair"
                if any(tok in role for tok in _UA_CHAIR_TOKENS)
                else "boardMember"
            ),
            "directOrIndirect": "direct",
        }
        if row.get("role"):
            interest["details"] = row["role"]
        set_beneficial_ownership(interest, "edr_ukraine", record_kind="member")
        yield make_relationship_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:member-rel:{seq}",
            subject_statement_id=company_id,
            interested_party_statement_id=person["statementId"],
            interested_party_type="person",
            interests=[interest],
            source_url=source_url,
        )

    # ── State control (BODS SOE modelling) ────────────────────────────────
    #
    # "If an entity's status as a state-owned enterprise needs to be
    # represented, then its Entity statement MUST be the subject of
    # Relationship statements connecting it, either directly or indirectly, to
    # an Entity statement with entityType.type of 'state' or 'stateBody'."
    # (standard.openownership.org — representing state-owned enterprises.)
    #
    # EXECUTIVE_POWER is the register's own field for exactly this: per its
    # schema annotation it holds the central or local executive authority a
    # state enterprise belongs to, OR the state's holding in the entity where
    # that holding is not less than 25 %. The two readings cannot be told apart
    # from the data, and both are control established by law rather than by a
    # filed shareholding — hence controlByLegalFramework, and hence no share.
    exec_power = bundle.get("executive_power") or {}
    authority = (exec_power.get("name") or "").strip()
    if authority:
        authority_code = exec_power.get("code")
        state_body = make_entity_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:executive-power:{authority_code or authority}",
            name=authority,
            # A state/stateBody node carries jurisdiction to say WHICH state.
            jurisdiction=_UA_JURISDICTION,
            identifiers=(
                [{"id": authority_code, "scheme": _UA_SCHEME, "schemeName": _UA_SCHEME_NAME}]
                if authority_code
                else []
            ),
            entity_type="stateBody",
            entity_details=(
                "Executive authority recorded by the ЄДР as responsible for "
                "this entity, or as holding at least 25 % of its capital."
            ),
            source_url=source_url,
        )
        yield state_body
        interest = {
            "type": "controlByLegalFramework",
            "directOrIndirect": "direct",
            "details": (
                f"Recorded in the ЄДР under EXECUTIVE_POWER as {authority}. The "
                "register uses this field both for the executive authority a "
                "state enterprise belongs to and for a state holding of at "
                "least 25 %; it does not distinguish the two, and publishes no "
                "percentage."
            ),
        }
        set_beneficial_ownership(
            interest, "edr_ukraine", record_kind="executive_power"
        )
        yield make_relationship_statement(
            source_id="edr_ukraine",
            local_id=f"{edrpou}:state-control",
            subject_statement_id=company_id,
            interested_party_statement_id=state_body["statementId"],
            interested_party_type="entity",
            interests=[interest],
            source_url=source_url,
        )
