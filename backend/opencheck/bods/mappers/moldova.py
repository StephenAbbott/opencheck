"""Moldova — ASP State Register of Legal Entities → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..annotations import annotate, pointer, transformation
from ..statements import (
    SOURCE_NAMES,
    _today,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ---------------------------------------------------------------------------
# Moldova — ASP State Register of Legal Entities (weekly open data) → BODS
#
# The register publishes directors with their role and founders with their
# share of the capital, by name only. Nothing below is a beneficial ownership
# claim: every interest asserts ``beneficialOwnershipOrControl: false``.
# ---------------------------------------------------------------------------

MD_IDNO_SCHEME = "MD-IDNO"
MD_IDNO_SCHEME_NAME = "IDNO — State Register of Legal Entities (Moldova)"
_MD_JURISDICTION = ("Moldova", "MD")


def _md_person_key(idno: str, name: str) -> str:
    """One person node per folded name within one company's filing.

    The register files the same person as administrator and as founder of
    the same SRL, spelled the same way. Keying on the folded name inside one
    company lets that person appear once, with both interests; across
    companies nothing is merged, because a name is not an identity.
    """
    import unicodedata as _ud

    folded = "".join(
        c for c in _ud.normalize("NFKD", name) if not _ud.combining(c)
    ).upper()
    return f"{idno}:person:{' '.join(folded.split())}"


def map_asp_moldova(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AspMoldovaAdapter bundle to BODS v0.4 statements.

    Yields the company; then each director and founder as a person or entity
    statement with the relationship that gives them their role.

    * **Directors** are typed by ``ROLE_INTEREST`` in the adapter, decided at
      index-build time, with the register's own role word in ``details``.
    * **Founders with a filed percentage** are ``shareholding`` with
      ``share.exact`` — the register's percentage of the capital, not a
      computed one. A stake filed against two names together carries the
      filed percentage in ``details`` and no exact share for either holder.
      A founder with no share filed is ``unknownInterest``: a founder of a
      public institution is not a shareholder, and the register does not say
      what else they hold.
    * **A corporate founder the index identified** carries an ``MD-IDNO``
      identifier. When that identification came from a name match rather
      than the register's text, the identifier is annotated as OpenCheck's
      work, because the register never states it.
    """
    if not bundle or bundle.get("is_stub"):
        return
    if bundle.get("not_found") or bundle.get("coverage_note"):
        return
    company: dict[str, Any] = bundle.get("company") or {}
    idno = str(bundle.get("idno") or company.get("idno") or "").strip()
    name = str(company.get("name") or "").strip()
    if not idno or not name:
        return
    link = bundle.get("link")

    officers = [o for o in (bundle.get("officers") or []) if (o.get("name") or "").strip()]
    founders = [f for f in (bundle.get("founders") or []) if (f.get("name") or "").strip()]

    address = (company.get("address") or "").strip()
    entity = make_entity_statement(
        source_id="asp_moldova",
        local_id=idno,
        name=name,
        jurisdiction=_MD_JURISDICTION,
        identifiers=[
            {"id": idno, "scheme": MD_IDNO_SCHEME, "schemeName": MD_IDNO_SCHEME_NAME}
        ],
        founding_date=(company.get("registered_on") or None),
        addresses=(
            [{"type": "registered", "address": address, "country": {"name": "Moldova", "code": "MD"}}]
            if address
            else []
        ),
        entity_type="registeredEntity",
        entity_details=(company.get("legal_form") or None),
        source_url=link,
    )
    # The index holds only companies with no liquidation date, so the
    # register has this company as registered. A liquidator or insolvency
    # administrator on file means it is being wound up.
    winding_up = [
        o for o in officers if o.get("interest_type") == "controlByLegalFramework"
    ]
    if winding_up:
        _liveness.apply_register_status(
            entity,
            source_label=SOURCE_NAMES["asp_moldova"],
            liveness=_liveness.PENDING,
            raw=(winding_up[0].get("role") or None),
        )
    else:
        _liveness.apply_register_status(
            entity,
            source_label=SOURCE_NAMES["asp_moldova"],
            liveness=_liveness.LIVE,
            raw=None,
        )
    yield entity

    people: dict[str, dict[str, Any]] = {}

    def person_for(party_name: str) -> tuple[dict[str, Any], bool]:
        key = _md_person_key(idno, party_name)
        if key in people:
            return people[key], False
        stmt = make_person_statement(
            source_id="asp_moldova",
            local_id=key,
            full_name=party_name,
            source_url=link,
        )
        people[key] = stmt
        return stmt, True

    for index, officer in enumerate(officers):
        party_name = " ".join(str(officer["name"]).split())
        if officer.get("is_entity"):
            party = make_entity_statement(
                source_id="asp_moldova",
                local_id=f"{idno}:officer-entity:{party_name}",
                name=party_name,
                entity_type="legalEntity",
                source_url=link,
            )
            party_type, new = "entity", True
        else:
            party, new = person_for(party_name)
            party_type = "person"
        if new:
            yield party
        interest: dict[str, Any] = {
            "type": officer.get("interest_type") or "unknownInterest",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if officer.get("role"):
            interest["details"] = officer["role"]
        yield make_relationship_statement(
            source_id="asp_moldova",
            local_id=f"{idno}:officer:{index}",
            subject_statement_id=entity["statementId"],
            interested_party_statement_id=party["statementId"],
            interested_party_type=party_type,
            interests=[interest],
            source_url=link,
        )

    for index, founder in enumerate(founders):
        party_name = " ".join(str(founder["name"]).split())
        kind = founder.get("kind") or "person"
        resolved = (founder.get("resolved_idno") or "").strip()
        resolution = founder.get("resolution")
        if kind == "person":
            party, new = person_for(party_name)
            party_type = "person"
        else:
            identifiers = (
                [{"id": resolved, "scheme": MD_IDNO_SCHEME, "schemeName": MD_IDNO_SCHEME_NAME}]
                if resolved
                else []
            )
            party = make_entity_statement(
                source_id="asp_moldova",
                local_id=f"{idno}:founder:{index}:{resolved or party_name}",
                name=(founder.get("resolved_name") or party_name),
                jurisdiction=_MD_JURISDICTION if (resolved or kind == "state") else None,
                identifiers=identifiers,
                entity_type=(
                    "state" if kind == "state" else "registeredEntity" if resolved else "legalEntity"
                ),
                source_url=link,
            )
            if resolved and resolution == "name_match":
                annotate(
                    party,
                    transformation(
                        pointer("recordDetails", "identifiers", 0, "id"),
                        (
                            f"The register names this founder as “{party_name}” and "
                            "publishes no identifier for it. OpenCheck matched the "
                            "name to the only company in the same weekly export whose "
                            "name is identical once diacritics, punctuation and "
                            "legal-form words are removed."
                        ),
                        transformed_content=resolved,
                        creation_date=_today(),
                    ),
                )
            party_type, new = "entity", True
        if new:
            yield party

        pct = founder.get("share_pct")
        share_text = founder.get("share_text")
        if founder.get("joint") and share_text:
            interest = {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": f"Fondator — stake of {share_text} filed jointly with another holder",
            }
        elif pct is not None:
            interest = {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "share": {"exact": pct},
                "details": "Fondator",
            }
        else:
            interest = {
                "type": "unknownInterest",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "Fondator — no share of the capital filed",
            }
        yield make_relationship_statement(
            source_id="asp_moldova",
            local_id=f"{idno}:founder:{index}",
            subject_statement_id=entity["statementId"],
            interested_party_statement_id=party["statementId"],
            interested_party_type=party_type,
            interests=[interest],
            source_url=link,
        )
