"""France — INPI Registre National des Entreprises → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from ..statements import (
    _addr,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# INPI (France — Registre National des Entreprises) → BODS
# ----------------------------------------------------------------------
#
# The RNE API returns a rich JSON document keyed under ``content``.
# Only ``personneMorale`` companies are handled here; ``personnePhysique``
# (sole traders / auto-entrepreneurs) are out of scope for Phase 1.
#
# Identifier scheme: "FR-INSEE"  (follows GB-COH / CH-FDJP / NL-KVK pattern)
# Jurisdiction: France ("FR")
# Source: https://registre-national-entreprises.inpi.fr/
#
# ⚠️  BO restriction (Loi Sapin II / décret 2017-1094): any pouvoir entry
# where ``beneficiaireEffectif == true`` is a beneficial-ownership record
# and MUST NOT be republished without legitimate-interest authorisation.
# These are silently skipped.
#
# Non-BO individuals (``typeDePersonne == "INDIVIDU"`` AND
# ``beneficiaireEffectif == false``) are management / governance officers
# and ARE emitted as BODS person + relationship statements.  The
# ``roleEntreprise`` integer code from ``individu.descriptionPersonne``
# drives the BODS interest type (see ``_inpi_role_interest_type``) and
# its French Libellé from the INPI data dictionary is passed into the
# ``details`` field of the interest so the source register's precise role
# description is preserved.
#
# Field mapping for the relationship interest:
#   roleEntreprise code → interest type (seniorManagingOfficial or
#                         otherInfluenceOrControl)
#   roleEntreprise label (French) → details
#   dateEffetRoleDeclarant → startDate   (ISO date string; the sibling
#                                          boolean dateEffetRoleDeclarantPresent
#                                          indicates whether this date is present)

# ---------------------------------------------------------------------------
# INPI roleEntreprise codelist (source: INPI data dictionary, tab roleEntreprise)
# ---------------------------------------------------------------------------

_INPI_ROLE_LABELS: dict[int, str] = {
    11: "Membre",
    13: "Contrôleur de gestion",
    14: "Contrôleur des comptes",
    23: "Autre associé majoritaire",
    28: "Gérant et associé indéfiniment et solidairement responsable",
    29: "Gérant et associé indéfiniment responsable",
    30: "Gérant",
    40: "Liquidateur",
    41: "Associé unique (qui participe à l'activité EURL)",
    51: "Président du conseil d'administration",
    52: "Président du directoire",
    53: "Directeur Général",
    55: "Dirigeant à l'étranger d'une personne morale étrangère",
    56: "Dirigeant en France d'une personne morale étrangère",
    60: "Président du conseil d'administration et directeur général",
    61: "Président du conseil de surveillance",
    63: "Membre du directoire",
    64: "Membre du conseil de surveillance",
    65: "Administrateur",
    66: "Personne ayant le pouvoir d'engager à titre habituel la société",
    67: "Personne ayant le pouvoir d'engager l'établissement",
    69: "Directeur général unique de SA à directoire",
    70: "Directeur général délégué",
    71: "Commissaire aux comptes titulaire",
    72: "Commissaire aux comptes suppléant",
    73: "Président de SAS",
    74: "Associé indéfiniment et solidairement responsable",
    75: "Associé indéfiniment responsable",
    76: "Représentant social d'une entreprise personne étrangère sans établissement en France",
    77: "Représentant fiscal d'une entreprise personne étrangère sans établissement en France",
    82: "Indivisaire",
    86: "Exploitant pour le compte de l'indivision",
    90: "Personne physique, exploitant en commun",
    94: "Membre non salarié participant aux travaux",
    95: "Associé qui participe à la gestion",
    96: "Associé non salarié",
    97: "Mandataire ad hoc",
    98: "Administrateur provisoire",
    99: "Autre",
    100: "Repreneur",
    101: "Entrepreneur",
    103: "Suppléant",
    104: "Personne chargée du contrôle",
    105: "Personne décisionnaire désignée",
    106: "Comptable",
    107: "Héritier indivisaire",
    108: "Loueur",
    109: "Mandataire fiscal",
    110: "Vice-Président",
    111: "Vice-Président du Directoire",
    120: "Vice-Président du Conseil d'Orientation et de Surveillance",
    121: "Président du Conseil d'Orientation et de Surveillance",
    122: "Membre du Conseil d'Orientation et de Surveillance",
    130: "Associé unique qui récupère le patrimoine",
    131: "Associé commandité",
    132: "Associé commanditaire",
    135: "Administrateurs représentant les salariés",
    140: "Président de l'EPIC",
    150: "Avocat",
    200: "Fiduciaire",
    201: "Dirigeant",
    202: "Représentant de l'assujetti unique",
    203: "Membre bénéficiant d'un mandat général d'administration",
    204: "Personne capable d'engager l'entité",
    205: "Président",
    206: "Directeur",
    207: "Trésorier",
    208: "Secrétaire",
    209: "Secrétaire général",
    210: "Membre du conseil syndical",
    211: "Président du conseil syndical",
    212: "Personne désignée par les statuts",
    213: "Vice-Président du conseil de surveillance",
    214: "Personne ayant le pouvoir d'engager à titre habituel l'entité",
    215: "Personne ayant le pouvoir de représenter l'entité",
    216: "Trésorier adjoint",
    217: "Secrétaire adjoint",
    218: "Membre de l'organe collégial de contrôle",
    219: "Président de l'organe collégial de contrôle",
    220: "Auditeur de durabilité",
    231: "Gouverneur",
    232: "Premier sous-gouverneur",
    233: "Deuxième sous-gouverneur",
    234: "Conseiller général",
}

# External professional service providers — mapped to otherInfluenceOrControl.
# All other roleEntreprise codes default to seniorManagingOfficial.
_INPI_OTHER_INFLUENCE_CODES: frozenset[int] = frozenset({
    14,   # Contrôleur des comptes
    71,   # Commissaire aux comptes titulaire
    72,   # Commissaire aux comptes suppléant
    77,   # Représentant fiscal d'une entreprise personne étrangère sans établissement en France
    109,  # Mandataire fiscal
    150,  # Avocat
    220,  # Auditeur de durabilité
})


def _inpi_role_interest_type(role_code: int | str | None) -> str:
    """Return the BODS v0.4 interest type for an INPI roleEntreprise code.

    External professional service providers (auditors, lawyers, fiscal
    representatives) → ``otherInfluenceOrControl``.
    All other roles (and unknown codes) → ``seniorManagingOfficial``.
    """
    try:
        code = int(role_code) if role_code is not None else None
    except (ValueError, TypeError):
        code = None
    return (
        "otherInfluenceOrControl"
        if code in _INPI_OTHER_INFLUENCE_CODES
        else "seniorManagingOfficial"
    )


def map_inpi(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an INPI RNE fetch bundle to BODS v0.4 statements.

    Yields an entity statement for the company, followed by person +
    relationship statements for each active non-BO individual in
    ``composition.pouvoirs`` (``typeDePersonne == "INDIVIDU"`` and
    ``beneficiaireEffectif == false``).

    Pouvoirs where ``beneficiaireEffectif == true`` are BO records subject
    to the Loi Sapin II / décret 2017-1094 redistribution restriction and
    are silently skipped.

    Returns an empty iterable for stub bundles (including non-diffusable
    companies), missing company data, or missing entity name.

    The RNE API response wraps the rich company data under ``formality.content``
    and exposes a condensed ``identite`` block at the top level.  Actual
    structure (abbreviated):

    .. code-block:: python

        {
            "source_id": "inpi",
            "siren": "055804124",
            "company": {
                "diffusionINSEE": "O",
                "identite": {               # top-level condensed block
                    "entreprise": {"denomination": "BOLLORE SE", ...}
                },
                "formality": {
                    "content": {
                        "personneMorale": {
                            "adresseEntreprise": {
                                "adresse": {
                                    "typeVoie": "QUAI", "voie": "DE DION BOUTON",
                                    "numVoie": "31", "codePostal": "92800",
                                    "commune": "PUTEAUX", ...
                                }
                            },
                            "composition": {
                                "pouvoirs": [
                                    {
                                        "typeDePersonne": "INDIVIDU",
                                        "beneficiaireEffectif": False,
                                        "individu": {
                                            "descriptionPersonne": {
                                                "nom": "DUPONT",
                                                "prenoms": ["JEAN"],
                                                "nationalite": "Française",
                                                "roleEntreprise": 30,
                                                "dateEffetRoleDeclarant": "2020-01-15",
                                                "dateEffetRoleDeclarantPresent": True,
                                            }
                                        },
                                    }
                                ]
                            },
                        },
                        "natureCreation": {"dateCreation": "1990-09-13", ...},
                    }
                },
            },
            "is_stub": False,
        }
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    # The normalised SIREN is always put in the bundle by InpiAdapter.fetch;
    # do not fall back to company["siren"] so that the early-exit is reliable.
    siren: str = bundle.get("siren") or ""
    if not siren:
        return

    # The RNE API nests the full company data under formality.content.
    formality: dict[str, Any] = company.get("formality") or {}
    content: dict[str, Any] = formality.get("content") or {}
    pm: dict[str, Any] = content.get("personneMorale") or {}
    if not pm:
        # personnePhysique (sole trader) — out of scope for Phase 1.
        return

    # Company name — prefer the top-level identite block (condensed but stable),
    # fall back to the nested personneMorale.identite path.
    top_identite: dict[str, Any] = company.get("identite") or {}
    name: str = (
        (top_identite.get("entreprise") or {}).get("denomination")
        or (pm.get("identite") or {}).get("entreprise", {}).get("denomination")
        or ""
    )
    if not name:
        return

    # Founding date — dateCreation is ISO 8601 (YYYY-MM-DD) from the RNE.
    nature_creation: dict[str, Any] = content.get("natureCreation") or {}
    founding_date: str | None = nature_creation.get("dateCreation") or None

    # Identifier: FR-INSEE (SIREN — Système d'Identification du Répertoire des Entreprises)
    identifiers: list[dict[str, str]] = [
        {
            "id": siren,
            "scheme": "FR-INSEE",
            "schemeName": "INSEE — Système d'Identification du Répertoire des Entreprises",
        }
    ]

    # Address from the first registered address block.
    addr_block: dict[str, Any] = (pm.get("adresseEntreprise") or {}).get("adresse") or {}
    addresses = _inpi_address(addr_block)

    source_url = (
        f"https://registre-national-entreprises.inpi.fr/api/companies/{siren}"
    )

    entity = make_entity_statement(
        source_id="inpi",
        local_id=siren,
        name=name,
        jurisdiction=("France", "FR"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    yield entity
    entity_sid = entity["statementId"]

    # Emit person + relationship statements for non-BO INDIVIDU pouvoirs.
    seen_sids: set[str] = {entity_sid}
    pouvoirs = (pm.get("composition") or {}).get("pouvoirs") or []
    for pouvoir in pouvoirs:
        if (pouvoir.get("typeDePersonne") or "").upper() != "INDIVIDU":
            continue
        if pouvoir.get("beneficiaireEffectif") is True:
            # BO record — redistribution restricted; skip.
            continue
        for stmt in _inpi_individu_statements(siren, pouvoir, entity_sid, source_url, seen_sids):
            seen_sids.add(stmt["statementId"])
            yield stmt


def _inpi_individu_statements(
    siren: str,
    pouvoir: dict[str, Any],
    entity_sid: str,
    source_url: str,
    seen_sids: set[str],
) -> list[dict[str, Any]]:
    """Emit person + relationship statements for one non-BO INDIVIDU pouvoir.

    Returns an empty list if the person's name cannot be determined.

    The ``roleEntreprise`` code drives the BODS interest type
    (``seniorManagingOfficial`` or ``otherInfluenceOrControl``) and its
    French Libellé from the INPI data dictionary is passed into ``details``.

    ``dateEffetRoleDeclarant`` (ISO date string) maps to ``startDate``.
    The sibling boolean ``dateEffetRoleDeclarantPresent`` simply indicates
    whether that date field is populated; it is not mapped directly.

    The same person may hold multiple roles: a single person statement is
    emitted (keyed on siren + name), but a separate relationship statement
    is emitted per role so all roles are captured.
    """
    individu: dict[str, Any] = pouvoir.get("individu") or {}
    dp: dict[str, Any] = individu.get("descriptionPersonne") or {}

    nom = (dp.get("nom") or "").strip()
    prenoms_raw = dp.get("prenoms") or []
    prenoms: list[str] = prenoms_raw if isinstance(prenoms_raw, list) else [str(prenoms_raw)]
    prenom_str = " ".join(p for p in prenoms if p).strip()

    # Build full name: "PRENOMS NOM" (French convention in the RNE).
    full_name = f"{prenom_str} {nom}".strip() if prenom_str else nom
    if not full_name:
        return []

    nom_usage = (dp.get("nomUsage") or "").strip()
    nationalite = (dp.get("nationalite") or "").strip()

    # roleEntreprise may be an int or a string in the raw payload;
    # fall back to the top-level pouvoir dict if not on descriptionPersonne.
    role_raw = dp.get("roleEntreprise") if "roleEntreprise" in dp else pouvoir.get("roleEntreprise")
    try:
        role_code: int | None = int(role_raw) if role_raw is not None else None
    except (ValueError, TypeError):
        role_code = None

    role_label = _INPI_ROLE_LABELS.get(role_code, "") if role_code is not None else ""
    interest_type = _inpi_role_interest_type(role_code)

    # dateEffetRoleDeclarant → BODS startDate.
    start_date: str | None = (dp.get("dateEffetRoleDeclarant") or "").strip() or None

    # Person local_id: stable per (siren, nom, prenoms) across multiple roles.
    person_key = f"{siren}|individu|{nom}|{prenom_str}"
    person_local_id = (
        f"inpi:{siren}:individu:{hashlib.sha256(person_key.encode()).hexdigest()[:16]}"
    )

    nationalities: list[dict[str, str]] = [{"name": nationalite}] if nationalite else []
    person = make_person_statement(
        source_id="inpi",
        local_id=person_local_id,
        full_name=full_name,
        person_type="knownPerson",
        nationalities=nationalities,
        source_url=source_url,
    )
    person_sid = person["statementId"]

    stmts: list[dict[str, Any]] = []
    if person_sid not in seen_sids:
        stmts.append(person)

    # Relationship local_id: stable per (siren, nom, prenoms, role_code) so
    # the same person with multiple roles produces distinct relationships.
    rel_key = f"{siren}|individu|{nom}|{prenom_str}|{role_code}"
    rel_local_id = (
        f"inpi:{siren}:individu-rel:{hashlib.sha256(rel_key.encode()).hexdigest()[:16]}"
    )

    # details: French role label + nom d'usage if present.
    details_parts: list[str] = []
    if role_label:
        details_parts.append(role_label)
    elif role_code is not None:
        details_parts.append(f"Code {role_code}")
    if nom_usage:
        details_parts.append(f"Nom d'usage : {nom_usage}")
    details = "; ".join(details_parts) if details_parts else "Pouvoir (INPI RNE)"

    interest: dict[str, Any] = {
        "type": interest_type,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": False,
        "details": details,
    }
    if start_date:
        interest["startDate"] = start_date

    rel = make_relationship_statement(
        source_id="inpi",
        local_id=rel_local_id,
        subject_statement_id=entity_sid,
        interested_party_statement_id=person_sid,
        interested_party_type="person",
        interests=[interest],
        source_url=source_url,
        # start_date is when the role began (already interest.startDate), not
        # when the RNE declared it or when OpenCheck published — same class of
        # error as the Companies House officer path above.
    )
    rel_sid = rel["statementId"]
    if rel_sid not in seen_sids:
        stmts.append(rel)

    return stmts


def _inpi_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a raw RNE adresse block.

    The actual API field for the street name is ``voie``, not ``libelleVoie``
    (which was documented but not present in live responses).
    """
    if not block:
        return []
    parts = [
        block.get("numVoie"),
        block.get("indiceRepetition"),
        block.get("typeVoie"),
        block.get("voie") or block.get("libelleVoie"),  # live field is "voie"
        block.get("complementLocalisation"),
        block.get("codePostal"),
        block.get("commune") or block.get("libelleCommune"),
    ]
    joined = " ".join(p for p in parts if p)
    if not joined:
        return []
    return [_addr("registered", joined, "FR")]
