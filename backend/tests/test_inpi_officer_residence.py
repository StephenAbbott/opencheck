"""INPI (France): an officer's country of residence from the RNE home address.

From the country-of-residence ticket's live payload check (3 Nov 2026). The
RNE publishes, on every non-BO officer (``pouvoirs[].individu``), the country
of their home address, ``adresseDomicile.codePays`` (ISO 3166-1 alpha-3) and
``adresseDomicile.pays`` (the French name), with the street withheld. Danone
SA: 11 of 11 officers carried a country. The mapper read none of it; and
``descriptionPersonne.nationalite`` was empty on every officer checked, so
these people had no country at all. Only the country is read.
"""

from __future__ import annotations

import copy
from typing import Any

from opencheck.bods import map_inpi, validate_shape


def _company(*pouvoirs: dict[str, Any]) -> dict[str, Any]:
    return {
        "diffusionINSEE": "O",
        "siren": "552032534",
        "identite": {"entreprise": {"siren": "552032534", "denomination": "DANONE"}},
        "formality": {
            "siren": "552032534",
            "content": {
                "personneMorale": {
                    "adresseEntreprise": {},
                    "composition": {"pouvoirs": list(pouvoirs)},
                },
                "natureCreation": {"dateCreation": "1899-12-31"},
            },
        },
    }


def _officer(
    nom: str, domicile: dict[str, Any] | None, *, bo: bool = False
) -> dict[str, Any]:
    individu: dict[str, Any] = {
        "descriptionPersonne": {"nom": nom, "prenoms": ["ANNE"], "roleEntreprise": 65},
    }
    if domicile is not None:
        individu["adresseDomicile"] = domicile
    return {"typeDePersonne": "INDIVIDU", "beneficiaireEffectif": bo, "individu": individu}


# The live shape: street fields reduced to *Present flags, the commune kept.
_SWISS_HOME = {
    "pays": "SUISSE", "codePays": "CHE", "commune": "GENEVE",
    "voiePresent": False, "numVoiePresent": False, "codePostalPresent": False,
}


def _people(company: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stmts = list(
        map_inpi({"source_id": "inpi", "siren": "552032534", "company": company,
                  "is_stub": False})
    )
    assert validate_shape(stmts) == []
    return {
        s["recordDetails"]["names"][0]["fullName"]: s
        for s in stmts
        if s["recordType"] == "person"
    }


def test_an_officer_carries_the_country_of_their_home_address_and_nothing_else() -> None:
    (person,) = _people(_company(_officer("MARTIN", _SWISS_HOME))).values()
    assert person["recordDetails"]["addresses"] == [
        {"type": "residence", "country": {"name": "Switzerland", "code": "CH"}}
    ]
    # The filed code is the ISO code itself: nothing to annotate.
    assert not person.get("annotations")
    # No commune, no postcode: the country is the only part of the address read.
    assert "GENEVE" not in repr(person)


def test_a_french_name_with_no_code_is_kept_as_filed_and_said_so() -> None:
    home = {"pays": "ETATS-UNIS D'AMERIQUE", "commune": "NEW YORK"}
    (person,) = _people(_company(_officer("SMITH", home))).values()
    (address,) = person["recordDetails"]["addresses"]
    assert address == {"type": "residence", "country": {"name": "ETATS-UNIS D'AMERIQUE"}}
    (note,) = person["annotations"]
    assert note["motivation"] == "commenting"
    assert note["statementPointerTarget"] == "/recordDetails/addresses/0/country"
    assert "Registre National des Entreprises" in note["description"]


def test_no_home_address_means_no_address() -> None:
    (person,) = _people(_company(_officer("DUPONT", None))).values()
    assert not person["recordDetails"].get("addresses")
    (person,) = _people(_company(_officer("DUPONT", {"codePays": "", "pays": ""}))).values()
    assert not person["recordDetails"].get("addresses")


def test_a_beneficial_owner_row_is_still_never_read() -> None:
    company = _company(
        _officer("MARTIN", _SWISS_HOME),
        _officer("SECRET", {"pays": "BELGIQUE", "codePays": "BEL"}, bo=True),
    )
    people = _people(company)
    assert list(people) == ["ANNE MARTIN"]
    assert "BEL" not in repr(people) and "Belgium" not in repr(people)


def test_the_residence_does_not_change_the_persons_record_id() -> None:
    before = _people(_company(_officer("MARTIN", None)))["ANNE MARTIN"]
    after = _people(_company(_officer("MARTIN", copy.deepcopy(_SWISS_HOME))))["ANNE MARTIN"]
    assert before["recordId"] == after["recordId"]
    assert before["statementId"] != after["statementId"]
