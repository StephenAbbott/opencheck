"""Map source payloads to BODS v0.4 statements.

BODS v0.4 statements come in three kinds — entity, person, relationship —
each wrapped in a ``recordDetails`` object. OpenCheck uses deterministic
statement IDs derived from the source adapter ID plus a stable local key,
so re-mapping the same payload always produces the same IDs. This matters
for deduplication across runs and for the visualisation library, which
keys on statement IDs.

Reference: https://standard.openownership.org/en/0.4.0/
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

# ----------------------------------------------------------------------
# PSC "nature of control" → BODS v0.4 interest codelist
# ----------------------------------------------------------------------
#
# UK PSC "natures of control" are strings like
# ``ownership-of-shares-50-to-75-percent-as-trust`` or
# ``voting-rights-25-to-50-percent``. We extract:
#   1. The interest type (shareholding / votingRights / ...).
#   2. The share band, if present.
#
# BODS v0.4 interest types (camelCase): shareholding, votingRights,
# appointmentOfBoard, otherInfluenceOrControl, controlViaCompanyRulesOrArticles,
# controlByLegalFramework, boardMember, boardChair, unknownInterest,
# unpublishedInterest, enjoymentAndUseOfAssets, rightToProfitOrIncomeFromAssets.

_INTEREST_PREFIX = {
    "ownership-of-shares": "shareholding",
    "voting-rights": "votingRights",
    "right-to-appoint-and-remove-directors": "appointmentOfBoard",
    "right-to-appoint-and-remove-members": "appointmentOfBoard",
    "right-to-appoint-and-remove-persons": "appointmentOfBoard",
    "significant-influence-or-control": "otherInfluenceOrControl",
}

_SHARE_BAND_RE = re.compile(r"(\d+)-to-(\d+)-percent")


def _parse_nature(nature: str) -> dict[str, Any]:
    """Return a BODS ``interests`` entry for a single PSC nature-of-control string."""
    lowered = nature.lower()

    interest_type = "otherInfluenceOrControl"
    for prefix, mapped in _INTEREST_PREFIX.items():
        if lowered.startswith(prefix):
            interest_type = mapped
            break

    entry: dict[str, Any] = {
        "type": interest_type,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": True,
        "details": nature,
    }

    band = _SHARE_BAND_RE.search(lowered)
    if band:
        entry["share"] = {
            "minimum": int(band.group(1)),
            "maximum": int(band.group(2)),
            "exclusiveMinimum": True,
        }
    elif "75-to-100-percent" in lowered:
        entry["share"] = {"minimum": 75, "maximum": 100, "exclusiveMinimum": True}

    return entry


# ----------------------------------------------------------------------
# Statement ID generation
# ----------------------------------------------------------------------


def _stable_id(*parts: str) -> str:
    """Deterministic, stable statement/record ID from source parts."""
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"opencheck-{digest[:24]}"


def _today() -> str:
    return date.today().isoformat()


# ----------------------------------------------------------------------
# Statement factories
# ----------------------------------------------------------------------


def make_entity_statement(
    *,
    source_id: str,
    local_id: str,
    name: str,
    jurisdiction: tuple[str, str] | None = None,
    identifiers: Iterable[dict[str, str]] = (),
    founding_date: str | None = None,
    addresses: Iterable[dict[str, str]] = (),
    entity_type: str = "registeredEntity",
    source_url: str | None = None,
) -> dict[str, Any]:
    statement_id = _stable_id(source_id, "entity", local_id)
    record_id = _stable_id(source_id, "entity-record", local_id)

    record_details: dict[str, Any] = {
        "entityType": {"type": entity_type},
        "name": name,
        "identifiers": list(identifiers),
    }
    if jurisdiction:
        record_details["incorporatedInJurisdiction"] = {
            "name": jurisdiction[0],
            "code": jurisdiction[1],
        }
    if founding_date:
        record_details["foundingDate"] = founding_date
    addresses = list(addresses)
    if addresses:
        record_details["addresses"] = addresses

    return {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "statementDate": _today(),
        "recordDetails": record_details,
        "source": _source_block(source_id, source_url),
    }


def make_person_statement(
    *,
    source_id: str,
    local_id: str,
    full_name: str,
    person_type: str = "knownPerson",
    nationalities: Iterable[dict[str, str]] = (),
    birth_date: str | None = None,
    addresses: Iterable[dict[str, str]] = (),
    identifiers: Iterable[dict[str, str]] = (),
    source_url: str | None = None,
) -> dict[str, Any]:
    statement_id = _stable_id(source_id, "person", local_id)
    record_id = _stable_id(source_id, "person-record", local_id)

    record_details: dict[str, Any] = {
        "personType": person_type,
        "names": [{"type": "individual", "fullName": full_name}],
    }
    identifiers = list(identifiers)
    if identifiers:
        record_details["identifiers"] = identifiers
    nationalities = list(nationalities)
    if nationalities:
        record_details["nationalities"] = nationalities
    if birth_date:
        record_details["birthDate"] = birth_date
    addresses = list(addresses)
    if addresses:
        record_details["addresses"] = addresses

    return {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "person",
        "recordStatus": "new",
        "statementDate": _today(),
        "recordDetails": record_details,
        "source": _source_block(source_id, source_url),
    }


def make_relationship_statement(
    *,
    source_id: str,
    local_id: str,
    subject_statement_id: str,
    interested_party_statement_id: str,
    interested_party_type: str = "person",
    interests: Iterable[dict[str, Any]] = (),
    source_url: str | None = None,
) -> dict[str, Any]:
    statement_id = _stable_id(source_id, "relationship", local_id)
    record_id = _stable_id(source_id, "relationship-record", local_id)

    interested_party_key = (
        "describedByPersonStatement"
        if interested_party_type == "person"
        else "describedByEntityStatement"
    )

    return {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "relationship",
        "recordStatus": "new",
        "statementDate": _today(),
        "recordDetails": {
            "subject": {"describedByEntityStatement": subject_statement_id},
            "interestedParty": {interested_party_key: interested_party_statement_id},
            "interests": list(interests),
        },
        "source": _source_block(source_id, source_url),
    }


def _source_block(source_id: str, source_url: str | None) -> dict[str, Any]:
    source_names = {
        "companies_house": "UK Companies House",
        "gleif": "GLEIF",
        "opensanctions": "OpenSanctions",
        "openaleph": "OpenAleph",
        "everypolitician": "EveryPolitician",
        "wikidata": "Wikidata",
    }
    block: dict[str, Any] = {
        "type": "officialRegister" if source_id == "companies_house" else "thirdParty",
        "description": source_names.get(source_id, source_id),
        "retrievedAt": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if source_url:
        block["url"] = source_url
    return block


# ----------------------------------------------------------------------
# Companies House → BODS
# ----------------------------------------------------------------------


@dataclass
class BODSBundle:
    """A bundle of BODS statements about a single subject entity."""

    statements: list[dict[str, Any]] = field(default_factory=list)

    def extend(self, more: Iterable[dict[str, Any]]) -> None:
        self.statements.extend(more)

    def __iter__(self):
        return iter(self.statements)

    def __len__(self):
        return len(self.statements)


def map_companies_house(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Companies House company bundle (profile + officers + PSCs) to BODS.

    Input shape matches ``CompaniesHouseAdapter._fetch_company_bundle`` output:
    ``{"company_number": ..., "profile": {...}, "officers": {...}, "pscs": {...}}``.
    """
    result = BODSBundle()

    number = str(bundle.get("company_number", ""))
    profile = bundle.get("profile") or {}
    pscs = (bundle.get("pscs") or {}).get("items") or []

    # --- Subject entity ---
    company_url = (
        f"https://find-and-update.company-information.service.gov.uk/company/{number}"
    )
    entity = make_entity_statement(
        source_id="companies_house",
        local_id=number,
        name=profile.get("company_name", f"Company {number}"),
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"id": number, "scheme": "GB-COH", "schemeName": "Companies House"}
        ],
        founding_date=profile.get("date_of_creation"),
        addresses=_profile_addresses(profile),
        source_url=company_url,
    )
    result.statements.append(entity)
    entity_sid = entity["statementId"]

    # --- PSCs (individuals + corporate PSCs) ---
    for psc in pscs:
        if psc.get("ceased_on"):
            # Skip ceased PSCs in Phase 1 — future work: emit a closed record.
            continue
        psc_kind = (psc.get("kind") or "").lower()

        if "corporate-entity" in psc_kind or "legal-person" in psc_kind:
            ip = _map_corporate_psc(number, psc, company_url)
            ip_type = "entity"
        elif "individual" in psc_kind:
            ip = _map_individual_psc(number, psc, company_url)
            ip_type = "person"
        else:
            # super-secure-person / unknown — represent as anonymousPerson
            ip = make_person_statement(
                source_id="companies_house",
                local_id=f"{number}:anon:{psc.get('etag', '0')}",
                full_name=psc.get("name", "Anonymous PSC"),
                person_type="anonymousPerson",
                source_url=company_url,
            )
            ip_type = "person"

        result.statements.append(ip)

        natures = psc.get("natures_of_control") or []
        interests = [_parse_nature(n) for n in natures] or [
            {
                "type": "unknownInterest",
                "directOrIndirect": "unknown",
                "beneficialOwnershipOrControl": True,
            }
        ]

        rel = make_relationship_statement(
            source_id="companies_house",
            local_id=f"{number}:{ip['statementId']}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=ip["statementId"],
            interested_party_type=ip_type,
            interests=interests,
            source_url=company_url,
        )
        result.statements.append(rel)

    return result


def _profile_addresses(profile: dict[str, Any]) -> list[dict[str, str]]:
    ra = profile.get("registered_office_address")
    if not ra:
        return []
    parts = [
        ra.get("care_of"),
        ra.get("po_box"),
        ra.get("address_line_1"),
        ra.get("address_line_2"),
        ra.get("locality"),
        ra.get("region"),
        ra.get("postal_code"),
        ra.get("country"),
    ]
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return []
    return [{"type": "registered", "address": joined, "country": ra.get("country", "")}]


def _map_individual_psc(
    company_number: str, psc: dict[str, Any], source_url: str
) -> dict[str, Any]:
    nd = psc.get("name_elements") or {}
    full_name = psc.get("name") or " ".join(
        [nd.get("forename", ""), nd.get("middle_name", ""), nd.get("surname", "")]
    ).strip()

    dob = psc.get("date_of_birth")
    birth_date = None
    if isinstance(dob, dict) and "year" in dob:
        # Companies House exposes month/year only — emit YYYY-MM or YYYY.
        if "month" in dob:
            birth_date = f"{dob['year']:04d}-{dob['month']:02d}"
        else:
            birth_date = f"{dob['year']:04d}"

    nationalities = []
    if psc.get("nationality"):
        nationalities.append({"name": psc["nationality"]})

    # Companies House returns addresses for PSCs under "address".
    address_block = psc.get("address") or {}
    addresses: list[dict[str, str]] = []
    if address_block:
        parts = [
            address_block.get("premises"),
            address_block.get("address_line_1"),
            address_block.get("address_line_2"),
            address_block.get("locality"),
            address_block.get("region"),
            address_block.get("postal_code"),
            address_block.get("country"),
        ]
        joined = ", ".join([p for p in parts if p])
        if joined:
            addresses.append(
                {"type": "service", "address": joined, "country": address_block.get("country", "")}
            )

    etag = psc.get("etag") or psc.get("name", "")
    local_id = f"{company_number}:psc:{etag}"

    return make_person_statement(
        source_id="companies_house",
        local_id=local_id,
        full_name=full_name,
        person_type="knownPerson",
        nationalities=nationalities,
        birth_date=birth_date,
        addresses=addresses,
        source_url=source_url,
    )


def _map_corporate_psc(
    company_number: str, psc: dict[str, Any], source_url: str
) -> dict[str, Any]:
    identification = psc.get("identification") or {}
    identifiers: list[dict[str, str]] = []
    reg_number = identification.get("registration_number")
    reg_country = identification.get("country_registered")
    if reg_number:
        identifiers.append(
            {
                "id": reg_number,
                "scheme": "OC-OPENCORPORATES" if not reg_country else f"REG-{reg_country.upper()[:3]}",
                "schemeName": identification.get("place_registered") or "Registering authority",
            }
        )

    etag = psc.get("etag") or psc.get("name", "")
    local_id = f"{company_number}:psc-corp:{etag}"

    return make_entity_statement(
        source_id="companies_house",
        local_id=local_id,
        name=psc.get("name", "Corporate PSC"),
        jurisdiction=(
            (reg_country, _country_code(reg_country))
            if reg_country
            else None
        ),
        identifiers=identifiers,
        source_url=source_url,
    )


def _country_code(name: str | None) -> str:
    if not name:
        return ""
    # Phase 1: one-liner mapping — full country table deferred.
    lowered = name.lower()
    table = {
        "united kingdom": "GB",
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "northern ireland": "GB",
        "united states": "US",
        "usa": "US",
        "jersey": "JE",
        "guernsey": "GG",
        "isle of man": "IM",
        "ireland": "IE",
        "luxembourg": "LU",
        "netherlands": "NL",
        "cayman islands": "KY",
        "british virgin islands": "VG",
    }
    return table.get(lowered, "")
