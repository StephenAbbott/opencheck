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

import pycountry

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
        "opencorporates": "OpenCorporates",
        "opensanctions": "OpenSanctions",
        "openaleph": "OpenAleph",
        "everypolitician": "EveryPolitician",
        "wikidata": "Wikidata",
    }
    _official_registers = {"companies_house", "opencorporates"}
    block: dict[str, Any] = {
        "type": "officialRegister" if source_id in _official_registers else "thirdParty",
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


# Country strings Companies House uses in PSC identification blocks to
# indicate a UK-registered entity (mirrors the set in the CH source adapter).
_CH_UK_COUNTRY_STRINGS: frozenset[str] = frozenset({
    "united kingdom", "england", "scotland", "wales", "northern ireland", "gb", "uk",
})


def map_companies_house(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Companies House bundle to BODS.

    Two dispatch shapes:

    * ``{"company_number": ..., "profile": ..., "officers": ..., "pscs": ...,
         "related_companies": {...}}``
      — produced by ``_fetch_company_bundle``. Yields the company entity
      + a personStatement / entityStatement per active PSC, plus an
      ownership-or-control relationship per PSC. UK corporate PSC chains
      (up to ``max_depth`` hops, fetched recursively by the adapter) are
      emitted from ``related_companies``.
    * ``{"officer_id": ..., "appointments": {...}}`` — produced by
      ``_fetch_officer_bundle``. Yields the officer as a
      personStatement, plus a "boardMember" relationship for every
      appointment (both current and historical).
    """
    if "officer_id" in bundle:
        return _map_companies_house_officer(bundle)

    result = BODSBundle()
    # Track statement IDs emitted so far to avoid duplicates when a UK
    # corporate PSC appears both as a PSC reference and as a related company.
    seen_sids: set[str] = set()

    _emit_company_statements(bundle, result, seen_sids)

    for sub_bundle in (bundle.get("related_companies") or {}).values():
        _emit_company_statements(sub_bundle, result, seen_sids)

    return result


def _emit_company_statements(
    bundle: dict[str, Any],
    result: BODSBundle,
    seen_sids: set[str],
) -> None:
    """Emit entity + PSC statements for one company bundle into *result*.

    *seen_sids* is updated in place; statements whose ``statementId`` is
    already present are silently skipped so the same entity/relationship is
    never duplicated across the root + related-company passes.
    """
    number = str(bundle.get("company_number", ""))
    profile = bundle.get("profile") or {}
    pscs = (bundle.get("pscs") or {}).get("items") or []

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
    entity_sid = entity["statementId"]
    if entity_sid not in seen_sids:
        result.statements.append(entity)
        seen_sids.add(entity_sid)

    for psc in pscs:
        if psc.get("ceased_on"):
            # Skip ceased PSCs in Phase 1 — future work: emit a closed record.
            continue
        psc_kind = (psc.get("kind") or "").lower()

        if "corporate-entity" in psc_kind or "legal-person" in psc_kind:
            # Detect UK CH registration numbers so the entity statementId
            # produced here aligns with the statementId the related-company
            # pass emits for the same company (both use local_id = reg_no).
            ident = psc.get("identification") or {}
            reg_no = (ident.get("registration_number") or "").strip()
            reg_country = (ident.get("country_registered") or "").lower().strip()
            uk_number = (
                reg_no
                if (
                    len(reg_no) == 8
                    and reg_no.isalnum()
                    and reg_country in _CH_UK_COUNTRY_STRINGS
                )
                else None
            )
            ip = _map_corporate_psc(number, psc, company_url, uk_number=uk_number)
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

        ip_sid = ip["statementId"]
        if ip_sid not in seen_sids:
            result.statements.append(ip)
            seen_sids.add(ip_sid)

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
            local_id=f"{number}:{ip_sid}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=ip_sid,
            interested_party_type=ip_type,
            interests=interests,
            source_url=company_url,
        )
        rel_sid = rel["statementId"]
        if rel_sid not in seen_sids:
            result.statements.append(rel)
            seen_sids.add(rel_sid)


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


def _map_companies_house_officer(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Companies House officer-appointments bundle to BODS.

    The officer becomes a single ``personStatement``; each appointment
    becomes an ``entityStatement`` (the company appointed-to) plus a
    ``relationship`` statement with a ``boardMember`` interest. Resigned
    appointments carry ``endDate`` so consumers can distinguish current
    from historical board membership.

    The Companies House appointments endpoint returns the officer's
    canonical name + DOB + nationality + occupation + country of
    residence on the *appointments envelope* — those fields are used
    for the personStatement; the per-appointment block carries
    appointment-specific data.
    """
    result = BODSBundle()

    officer_id = str(bundle.get("officer_id", ""))
    appointments = bundle.get("appointments") or {}
    items = appointments.get("items") or []

    full_name = appointments.get("name") or "Unknown officer"
    dob = appointments.get("date_of_birth")
    birth_date = None
    if isinstance(dob, dict) and "year" in dob:
        if "month" in dob:
            birth_date = f"{dob['year']:04d}-{dob['month']:02d}"
        else:
            birth_date = f"{dob['year']:04d}"

    nationalities: list[dict[str, str]] = []
    nationality = appointments.get("nationality")
    if nationality:
        nationalities.append({"name": nationality})

    person_url = (
        f"https://find-and-update.company-information.service.gov.uk/officers/"
        f"{officer_id}/appointments"
    )

    person = make_person_statement(
        source_id="companies_house",
        local_id=f"officer:{officer_id}",
        full_name=full_name,
        person_type="knownPerson",
        nationalities=nationalities,
        birth_date=birth_date,
        identifiers=[
            {
                "id": officer_id,
                "scheme": "GB-COH-OFFICER",
                "schemeName": "Companies House officer id",
            }
        ],
        source_url=person_url,
    )
    result.statements.append(person)
    person_sid = person["statementId"]

    for idx, appointment in enumerate(items):
        appointed_to = appointment.get("appointed_to") or {}
        company_number = str(appointed_to.get("company_number") or f"unknown-{idx}")
        company_name = (
            appointed_to.get("company_name")
            or f"Company {company_number}"
        )
        company_url = (
            f"https://find-and-update.company-information.service.gov.uk/company/"
            f"{company_number}"
        )

        entity = make_entity_statement(
            source_id="companies_house",
            local_id=f"officer:{officer_id}:co:{company_number}",
            name=company_name,
            jurisdiction=("United Kingdom", "GB"),
            identifiers=[
                {
                    "id": company_number,
                    "scheme": "GB-COH",
                    "schemeName": "Companies House",
                }
            ],
            source_url=company_url,
        )
        result.statements.append(entity)
        entity_sid = entity["statementId"]

        # Map the officer role to a BODS interest. Directors and
        # secretaries become boardMember; LLP members are otherInfluence
        # (no board) — but everyone gets the appointment surfaced.
        role = (appointment.get("officer_role") or "").lower()
        if "director" in role:
            interest_type = "boardMember"
        elif "chair" in role:
            interest_type = "boardChair"
        else:
            interest_type = "otherInfluenceOrControl"

        details_bits = [appointment.get("officer_role") or "appointment"]
        if appointment.get("appointed_on"):
            details_bits.append(f"from {appointment['appointed_on']}")
        if appointment.get("resigned_on"):
            details_bits.append(f"to {appointment['resigned_on']}")

        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "details": " ".join(details_bits),
        }
        if appointment.get("appointed_on"):
            interest["startDate"] = appointment["appointed_on"]
        if appointment.get("resigned_on"):
            interest["endDate"] = appointment["resigned_on"]

        rel = make_relationship_statement(
            source_id="companies_house",
            local_id=f"officer-rel:{officer_id}:{company_number}:{idx}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=person_url,
        )
        result.statements.append(rel)

    return result


def _map_corporate_psc(
    company_number: str,
    psc: dict[str, Any],
    source_url: str,
    *,
    uk_number: str | None = None,
) -> dict[str, Any]:
    """Map a corporate / legal-person PSC to a BODS entityStatement.

    When *uk_number* is provided it is used as the ``local_id`` so that the
    ``statementId`` produced here matches the one emitted when the same
    company is processed as a related-company root (both sides use
    ``local_id = company_number``).  Without this alignment, the dagre
    visualiser can't connect the PSC node to the full ownership subgraph.
    """
    identification = psc.get("identification") or {}
    identifiers: list[dict[str, str]] = []
    reg_number = identification.get("registration_number")
    reg_country = identification.get("country_registered")
    if reg_number:
        alpha2 = _country_code(reg_country)
        place = (identification.get("place_registered") or "").lower()
        # Map well-known registries to their canonical BODS scheme codes;
        # fall back to REG-{alpha2} (2-letter, not the old 3-letter truncation)
        # so reconcilers can bridge to other sources on the same identifier.
        if alpha2 == "GB" and ("companies house" in place or not place):
            scheme = "GB-COH"
            scheme_name = "UK Companies House"
        elif alpha2:
            scheme = f"REG-{alpha2}"
            scheme_name = identification.get("place_registered") or f"{alpha2} company register"
        else:
            scheme = "REG"
            scheme_name = identification.get("place_registered") or "Company register"
        identifiers.append(
            {
                "id": reg_number,
                "scheme": scheme,
                "schemeName": scheme_name,
            }
        )

    # Use the UK company number as local_id when available so that the
    # statementId here aligns with the entity statement emitted by the
    # related-company pass for the same company.
    if uk_number:
        local_id = uk_number
    else:
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
    """Resolve a free-text country name to an ISO 3166-1 alpha-2 code.

    Uses pycountry for the bulk of lookups (handles ~250 countries and
    many common aliases such as "Cayman Islands", "British Virgin Islands",
    "Isle of Man", etc.).  A small overrides dict handles names that
    pycountry cannot resolve — primarily UK constituent nations and common
    abbreviations that companies registries use but that aren't in ISO 3166-1.
    """
    if not name:
        return ""
    stripped = name.strip()
    # Already a two-letter code — pass through normalised.
    if len(stripped) == 2 and stripped.isalpha():
        return stripped.upper()
    # Overrides for names pycountry can't look up.
    _OVERRIDES: dict[str, str] = {
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "northern ireland": "GB",
        "uae": "AE",
    }
    override = _OVERRIDES.get(stripped.lower())
    if override:
        return override
    try:
        return pycountry.countries.lookup(stripped).alpha_2
    except LookupError:
        return ""


# ----------------------------------------------------------------------
# GLEIF → BODS
# ----------------------------------------------------------------------
#
# Mirrors OpenOwnership's canonical GLEIF → BODS pipeline
# (https://github.com/openownership/bods-gleif-pipeline):
#
# * Subject entity: one ``registeredEntity`` statement, identified by LEI
#   (``XI-LEI``) and by the GLEIF ``RegistrationAuthority`` scheme when
#   the record carries a ``registeredAt.id`` (e.g. ``RA000585`` for UK
#   Companies House).
# * Each accounting consolidation parent (direct / ultimate) → one entity
#   statement for the parent + one relationship statement with an
#   ``otherInfluenceOrControl`` interest. ``beneficialOwnershipOrControl``
#   is always ``false`` — LEI-RR captures accounting consolidation, not
#   beneficial ownership.
# * Reporting exceptions (``NO_LEI``, ``NATURAL_PERSONS``,
#   ``NON_CONSOLIDATING`` etc.) produce a bridging statement
#   (``anonymousEntity`` or ``unknownPerson``) plus a relationship whose
#   interest ``details`` carry the GLEIF exception reason — so companies
#   that report "my parent is a natural person" don't silently disappear.

# Exception reason → (interested_party_type, person_type or entity_type,
#                    human-readable details).
_GLEIF_EXCEPTION_REASONS = {
    "NATURAL_PERSONS": (
        "person",
        "unknownPerson",
        "GLEIF reporting exception: parent is one or more natural persons",
    ),
    "NO_KNOWN_PERSON": (
        "person",
        "unknownPerson",
        "GLEIF reporting exception: no known person can be identified",
    ),
    "NO_LEI": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: parent exists but has no LEI",
    ),
    "NON_CONSOLIDATING": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: parent does not consolidate the subject",
    ),
    "NON_PUBLIC": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: parent is known but not publicly disclosable",
    ),
    "BINDING_LEGAL_COMMITMENTS": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: binding legal commitments prevent disclosure",
    ),
}


def map_gleif(bundle: dict[str, Any]) -> BODSBundle:
    """Map a GLEIF adapter bundle to BODS v0.4 statements.

    Input shape matches ``GleifAdapter.fetch`` output:

        {
          "lei": ...,
          "record": {...},                            # Level 1 CDF
          "direct_parent": {...} | None,              # Level 2 RR
          "ultimate_parent": {...} | None,            # Level 2 RR
          "direct_parent_exception": {...} | None,    # Reporting exception
          "ultimate_parent_exception": {...} | None,  # Reporting exception
        }
    """
    result = BODSBundle()

    record = bundle.get("record") or {}
    subject_attrs = record.get("attributes") or record
    subject_entity_block = subject_attrs.get("entity") or {}
    lei = (
        bundle.get("lei")
        or subject_attrs.get("lei")
        or record.get("id")
        or ""
    )
    if not lei:
        return result

    subject_url = f"https://www.gleif.org/lei/{lei}"
    subject_statement = _gleif_entity_statement(lei, subject_entity_block, subject_url)
    result.statements.append(subject_statement)
    subject_sid = subject_statement["statementId"]

    for kind, parent, exception in (
        (
            "direct",
            bundle.get("direct_parent"),
            bundle.get("direct_parent_exception"),
        ),
        (
            "ultimate",
            bundle.get("ultimate_parent"),
            bundle.get("ultimate_parent_exception"),
        ),
    ):
        if parent:
            result.extend(_gleif_parent_statements(lei, subject_sid, kind, parent))
        elif exception:
            result.extend(
                _gleif_exception_statements(lei, subject_sid, kind, exception)
            )

    return result


def _gleif_parent_statements(
    lei: str, subject_sid: str, kind: str, parent: dict[str, Any]
) -> list[dict[str, Any]]:
    parent_attrs = parent.get("attributes") or parent
    parent_entity_block = parent_attrs.get("entity") or {}
    parent_lei = parent_attrs.get("lei") or parent.get("id") or ""
    if not parent_lei:
        return []

    parent_url = f"https://www.gleif.org/lei/{parent_lei}"
    parent_statement = _gleif_entity_statement(
        parent_lei, parent_entity_block, parent_url
    )
    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent:{parent_lei}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=parent_statement["statementId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": (
                    f"GLEIF Level 2 {kind}-parent (accounting consolidation)"
                ),
            }
        ],
        source_url=parent_url,
    )
    return [parent_statement, rel]


def _gleif_exception_statements(
    lei: str, subject_sid: str, kind: str, exception: dict[str, Any]
) -> list[dict[str, Any]]:
    """Emit bridging anonymousEntity / unknownPerson + relationship for an exception."""
    attrs = exception.get("attributes") or exception
    reason = (attrs.get("exceptionReason") or "").upper()
    ip_type, ip_subtype, details = _GLEIF_EXCEPTION_REASONS.get(
        reason,
        (
            "entity",
            "unknownEntity",
            f"GLEIF reporting exception: {reason or 'unspecified reason'}",
        ),
    )

    bridge_local_id = f"{lei}:{kind}-parent-exception:{reason or 'unspecified'}"
    if ip_type == "person":
        bridge = make_person_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            full_name="Unknown parent (GLEIF reporting exception)",
            person_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )
    else:
        bridge = make_entity_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            name="Unknown parent (GLEIF reporting exception)",
            entity_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )

    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent-exception-rel:{reason or 'unspecified'}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=bridge["statementId"],
        interested_party_type=ip_type,
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": details,
            }
        ],
        source_url=f"https://www.gleif.org/lei/{lei}",
    )
    return [bridge, rel]


def _gleif_entity_statement(
    lei: str, entity_block: dict[str, Any], source_url: str
) -> dict[str, Any]:
    legal_name = (entity_block.get("legalName") or {}).get("name") or f"LEI {lei}"
    jurisdiction_code = entity_block.get("jurisdiction")
    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        jurisdiction = _gleif_jurisdiction(jurisdiction_code)

    identifiers: list[dict[str, str]] = [
        {
            "id": lei,
            "scheme": "XI-LEI",
            "schemeName": "Global Legal Entity Identifier Index",
        }
    ]

    # GLEIF records the registration authority in ``entity.registeredAt``:
    #   {"id": "RA000585", "other": null}   # standard scheme
    #   {"id": "RA999999", "other": "My Authority"}   # free-text scheme
    # OpenOwnership's pipeline preserves the RA code as ``scheme`` so the
    # identifier can bridge to Companies House, OpenCorporates, etc.
    registered_as = entity_block.get("registeredAs")
    registered_at = entity_block.get("registeredAt") or {}
    ra_id = registered_at.get("id")
    ra_other = registered_at.get("other")
    if registered_as and ra_id:
        identifiers.append(
            {
                "id": registered_as,
                "scheme": ra_id,
                "schemeName": ra_other or f"GLEIF Registration Authority {ra_id}",
            }
        )

    addresses = _gleif_addresses(entity_block)

    return make_entity_statement(
        source_id="gleif",
        local_id=lei,
        name=legal_name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        source_url=source_url,
    )


def _gleif_jurisdiction(code: str) -> tuple[str, str]:
    """Resolve a GLEIF jurisdiction code to ``(name, code)``.

    GLEIF uses ISO 3166-1 alpha-2 codes at the country level and
    ISO 3166-2 codes (e.g. ``GB-ENG``) at the subdivision level.
    """
    upper = code.upper()
    alpha_2 = upper.split("-")[0]
    country = pycountry.countries.get(alpha_2=alpha_2)
    if not country:
        return (code, code)
    if "-" in upper:
        subdivision = pycountry.subdivisions.get(code=upper)
        if subdivision:
            return (f"{subdivision.name}, {country.name}", upper)
    return (country.name, alpha_2)


def _gleif_addresses(entity_block: dict[str, Any]) -> list[dict[str, str]]:
    addresses: list[dict[str, str]] = []
    legal_address = entity_block.get("legalAddress")
    if legal_address:
        addresses.append(_gleif_address(legal_address, address_type="registered"))
    hq_address = entity_block.get("headquartersAddress")
    if hq_address:
        addresses.append(_gleif_address(hq_address, address_type="business"))
    return addresses


def _gleif_address(block: dict[str, Any], *, address_type: str) -> dict[str, str]:
    parts = [
        *(block.get("addressLines") or []),
        block.get("city"),
        block.get("region"),
        block.get("postalCode"),
        block.get("country"),
    ]
    joined = ", ".join([p for p in parts if p])
    return {
        "type": address_type,
        "address": joined,
        "country": block.get("country", ""),
    }


# ----------------------------------------------------------------------
# FtM (OpenSanctions / OpenAleph) → BODS
# ----------------------------------------------------------------------
#
# FollowTheMoney (FtM) is the shared schema behind both OpenSanctions
# and OpenAleph. For Phase 2 we map the search-time properties into a
# single-statement BODS bundle: one entity or person statement with
# whatever cross-identifiers FtM carried. Ownership relationships
# embedded in richer FtM payloads (Ownership/Directorship interval
# schemas) get picked up when their child entities are present via
# ``related_entities``.

# FtM schemas we treat as "entity-like" rather than "person-like".
_FTM_ENTITY_SCHEMAS = {
    "Company",
    "Organization",
    "LegalEntity",
    "PublicBody",
    "Asset",
    "Airplane",
    "Vessel",
}
_FTM_PERSON_SCHEMAS = {"Person"}

# FtM topics (sanction, role.pep, etc.) are intentionally NOT converted into
# BODS interests here — they are risk signals handled by the risk engine, not
# ownership or control relationships.


def map_ftm(
    payload: dict[str, Any],
    *,
    source_id: str,
    source_url_builder: Any = None,
) -> BODSBundle:
    """Map a FtM-shaped entity payload (OpenSanctions/OpenAleph) to BODS.

    ``payload`` is the single FtM record (the ``entity`` block from the
    adapter's ``fetch`` output, or a hit's ``raw``). ``source_url_builder``
    is an optional callable ``(ftm_id) -> url`` for populating the BODS
    source block.
    """
    result = BODSBundle()

    subject = _ftm_statement(
        payload, source_id=source_id, source_url_builder=source_url_builder
    )
    if subject is None:
        return result
    result.statements.append(subject)
    subject_sid = subject["statementId"]
    subject_type = "entity" if subject["recordType"] == "entity" else "person"

    # FtM ownership-like properties can carry nested entities.
    # We walk the canonical control-bearing properties and emit a
    # relationship for each resolved child entity.
    props = payload.get("properties") or {}
    control_props = {
        "ownersOf": "shareholding",
        "owners": "shareholding",
        "directorshipDirector": "appointmentOfBoard",
        "directorshipOrganization": "appointmentOfBoard",
        "associates": "otherInfluenceOrControl",
    }
    for key, interest_type in control_props.items():
        for related in props.get(key) or []:
            # FtM emits either string IDs or nested entity dicts.
            if not isinstance(related, dict):
                continue
            related_stmt = _ftm_statement(
                related,
                source_id=source_id,
                source_url_builder=source_url_builder,
            )
            if related_stmt is None:
                continue
            result.statements.append(related_stmt)
            related_type = "entity" if related_stmt["recordType"] == "entity" else "person"

            # When the FtM property expresses "owner of X", the related
            # record is the *subject* and `payload` is the interested party.
            if key in {"ownersOf", "directorshipOrganization"}:
                rel_subject_sid = related_stmt["statementId"]
                rel_ip_sid = subject_sid
                rel_ip_type = subject_type
            else:
                rel_subject_sid = subject_sid
                rel_ip_sid = related_stmt["statementId"]
                rel_ip_type = related_type

            rel = make_relationship_statement(
                source_id=source_id,
                local_id=f"{payload.get('id', '?')}:{key}:{related.get('id', '?')}",
                subject_statement_id=rel_subject_sid,
                interested_party_statement_id=rel_ip_sid,
                interested_party_type=rel_ip_type,
                interests=[
                    {
                        "type": interest_type,
                        "directOrIndirect": "direct",
                        "beneficialOwnershipOrControl": interest_type == "shareholding",
                        "details": f"FtM property '{key}'",
                    }
                ],
                source_url=subject.get("source", {}).get("url"),
            )
            result.statements.append(rel)

    return result


def _ftm_statement(
    payload: dict[str, Any],
    *,
    source_id: str,
    source_url_builder: Any,
) -> dict[str, Any] | None:
    ftm_id = payload.get("id")
    if not ftm_id:
        return None
    schema = payload.get("schema") or ""
    props = payload.get("properties") or {}

    source_url = source_url_builder(ftm_id) if callable(source_url_builder) else None

    if schema in _FTM_PERSON_SCHEMAS:
        return _ftm_person_statement(payload, source_id, source_url)
    # Everything else — including unknown schemas — becomes an entity.
    return _ftm_entity_statement(payload, source_id, source_url)


def _ftm_entity_statement(
    payload: dict[str, Any], source_id: str, source_url: str | None
) -> dict[str, Any]:
    ftm_id = payload.get("id") or ""
    props = payload.get("properties") or {}
    name = (
        (props.get("name") or [None])[0]
        or payload.get("caption")
        or f"Entity {ftm_id}"
    )

    jurisdiction = _ftm_jurisdiction(props)
    identifiers = _ftm_identifiers(ftm_id, source_id, props)
    addresses = _ftm_addresses(props)
    founding_date = (props.get("incorporationDate") or [None])[0]

    return make_entity_statement(
        source_id=source_id,
        local_id=ftm_id,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        founding_date=founding_date,
        source_url=source_url,
    )


def _ftm_person_statement(
    payload: dict[str, Any], source_id: str, source_url: str | None
) -> dict[str, Any]:
    ftm_id = payload.get("id") or ""
    props = payload.get("properties") or {}
    full_name = (
        (props.get("name") or [None])[0]
        or payload.get("caption")
        or f"Person {ftm_id}"
    )
    nationalities = [_ftm_resolve_nationality(n) for n in (props.get("nationality") or [])]
    birth_date = (props.get("birthDate") or [None])[0]
    addresses = _ftm_addresses(props)
    identifiers = _ftm_identifiers(ftm_id, source_id, props)

    return make_person_statement(
        source_id=source_id,
        local_id=ftm_id,
        full_name=full_name,
        nationalities=nationalities,
        birth_date=birth_date,
        addresses=addresses,
        identifiers=identifiers,
        source_url=source_url,
    )


def _ftm_resolve_nationality(raw: str) -> dict[str, str]:
    """Resolve a FtM nationality string (ISO code or full name) to a BODS entry.

    Returns ``{"name": ..., "code": ...}`` when pycountry can resolve the
    value, or ``{"name": raw}`` as a safe fallback.
    """
    try:
        country = pycountry.countries.lookup(raw.strip())
        return {"name": country.name, "code": country.alpha_2}
    except LookupError:
        return {"name": raw.strip()}


def _ftm_jurisdiction(props: dict[str, Any]) -> tuple[str, str] | None:
    """Resolve a FtM jurisdiction/country property array to ``(name, alpha-2)``.

    FtM stores jurisdiction as an array of strings that may be ISO 3166-1
    alpha-2 codes (``"RU"``), lowercase codes (``"ru"``), or full country
    names. We resolve all forms via pycountry so the BODS
    ``incorporatedInJurisdiction.name`` is always a human-readable string.
    """
    jur = (props.get("jurisdiction") or props.get("country") or [None])[0]
    if not jur:
        return None
    try:
        country = pycountry.countries.lookup(jur.strip())
        return (country.name, country.alpha_2)
    except LookupError:
        # Unknown/custom jurisdiction — surface as-is so it's not silently lost.
        return (jur.strip(), _country_code(jur) or jur.strip())


def _ftm_identifiers(
    ftm_id: str, source_id: str, props: dict[str, Any]
) -> list[dict[str, str]]:
    scheme_name = "OpenSanctions" if source_id == "opensanctions" else "OpenAleph"
    scheme_code = "OPENSANCTIONS" if source_id == "opensanctions" else "OPENALEPH"
    identifiers: list[dict[str, str]] = [
        {"id": ftm_id, "scheme": scheme_code, "schemeName": scheme_name}
    ]

    # Resolve jurisdiction so registrationNumber gets a country-qualified scheme
    # (e.g. "REG-RU" instead of the generic "REG") when the entity's
    # jurisdiction is known. This lets reconcilers bridge to other sources on
    # the same identifier without guessing the registry.
    jur_raw = (props.get("jurisdiction") or props.get("country") or [None])[0]
    reg_scheme = "REG"
    if jur_raw:
        try:
            alpha2 = pycountry.countries.lookup(jur_raw.strip()).alpha_2
            reg_scheme = f"REG-{alpha2}"
        except LookupError:
            pass

    for key, scheme, name in (
        ("leiCode", "XI-LEI", "Legal Entity Identifier"),
        ("wikidataId", "WIKIDATA", "Wikidata"),
        ("registrationNumber", reg_scheme, "Local registry identifier"),
        ("ogrnCode", "RU-OGRN", "Russian OGRN"),
        ("innCode", "RU-INN", "Russian INN"),
    ):
        values = props.get(key) or []
        if values:
            identifiers.append(
                {"id": values[0], "scheme": scheme, "schemeName": name}
            )
    return identifiers


def _ftm_addresses(props: dict[str, Any]) -> list[dict[str, str]]:
    raw = props.get("address") or props.get("addressEntity") or []
    result: list[dict[str, str]] = []
    for entry in raw:
        if isinstance(entry, str):
            result.append({"type": "registered", "address": entry, "country": ""})
        elif isinstance(entry, dict):
            p = entry.get("properties") or {}
            parts = [
                *(p.get("street") or []),
                *(p.get("city") or []),
                *(p.get("region") or []),
                *(p.get("postalCode") or []),
                *(p.get("country") or []),
            ]
            joined = ", ".join([str(x) for x in parts if x])
            if joined:
                result.append(
                    {
                        "type": "registered",
                        "address": joined,
                        "country": (p.get("country") or [""])[0],
                    }
                )
    return result


def map_opensanctions(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper: ``bundle`` is the adapter's fetch output."""
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="opensanctions",
        source_url_builder=lambda _id: f"https://www.opensanctions.org/entities/{_id}/",
    )


def map_openaleph(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper: ``bundle`` is the adapter's fetch output."""
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="openaleph",
        source_url_builder=lambda _id: f"https://search.openaleph.org/entities/{_id}",
    )


def map_everypolitician(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper for EveryPolitician — same FtM shape as OpenSanctions.

    Politicians never carry ownership data, so the mapper simply emits
    a single ``personStatement``. ``positions held`` is intentionally
    *not* converted to BODS interests — those are PEP signals, surfaced
    separately by the risk engine.
    """
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="everypolitician",
        source_url_builder=lambda _id: f"https://www.opensanctions.org/entities/{_id}/",
    )


# ----------------------------------------------------------------------
# Wikidata → BODS
# ----------------------------------------------------------------------


def map_wikidata(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Wikidata fetch bundle to a single BODS person or entity statement.

    Wikidata's role in OpenCheck is identifier-bridging — its records
    rarely contain ownership relationships in a useful form, so we
    emit one statement (person or entity, decided by P31) carrying:

    * ``WIKIDATA`` as a primary scheme identifier (the Q-ID itself).
    * Cross-source bridge identifiers (``XI-LEI``, ``OPENCORPORATES``,
      ``ISIN``) when present, so reconcilers downstream can match.
    * Birth date / death date for persons (no narrative).
    * Citizenships → ``nationalities``.
    * Country (P17) → ``incorporatedInJurisdiction`` for entities.
    * Inception (P571) → ``foundingDate``.

    Positions held (``positions``) are intentionally not converted to
    BODS interests — they are PEP signals, surfaced separately by the
    risk engine.
    """
    summary = bundle.get("summary") or {}
    qid = summary.get("qid") or bundle.get("qid") or "Q0"
    label = summary.get("label") or qid
    source_url = f"https://www.wikidata.org/wiki/{qid}"

    base_identifiers: list[dict[str, str]] = [
        {
            "id": qid,
            "scheme": "WIKIDATA",
            "schemeName": "Wikidata Q identifier",
            "uri": f"https://www.wikidata.org/wiki/{qid}",
        }
    ]
    cross_ids = summary.get("identifiers") or {}
    if cross_ids.get("lei"):
        base_identifiers.append(
            {
                "id": cross_ids["lei"],
                "scheme": "XI-LEI",
                "schemeName": "Global Legal Entity Identifier Index",
            }
        )
    if cross_ids.get("opencorporates"):
        base_identifiers.append(
            {
                "id": cross_ids["opencorporates"],
                "scheme": "OPENCORPORATES",
                "schemeName": "OpenCorporates company identifier",
            }
        )
    if cross_ids.get("isin"):
        base_identifiers.append(
            {
                "id": cross_ids["isin"],
                "scheme": "ISIN",
                "schemeName": "International Securities Identification Number",
            }
        )

    result = BODSBundle()

    if summary.get("is_person"):
        nationalities: list[dict[str, str]] = []
        for citizenship in summary.get("citizenships") or []:
            country_qid = citizenship.get("qid")
            country_label = citizenship.get("label") or country_qid
            if country_qid and country_label:
                nationalities.append(
                    {"name": country_label, "code": country_qid}
                )

        person = make_person_statement(
            source_id="wikidata",
            local_id=qid,
            full_name=label,
            nationalities=nationalities,
            birth_date=_normalise_wikidata_date(summary.get("dob")),
            identifiers=base_identifiers,
            source_url=source_url,
        )
        result.statements.append(person)
        return result

    # Anything that's not a Q5 we treat as an entity. If P31 was empty
    # entirely (rare for live data) we still emit an entity statement —
    # the BODS validator accepts ``unknownEntity`` as the entityType for
    # such cases.
    entity_type = "registeredEntity" if summary.get("is_entity") else "unknownEntity"
    jurisdiction = _wikidata_jurisdiction(summary.get("country") or {})
    entity = make_entity_statement(
        source_id="wikidata",
        local_id=qid,
        name=label,
        jurisdiction=jurisdiction,
        identifiers=base_identifiers,
        founding_date=_normalise_wikidata_date(summary.get("inception")),
        entity_type=entity_type,
        source_url=source_url,
    )
    result.statements.append(entity)
    return result


def _normalise_wikidata_date(value: str | None) -> str | None:
    """Convert ``+1952-10-07T00:00:00Z`` → ``1952-10-07``.

    Wikidata's SPARQL service returns dates as XSD dateTime strings
    (sometimes with a ``+`` sign prefix); BODS expects ISO date.
    """
    if not value:
        return None
    cleaned = value.lstrip("+")
    if "T" in cleaned:
        cleaned = cleaned.split("T", 1)[0]
    return cleaned or None


def _wikidata_jurisdiction(country: dict[str, Any]) -> tuple[str, str] | None:
    """Resolve a Wikidata ``country`` object to a ``(name, ISO code)`` tuple.

    Wikidata's P17 returns a Q-ID — we use the country's English label
    and pass it through pycountry to recover the alpha-2 code so the
    BODS jurisdiction block carries an ISO code (matching every other
    source). When the lookup fails we fall back to the raw label/Q-ID.
    """
    if not country:
        return None
    name = country.get("label")
    if not name:
        return None
    try:
        match = pycountry.countries.lookup(name)
    except LookupError:
        return (name, country.get("qid", name))
    return (match.name, match.alpha_2)


# ----------------------------------------------------------------------
# OpenTender (DIGIWHIST) → BODS
# ----------------------------------------------------------------------


# DIGIWHIST BodyIdentifier.type → (BODS scheme code, schemeName) mapping.
# The list mirrors the strong-bridge identifier scheme used elsewhere in
# OpenCheck — VAT / LEI / GB-COH / OpenCorporates — so the reconciler
# can bridge a procurement supplier to its GLEIF / Companies House /
# OpenSanctions presence on the same identifier.
_DIGIWHIST_ID_SCHEMES = {
    "VAT": ("EU-VAT", "EU VAT identifier"),
    "BVD_ID": ("BVD", "Bureau van Dijk identifier"),
    "ETALON_ID": ("ETALON", "Etalon registry id"),
    "HEADER_ICO": ("REG", "Local registry identifier"),
    "TAX_ID": ("TAX", "National tax identifier"),
    "TRADE_REGISTER": ("REG", "Local registry identifier"),
    "STATISTICAL": ("STAT", "National statistical id"),
    "ORGANIZATION_ID": ("ORG", "National organisation id"),
}


def map_opentender(bundle: dict[str, Any]) -> BODSBundle:
    """Map an OpenTender (DIGIWHIST) tender bundle to BODS v0.4.

    Procurement records are not beneficial-ownership records, but the
    *parties* to the procurement are. We surface every Body — buyer,
    bidder, subcontractor — as an entityStatement so the reconciler
    can bridge them to GLEIF / Companies House / OpenSanctions on
    shared identifiers (VAT, registration_number, GB-COH).

    Each *winning* bid produces a relationshipStatement linking the
    winning bidder (interestedParty) to the buyer (subject) with an
    ``otherInfluenceOrControl`` interest annotated with the tender id,
    award decision date, and final price. ``beneficialOwnershipOrControl``
    is set to false: this is a commercial engagement, not ownership.
    """
    result = BODSBundle()
    tender = bundle.get("tender") or bundle
    tender_id = (
        bundle.get("tender_id")
        or tender.get("id")
        or tender.get("persistentId")
        or ""
    )
    if not tender_id:
        return result

    tender_url = tender.get("publications", [{}])[0].get("humanReadableURL") or (
        f"https://opentender.eu/{tender.get('country', '').lower()}/tender/{tender_id}"
    )

    # ---- Buyers ----
    buyer_sids: list[str] = []
    for buyer in tender.get("buyers") or []:
        sid = _opentender_body_statement(
            buyer, source_id="opentender", local_prefix=f"{tender_id}:buyer", url=tender_url
        )
        if sid is None:
            continue
        result.statements.append(sid)
        buyer_sids.append(sid["statementId"])

    # ---- Bidders (lots → bids → bidders) ----
    for lot in tender.get("lots") or []:
        award_date = lot.get("awardDecisionDate") or tender.get("awardDecisionDate")
        for bid in lot.get("bids") or []:
            is_winning = bool(bid.get("isWinning"))
            price = bid.get("price")
            for bidder in bid.get("bidders") or []:
                stmt = _opentender_body_statement(
                    bidder,
                    source_id="opentender",
                    local_prefix=f"{tender_id}:bidder",
                    url=tender_url,
                )
                if stmt is None:
                    continue
                result.statements.append(stmt)
                if not is_winning:
                    continue
                # Emit a relationship per (winning bidder, buyer) pair.
                for buyer_sid in buyer_sids:
                    result.statements.append(
                        make_relationship_statement(
                            source_id="opentender",
                            local_id=f"{tender_id}:award:{stmt['statementId']}:{buyer_sid}",
                            subject_statement_id=buyer_sid,
                            interested_party_statement_id=stmt["statementId"],
                            interested_party_type="entity",
                            interests=[
                                {
                                    "type": "otherInfluenceOrControl",
                                    "directOrIndirect": "direct",
                                    "beneficialOwnershipOrControl": False,
                                    "details": _format_award_details(
                                        tender_id=tender_id,
                                        title=tender.get("title", ""),
                                        award_date=award_date,
                                        price=price,
                                    ),
                                    **(
                                        {"startDate": award_date} if award_date else {}
                                    ),
                                }
                            ],
                            source_url=tender_url,
                        )
                    )

    return result


def _opentender_body_statement(
    body: dict[str, Any], *, source_id: str, local_prefix: str, url: str | None
) -> dict[str, Any] | None:
    """Render a DIGIWHIST ``Body`` as a BODS entityStatement (or None)."""
    name = body.get("name")
    if not name:
        return None

    # Stable local id: prefer a body identifier, else hash the name.
    local_keys = [
        ident.get("id")
        for ident in (body.get("bodyIds") or [])
        if ident.get("id")
    ]
    local_seed = local_keys[0] if local_keys else name
    local_id = f"{local_prefix}:{local_seed}"

    identifiers: list[dict[str, str]] = []
    for ident in body.get("bodyIds") or []:
        scheme = _DIGIWHIST_ID_SCHEMES.get(
            (ident.get("type") or "").upper()
        )
        if scheme is None:
            continue
        scope = (ident.get("scope") or "").upper()
        scheme_code, scheme_name = scheme
        # Country-scope ETALON / HEADER_ICO is more useful with the
        # country prefix to disambiguate (DE-REG vs CZ-REG).
        if scope and len(scope) == 2 and scheme_code in {"REG", "TAX", "STAT", "ORG"}:
            scheme_code = f"{scope}-{scheme_code}"
        identifiers.append(
            {"id": str(ident.get("id")), "scheme": scheme_code, "schemeName": scheme_name}
        )

    address = body.get("address") or {}
    addresses: list[dict[str, str]] = []
    parts = [
        address.get("street"),
        address.get("city"),
        address.get("postcode"),
    ]
    addr_str = ", ".join(p for p in parts if p)
    if addr_str:
        addresses.append(
            {
                "type": "registered",
                "address": addr_str,
                "country": (address.get("country") or "").upper(),
            }
        )

    jurisdiction = None
    country_code = (address.get("country") or "").upper()
    if country_code:
        try:
            match = pycountry.countries.lookup(country_code)
            jurisdiction = (match.name, match.alpha_2)
        except LookupError:
            jurisdiction = (country_code, country_code)

    return make_entity_statement(
        source_id=source_id,
        local_id=local_id,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        entity_type="registeredEntity",
        source_url=url,
    )


def _format_award_details(
    *,
    tender_id: str,
    title: str,
    award_date: str | None,
    price: dict[str, Any] | None,
) -> str:
    parts = [f"Awarded contract under tender {tender_id}"]
    if title:
        parts.append(f'"{title}"')
    if award_date:
        parts.append(f"on {award_date}")
    if price and price.get("netAmount") and price.get("currency"):
        parts.append(f"value {price['netAmount']} {price['currency']}")
    return ", ".join(parts) + "."


# ----------------------------------------------------------------------
# OpenCorporates → BODS
# ----------------------------------------------------------------------


def map_opencorporates(bundle: dict[str, Any]) -> BODSBundle:
    """Map an OpenCorporates fetch bundle to BODS v0.4 statements.

    Produces:
    * One entity statement for the company itself.
    * One person statement + relationship per officer where the
      officer is a natural person and has a current position.

    Officers are entered via the ``/companies/{j}/{n}/officers``
    endpoint and carry a ``position`` string (e.g. "director",
    "secretary") plus optional start/end dates.
    """
    result = BODSBundle()
    company = bundle.get("company") or {}
    ocid = bundle.get("ocid") or bundle.get("hit_id") or ""
    officers = bundle.get("officers") or []

    if not company:
        return result

    # --- Entity statement for the company --------------------------------

    name = company.get("name") or "Unknown company"
    jurisdiction_code = (company.get("jurisdiction_code") or "").upper()
    company_number = company.get("company_number") or ""
    incorporation_date = company.get("incorporation_date")
    company_type = company.get("company_type") or ""
    status = company.get("current_status") or ""
    oc_url = company.get("opencorporates_url") or (
        f"https://opencorporates.com/companies/{ocid}" if ocid else None
    )

    # Resolve jurisdiction tuple (alpha-2, display name)
    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        # OC jurisdiction codes are ISO 3166-1 alpha-2 (lower), with
        # sub-national variants like "us_de". We use the top-level code.
        # Tuple convention (matches make_entity_statement): (name, code).
        top_code = jurisdiction_code.split("_")[0].upper()
        try:
            country = pycountry.countries.get(alpha_2=top_code)
            country_name = country.name if country else top_code
            jurisdiction = (country_name, top_code)
        except Exception:  # noqa: BLE001
            jurisdiction = (top_code, top_code)

    identifiers: list[dict[str, str]] = []
    if ocid:
        identifiers.append(
            {
                "id": ocid,
                "scheme": "OPENCORPORATES",
                "schemeName": "OpenCorporates company identifier",
                "uri": oc_url or "",
            }
        )
    if company_number and jurisdiction_code:
        identifiers.append(
            {
                "id": company_number,
                "scheme": f"OC-{jurisdiction_code.upper()}",
                "schemeName": f"OpenCorporates {jurisdiction_code.upper()} company number",
            }
        )

    subject_stmt = make_entity_statement(
        source_id="opencorporates",
        local_id=ocid or company_number,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        founding_date=incorporation_date,
        entity_type="registeredEntity",
        source_url=oc_url,
    )
    subject_stmt_id: str = subject_stmt["statementId"]
    result.extend([subject_stmt])

    # --- Officer statements -----------------------------------------------
    # OC officers carry a ``position`` string (e.g. "director"), optional
    # ``start_date`` / ``end_date``, and a nested ``officer`` sub-object.
    # We only surface current officers (no end_date set).
    for officer_item in officers:
        officer_data = officer_item.get("officer") or officer_item
        position = (officer_data.get("position") or "").strip()
        end_date = officer_data.get("end_date")
        if end_date:
            continue  # skip resigned officers

        officer_name = officer_data.get("name") or ""
        if not officer_name:
            continue

        officer_id = str(officer_data.get("id") or officer_data.get("uid") or "")
        local_key = f"{ocid}/{officer_id or _stable_id('oc', 'officer', officer_name)}"

        person_stmt = make_person_statement(
            source_id="opencorporates",
            local_id=local_key,
            full_name=officer_name,
            source_url=oc_url,
        )
        person_stmt_id: str = person_stmt["statementId"]

        # Map OC position to a BODS interest type.
        position_lower = position.lower()
        if "director" in position_lower or "chair" in position_lower:
            interest_type = "appointmentOfBoard"
        elif "secretary" in position_lower:
            interest_type = "appointmentOfBoard"
        else:
            interest_type = "otherInfluenceOrControl"

        start_date = officer_data.get("start_date")
        interest_entry: dict[str, Any] = {
            "type": interest_type,
            "details": position or "officer",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if start_date:
            interest_entry["startDate"] = start_date

        rel_stmt = make_relationship_statement(
            source_id="opencorporates",
            local_id=f"rel/{local_key}",
            subject_statement_id=subject_stmt_id,
            interested_party_statement_id=person_stmt_id,
            interested_party_type="person",
            interests=[interest_entry],
            source_url=oc_url,
        )

        result.extend([person_stmt, rel_stmt])

    return result
