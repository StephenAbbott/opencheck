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
    # bods-dagre v0.4 resolves graph edges by matching the relationship's
    # referenced statementId against each entity/person statement's recordId.
    # Using statementId == recordId ensures that lookup succeeds without
    # breaking BODS semantics: we never version records in opencheck so the
    # distinction between "statement id" and "record id" doesn't apply.
    record_id = statement_id

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
    record_id = statement_id  # see make_entity_statement for reasoning

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
        "ariregister": "Estonian e-Business Register (e-Äriregister)",
        "bolagsverket": "Bolagsverket — Swedish Companies Registration Office",
        "companies_house": "UK Companies House",
        "gleif": "GLEIF",
        "inpi": "INPI — Registre National des Entreprises",
        "kvk": "KvK — Netherlands Chamber of Commerce",
        "opencorporates": "OpenCorporates",
        "brightquery": "BrightQuery / OpenData.org",
        "opensanctions": "OpenSanctions",
        "openaleph": "OpenAleph",
        "everypolitician": "EveryPolitician",
        "wikidata": "Wikidata",
        "zefix": "Zefix — Swiss Commercial Registry",
        "opentender": "OpenTender",
    }
    _official_registers = {"ariregister", "bolagsverket", "companies_house", "inpi", "kvk", "opencorporates", "zefix"}
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
    "DETRIMENT_NOT_EXCLUDED": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: detriment to parent not excluded by law",
    ),
    "AUTHORITIES_DISCRETION": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: regulatory authority has exercised discretion not to require disclosure",
    ),
    "CONSENT_NOT_OBTAINED": (
        "entity",
        "anonymousEntity",
        "GLEIF reporting exception: consent of parent entity not obtained",
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
    subject_statement = _gleif_entity_statement(
        lei, subject_entity_block, subject_url, attrs=subject_attrs
    )
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
        parent_lei, parent_entity_block, parent_url, attrs=parent_attrs
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
    # Live GLEIF API uses "reason"; OO SQLite dump uses "exceptionReason".
    reason = (attrs.get("reason") or attrs.get("exceptionReason") or "").upper()
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
    lei: str,
    entity_block: dict[str, Any],
    source_url: str,
    *,
    attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a BODS entity statement from a GLEIF Level 1 entity block.

    ``attrs`` is the full ``record.attributes`` dict (one level above
    ``entity``). It carries the cross-reference identifiers that GLEIF
    publishes via its LEI Mapping programme:

    * ``ocid``  — OpenCorporates identifier (e.g. ``"gb/00102498"``)
    * ``qcc``   — QCC Global Enterprise Identifier / QCC Code (e.g. ``"QGBVC89DTN"``)
    * ``mic``   — Market Identifier Code ISO 10383 (e.g. ``"XLON"``)
    * ``bic``   — Bank Identifier Code ISO 9362 (e.g. ``"BARCGB22"``)

    These are mapped to BODS identifiers when non-null, enabling
    downstream adapters to use them for additional cross-source queries.
    """
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
    # The GLEIF Registration Authorities List (RAL) is not on org-id.guide so
    # per BODS guidance on real-world entity identifiers we leave ``scheme``
    # blank and use ``schemeName`` to identify the list instead.
    registered_as = entity_block.get("registeredAs")
    registered_at = entity_block.get("registeredAt") or {}
    ra_id = registered_at.get("id")
    if registered_as and ra_id:
        identifiers.append(
            {
                "id": registered_as,
                "scheme": "",
                "schemeName": "GLEIF Registration Authorities List",
            }
        )

    # GLEIF LEI Mapping cross-reference identifiers (from ``record.attributes``).
    # Each is only included when the GLEIF API returns a non-null value.
    if attrs:
        ocid = attrs.get("ocid")
        if ocid:
            identifiers.append(
                {
                    "id": ocid,
                    "scheme": "OPENCORPORATES",
                    "schemeName": "OpenCorporates company identifier",
                    "uri": f"https://opencorporates.com/companies/{ocid}",
                }
            )

        qcc = attrs.get("qcc")
        if qcc:
            identifiers.append(
                {
                    "id": qcc,
                    "scheme": "QCC Code",
                    "schemeName": "QCC Global Enterprise Identifier (QCC Code)",
                }
            )

        mic = attrs.get("mic")
        if mic:
            identifiers.append(
                {
                    "id": mic,
                    "scheme": "ISO-10383",
                    "schemeName": "Market Identifier Code (ISO 10383)",
                }
            )

        bic = attrs.get("bic")
        if bic:
            identifiers.append(
                {
                    "id": bic,
                    "scheme": "ISO-9362",
                    "schemeName": "Bank Identifier Code (ISO 9362)",
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
# Zefix (Swiss Federal Commercial Registry) → BODS
# ----------------------------------------------------------------------
#
# Zefix exposes the ``CompanyFull`` schema via ``GET /api/v1/company/uid/{uid}``.
# We map the entity-level fields to a BODS entity statement.  Zefix does not
# expose natural persons through this API, so only entity statements are emitted.
#
# Identifier scheme: ``CH-UID`` for the Swiss Unternehmens-Identifikationsnummer
# (UID), issued by the Federal Statistical Office (FSO / BFS).  Format used in
# BODS identifiers is ``CHE-NNN.NNN.NNN`` (the official display format).
#
# The ``bundle`` shape expected here matches what ZefixAdapter.fetch() returns:
#   {
#     "source_id": "zefix",
#     "uid": "CHE313550547",        # normalised (no separators)
#     "company": {<CompanyFull>},   # from Zefix API
#     "is_stub": False,
#   }

import re as _re

_ZEFIX_UID_RE = _re.compile(r"CHE(\d{3})(\d{3})(\d{3})", _re.IGNORECASE)

_ZEFIX_CANTON_TO_NAME: dict[str, str] = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Geneva", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Lucerne", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen", "SO": "Solothurn",
    "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zurich",
}


def _zefix_format_uid(uid: str) -> str:
    """``CHE313550547`` → ``CHE-313.550.547`` (official display format)."""
    m = _ZEFIX_UID_RE.match(uid.strip())
    if m:
        return f"CHE-{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return uid


def map_zefix(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Zefix fetch bundle to BODS v0.4 entity statements.

    Returns an empty iterable for stub bundles or missing company data.
    Only entity statements are emitted — Zefix does not expose natural persons.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    uid_raw: str = company.get("uid") or bundle.get("uid") or ""
    name: str = company.get("name") or ""
    if not uid_raw or not name:
        return

    uid_display = _zefix_format_uid(uid_raw)
    canton: str = company.get("canton") or ""

    # Jurisdiction: use canton subdivision code where available (e.g. CH-ZH),
    # falling back to country-level CH.
    if canton and canton.upper() in _ZEFIX_CANTON_TO_NAME:
        jur_code = f"CH-{canton.upper()}"
        jur_name = f"{_ZEFIX_CANTON_TO_NAME[canton.upper()]}, Switzerland"
    else:
        jur_code = "CH"
        jur_name = "Switzerland"

    # Identifiers — Swiss UID is the primary cross-reference.
    identifiers: list[dict[str, str]] = [
        {
            "id": uid_display,
            "scheme": "CH-UID",
            "schemeName": "Swiss Federal Statistical Office — UID Register",
        }
    ]
    # EHRA-ID (internal Zefix identifier) as a secondary cross-reference.
    ehraid = company.get("ehraid")
    if ehraid is not None:
        identifiers.append(
            {
                "id": str(ehraid),
                "scheme": "CH-ZEFIX",
                "schemeName": "Zefix (FCRO/EHRA) internal ID",
            }
        )

    # Address
    addr_block = company.get("address") or {}
    addresses = _zefix_address(addr_block)

    source_url = (
        ((company.get("zefixDetailWeb") or {}).get("en"))
        or company.get("cantonalExcerptWeb")
        or f"https://www.zefix.ch/en/search/entity/list/firm/{company.get('ehraid', '')}"
    )

    entity = make_entity_statement(
        source_id="zefix",
        local_id=uid_raw,
        name=name,
        jurisdiction=(jur_name, jur_code),
        identifiers=identifiers,
        addresses=addresses,
        source_url=source_url or None,
    )
    yield entity


def _zefix_address(block: dict[str, Any]) -> list[dict[str, str]]:
    if not block:
        return []
    parts = [
        block.get("organisation"),
        block.get("careOf"),
        " ".join(filter(None, [block.get("street"), block.get("houseNumber")])),
        block.get("addon"),
        block.get("poBox"),
        " ".join(filter(None, [block.get("swissZipCode"), block.get("city")])),
    ]
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return []
    return [{"type": "registered", "address": joined, "country": "CH"}]


# ----------------------------------------------------------------------
# KvK (Netherlands Chamber of Commerce) → BODS
# ----------------------------------------------------------------------
#
# The KvK open-data endpoint returns limited fields: registration status,
# legal form code (rechtsvormCode), SBI activity codes, start date, and a
# 2-digit postal-code region.  Company name is NOT available from this API
# tier; it is passed via bundle["legal_name"] (sourced from GLEIF).
#
# Identifier scheme: "NL-KVK"  (follows the GB-COH / CH-UID pattern)
# Jurisdiction: Netherlands ("NL")
# Source: https://developers.kvk.nl/nl/documentation/open-dataset-basis-bedrijfsgegevens-api


def map_kvk(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a KvK fetch bundle to a BODS v0.4 entity statement.

    Returns an empty iterable for stub bundles, missing company data,
    or missing entity name.  KvK open data does not expose natural
    persons, so only entity statements are emitted.

    Bundle shape (as returned by KvKAdapter.fetch):

    .. code-block:: python

        {
            "source_id": "kvk",
            "kvk_number": "96332751",
            "company": {          # raw KvK open-data API response
                "datumAanvang": "20250202",
                "actief": "J",
                "rechtsvormCode": "BV",
                "postcodeRegio": 10,
                "activiteiten": [{"sbiCode": "6201", "soortActiviteit": "Hoofdactiviteit"}],
                "lidstaat": "NL",
            },
            "legal_name": "Splitty B.V.",   # from GLEIF, may be empty
            "is_stub": False,
        }
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    kvk_number: str = bundle.get("kvk_number") or ""
    name: str = bundle.get("legal_name") or ""
    if not kvk_number or not name:
        return

    # Founding date: datumAanvang is YYYYMMDD — convert to ISO format.
    raw_date = str(company.get("datumAanvang") or "").strip()
    founding_date: str | None = None
    if len(raw_date) == 8 and raw_date.isdigit():
        founding_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    identifiers: list[dict[str, str]] = [
        {
            "id": kvk_number,
            "scheme": "NL-KVK",
            "schemeName": "Netherlands Chamber of Commerce (KvK) registration number",
        }
    ]

    entity = make_entity_statement(
        source_id="kvk",
        local_id=kvk_number,
        name=name,
        jurisdiction=("Netherlands", "NL"),
        identifiers=identifiers,
        founding_date=founding_date,
        source_url=f"https://www.kvk.nl/zoeken/handelsnaam/?q={kvk_number}",
    )
    yield entity


# ----------------------------------------------------------------------
# INPI (France — Registre National des Entreprises) → BODS
# ----------------------------------------------------------------------
#
# The RNE API returns a rich JSON document keyed under ``content``.
# Only ``personneMorale`` companies are handled here; ``personnePhysique``
# (sole traders / auto-entrepreneurs) are out of scope for Phase 1.
#
# Identifier scheme: "FR-SIREN"  (follows GB-COH / CH-UID / NL-KVK pattern)
# Jurisdiction: France ("FR")
# Source: https://registre-national-entreprises.inpi.fr/
#
# ⚠️  Person statements are deliberately NOT emitted.  Any entry in
# ``content.personneMorale.composition.pouvoirs[]`` that carries
# ``beneficiaireEffectif: true`` is a beneficial-ownership record whose
# redistribution requires legitimate-interest authorisation under French
# law (Loi Sapin II / décret 2017-1094).  To avoid violating that
# restriction we map entity data only and ignore the dirigeants array
# entirely in Phase 1.


def map_inpi(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an INPI RNE fetch bundle to a BODS v0.4 entity statement.

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

    # Identifier: FR-SIREN
    identifiers: list[dict[str, str]] = [
        {
            "id": siren,
            "scheme": "FR-SIREN",
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
    return [{"type": "registered", "address": joined, "country": "FR"}]


# ----------------------------------------------------------------------
# Bolagsverket (Swedish Companies Registration Office) → BODS
# ----------------------------------------------------------------------
#
# Bolagsverket publishes company information via a WSO2 API gateway.
# The register is fully public for officer/board data — unlike INPI,
# there is no BO restriction; board members, CEO, and signatories are
# safe to republish as BODS person statements.
#
# Identifier scheme: "SE-BLV"  (follows GB-COH / CH-UID / NL-KVK / FR-SIREN)
# Jurisdiction: Sweden ("SE")
# Source: https://www.bolagsverket.se/
#
# Response shape confirmed from Bolagsverket API documentation.
# POST /organisationer → {"organisationer": [{...}]}
# The mapper receives the first element of that array as bundle["company"].
#
# Confirmed bundle shape:
#
#   {
#     "source_id": "bolagsverket",
#     "org_number": "5299999994",
#     "company": {
#       "organisationsidentitet": {"identitetsbeteckning": "5299999994"},
#       "organisationsnamn": {
#         "organisationsnamnLista": [
#           {"namn": "Cykelbolaget AB", "registreringsdatum": "2024-03-15"}
#         ]
#       },
#       "organisationsdatum": {"registreringsdatum": "2000-01-23"},
#       "organisationsform": {"kod": "AB", "klartext": "Aktiebolag"},
#       "juridiskForm": {"kod": "49", "klartext": "Övriga aktiebolag"},
#       "postadressOrganisation": {
#         "postadress": {
#           "utdelningsadress": "Jobbstigen 2",
#           "postnummer": "12345",
#           "postort": "Grönköping",
#           "land": "Sverige",
#           "coAdress": "C/o Annat företag"
#         }
#       },
#       "verksamhetsbeskrivning": {"beskrivning": "Handel med skor"},
#       "verksamOrganisation": {"kod": "JA"},   # JA = active
#       "avregistreradOrganisation": {"avregistreringsdatum": "2023-05-05T..."},
#       "avregistreringsorsak": {"klartext": "Likvidation"},
#       "pagaendeAvvecklingsEllerOmstruktureringsforfarande": {
#         "pagaendeAvvecklingsEllerOmstruktureringsforfarandeLista": [
#           {"kod": "KK", "klartext": "Konkurs", "fromDatum": "..."}
#         ]
#       }
#     },
#     "legal_name": "Cykelbolaget AB",
#     "is_stub": False,
#   }
#
# Note: Officer/board member data is NOT returned by /organisationer.
# This endpoint covers the EU high-value company dataset only.


def map_bolagsverket(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Bolagsverket fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the registered company. Officer data
    is not available from the /organisationer endpoint so no person or
    relationship statements are emitted.

    Returns an empty iterable for stub bundles or missing company data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    org_number: str = bundle.get("org_number") or ""
    if not org_number:
        return

    # Company name: organisationsnamn.organisationsnamnLista[0].namn
    # Fall back to the GLEIF-supplied legal_name if missing.
    namn_lista: list[dict[str, Any]] = (
        (company.get("organisationsnamn") or {}).get("organisationsnamnLista") or []
    )
    name: str = ""
    if namn_lista:
        # The list may contain multiple names (trading names, historical).
        # Take the first entry — the API returns the current registered name first.
        name = (namn_lista[0].get("namn") or "").strip()
    if not name:
        name = (bundle.get("legal_name") or "").strip()
    if not name:
        return

    # Format org number for display: NNNNNN-NNNN
    org_display = f"{org_number[:6]}-{org_number[6:]}" if len(org_number) == 10 else org_number

    # Founding / registration date: organisationsdatum.registreringsdatum (YYYY-MM-DD)
    founding_date: str | None = (
        (company.get("organisationsdatum") or {}).get("registreringsdatum") or None
    )
    # Guard against non-ISO or timestamp strings
    if founding_date and len(founding_date) != 10:
        founding_date = None

    identifiers: list[dict[str, str]] = [
        {
            "id": org_display,
            "scheme": "SE-BLV",
            "schemeName": "Bolagsverket — Swedish Companies Registration Office",
        }
    ]

    # Address: postadressOrganisation.postadress
    addr_block: dict[str, Any] = (
        (company.get("postadressOrganisation") or {}).get("postadress") or {}
    )
    addresses = _bv_address(addr_block)

    source_url = "https://www.bolagsverket.se/"

    entity = make_entity_statement(
        source_id="bolagsverket",
        local_id=org_number,
        name=name,
        jurisdiction=("Sweden", "SE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    yield entity


def _bv_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a Bolagsverket postadress block.

    Field names confirmed from API documentation:
    utdelningsadress (street), postnummer, postort (city), land (country),
    coAdress (c/o line).
    """
    if not block:
        return []
    parts = [
        block.get("coAdress"),
        block.get("utdelningsadress"),
        block.get("postnummer"),
        block.get("postort"),
        block.get("land"),
    ]
    joined = ", ".join(p for p in parts if p)
    if not joined:
        return []
    country = block.get("land") or "SE"
    return [{"type": "registered", "address": joined, "country": country}]


# ----------------------------------------------------------------------
# Estonian e-Business Register (ariregister) → BODS
# ----------------------------------------------------------------------
#
# Maps a bundle from AriregisterAdapter.fetch() to BODS v0.4 statements.
#
# Emitted statements:
#   1. One entityStatement for the company itself.
#   2. One personStatement  + ownershipOrControlStatement per shareholder
#      (osanikud). For corporate shareholders (isiku_tyyp == "J") with an
#      Estonian registry code, an entityStatement is emitted instead of a
#      personStatement, and the interest type is "shareholding".
#   3. One personStatement  + ownershipOrControlStatement per officer on the
#      registry card (kaardile_kantud_isikud), role-mapped to BODS interest
#      types (boardMember, seniorManagingOfficial, etc.).
#   4. One personStatement  + ownershipOrControlStatement per declared
#      beneficial owner (kasusaajad), interest type "beneficialOwner".
#      These statements are only emitted when the bundle contains BO data
#      (controlled by include_beneficial_owners in the adapter).
#
# Personal identity: since November 2024 the open data files no longer
# contain personal identification numbers (isikukood_registrikood is null
# for natural persons). The `isikukood_hash` UUID field is used as a stable
# cross-file identifier and is surfaced as an identifier with scheme
# "EE-ARIREGISTER-HASH" when present.
#
# Date format in source: DD.MM.YYYY — converted to ISO YYYY-MM-DD here.

_EE_OFFICER_ROLE_MAP: dict[str, tuple[str, str]] = {
    # (BODS interest type, descriptive label)
    "JUHL":   ("boardMember",              "Board member (juhatuse liige)"),
    "PROK":   ("seniorManagingOfficial",   "Procurist (prokurist)"),
    "LIKV":   ("seniorManagingOfficial",   "Liquidator (likvideerija)"),
    "LIKVJ":  ("boardMember",              "Liquidator (board member)"),
    "TOSAN":  ("boardMember",              "General partner (täisosanik)"),
    "UOSAN":  ("boardMember",              "Limited partner (usaldusosanik)"),
    "ASES":   ("seniorManagingOfficial",   "Authorised representative"),
    "SJESI":  ("seniorManagingOfficial",   "Legal representative"),
    "VFILJ":  ("seniorManagingOfficial",   "Branch manager (filiaali juhataja)"),
    "FV":     ("seniorManagingOfficial",   "Fund manager (fondivalitseja)"),
}

# Maps Estonian BO control-mechanism code → (BODS interest type, human-readable detail).
# BODS v0.4 does not have a "beneficialOwner" interest type; BO is expressed via
# beneficialOwnershipOrControl=True on a typed interest.
_EE_BO_CONTROL_MAP: dict[str, tuple[str, str]] = {
    "O": ("shareholding",            "Direct participation"),
    "K": ("otherInfluenceOrControl", "Indirect participation"),
    "H": ("votingRights",            "Through voting rights"),
    "M": ("otherInfluenceOrControl", "Other means"),
    "F": ("otherInfluenceOrControl", "Other means of control or influence"),
}


def _ee_date(s: str | None) -> str | None:
    """Convert Estonian DD.MM.YYYY date string to ISO YYYY-MM-DD."""
    if not s:
        return None
    parts = s.strip().split(".")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 4 and d.isdigit() and m.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None


def _ee_person_id(person: dict[str, Any]) -> str:
    """Derive a stable local ID for a person record.

    Prefers isikukood_hash (UUID present in all modern records) then falls
    back to combining kirje_id with eesnimi+nimi to avoid collisions.
    """
    h = person.get("isikukood_hash")
    if h:
        return h
    kirje = person.get("kirje_id")
    first = person.get("eesnimi") or ""
    last = person.get("nimi_arinimi") or person.get("nimi") or ""
    return f"{kirje or 'x'}-{first}-{last}"


def _ee_full_name(person: dict[str, Any]) -> str:
    first = (person.get("eesnimi") or "").strip()
    last = (person.get("nimi_arinimi") or person.get("nimi") or "").strip()
    if first and last:
        return f"{first} {last}"
    return last or first or "Unknown"


def map_ariregister(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AriregisterAdapter fetch bundle to BODS v0.4 statements.

    Returns an empty iterable for stub bundles or missing company data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    registry_code: str = bundle.get("registry_code") or ""
    name: str = bundle.get("name") or ""
    if not registry_code or not name:
        return

    source_url = bundle.get("link") or (
        f"https://ariregister.rik.ee/eng/company/{registry_code}"
    )

    # ── 1. Entity statement for the company ─────────────────────────────
    reg_date = bundle.get("registration_date")
    if reg_date and len(reg_date) == 10 and reg_date[4] == "-":
        founding_date = reg_date  # already ISO
    else:
        founding_date = _ee_date(reg_date)

    vat = bundle.get("vat_number") or ""
    identifiers: list[dict[str, str]] = [
        {
            "id": registry_code,
            "scheme": "EE-ARIREGISTER",
            "schemeName": "Estonian e-Business Register",
        }
    ]
    if vat:
        identifiers.append({
            "id": vat,
            "scheme": "EE-VAT",
            "schemeName": "Estonian VAT number",
        })

    address_str = bundle.get("address") or ""
    addresses = (
        [{"type": "registered", "address": address_str, "country": "EE"}]
        if address_str
        else []
    )

    company_stmt = make_entity_statement(
        source_id="ariregister",
        local_id=registry_code,
        name=name,
        jurisdiction=("Estonia", "EE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. Shareholders ──────────────────────────────────────────────────
    seen_person_ids: set[str] = set()

    for sh in bundle.get("shareholders") or []:
        isiku_tyyp = sh.get("isiku_tyyp") or "F"
        pct_str = sh.get("osaluse_protsent") or ""
        share_size = sh.get("osaluse_suurus") or ""
        currency = sh.get("osaluse_valuuta") or ""
        start_date = _ee_date(sh.get("algus_kpv"))
        end_date = _ee_date(sh.get("lopp_kpv"))
        kirje_id = str(sh.get("kirje_id") or "")

        interests: list[dict[str, Any]] = [{"type": "shareholding"}]
        try:
            pct = float(pct_str) if pct_str else None
        except ValueError:
            pct = None
        if pct is not None:
            interests[0]["share"] = {
                "exact": pct,
                "minimum": pct,
                "maximum": pct,
                "exclusiveMinimum": False,
                "exclusiveMaximum": False,
            }
        if share_size:
            interests[0]["details"] = (
                f"Share value: {share_size} {currency}".strip()
            )
        if start_date:
            interests[0]["startDate"] = start_date
        if end_date:
            interests[0]["endDate"] = end_date

        if isiku_tyyp == "J":
            # Corporate shareholder — emit an entity statement
            corp_code = sh.get("isikukood_registrikood") or ""
            corp_name = (sh.get("nimi_arinimi") or "").strip()
            if not corp_name:
                continue
            corp_local_id = corp_code if corp_code else f"sh-corp-{kirje_id}"
            corp_ids: list[dict[str, str]] = []
            if corp_code:
                corp_ids.append({
                    "id": corp_code,
                    "scheme": "EE-ARIREGISTER",
                    "schemeName": "Estonian e-Business Register",
                })
            corp_stmt = make_entity_statement(
                source_id="ariregister",
                local_id=corp_local_id,
                name=corp_name,
                jurisdiction=("Estonia", "EE") if corp_code else None,
                identifiers=corp_ids,
                source_url=(
                    f"https://ariregister.rik.ee/eng/company/{corp_code}"
                    if corp_code
                    else None
                ),
            )
            yield corp_stmt
            yield make_relationship_statement(
                source_id="ariregister",
                local_id=f"sh-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=corp_stmt["statementId"],
                interested_party_type="entity",
                interests=interests,
                source_url=source_url,
            )
        else:
            # Natural person shareholder
            person_id = _ee_person_id(sh)
            full_name = _ee_full_name(sh)
            if not full_name or full_name == "Unknown":
                continue
            birth_date = _ee_date(sh.get("synniaeg"))
            country_code = sh.get("valis_kood_riik") or ""
            nationalities = (
                [{"code": country_code}]
                if country_code and country_code not in ("XXX", "EST")
                else []
            )
            p_ids: list[dict[str, str]] = []
            if sh.get("isikukood_hash"):
                p_ids.append({
                    "id": sh["isikukood_hash"],
                    "scheme": "EE-ARIREGISTER-HASH",
                    "schemeName": "Estonian e-Business Register person hash",
                })
            if person_id not in seen_person_ids:
                person_stmt = make_person_statement(
                    source_id="ariregister",
                    local_id=person_id,
                    full_name=full_name,
                    nationalities=nationalities,
                    birth_date=birth_date,
                    identifiers=p_ids,
                    source_url=source_url,
                )
                yield person_stmt
                seen_person_ids.add(person_id)
            else:
                person_stmt = {
                    "statementId": _stable_id("ariregister", "person", person_id)
                }
            yield make_relationship_statement(
                source_id="ariregister",
                local_id=f"sh-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=person_stmt["statementId"],
                interested_party_type="person",
                interests=interests,
                source_url=source_url,
            )

    # ── 3. Officers (kaardile_kantud_isikud) ─────────────────────────────
    for officer in bundle.get("officers") or []:
        role_code = officer.get("isiku_roll") or ""
        if role_code not in _EE_OFFICER_ROLE_MAP:
            continue  # skip roles we don't map (e.g. KISIK contact, ORP share registrar)
        interest_type, role_label = _EE_OFFICER_ROLE_MAP[role_code]
        start_date = _ee_date(officer.get("algus_kpv"))
        end_date = _ee_date(officer.get("lopp_kpv"))
        kirje_id = str(officer.get("kirje_id") or "")

        interests = [{"type": interest_type, "details": role_label}]
        if start_date:
            interests[0]["startDate"] = start_date
        if end_date:
            interests[0]["endDate"] = end_date

        person_id = _ee_person_id(officer)
        full_name = _ee_full_name(officer)
        if not full_name or full_name == "Unknown":
            continue

        birth_date = _ee_date(officer.get("synniaeg"))
        country_code = officer.get("valis_kood_riik") or ""
        nationalities = (
            [{"code": country_code}]
            if country_code and country_code not in ("XXX", "EST")
            else []
        )
        p_ids = []
        if officer.get("isikukood_hash"):
            p_ids.append({
                "id": officer["isikukood_hash"],
                "scheme": "EE-ARIREGISTER-HASH",
                "schemeName": "Estonian e-Business Register person hash",
            })
        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=nationalities,
                birth_date=birth_date,
                identifiers=p_ids,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"off-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )

    # ── 4. Beneficial owners (kasusaajad) ─────────────────────────────────
    # NOTE: Include only while Estonian law makes this data publicly available.
    # Set include_beneficial_owners=False in the adapter call to suppress.
    for bo in bundle.get("beneficial_owners") or []:
        kirje_id = str(bo.get("kirje_id") or "")
        start_date = _ee_date(bo.get("algus_kpv"))
        end_date = _ee_date(bo.get("lopp_kpv"))
        control_code = bo.get("kontrolli_teostamise_viis") or ""
        interest_type, control_label = _EE_BO_CONTROL_MAP.get(
            control_code, ("otherInfluenceOrControl", "")
        )

        interest: dict[str, Any] = {
            "type": interest_type,
            "beneficialOwnershipOrControl": True,
        }
        if control_label:
            interest["details"] = control_label
        if start_date:
            interest["startDate"] = start_date
        if end_date:
            interest["endDate"] = end_date
        interests: list[dict[str, Any]] = [interest]

        first = (bo.get("eesnimi") or "").strip()
        last = (bo.get("nimi") or "").strip()
        full_name = f"{first} {last}".strip() if first or last else ""
        if not full_name:
            continue

        person_id = _ee_person_id({
            "isikukood_hash": bo.get("isikukood_hash"),
            "kirje_id": kirje_id,
            "eesnimi": first,
            "nimi_arinimi": last,
        })
        birth_date = _ee_date(bo.get("synniaeg"))
        country_code = bo.get("valis_kood_riik") or ""
        res_country = bo.get("aadress_riik") or ""
        nationalities = (
            [{"code": country_code}]
            if country_code and country_code not in ("XXX",)
            else []
        )
        addresses = (
            [{"type": "residence", "address": "", "country": res_country}]
            if res_country
            else []
        )
        p_ids = []
        if bo.get("isikukood_hash"):
            p_ids.append({
                "id": bo["isikukood_hash"],
                "scheme": "EE-ARIREGISTER-HASH",
                "schemeName": "Estonian e-Business Register person hash",
            })
        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=nationalities,
                birth_date=birth_date,
                addresses=addresses,
                identifiers=p_ids,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"bo-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )


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

# Mapping from OpenCorporates officer position strings (lowercased) to BODS
# interest types.  Derived from the bods-opencorporates reference implementation
# (https://github.com/StephenAbbott/bods-opencorporates).  Matched
# case-insensitively; substring matching is used as a fallback.
_OC_POSITION_TO_INTEREST_TYPE: dict[str, str] = {
    # Board-level appointments
    "director": "appointmentOfBoard",
    "managing director": "appointmentOfBoard",
    "executive director": "appointmentOfBoard",
    "non-executive director": "appointmentOfBoard",
    "alternate director": "appointmentOfBoard",
    "shadow director": "appointmentOfBoard",
    "de facto director": "appointmentOfBoard",
    "deputy director": "appointmentOfBoard",
    "associate director": "appointmentOfBoard",
    "joint director": "appointmentOfBoard",
    "directeur": "appointmentOfBoard",
    "directeur general": "appointmentOfBoard",
    "geschaeftsfuehrer": "appointmentOfBoard",
    "direktor": "appointmentOfBoard",
    "bestuurder": "appointmentOfBoard",
    "amministratore": "appointmentOfBoard",
    "administrador": "appointmentOfBoard",
    # Board membership (non-chair)
    "board member": "boardMember",
    "member of the board": "boardMember",
    "supervisory board member": "boardMember",
    "aufsichtsratsmitglied": "boardMember",
    "bestuurslid": "boardMember",
    "vice president": "boardMember",
    "vorsitzender": "boardMember",
    "voorzitter": "boardMember",
    "presidente": "boardMember",
    # Board chair (BODS v0.4 has a separate boardChair type)
    "chairman": "boardChair",
    "chairwoman": "boardChair",
    "chairperson": "boardChair",
    "chair": "boardChair",
    "president": "boardChair",
    "vice chairman": "boardChair",
    "deputy chairman": "boardChair",
    # Senior management / officers
    "secretary": "seniorManagingOfficial",
    "company secretary": "seniorManagingOfficial",
    "corporate secretary": "seniorManagingOfficial",
    "assistant secretary": "seniorManagingOfficial",
    "joint secretary": "seniorManagingOfficial",
    "chief executive": "seniorManagingOfficial",
    "chief executive officer": "seniorManagingOfficial",
    "ceo": "seniorManagingOfficial",
    "chief financial officer": "seniorManagingOfficial",
    "cfo": "seniorManagingOfficial",
    "chief operating officer": "seniorManagingOfficial",
    "coo": "seniorManagingOfficial",
    "chief technology officer": "seniorManagingOfficial",
    "cto": "seniorManagingOfficial",
    "treasurer": "seniorManagingOfficial",
    "manager": "seniorManagingOfficial",
    "general manager": "seniorManagingOfficial",
    "partner": "seniorManagingOfficial",
    "general partner": "seniorManagingOfficial",
    "limited partner": "seniorManagingOfficial",
    "managing partner": "seniorManagingOfficial",
    "member": "seniorManagingOfficial",
    "managing member": "seniorManagingOfficial",
    "liquidator": "seniorManagingOfficial",
    "receiver": "seniorManagingOfficial",
    "administrator": "seniorManagingOfficial",
    "gerant": "seniorManagingOfficial",
    # Nominees / agents
    "nominee": "nominee",
    "nominee director": "nominee",
    "nominee shareholder": "nominee",
    "nominee secretary": "nominee",
    "agent": "otherInfluenceOrControl",
    "authorized representative": "otherInfluenceOrControl",
    "authorised representative": "otherInfluenceOrControl",
    "representative": "otherInfluenceOrControl",
    "legal representative": "otherInfluenceOrControl",
    "proxy": "otherInfluenceOrControl",
    "power of attorney": "otherInfluenceOrControl",
    # Trust roles
    "trustee": "trustee",
    "co-trustee": "trustee",
    "settlor": "settlor",
    "protector": "protector",
    "beneficiary": "beneficiaryOfLegalArrangement",
    "guardian": "otherInfluenceOrControl",
    # Ownership
    "shareholder": "shareholding",
    "owner": "shareholding",
    "subscriber": "shareholding",
    "incorporator": "otherInfluenceOrControl",
    "founder": "otherInfluenceOrControl",
}

# Relationship types from the OC Relationships Supplement → BODS interest type.
_OC_RELATIONSHIP_TYPE_TO_INTEREST: dict[str, str] = {
    "control_statement": "otherInfluenceOrControl",
    "control": "otherInfluenceOrControl",
    "subsidiary": "shareholding",
    "parent": "shareholding",
    "branch": "otherInfluenceOrControl",
    "share_parcel": "shareholding",
    "share": "shareholding",
}


def _oc_match_position(position: str) -> str:
    """Map an OC officer position string to a BODS interestType.

    Strategy: exact match → substring match → regex patterns → default.
    Officer positions never carry beneficialOwnershipOrControl=True
    (they represent governance roles, not ownership claims).
    """
    if not position:
        return "otherInfluenceOrControl"
    norm = position.strip().lower()
    if norm in _OC_POSITION_TO_INTEREST_TYPE:
        return _OC_POSITION_TO_INTEREST_TYPE[norm]
    for known, itype in _OC_POSITION_TO_INTEREST_TYPE.items():
        if known in norm:
            return itype
    # Regex fallbacks for multilingual variants
    import re as _re
    if _re.search(r"\bdirect(or|eur|ör)\b", norm):
        return "appointmentOfBoard"
    if _re.search(r"\bsecretar", norm):
        return "seniorManagingOfficial"
    if _re.search(r"\bmanag", norm):
        return "seniorManagingOfficial"
    if _re.search(r"\bchair", norm):
        return "boardChair"
    if _re.search(r"\btrustee", norm):
        return "trustee"
    if _re.search(r"\bnominee", norm):
        return "nominee"
    return "otherInfluenceOrControl"


def _oc_parse_network_relationships(
    network: dict[str, Any],
    focal_ocid: str,
) -> list[dict[str, Any]]:
    """Extract a list of normalised relationship dicts from a raw OC network payload.

    The OC ``/network`` endpoint (Relationships Supplement) is a premium
    API product.  Its exact JSON shape is not publicly documented, so we
    probe multiple plausible structures and normalise to a common internal
    dict::

        {
          "relationship_type": str,
          "source": {"name": str, "jurisdiction_code": str, "company_number": str},
          "target": {"name": str, "jurisdiction_code": str, "company_number": str},
          "percentage_min_share_ownership": float | None,
          "percentage_max_share_ownership": float | None,
          "percentage_min_voting_rights": float | None,
          "percentage_max_voting_rights": float | None,
          "start_date": str | None,
          "end_date": str | None,
        }

    Relationships with ``end_date`` set are skipped (historical only).
    """

    def _extract_company(obj: dict[str, Any]) -> dict[str, str]:
        """Unwrap a possibly-nested company dict → {name, jurisdiction_code, company_number}."""
        if "company" in obj and isinstance(obj["company"], dict):
            obj = obj["company"]
        return {
            "name": str(obj.get("name") or ""),
            "jurisdiction_code": str(obj.get("jurisdiction_code") or ""),
            "company_number": str(obj.get("company_number") or ""),
        }

    def _float_or_none(val: Any) -> float | None:
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None

    results: list[dict[str, Any]] = []

    # --- Try to locate the relationships list inside the network payload ---
    # Possible structures:
    #   A) network["relationships"] → list of {"relationship": {...}}
    #   B) network["network"] → list of {"relationship": {...}} or flat dicts
    #   C) network["edges"] → list of flat relationship dicts
    #   D) network is itself a list
    candidates: list[Any] = []
    if isinstance(network, list):
        candidates = network
    elif isinstance(network, dict):
        for key in ("relationships", "network", "edges"):
            val = network.get(key)
            if isinstance(val, list):
                candidates = val
                break

    for item in candidates:
        # Unwrap {"relationship": {...}} wrapper if present
        rel = item.get("relationship", item) if isinstance(item, dict) else item
        if not isinstance(rel, dict):
            continue

        end_date = rel.get("end_date")
        if end_date:
            continue  # skip historical relationships

        rel_type = (rel.get("relationship_type") or rel.get("type") or "").strip()

        # Source / target — OC may use source/target, subject/object, or from/to
        src_raw = (
            rel.get("source")
            or rel.get("subject")
            or rel.get("from")
            or {}
        )
        tgt_raw = (
            rel.get("target")
            or rel.get("object")
            or rel.get("to")
            or {}
        )

        # For endpoints that return a flat list of related companies (no
        # explicit source/target), the focal company is always the subject.
        if not src_raw and not tgt_raw:
            tgt_raw = rel  # the item itself is the related company
            jc, num = focal_ocid.split("/", 1) if "/" in focal_ocid else ("", focal_ocid)
            src_raw = {"jurisdiction_code": jc, "company_number": num, "name": ""}

        results.append({
            "relationship_type": rel_type,
            "source": _extract_company(src_raw) if isinstance(src_raw, dict) else {},
            "target": _extract_company(tgt_raw) if isinstance(tgt_raw, dict) else {},
            "percentage_min_share_ownership": _float_or_none(
                rel.get("percentage_min_share_ownership")
                or rel.get("percentage_min")
                or rel.get("min_percentage")
            ),
            "percentage_max_share_ownership": _float_or_none(
                rel.get("percentage_max_share_ownership")
                or rel.get("percentage_max")
                or rel.get("max_percentage")
            ),
            "percentage_min_voting_rights": _float_or_none(
                rel.get("percentage_min_voting_rights")
            ),
            "percentage_max_voting_rights": _float_or_none(
                rel.get("percentage_max_voting_rights")
            ),
            "start_date": rel.get("start_date"),
            "end_date": end_date,
        })

    return results


def _oc_build_interests_from_relationship(rel: dict[str, Any]) -> list[dict[str, Any]]:
    """Produce a BODS interests list from a normalised OC network relationship dict."""
    interests: list[dict[str, Any]] = []
    pmin_own = rel.get("percentage_min_share_ownership")
    pmax_own = rel.get("percentage_max_share_ownership")
    pmin_vot = rel.get("percentage_min_voting_rights")
    pmax_vot = rel.get("percentage_max_voting_rights")
    start = rel.get("start_date")

    def _share_obj(mn: float | None, mx: float | None) -> dict[str, float]:
        if mn is not None and mx is not None:
            return {"exact": mn} if mn == mx else {"minimum": mn, "maximum": mx}
        if mn is not None:
            return {"minimum": mn}
        if mx is not None:
            return {"maximum": mx}
        return {}

    if pmin_own is not None or pmax_own is not None:
        entry: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": True,
            "share": _share_obj(pmin_own, pmax_own),
        }
        if start:
            entry["startDate"] = start
        interests.append(entry)

    if pmin_vot is not None or pmax_vot is not None:
        entry = {
            "type": "votingRights",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": True,
            "share": _share_obj(pmin_vot, pmax_vot),
        }
        if start:
            entry["startDate"] = start
        interests.append(entry)

    # Fallback: no percentage data — use the relationship type to pick an interest
    if not interests:
        rel_type = rel.get("relationship_type", "")
        interest_type = _OC_RELATIONSHIP_TYPE_TO_INTEREST.get(
            rel_type.lower(), "otherInfluenceOrControl"
        )
        entry = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": interest_type in ("shareholding", "votingRights"),
            "details": f"OpenCorporates relationship: {rel_type}" if rel_type else "OpenCorporates network relationship",
        }
        if start:
            entry["startDate"] = start
        interests.append(entry)

    return interests


def map_opencorporates(bundle: dict[str, Any]) -> BODSBundle:
    """Map an OpenCorporates fetch bundle to BODS v0.4 statements.

    Produces:
    * One entity statement for the company itself.
    * One person or entity statement + relationship per current officer.
    * When the ``network`` key is present (OC Relationships Supplement),
      additional entity statements for related companies and ownership-or-
      control relationship statements for each active network relationship.

    Officers are sourced from ``/companies/{j}/{n}/officers`` (``position``,
    optional start/end dates).  Network relationships come from the premium
    ``/companies/{j}/{n}/network`` endpoint and cover ``control_statement``,
    ``subsidiary``, ``branch``, and ``share_parcel`` types.
    """
    result = BODSBundle()
    company = bundle.get("company") or {}
    ocid = bundle.get("ocid") or bundle.get("hit_id") or ""
    # The dedicated /officers endpoint requires a premium API tier and returns
    # null (402/403) for standard keys. Fall back to the officers list embedded
    # in the company profile endpoint response, which is available on all tiers
    # (typically up to 50 officers, wrapped as {"officer": {...}} items).
    officers = bundle.get("officers") or company.get("officers") or []
    network_raw = bundle.get("network")  # None when Supplement not available

    if not company:
        return result

    # --- Entity statement for the focal company ---------------------------

    name = company.get("name") or "Unknown company"
    jurisdiction_code = (company.get("jurisdiction_code") or "").upper()
    company_number = company.get("company_number") or ""
    incorporation_date = company.get("incorporation_date")
    oc_url = company.get("opencorporates_url") or (
        f"https://opencorporates.com/companies/{ocid}" if ocid else None
    )

    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        # OC uses ISO 3166-1 alpha-2 lower, with sub-national variants like
        # "us_de".  Use the top-level alpha-2 code for display.
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

    # Track emitted entity statementIds to avoid duplicates across officers
    # and network relationships.
    seen_entity_sids: set[str] = {subject_stmt_id}

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

        officer_type = (officer_data.get("type") or "").lower()
        is_corporate = officer_type == "company"

        if is_corporate:
            # Corporate officer → entity statement
            corp_stmt = make_entity_statement(
                source_id="opencorporates",
                local_id=local_key,
                name=officer_name,
                source_url=oc_url,
            )
            ip_sid: str = corp_stmt["statementId"]
            if ip_sid not in seen_entity_sids:
                result.extend([corp_stmt])
                seen_entity_sids.add(ip_sid)
            ip_type = "entity"
        else:
            # Natural person → person statement.
            # OC returns date_of_birth as "YYYY-MM" from the company profile
            # endpoint. Extract it so the cross-check's birth-year filter
            # can disambiguate common names against OpenSanctions / EP records.
            dob_raw = officer_data.get("date_of_birth") or ""
            birth_date: str | None = dob_raw if dob_raw else None

            # Nationality comes back as a plain string (e.g. "ITALIAN").
            nationality_str = (officer_data.get("nationality") or "").strip().title()
            nationalities = [{"name": nationality_str}] if nationality_str else []

            person_stmt = make_person_statement(
                source_id="opencorporates",
                local_id=local_key,
                full_name=officer_name,
                birth_date=birth_date,
                nationalities=nationalities,
                source_url=oc_url,
            )
            ip_sid = person_stmt["statementId"]
            result.extend([person_stmt])
            ip_type = "person"

        interest_type = _oc_match_position(position)
        start_date = officer_data.get("start_date")
        interest_entry: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,  # officer roles ≠ ownership
        }
        if interest_type == "otherInfluenceOrControl" and position:
            interest_entry["details"] = f"Officer position: {position}"
        if start_date:
            interest_entry["startDate"] = start_date

        rel_stmt = make_relationship_statement(
            source_id="opencorporates",
            local_id=f"rel/{local_key}",
            subject_statement_id=subject_stmt_id,
            interested_party_statement_id=ip_sid,
            interested_party_type=ip_type,
            interests=[interest_entry],
            source_url=oc_url,
        )
        result.extend([rel_stmt])

    # --- Network relationship statements ----------------------------------
    # These come from the OC Relationships Supplement (premium API tier).
    # Absent when the API key does not have access.
    if network_raw:
        parsed_rels = _oc_parse_network_relationships(network_raw, focal_ocid=ocid)
        for rel in parsed_rels:
            src = rel["source"]
            tgt = rel["target"]

            def _entity_for_company(co: dict[str, str]) -> dict[str, Any] | None:
                co_number = co.get("company_number") or ""
                co_jur = co.get("jurisdiction_code") or ""
                co_name = co.get("name") or "Unknown entity"
                if not co_number and not co_jur:
                    return None
                co_ocid = f"{co_jur}/{co_number}" if co_jur and co_number else co_number
                co_url = f"https://opencorporates.com/companies/{co_ocid}" if co_ocid else None
                co_jur_upper = co_jur.upper().split("_")[0]
                co_jurisdiction: tuple[str, str] | None = None
                if co_jur_upper:
                    try:
                        c = pycountry.countries.get(alpha_2=co_jur_upper)
                        co_jurisdiction = (c.name if c else co_jur_upper, co_jur_upper)
                    except Exception:  # noqa: BLE001
                        co_jurisdiction = (co_jur_upper, co_jur_upper)
                co_ids: list[dict[str, str]] = []
                if co_ocid:
                    co_ids.append({
                        "id": co_ocid,
                        "scheme": "OPENCORPORATES",
                        "schemeName": "OpenCorporates company identifier",
                        "uri": co_url or "",
                    })
                return make_entity_statement(
                    source_id="opencorporates",
                    local_id=co_ocid or co_number,
                    name=co_name,
                    jurisdiction=co_jurisdiction,
                    identifiers=co_ids,
                    entity_type="registeredEntity",
                    source_url=co_url,
                )

            src_stmt = _entity_for_company(src)
            tgt_stmt = _entity_for_company(tgt)
            if not src_stmt or not tgt_stmt:
                continue

            # In BODS, the relationship is: subject (the company being
            # controlled/owned) ← interestedParty (the owner/controller).
            # OC relationship direction: source controls/owns target.
            # → subject = target, interestedParty = source.
            subj_sid = tgt_stmt["statementId"]
            party_sid = src_stmt["statementId"]

            if subj_sid not in seen_entity_sids:
                result.extend([tgt_stmt])
                seen_entity_sids.add(subj_sid)
            if party_sid not in seen_entity_sids:
                result.extend([src_stmt])
                seen_entity_sids.add(party_sid)

            interests = _oc_build_interests_from_relationship(rel)
            rel_local_id = (
                f"network-rel/{src.get('company_number','?')}/"
                f"{tgt.get('company_number','?')}/"
                f"{rel.get('relationship_type','?')}"
            )
            network_rel_stmt = make_relationship_statement(
                source_id="opencorporates",
                local_id=rel_local_id,
                subject_statement_id=subj_sid,
                interested_party_statement_id=party_sid,
                interested_party_type="entity",
                interests=interests,
                source_url=oc_url,
            )
            result.extend([network_rel_stmt])

    return result


# ----------------------------------------------------------------------
# BrightQuery / OpenData.org → BODS
# ----------------------------------------------------------------------
#
# BrightQuery's COMPANY dataset provides US entities; PEOPLE_BUSINESS
# provides their executives / contacts.  Because BQ records executive
# affiliations rather than beneficial ownership, all people relationships
# are mapped to ``otherInfluenceOrControl`` with
# ``beneficialOwnershipOrControl = false`` — mirroring the approach taken
# by the reference bods-brightquery adapter.
#
# Identifier mapping (OTHER_ID_TYPE → BODS scheme):
#   CIK          → US-SEC
#   PERMID       → PERMID
#   SAM_UEI      → US-SAM-UEI
#   SAM_CAGE     → US-SAM-CAGE
#   CAPIQ        → CAPIQ
#   PITCHBOOK_ID → PITCHBOOK
#   NPI          → US-NPI
#   OPEN_FIGI    → OPENFIGI
#   ISIN         → ISIN
#   TICKER       → TICKER

_BQ_IDENTIFIER_MAP: list[tuple[str, str, str]] = [
    # (OTHER_ID_TYPE, BODS scheme code, human name)
    ("CIK",          "US-SEC",    "US SEC Central Index Key"),
    ("PERMID",       "PERMID",    "Refinitiv PermID"),
    ("SAM_UEI",      "US-SAM-UEI","US SAM Unique Entity Identifier"),
    ("SAM_CAGE",     "US-SAM-CAGE","US SAM CAGE Code"),
    ("CAPIQ",        "CAPIQ",     "S&P Capital IQ"),
    ("PITCHBOOK_ID", "PITCHBOOK", "PitchBook"),
    ("NPI",          "US-NPI",    "US National Provider Identifier"),
    ("OPEN_FIGI",    "OPENFIGI",  "OpenFIGI"),
    ("ISIN",         "ISIN",      "International Securities Identification Number"),
    ("TICKER",       "TICKER",    "Stock Ticker"),
]

_BQ_SOURCE_URL = "https://opendata.org/"


def _bq_features(record: dict[str, Any]) -> list[dict[str, Any]]:
    return record.get("FEATURES") or []


def _bq_get_feature(feats: list[dict], key: str) -> dict | None:
    """Return the first feature dict that contains *key*."""
    for f in feats:
        if key in f:
            return f
    return None


def _bq_get_value(feats: list[dict], key: str, default: str = "") -> str:
    f = _bq_get_feature(feats, key)
    return str(f[key]).strip() if f and f.get(key) is not None else default


def _bq_other_ids(feats: list[dict]) -> dict[str, str]:
    """Return all OTHER_ID_TYPE → OTHER_ID_NUMBER pairs from FEATURES."""
    result: dict[str, str] = {}
    for f in feats:
        id_type = f.get("OTHER_ID_TYPE")
        id_number = f.get("OTHER_ID_NUMBER")
        if id_type and id_number:
            result[str(id_type)] = str(id_number).strip()
    return result


def map_brightquery(bundle: dict[str, Any]) -> BODSBundle:
    """Map a BrightQuery bundle to BODS v0.4 statements.

    ``bundle`` shape (as returned by ``BrightQueryAdapter.fetch()``):

    .. code-block:: python

        {
            "source_id": "brightquery",
            "hit_id": "<LEI>",
            "lei": "<LEI>",
            "bq_id": "<RECORD_ID>",
            "name": "<primary name string>",
            "company": {<Senzing COMPANY record>},
            "people":  [{<Senzing PEOPLE_BUSINESS record>}, ...],
        }

    Produces:
    * One ``entity`` statement for the company.
    * One ``person`` + one ``relationship`` statement per named executive.
    """
    result = BODSBundle()

    company = bundle.get("company") or {}
    people = bundle.get("people") or []
    lei = bundle.get("lei") or bundle.get("hit_id") or ""
    bq_id = bundle.get("bq_id") or str(company.get("RECORD_ID") or "")

    if not company or not bq_id:
        return result

    feats = _bq_features(company)
    name = _bq_get_value(feats, "NAME_ORG") or bundle.get("name") or f"BrightQuery {bq_id}"
    other_ids = _bq_other_ids(feats)

    # --- Entity identifiers ---
    identifiers: list[dict[str, str]] = [
        {"id": bq_id, "scheme": "BRIGHTQUERY", "schemeName": "BrightQuery"},
    ]
    if lei:
        identifiers.append(
            {"id": lei, "scheme": "XI-LEI", "schemeName": "Legal Entity Identifier"}
        )
    for id_type, scheme, scheme_name in _BQ_IDENTIFIER_MAP:
        val = other_ids.get(id_type)
        if val:
            identifiers.append({"id": val, "scheme": scheme, "schemeName": scheme_name})

    # --- Business address ---
    addresses: list[dict[str, str]] = []
    addr_feat = _bq_get_feature(feats, "ADDR_LINE1") or _bq_get_feature(feats, "ADDR_CITY")
    if addr_feat:
        parts = [
            addr_feat.get("ADDR_LINE1"),
            addr_feat.get("ADDR_CITY"),
            addr_feat.get("ADDR_STATE"),
            addr_feat.get("ADDR_POSTAL_CODE"),
            addr_feat.get("ADDR_COUNTRY"),
        ]
        joined = ", ".join(p for p in parts if p)
        if joined:
            country = addr_feat.get("ADDR_COUNTRY", "")
            # Normalise "USA" → "US" for BODS country field.
            if country.upper() == "USA":
                country = "US"
            addresses.append({"type": "registered", "address": joined, "country": country})

    entity = make_entity_statement(
        source_id="brightquery",
        local_id=bq_id,
        name=name,
        jurisdiction=("United States", "US"),
        identifiers=identifiers,
        addresses=addresses,
        source_url=_BQ_SOURCE_URL,
    )
    result.statements.append(entity)
    entity_sid = entity["statementId"]

    # --- Executives (PEOPLE_BUSINESS records) ---
    for person_record in people:
        pfeats = _bq_features(person_record)
        person_id = str(person_record.get("RECORD_ID") or "").strip()
        if not person_id:
            continue

        # Build a display name; skip truly nameless records.
        full_name = _bq_get_value(pfeats, "NAME_FULL")
        if not full_name:
            first = _bq_get_value(pfeats, "NAME_FIRST")
            last = _bq_get_value(pfeats, "NAME_LAST")
            full_name = f"{first} {last}".strip()
        if not full_name:
            continue

        # Role from REL_POINTER_ROLE (e.g. "Executive", "Director").
        role = ""
        for f in pfeats:
            if "REL_POINTER_ROLE" in f:
                role = str(f["REL_POINTER_ROLE"]).strip()
                break

        local_person_id = f"{bq_id}:person:{person_id}"

        person = make_person_statement(
            source_id="brightquery",
            local_id=local_person_id,
            full_name=full_name.title(),
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(person)
        person_sid = person["statementId"]

        interest: dict[str, Any] = {
            "type": "otherInfluenceOrControl",
            "directOrIndirect": "unknown",
            "beneficialOwnershipOrControl": False,
        }
        if role:
            interest["details"] = role

        rel = make_relationship_statement(
            source_id="brightquery",
            local_id=f"{bq_id}:rel:{person_id}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(rel)

    return result
