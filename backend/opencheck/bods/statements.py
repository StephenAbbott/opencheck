"""The BODS statement factories every mapper is built from.

Split out of ``mapper.py`` in Phase 168, unchanged. It had grown to 10,824
lines — one module holding both *how a BODS statement is made* and *how
forty-odd sources are read* — and the first of those is the part a new
adapter actually needs. Everything here is source-agnostic:

* ``make_entity_statement`` / ``make_person_statement`` /
  ``make_relationship_statement`` — the three statement kinds, each wrapping
  a ``recordDetails`` object;
* ``_stable_id`` — deterministic statement IDs (source id + stable local
  key), so re-mapping a payload twice produces the same IDs, which is what
  dedup across runs and the graph's node keys both depend on;
* ``_source_block``, ``_statement_date``, ``_addr``, ``_country_obj`` —
  the shared blocks;
* ``set_beneficial_ownership`` — the one place BODS's three-state
  ``beneficialOwnershipOrControl`` policy is decided, moved here from the
  middle of the FtM section, which is not where a policy about every source
  belongs;
* ``BODSBundle`` — what a mapper returns.

``mapper.py`` re-exports all of it, so every existing import path still
works, including the private helpers that tests and ``routers/lookup.py``
reach for by name.

Reference: https://standard.openownership.org/en/0.4.0/
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable

import pycountry

from .. import names as _names_mod
from .. import provenance as _provenance
from .annotations import commenting, pointer
from .psc_natures import describe_nature

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
    # Scottish-partnership codes use the singular "person"
    # (``right-to-appoint-and-remove-person[-as-firm|-as-trust]``). The previous
    # plural prefix never matched them, so they fell through to
    # ``otherInfluenceOrControl``. The singular prefix matches the singular form
    # and any plural form via ``startswith``.
    "right-to-appoint-and-remove-person": "appointmentOfBoard",
    # LLP (``right-to-share-surplus-assets-*``) and Scottish-partnership
    # (``part-right-to-share-surplus-assets-*``) surplus-asset rights.
    "right-to-share-surplus-assets": "rightsToSurplusAssetsOnDissolution",
    "part-right-to-share-surplus-assets": "rightsToSurplusAssetsOnDissolution",
    "significant-influence-or-control": "otherInfluenceOrControl",
    # NOTE: ``registered-owner-as-nominee-*`` (registered overseas entity) codes
    # are intentionally NOT mapped to the ``nominee`` interest type here. BODS
    # requires nominee arrangements to be modelled via an intermediary
    # ``arrangement`` entity (entityType.subtype ``nomination``) linked by
    # ``nominator``/``nominee`` relationships — not a bare ``nominee`` interest
    # on a direct PSC relationship. See
    # https://standard.openownership.org/en/0.4.0/standard/modelling/repr-nominations.html
    # Until that arrangement model is implemented these fall through to
    # ``otherInfluenceOrControl``; the descriptor carried in ``interest.details``
    # preserves the nominee meaning.
}

_SHARE_BAND_RE = re.compile(r"(\d+)-to-(\d+)-percent")


def _birth_date_precision_note(birth_date: str | None) -> dict[str, Any] | None:
    """Explain an imprecise birthDate rather than leaving it ambiguous.

    BODS permits ``YYYY``, ``YYYY-MM`` and ``YYYY-MM-DD`` for ``birthDate``, so
    an imprecise value here is correct output, not a defect — Companies House
    publishes only month and year for PSCs and officers, deliberately, for
    privacy. But a consumer reading "1975-08" cannot tell a privacy-limited
    register from a truncation on our side. The annotation says which, and uses
    motivation ``commenting`` rather than ``transformation`` because nothing
    was transformed: the value is exactly what the register published.

    Rounding it to a full date would be actively wrong here — it would
    fabricate a day the register withheld on purpose.
    """
    if not birth_date:
        return None
    if len(birth_date) == 7:
        detail = "month and year only"
    elif len(birth_date) == 4:
        detail = "year only"
    else:
        return None
    return commenting(
        pointer("recordDetails", "birthDate"),
        (
            f"Source publishes {detail} for this person's date of birth; the "
            "remaining precision was never disclosed, not withheld or lost by "
            "OpenCheck. BODS permits an imprecise birthDate for this reason."
        ),
        creation_date=_today(),
    )


def _parse_nature(nature: str) -> dict[str, Any]:
    """Return a BODS ``interests`` entry for a single PSC nature-of-control string."""
    lowered = nature.lower()

    interest_type = "otherInfluenceOrControl"
    for prefix, mapped in _INTEREST_PREFIX.items():
        if lowered.startswith(prefix):
            interest_type = mapped
            break

    # Prefer the official human-readable descriptor for the code; fall back to
    # the raw code string for any code not in the vendored enumeration.
    # No beneficialOwnershipOrControl here: the flag depends on WHO holds the
    # interest (individual PSC vs corporate RLE), which only the caller knows —
    # _emit_company_statements routes it through the regimes registry.
    entry: dict[str, Any] = {
        "type": interest_type,
        "directOrIndirect": "direct",
        "details": describe_nature(nature) or nature,
    }

    band = _SHARE_BAND_RE.search(lowered)
    if band:
        # BODS v0.4: ``share.exclusiveMinimum`` is a *number* (the exclusive
        # lower bound), not a boolean.  PSC ranges like "25-to-50-percent"
        # mean strictly more than 25 % and up to (inclusive) 50 %.
        entry["share"] = {
            "exclusiveMinimum": int(band.group(1)),
            "maximum": int(band.group(2)),
        }
    elif "75-to-100-percent" in lowered:
        entry["share"] = {"exclusiveMinimum": 75, "maximum": 100}

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


def _country_obj(code: str) -> dict[str, str] | None:
    """Return a BODS-compliant country object ``{"name": ..., "code": ...}`` or
    ``None`` if *code* is empty.

    Accepts ISO 3166-1 alpha-2 codes, alpha-3 codes, or full country names in
    any language supported by pycountry.  Falls back gracefully: if pycountry
    can't resolve the string we still emit an object with the raw value so
    nothing is silently lost.
    """
    if not code:
        return None
    stripped = code.strip()
    upper = stripped.upper()
    # Direct alpha-2 lookup.
    c = pycountry.countries.get(alpha_2=upper)
    if c:
        return {"name": c.name, "code": c.alpha_2}
    # Try alpha-3 (some sources emit 3-letter codes).
    c = pycountry.countries.get(alpha_3=upper)
    if c:
        return {"name": c.name, "code": c.alpha_2}
    # Try pycountry name/common-name lookup (handles many language variants).
    try:
        c = pycountry.countries.lookup(stripped)
        return {"name": c.name, "code": c.alpha_2}
    except LookupError:
        pass
    # Overrides for native-language names pycountry can't resolve.
    # Includes UK sub-regions (England, Scotland, Wales, Northern Ireland) which
    # are ISO 3166-2 subdivisions, not ISO 3166-1 countries, so they must map to
    # "GB".  Mainly needed for Companies House addresses and other UK sources.
    # Also covers Bolagsverket (Swedish API) and other European sources.
    _NATIVE_NAMES: dict[str, str] = {
        # UK sub-regions + English variants pycountry no longer resolves
        "England": "GB",
        "ENGLAND": "GB",
        "Scotland": "GB",
        "SCOTLAND": "GB",
        "Wales": "GB",
        "WALES": "GB",
        "Northern Ireland": "GB",
        "NORTHERN IRELAND": "GB",
        "Great Britain": "GB",
        "GREAT BRITAIN": "GB",
        "United Kingdom": "GB",
        "UNITED KINGDOM": "GB",
        # pycountry renamed Turkey -> Türkiye, so the English name no longer resolves
        "Turkey": "TR",
        "TURKEY": "TR",
        # Bolagsverket / other Scandinavian sources
        "Sverige": "SE",
        "SVERIGE": "SE",
        "Norge": "NO",
        "NORGE": "NO",
        "Danmark": "DK",
        "DANMARK": "DK",
        "Finland": "FI",
        "FINLAND": "FI",
        "Suomi": "FI",
        "SUOMI": "FI",
        "Deutschland": "DE",
        "DEUTSCHLAND": "DE",
        "Frankreich": "FR",
        "Österreich": "AT",
        "Schweiz": "CH",
        "SCHWEIZ": "CH",
        "Pays-Bas": "NL",
        "España": "ES",
        "ESPAÑA": "ES",
        "Italia": "IT",
        "ITALIA": "IT",
        "Polska": "PL",
        "POLSKA": "PL",
    }
    alpha2 = _NATIVE_NAMES.get(stripped) or _NATIVE_NAMES.get(upper)
    if alpha2:
        c = pycountry.countries.get(alpha_2=alpha2)
        if c:
            return {"name": c.name, "code": c.alpha_2}
    # Unknown value — preserve the name but OMIT the code. BODS Country requires
    # `name` and only SHOULD carry a 2-letter ISO code; emitting the raw string as
    # a `code` violates the maxLength:2 / minLength:2 constraint (e.g. real PSC
    # addresses with country "Great Britain" or "Turkey" that pycountry can't
    # resolve). Name-only is conformant and loses nothing.
    return {"name": stripped}


def _addr(type_: str, address: str, country_code: str = "") -> dict[str, Any]:
    """Build a single BODS address dict with an optional country object.

    ``country_code`` should be an ISO 3166-1 alpha-2 string; if absent or
    unresolvable the ``country`` key is omitted so the address still validates.
    """
    d: dict[str, Any] = {"type": type_, "address": address}
    co = _country_obj(country_code)
    if co:
        d["country"] = co
    return d


# ----------------------------------------------------------------------
# Statement factories
# ----------------------------------------------------------------------


def _publication_details_block(publication_date: str | None = None) -> dict[str, Any]:
    """Build a BODS v0.4 ``publicationDetails`` block.

    ``bodsVersion``, ``publisher``, and ``publicationDate`` are all required
    by the BODS v0.4 schema.

    Semantics (per BODS dates guidance):
      publicationDate  — the date **this** statement was published, by the
                         publisher named in this same block. That publisher is
                         OpenCheck, so the date is OpenCheck's, and the default
                         (today) is nearly always the right answer.

    This used to hold the *source's* date instead — a PSC notification date, a
    SEC filing date, a GLEIF last-update date — which put a register's
    declaration into a field describing OpenCheck's publication. Those belong in
    ``statementDate``: "the date this statement was declared by the source".
    Open Ownership's own published bundles model it exactly that way
    (statementDate 2024-06-06, publicationDate 2025-02-28, publisher Open
    Ownership). See ``_statement_date``.

    The parameter survives for the rare case where a caller genuinely knows a
    different OpenCheck publication date — replaying an archived run, say.
    """
    return {
        "bodsVersion": "0.4",
        "publicationDate": publication_date or _today(),
        "publisher": {"name": "OpenCheck"},
    }


def make_entity_statement(
    *,
    source_id: str,
    local_id: str,
    name: str,
    jurisdiction: tuple[str, str] | None = None,
    identifiers: Iterable[dict[str, str]] = (),
    founding_date: str | None = None,
    dissolution_date: str | None = None,
    addresses: Iterable[dict[str, str]] = (),
    alternate_names: Iterable[str] = (),
    entity_type: str = "registeredEntity",
    entity_details: str | None = None,
    source_url: str | None = None,
    publication_date: str | None = None,
    statement_date: str | None = None,
) -> dict[str, Any]:
    statement_id = _stable_id(source_id, "entity", local_id)
    # bods-dagre v0.4 resolves graph edges by matching the relationship's
    # referenced statementId against each entity/person statement's recordId.
    # Using statementId == recordId ensures that lookup succeeds without
    # breaking BODS semantics: we never version records in opencheck so the
    # distinction between "statement id" and "record id" doesn't apply.
    record_id = statement_id

    entity_type_obj: dict[str, Any] = {"type": entity_type}
    if entity_details:
        entity_type_obj["details"] = entity_details
    record_details: dict[str, Any] = {
        "isComponent": False,
        "entityType": entity_type_obj,
        "name": name,
        "identifiers": list(identifiers),
    }
    if jurisdiction:
        record_details["jurisdiction"] = {
            "name": jurisdiction[0],
            "code": jurisdiction[1],
        }
    if founding_date:
        record_details["foundingDate"] = founding_date
    if dissolution_date:
        record_details["dissolutionDate"] = dissolution_date
    addresses = list(addresses)
    if addresses:
        record_details["addresses"] = addresses
    alternate_names = list(alternate_names)
    # Phase E (rigour adoption): a Cyrillic/Greek primary name gains a
    # deterministic Latin transliteration as an alternate, so downstream
    # consumers (search, exports, screening) always have a Latin form.
    translit = _names_mod.transliterate_display(name)
    if translit and translit != name and translit not in alternate_names:
        alternate_names.append(translit)
    if alternate_names:
        record_details["alternateNames"] = alternate_names

    return {
        "statementId": statement_id,
        "recordId": record_id,
        "declarationSubject": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "statementDate": _statement_date(statement_date),
        "publicationDetails": _publication_details_block(publication_date),
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
    publication_date: str | None = None,
    statement_date: str | None = None,
    political_exposure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    statement_id = _stable_id(source_id, "person", local_id)
    record_id = statement_id  # see make_entity_statement for reasoning

    person_names: list[dict[str, str]] = [{"type": "legal", "fullName": full_name}]
    # Phase E (rigour adoption): Cyrillic/Greek person names carry a typed
    # transliteration entry (BODS v0.4 nameType codelist: "transliteration").
    _translit = _names_mod.transliterate_display(full_name)
    if _translit and _translit != full_name:
        person_names.append({"type": "transliteration", "fullName": _translit})
    record_details: dict[str, Any] = {
        "isComponent": False,
        "personType": person_type,
        "names": person_names,
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
    if political_exposure:
        record_details["politicalExposure"] = political_exposure

    return {
        "statementId": statement_id,
        "recordId": record_id,
        "declarationSubject": record_id,
        "recordType": "person",
        "recordStatus": "new",
        "statementDate": _statement_date(statement_date),
        "publicationDetails": _publication_details_block(publication_date),
        "recordDetails": record_details,
        "source": _source_block(source_id, source_url),
    }


def make_relationship_statement(
    *,
    source_id: str,
    local_id: str,
    subject_statement_id: str,
    interested_party_statement_id: str | None = None,
    interested_party_type: str = "person",
    interested_party_unspecified: dict[str, Any] | None = None,
    interests: Iterable[dict[str, Any]] = (),
    source_url: str | None = None,
    publication_date: str | None = None,
    statement_date: str | None = None,
    record_status: str = "new",
) -> dict[str, Any]:
    """Build a BODS v0.4 relationship statement.

    Lifecycle (BODS 0.4 *Information updates* + *Record identifiers* modelling
    requirements):

    * The ``recordId`` of a relationship MUST be **stable over time** — every
      statement in the record's lifecycle (``new`` → ``updated`` → ``closed``)
      shares the same ``recordId``. It is derived purely from ``local_id`` here,
      so it never changes as the relationship's status changes. Consumers link a
      record's versions together through this shared ``recordId``.
    * Each ``statementId`` MUST be unique. For non-``new`` lifecycle stages the
      statementId is varied (by status + publication date) so the closed/updated
      statement is a distinct statement from the original ``new`` one, while the
      ``recordId`` stays put.

    BODS 0.4 **removed** the ``replacesStatements`` field (see the 0.4.0
    changelog "Removed" section): supersession is expressed solely through the
    shared, stable ``recordId``, so this factory does not emit
    ``replacesStatements``.
    """
    record_id = _stable_id(source_id, "relationship-record", local_id)
    if record_status == "new":
        statement_id = _stable_id(source_id, "relationship", local_id)
    else:
        statement_id = _stable_id(
            source_id, "relationship", local_id, record_status, publication_date or ""
        )

    statement: dict[str, Any] = {
        "statementId": statement_id,
        "recordId": record_id,
        "declarationSubject": subject_statement_id,
        "recordType": "relationship",
        "recordStatus": record_status,
        "statementDate": _statement_date(statement_date),
        "publicationDetails": _publication_details_block(publication_date),
        "recordDetails": {
            "isComponent": False,
            "subject": subject_statement_id,
            "interestedParty": (
                interested_party_unspecified
                if interested_party_unspecified is not None
                else interested_party_statement_id
            ),
            "interests": list(interests),
        },
        "source": _source_block(source_id, source_url),
    }
    return statement


#: ``source.description`` stamped on every statement a source contributes,
#: keyed by source id. Module-level (rather than local to ``_source_block``)
#: because the frontend lineage table is generated from it: the FullCheck
#: network holds these labels, not adapter ids, so the label is the join key.
SOURCE_NAMES: dict[str, str] = {
    "abr_australia": "Australian Business Register — ABN Lookup (Australian Taxation Office)",
    "acra_singapore": "ACRA — Accounting and Corporate Regulatory Authority (Singapore)",
    "ariregister": "Estonian e-Business Register (e-Äriregister)",
    "bce_belgium": "BCE/KBO — Banque-Carrefour des Entreprises (Belgian Business Register)",
    "bods_gleif": "GLEIF — Global LEI Foundation (BODS bulk dataset)",
    "bods_uk_psc": "UK Companies House — Persons with Significant Control (BODS bulk dataset)",
    "bolagsverket": "Bolagsverket — Swedish Companies Registration Office",
    "cac_nigeria": "CAC — Corporate Affairs Commission (Nigeria) Persons with Significant Control register",
    "brreg": "Brønnøysundregistrene — Norwegian Register Centre",
    "brightquery": "BrightQuery / OpenData.org",
    "climatetrace": "Global Energy Monitor / Climate TRACE",
    "companies_house": "UK Companies House",
    "corporations_canada": "Corporations Canada — ISED federal register",
    "cyprus_drcor": "Cyprus DRCOR — Department of Registrar of Companies and Intellectual Property",
    "cnpj_brazil": "Receita Federal — CNPJ register (Brazil)",
    "cro": "CRO — Companies Registration Office Ireland",
    "cvr_denmark": "CVR — Det Centrale Virksomhedsregister (Danish Business Authority)",
    "everypolitician": "EveryPolitician",
    "firmenbuch": "Firmenbuch — Austrian Commercial Register",
    "gleif": "GLEIF",
    "gemi_greece": "ΓΕΜΗ — Greek General Commercial Registry (Γενικό Εμπορικό Μητρώο)",
    "inpi": "INPI — Registre National des Entreprises",
    "jar_lithuania": "JAR — Juridinių asmenų registras (Lithuanian Register of Legal Entities)",
    "krs_poland": "KRS — Polish National Court Register (Krajowy Rejestr Sądowy)",
    "kvk": "KvK — Netherlands Chamber of Commerce",
    "malta_mbr": "Malta Business Registry (MBR)",
    "mca_india": "MCA — Ministry of Corporate Affairs Company Master Data (India)",
    "nz_companies": "New Zealand Companies Register (NZBN)",
    "openaleph": "OpenAleph",
    "opencorporates": "OpenCorporates",
    "opensanctions": "OpenSanctions",
    "prh": "PRH — Finnish Patent and Registration Office (Patentti- ja rekisterihallitus)",
    "rpo_slovakia": "RPO — Slovak Register of Legal Persons",
    "rpvs_slovakia": "RPVS — Slovak Public Sector Partners Register",
    "sec_edgar": "SEC EDGAR — U.S. Securities and Exchange Commission",
    "sudreg_croatia": "Sudski registar — Croatian Court Register",
    "ted_eu": "TED — Tenders Electronic Daily (EU procurement notices)",
    "eiti": "EITI — Extractive Industries Transparency Initiative",
    "eiti_bo": "EITI countries — national beneficial ownership registers (pooled: ITIE-RDC, Armenia State Register, Nigeria CAC/NEITI subset)",
    "eiti_soe": "EITI State-Owned Enterprises Database",
    "ur_latvia": "UR — Latvian Register of Enterprises (data.gov.lv)",
    "ares": "ARES — Czech Administrativní registr ekonomických subjektů",
    "wikidata": "Wikidata",
    "wikirate": "Wikirate",
    "zefix": "Zefix — Swiss Commercial Registry",
}


def _source_block(source_id: str, source_url: str | None) -> dict[str, Any]:
    source_names = SOURCE_NAMES
    _official_registers = {
        "abr_australia",
        "acra_singapore",
        "ariregister",
        "bce_belgium",
        "bods_gleif",
        "bods_uk_psc",
        "bolagsverket",
        "brreg",
        "cac_nigeria",
        "companies_house",
        "corporations_canada",
        "cyprus_drcor",
        "cnpj_brazil",
        "cro",
        "cvr_denmark",
        "eiti_bo",
        "firmenbuch",
        "gemi_greece",
        "inpi",
        "jar_lithuania",
        "krs_poland",
        "kvk",
        "malta_mbr",
        "mca_india",
        "nz_companies",
        "opencorporates",
        "prh",
        "rpo_slovakia",
        "rpvs_slovakia",
        "sec_edgar",
        "sudreg_croatia",
        "ur_latvia",
        "ares",
        "zefix",
    }
    block: dict[str, Any] = {
        "type": ["officialRegister"] if source_id in _official_registers else ["thirdParty"],
        "description": source_names.get(source_id, source_id),
    }
    # ``retrievedAt`` is a factual claim about when OpenCheck downloaded the
    # data, not a timestamp of when this function happened to run. It is
    # emitted only where the pipeline actually observed a retrieval — so stub
    # output, and curated fixtures whose real retrieval date is unknown, carry
    # no claim at all. (Previously every statement asserted datetime.utcnow()
    # at mapping time, which was false for cached, snapshot, curated and stub
    # payloads alike.)
    retrieved_at = _provenance.current_mapping_provenance().retrieved_at_iso()
    if retrieved_at:
        block["retrievedAt"] = retrieved_at
    if source_url:
        block["url"] = source_url
    return block


def _statement_date(explicit: str | None = None) -> str:
    """The date the source claimed this was true.

    Precedence:

    1. A date the source itself supplies (a filing or last-update date),
       passed in by the individual mapper where it has one.
    2. The date OpenCheck retrieved the payload. For a months-old bulk
       snapshot this is far closer to the truth than today's date, and the
       BODS dates guidance's consolidation reading — "the date on which
       several sources of information were resolved to make a coherent
       claim" — covers using it.
    3. Today, for stub and curated payloads where neither exists.
    """
    if explicit:
        return explicit
    retrieved = _provenance.current_mapping_provenance().retrieved_at
    if retrieved is not None:
        return retrieved.date().isoformat()
    return _today()


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
        # UK constituent nations and common CH sub-jurisdiction strings.
        "england": "GB",
        "england and wales": "GB",
        "england & wales": "GB",
        "scotland": "GB",
        "wales": "GB",
        "northern ireland": "GB",
        "great britain": "GB",
        "united kingdom": "GB",
        # Common abbreviations.
        "uae": "AE",
        "usa": "US",
        "us": "US",
    }
    override = _OVERRIDES.get(stripped.lower())
    if override:
        return override
    try:
        return pycountry.countries.lookup(stripped).alpha_2
    except LookupError:
        return ""


# ----------------------------------------------------------------------
# beneficialOwnershipOrControl — one policy, stated once
# ----------------------------------------------------------------------
# BODS distinguishes three states: true, false, and absent ("not stated").
# Collapsing "absent" into "true" is the error this policy exists to prevent.
#
# A shareholding is a LEGAL holding. Whether it is also a BENEFICIAL one is a
# separate fact that only a register, or a beneficial-ownership declaration
# regime, can supply. Equating the two is the error the nominee concept exists
# to name — and it is the wrong direction of error for a transparency tool,
# because over-claiming beneficial ownership is a reputational assertion about
# a named person that travels into every export (RDF, FtM, Senzing, BigQuery),
# well beyond any caveat the UI can attach.
#
# The reasoning was already written down for the FtM path
# (_FTM_BO_ASSERTING_DATASETS below): "otherwise leave the flag unset ('not
# stated') rather than asserting a registered holding is a beneficial one." It
# had simply never been applied to the commercial-register mappers, which
# inferred the flag from the shape of the interest — `interest_type in
# ("shareholding", "votingRights")` for OpenCorporates network relationships,
# `kind == "person"` for New Zealand, and a hard-coded True for every SEC
# 13D/13G filer.
#
# Sources that publish or validate an actual beneficial-ownership declaration,
# and may therefore assert the flag. Extend only with a source whose ownership
# records are demonstrably beneficial ownership, not merely registered holdings.
# The per-jurisdiction legal definitions behind this list — statute, threshold,
# reporting basis, per-record-kind policy — live in ``bo_regimes.py`` and are
# rendered to ``docs/bo-regimes.md``.
_BO_ASSERTING_SOURCES: frozenset[str] = frozenset({
    "bods_uk_psc",      # UK PSC register — a BO regime; the flag is copied verbatim
    "bods_gleif",       # Open Ownership's processed output; flag copied verbatim
    "companies_house",  # PSC records are BO declarations under the UK regime
    "rpvs_slovakia",    # Register of Public Sector Partners — verified KUV
    "ur_latvia",        # Latvian BO register records
    "cac_nigeria",      # CAC Persons with Significant Control register
    "ariregister",      # Estonian register files BO alongside shareholders
})


def source_may_assert_beneficial_ownership(source_id: str) -> bool:
    """True when *source_id* publishes or validates BO declarations.

    Every other source describes registered/legal holdings. For those, omit
    ``beneficialOwnershipOrControl`` entirely rather than guessing — see
    ``set_beneficial_ownership``.
    """
    return source_id in _BO_ASSERTING_SOURCES


def set_beneficial_ownership(
    interest: dict[str, Any],
    source_id: str,
    *,
    asserted: bool | None = None,
    record_kind: str | None = None,
) -> dict[str, Any]:
    """Set ``beneficialOwnershipOrControl`` on *interest*, or leave it unset.

    ``asserted`` is what the SOURCE said:

    * ``True`` / ``False`` — the register stated it. Emitted as given, for any
      source, because an explicit statement outranks our classification.
    * ``None`` — the source said nothing; what happens next depends on
      ``record_kind``.

    ``record_kind`` names what kind of record the interest came from (a BO
    declaration, a registered holding, an officer role, ...) and looks the
    policy up in the per-jurisdiction regimes registry
    (``bo_regimes.boc_policy``): ``assert_true`` -> True, ``assert_false`` ->
    False, anything else (``omit``, unknown kind, unknown source) -> the flag
    stays unset. Pass it whenever the record kind is known — it is the fix for
    the source-level/record-level conflation the 2026-08 audit flagged: a
    source that publishes BO declarations ALSO publishes plain holdings, and
    "the source may assert" never meant "every record asserts".

    Without ``record_kind`` the legacy source-level rule applies: the flag is
    emitted only if the source publishes BO declarations at all; otherwise it
    is omitted, which BODS reads as "not stated".

    Mutates and returns *interest* so it can be used inline.
    """
    if asserted is not None:
        interest["beneficialOwnershipOrControl"] = asserted
        return interest
    if record_kind is not None:
        from .bo_regimes import boc_policy

        policy = boc_policy(source_id, record_kind)
        if policy == "assert_true":
            interest["beneficialOwnershipOrControl"] = True
        elif policy == "assert_false":
            interest["beneficialOwnershipOrControl"] = False
        return interest
    if source_may_assert_beneficial_ownership(source_id):
        interest["beneficialOwnershipOrControl"] = True
    return interest
