"""United States — SEC EDGAR (Schedule 13D/13G) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any

from ..statements import (
    BODSBundle,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# SEC EDGAR (Schedule 13D/13G) → BODS
# ----------------------------------------------------------------------

try:
    import pycountry as _pycountry
except ImportError:  # pragma: no cover
    _pycountry = None  # type: ignore[assignment]


def _iso2_to_country_name(iso2: str) -> str:
    """Return a human-readable country name for an ISO 3166-1 alpha-2 code.

    Falls back to the code itself when pycountry is unavailable or the
    code is not found (which should not happen for codes from EDGAR's
    controlled vocabulary, but is defensive).
    """
    if not iso2:
        return ""
    if _pycountry is None:
        return iso2
    country = _pycountry.countries.get(alpha_2=iso2.upper())
    return country.name if country else iso2


# SEC Schedule 13D/13G ``typeOfReportingPerson`` codes that describe a filer
# acting in a CUSTODIAL or ADVISORY capacity rather than as the beneficial
# owner. A 13D/13G "beneficial owner" is an SEC-rules term meaning voting or
# dispositive power — an investment adviser voting client shares has it without
# any economic interest in the shares. The evidence to tell the two apart was
# already in the bundle (sec_edgar.py parses type_code alongside sole/shared
# voting power) and was simply never read: the mapper hard-coded
# beneficialOwnershipOrControl: True for every filer.
_SEC_CUSTODIAL_REPORTER_CODES: frozenset[str] = frozenset({
    "IA",  # Investment adviser
    "BD",  # Broker-dealer
    "IC",  # Investment company
    "EP",  # Employee benefit plan / ERISA
    "SA",  # Savings association
    "BK",  # Bank
    "IN",  # (see below — natural persons are NOT custodial; excluded in code)
})
# "IN" is a natural person and is emphatically not custodial; it is listed above
# only to make the omission deliberate rather than accidental.
_SEC_CUSTODIAL_REPORTER_CODES = _SEC_CUSTODIAL_REPORTER_CODES - {"IN"}


def _sec_beneficial_ownership(
    reporter: dict[str, Any], party_type: str = "person"
) -> bool | None:
    """What, if anything, a 13D/13G filing says about beneficial ownership.

    Returns ``None`` — "not stated" — when the filer reports in a custodial or
    advisory capacity, because the filing then asserts voting/dispositive power
    without asserting that the filer benefits. Returns ``True`` for an ordinary
    NATURAL-PERSON filer, where the SEC's own beneficial-ownership test (Rule
    13d-3: voting and/or dispositive power) has been met. Returns ``False`` for
    an ordinary ENTITY filer: Rule 13d-3 admits entities, but a BODS beneficial
    owner is a natural person, so an entity interested party never carries
    ``true`` — bo_regimes: sec_edgar/filer_entity -> assert_false, matching
    Open Ownership's entity-party convention (2026-08 audit).
    """
    code = (reporter.get("type_code") or "").strip().upper()
    if code in _SEC_CUSTODIAL_REPORTER_CODES:
        return None
    return party_type == "person"


def map_sec_edgar(bundle: dict[str, Any]) -> BODSBundle:
    """Map a SEC EDGAR 13D/13G bundle to BODS v0.4.

    Input shape (produced by ``SecEdgarAdapter.fetch``):
    ``{
        "source_id": "sec_edgar",
        "issuer_cik": "<cik>",
        "filings": [
            {
                "issuer": {"cik": ..., "name": ..., "cusip": ..., "address": {...}},
                "reporter": {
                    "reporter_cik": ...,
                    "name": ...,
                    "type_code": ...,
                    "is_individual": bool,
                    "citizenship_iso": ...,
                    "percent_of_class": float | None,
                    ...
                },
                "filing_url": ...,
                "form_type": ...,
                "filed": "YYYY-MM-DD",
            }, ...
        ]
    }``

    Output: one entity statement (the listed issuer) + one person/entity
    statement per unique reporter + one relationship statement per reporter,
    carrying a shareholding interest with ``share.exact`` = the percent of
    class reported in the filing.
    """
    result = BODSBundle()

    filings: list[dict[str, Any]] = bundle.get("filings") or []
    if not filings:
        return result

    # --- Subject (listed issuer) entity ---
    # Use the first filing's issuer block; all filings share the same subject.
    issuer: dict[str, Any] = filings[0].get("issuer") or {}
    issuer_cik: str = issuer.get("cik") or bundle.get("issuer_cik", "")
    issuer_name: str = issuer.get("name") or ""
    if not issuer_name or not issuer_cik:
        return result

    issuer_identifiers: list[dict[str, str]] = [
        {"id": issuer_cik, "scheme": "US-SEC-CIK", "schemeName": "SEC EDGAR CIK"},
    ]
    cusip = issuer.get("cusip") or ""
    if cusip:
        issuer_identifiers.append(
            {"id": cusip, "scheme": "CUSIP", "schemeName": "CUSIP"}
        )

    addr_raw: dict[str, str] = issuer.get("address") or {}
    issuer_addresses: list[dict[str, Any]] = []
    if addr_raw:
        parts = [
            addr_raw.get("street1", ""),
            addr_raw.get("street2", ""),
            addr_raw.get("city", ""),
            addr_raw.get("stateOrCountry", ""),
            addr_raw.get("zipCode", ""),
        ]
        address_str = ", ".join(p for p in parts if p)
        if address_str:
            issuer_addresses = [_addr("registered", address_str, "US")]

    subject_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={issuer_cik}&type=SCHEDULE+13D"
    )
    # The issuer block is read off the filings, so the latest filing date is
    # when the SEC last published these details.
    latest_filed = max(
        (f.get("filed") or "" for f in filings),
        default="",
    ) or None
    subject_entity = make_entity_statement(
        source_id="sec_edgar",
        local_id=issuer_cik,
        name=issuer_name,
        jurisdiction=("United States", "US"),
        identifiers=issuer_identifiers,
        addresses=issuer_addresses,
        source_url=subject_url,
        statement_date=latest_filed,
    )
    result.statements.append(subject_entity)
    subject_sid = subject_entity["statementId"]

    # --- Reporters ---
    for filing in filings:
        reporter: dict[str, Any] = filing.get("reporter") or {}
        name = reporter.get("name") or ""
        if not name:
            continue

        reporter_cik = reporter.get("reporter_cik") or ""
        is_individual = reporter.get("is_individual", False)
        citizenship_iso = reporter.get("citizenship_iso") or ""
        percent = reporter.get("percent_of_class")
        filing_url = filing.get("filing_url") or subject_url

        # Stable local ID: prefer CIK, fall back to a hash of the name.
        local_id = reporter_cik or _stable_id("sec_edgar_name", name)

        reporter_identifiers: list[dict[str, str]] = []
        if reporter_cik:
            reporter_identifiers.append(
                {
                    "id": reporter_cik,
                    "scheme": "US-SEC-CIK",
                    "schemeName": "SEC EDGAR CIK",
                }
            )

        if is_individual:
            nationalities: list[dict[str, str]] = []
            if citizenship_iso:
                country_name = _iso2_to_country_name(citizenship_iso)
                nationalities = [{"name": country_name, "code": citizenship_iso}]

            reporter_stmt = make_person_statement(
                source_id="sec_edgar",
                local_id=local_id,
                full_name=name,
                nationalities=nationalities,
                identifiers=reporter_identifiers,
                source_url=filing_url,
                statement_date=filing.get("filed") or None,
            )
            party_type = "person"
        else:
            jur: tuple[str, str] | None = None
            if citizenship_iso:
                country_name = _iso2_to_country_name(citizenship_iso)
                jur = (country_name, citizenship_iso)

            reporter_stmt = make_entity_statement(
                source_id="sec_edgar",
                local_id=local_id,
                name=name,
                jurisdiction=jur,
                identifiers=reporter_identifiers,
                source_url=filing_url,
                statement_date=filing.get("filed") or None,
            )
            party_type = "entity"

        result.statements.append(reporter_stmt)
        party_sid = reporter_stmt["statementId"]

        # Build interests — always at least a bare shareholding entry.
        shareholding: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
        }
        set_beneficial_ownership(
            shareholding,
            "sec_edgar",
            asserted=_sec_beneficial_ownership(reporter, party_type),
        )
        if shareholding.get("beneficialOwnershipOrControl") is True:
            # Name WHICH definition the flag is true under — Rule 13d-3 is a
            # securities-disclosure concept, not AML beneficial ownership
            # (bo_regimes: sec_edgar).
            shareholding["details"] = (
                "Beneficial owner under SEC Rule 13d-3 (voting and/or "
                "dispositive power) — a securities-disclosure concept distinct "
                "from AML beneficial ownership"
            )
        # Sole vs shared power is a materially different claim and was being
        # discarded; where the filing distinguishes them, say so.
        sole = reporter.get("sole_voting_power")
        shared = reporter.get("shared_voting_power")
        if sole is not None or shared is not None:
            power_parts = []
            if sole:
                power_parts.append(f"sole voting power over {sole:,.0f} shares")
            if shared:
                power_parts.append(f"shared voting power over {shared:,.0f} shares")
            if power_parts:
                shareholding["details"] = "; ".join(
                    filter(None, [shareholding.get("details"), *power_parts])
                )
        type_code = (reporter.get("type_code") or "").strip().upper()
        if type_code in _SEC_CUSTODIAL_REPORTER_CODES:
            note = (
                f"Filed as reporting-person type {type_code} "
                "(custodial/advisory capacity); the filing asserts voting or "
                "dispositive power, not that the filer is the beneficiary."
            )
            shareholding["details"] = (
                f"{shareholding['details']}. {note}"
                if shareholding.get("details")
                else note
            )
        if percent is not None:
            shareholding["share"] = {"exact": percent}

        rel_stmt = make_relationship_statement(
            source_id="sec_edgar",
            local_id=f"{issuer_cik}:{local_id}",
            subject_statement_id=subject_sid,
            interested_party_statement_id=party_sid,
            interested_party_type=party_type,
            interests=[shareholding],
            source_url=filing_url,
            # The 13D/13G filing date — when this holding was declared to the
            # SEC. That is the source's declaration date, so it is the
            # statementDate.
            statement_date=filing.get("filed") or None,
        )
        result.statements.append(rel_stmt)

    return result
