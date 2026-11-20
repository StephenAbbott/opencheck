"""Pydantic schema for Connecticut business registry rows from data.ct.gov (SODA 2.1).

Three row shapes reach a lookup bundle: a Business Master row, Principals
rows and Name Change History rows. The History tab's bundle adds Filing
History rows. SODA omits a column that is null on a row, so every
field but the keys is optional; ``extra="allow"`` keeps columns the register
adds later.

The bundle is also a privacy check: a principal's residence address or
street line, the business e-mail, and a filing's organizer or incorporator,
are never asked for (the adapter's ``$select`` is an allowlist), and the
validators refuse them.
"""

from __future__ import annotations

from pydantic import model_validator

from . import _Base

#: Columns that would carry a person's home or contact details.
_FORBIDDEN_PRINCIPAL_PREFIXES = ("residence", "business_street", "email", "phone")
_FORBIDDEN_MASTER_COLUMNS = frozenset(
    {"business_email_address", "category_survey_email_address", "mailing_address"}
)
#: A filing's organizer and incorporator: a person's name on most rows.
_FORBIDDEN_FILING_PREFIXES = ("organizer", "name_of_incorporator")


class CtMasterRow(_Base):
    """A ``n7gp-d28j`` *Business Master* row."""

    id: str
    accountnumber: str
    name: str | None = None
    business_type: str | None = None
    status: str | None = None
    sub_status: str | None = None
    citizenship: str | None = None
    formation_place: str | None = None
    state_or_territory_formation: str | None = None
    country_formation: str | None = None
    date_registration: str | None = None
    began_transacting_in_ct: str | None = None
    dissolution_date: str | None = None
    naics_code: str | None = None

    @model_validator(mode="after")
    def _no_contact_data(self) -> "CtMasterRow":
        leaked = sorted(set((self.model_extra or {}).keys()) & _FORBIDDEN_MASTER_COLUMNS)
        if leaked:
            raise ValueError(f"contact column(s) in a Connecticut business row: {leaked}")
        return self


class CtPrincipalRow(_Base):
    """A ``ka36-64k6`` *Principals* row."""

    business_id: str
    name__c: str | None = None
    firstname: str | None = None
    lastname: str | None = None
    designation: str | None = None
    business_city: str | None = None
    business_state: str | None = None
    business_country: str | None = None

    @model_validator(mode="after")
    def _no_personal_data(self) -> "CtPrincipalRow":
        leaked = sorted(
            k for k in (self.model_extra or {})
            if k.startswith(_FORBIDDEN_PRINCIPAL_PREFIXES)
        )
        if leaked:
            raise ValueError(f"personal-data column(s) in a Connecticut principal row: {leaked}")
        return self


class CtNameChangeRow(_Base):
    """An ``enwv-52we`` *Name Change History* row."""

    unique_key: str
    business_name_old: str | None = None
    business_name_new: str | None = None
    name_change_date: str | None = None


class CtFilingRow(_Base):
    """An ``ah3s-bes7`` *Filing History* row."""

    account: str
    name: str | None = None
    filing_date: str | None = None
    effective_date: str | None = None
    type: str | None = None
    filing_type: str | None = None
    report_year: str | None = None
    date_forfeited: str | None = None

    @model_validator(mode="after")
    def _no_personal_data(self) -> "CtFilingRow":
        leaked = sorted(
            k for k in (self.model_extra or {})
            if k.startswith(_FORBIDDEN_FILING_PREFIXES)
        )
        if leaked:
            raise ValueError(f"personal-data column(s) in a Connecticut filing row: {leaked}")
        return self


class CtSotsBundle(_Base):
    """Top-level shape returned by ``CtSotsAdapter.fetch`` for a record."""

    business_id: str
    master: CtMasterRow
    principals: list[CtPrincipalRow] = []
    name_changes: list[CtNameChangeRow] = []
    truncated: bool = False
    legal_name: str | None = None


class CtSotsTimelineBundle(CtSotsBundle):
    """``CtSotsAdapter.fetch_timeline_data``: the lookup bundle plus its filings."""

    filings: list[CtFilingRow] = []
    filings_truncated: bool = False
