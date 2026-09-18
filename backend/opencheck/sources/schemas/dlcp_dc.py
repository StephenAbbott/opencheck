"""Pydantic schema for Washington DC DLCP rows from the ArcGIS FeatureServer.

Two row shapes reach a bundle:

* ``company`` — a Table 0 *Corporate Registration* row. ``FILE_NUMBER`` is the
  register's unique key and the mapper's anchor, so it is required.
* ``owners`` — Table 2 *Beneficial Owners* rows, one per filed owner or
  controller, joined on ``INITIALFILENUMBER``.

Trade names arrive already reduced to a list of strings, so Table 1's row
shape is not declared here.

Note ``BUSNIESS_ADDRESS_LINE1``–``4``: the misspelling is the register's own
column name, not a typo here. Only the fields the mapper and finding read are
declared; ``extra="allow"`` on ``_Base`` keeps the rest, so a column DLCP adds
later cannot fail a lookup.
"""

from __future__ import annotations

from . import _Base


class DlcpCompanyRow(_Base):
    """A Table 0 *Corporate Registration* row."""

    FILE_NUMBER: str
    ENTITY_STATUS: str | None = None
    LOCALE: str | None = None
    MODELTYPE: str | None = None
    BUSINESS_NAME: str | None = None
    BUSNIESS_ADDRESS_LINE1: str | None = None
    BUSNIESS_ADDRESS_LINE2: str | None = None
    BUSNIESS_ADDRESS_LINE3: str | None = None
    BUSNIESS_ADDRESS_LINE4: str | None = None
    BUSINESS_CITY: str | None = None
    BUSINESS_STATE: str | None = None
    ZIPCODE: str | None = None
    BUSINESS_COUNTRY: str | None = None
    SUFFIX: str | None = None
    RA_NAME: str | None = None
    RA_CITY: str | None = None
    RA_STATE: str | None = None
    EFFECTIVE_DATE: int | float | None = None
    DCS_LAST_MOD_DTTM: int | float | None = None


class DlcpOwnerRow(_Base):
    """A Table 2 *Beneficial Owners* row."""

    INITIALFILENUMBER: str | None = None
    NAME: str | None = None
    ADDRESS: str | None = None
    BUSINESSNAME: str | None = None
    MODELTYPE: str | None = None
    LOCALE: str | None = None
    STATUS: str | None = None


class DlcpDcBundle(_Base):
    """Top-level shape returned by ``DlcpDcAdapter.fetch``."""

    file_number: str  # required — mapper key
    company: DlcpCompanyRow | None = None
    owners: list[DlcpOwnerRow] = []
    trade_names: list[str] = []
    matched_by: str | None = None
    legal_name: str | None = None
