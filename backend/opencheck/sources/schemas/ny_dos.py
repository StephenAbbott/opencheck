"""Pydantic schema for New York DOS rows from data.ny.gov (SODA 2.1).

Four row shapes reach a bundle, one per dataset, all keyed on ``corpid_num``
(the DOS ID). SODA omits a column that is null on a row rather than sending
``null``, so every field but the key is optional. Only the fields the mapper,
finding and History emitter read are declared; ``extra="allow"`` on ``_Base``
keeps the rest, so a column DOS adds later cannot fail a lookup.
"""

from __future__ import annotations

from . import _Base


class NyFilingRow(_Base):
    """A ``63wc-4exh`` *All Filings* row."""

    corpid_num: str
    film_num: str | None = None
    date_filed: str | None = None
    eff_date: str | None = None
    dis_eff_date: str | None = None
    for_inc_date: str | None = None
    documenttype: str | None = None
    entitytype: str | None = None
    law: str | None = None
    corp_name: str | None = None
    fict_name: str | None = None
    juris: str | None = None
    cnty_prin_ofc: str | None = None


class NyStatusRow(_Base):
    """A ``3gg2-jgnp`` *Entity Status History* row."""

    corpid_num: str
    film_num: str | None = None
    date_filed: str | None = None
    mod_cert_code: str | None = None
    status: str | None = None


class NyNameRow(_Base):
    """An ``ekwr-p59j`` *Name Status History* row."""

    corpid_num: str
    film_num: str | None = None
    date_filed: str | None = None
    name_type: str | None = None
    name_status: str | None = None
    corp_name: str | None = None


class NyAddressRow(_Base):
    """A ``2tms-hftb`` *Address* row (``addr_type`` 3 or 4 only)."""

    corpid_num: str
    film_num: str | None = None
    date_filed: str | None = None
    addr_type: str | None = None
    name: str | None = None
    addr1: str | None = None
    addr2: str | None = None
    city: str | None = None
    state: str | None = None
    zip5: str | None = None
    zip4: str | None = None
    country: str | None = None


class NyDosBundle(_Base):
    """Top-level shape returned by ``NyDosAdapter.fetch``."""

    dos_id: str  # required — mapper key
    filings: list[NyFilingRow] = []
    status_history: list[NyStatusRow] = []
    name_history: list[NyNameRow] = []
    addresses: list[NyAddressRow] = []
    truncated: bool = False
    legal_name: str | None = None
