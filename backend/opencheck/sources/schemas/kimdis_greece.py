"""Pydantic schema for the ΚΗΜΔΗΣ (Greek public contracts) bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``. The
contract rows are an allowlist built by ``reduce_record``: no person at the
authority and no other contractor is ever named.
"""

from __future__ import annotations

from pydantic import Field, model_validator

from . import _Base

#: Keys that would carry a person: the authority's officer, its signers, or
#: another contractor's name. Refused anywhere in a contract row.
_PERSONAL_KEYS = frozenset({
    "authorEmail", "signers", "unitsOperator", "contractingMembersDataList",
    "contractingDataDetails",
})


class KimdisSupplier(_Base):
    """Totals over the contracts read for one ΑΦΜ (latest versions only)."""

    afm: str
    name: str | None = None
    contracts: int = 0
    #: Contract values without VAT summed by OpenCheck, whole euros.
    value_eur: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None
    marked_cancelled: int = 0
    amended: int = 0
    shared: int = 0
    records_read: int = 0


class KimdisBuyer(_Base):
    """One of the supplier's largest contracting authorities."""

    name: str = ""
    contracts: int = 0
    value_eur: int = 0


class KimdisContract(_Base):
    """One contract, at its latest version read, with its ΚΗΜΔΗΣ document."""

    adam: str
    published: str | None = None
    signed: str | None = None
    buyer: str = ""
    title: str = ""
    value_eur: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    procedure: str = ""
    contract_type: str = ""
    bids: int | None = None
    amends_earlier: bool = False
    marked_cancelled: bool = False
    other_contractors: int = 0
    url: str = ""

    @model_validator(mode="before")
    @classmethod
    def _no_people(cls, data: object) -> object:
        if isinstance(data, dict) and _PERSONAL_KEYS & set(data):
            raise ValueError("a ΚΗΜΔΗΣ contract row carries a person field")
        return data


class KimdisGreeceBundle(_Base):
    source_id: str
    afm: str
    gr_argemi: str = ""
    matched_via: str = ""
    legal_name: str = ""
    supplier: KimdisSupplier | None = None
    buyers: list[KimdisBuyer] = Field(default_factory=list)
    contracts: list[KimdisContract] = Field(default_factory=list)
    read_from: str | None = None
    read_to: str | None = None
    truncated: bool = False
    incomplete: bool = False
    #: 180-day periods ΚΗΜΔΗΣ did not answer for: ``{"from", "to"}``.
    gaps: list[dict[str, str]] = Field(default_factory=list)
    link: str = ""
    not_found: bool = False
    no_afm: bool = False
    is_stub: bool = False
