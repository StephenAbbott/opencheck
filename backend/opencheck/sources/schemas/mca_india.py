"""Pydantic schema for MCA Company Master Data (India) bundles.

Only the fields the BODS mapper (``map_mca_india``) reads are declared;
everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from . import _Base


class MCABundle(_Base):
    """Top-level shape returned by McaIndiaAdapter.fetch."""

    # The CIN identifies the entity; name is the display value.
    cin: str
    name: str | None = None
    status: str | None = None
    category: str | None = None
    sub_category: str | None = None
    company_class: str | None = None
    listing_status: str | None = None
    authorized_capital: str | None = None
    paidup_capital: str | None = None
    registration_date: str | None = None
    address: str | None = None
    state_code: str | None = None
    roc_code: str | None = None
    indian_foreign: str | None = None
    nic_code: str | None = None
    industrial_classification: str | None = None
    link: str | None = None
