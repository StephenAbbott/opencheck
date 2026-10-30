"""Pydantic schema for Florida Division of Corporations (Sunbiz) fetch bundles.

Only the fields the BODS mapper (``map_sunbiz_florida``) and the finding read
are declared; everything else passes through via ``extra="allow"``. The
bundle is assembled by ``SunbizFloridaAdapter.fetch`` from the local SQLite
index built by ``scripts/build_sunbiz_florida_index.py`` — what this guards is
the index staying in step with the adapter after a rebuild, and that no
officer or agent street address reaches a bundle (decision D4).
"""

from __future__ import annotations

from pydantic import model_validator

from . import _Base


class SunbizOfficer(_Base):
    seq: int = 0
    title: str | None = None
    # ``P`` person, ``C`` corporation (the layout's two values).
    type: str | None = None
    surname: str | None = None
    given: str | None = None
    middle: str | None = None
    name: str | None = None
    city: str | None = None
    state: str | None = None

    @model_validator(mode="after")
    def _no_street(self) -> SunbizOfficer:
        extra = self.model_extra or {}
        for key in ("address", "street", "addr1", "zip"):
            if extra.get(key):
                raise ValueError(f"an officer's {key} must not reach a bundle")
        return self


class SunbizEvent(_Base):
    seq: int = 0
    code: str
    description: str | None = None
    effective: str | None = None
    filed: str | None = None
    other_doc: str | None = None
    name: str | None = None


class SunbizAgent(_Base):
    name: str | None = None
    type: str | None = None
    city: str | None = None
    state: str | None = None

    @model_validator(mode="after")
    def _no_street(self) -> SunbizAgent:
        extra = self.model_extra or {}
        for key in ("address", "street", "addr1", "zip"):
            if extra.get(key):
                raise ValueError(f"a registered agent's {key} must not reach a bundle")
        return self


class SunbizFloridaBundle(_Base):
    """Top-level shape returned by SunbizFloridaAdapter.fetch."""

    doc: str
    name: str | None = None
    legal_name: str | None = None
    status: str | None = None
    status_class: str | None = None
    status_words: str | None = None
    status_since: str | None = None
    filing_type: str | None = None
    # ISO days, from the register's MMDDYYYY.
    file_date: str | None = None
    last_transaction: str | None = None
    fei: str | None = None
    state_country: str | None = None
    registered_agent: SunbizAgent | None = None
    more_than_six_officers: bool = False
    officers: list[SunbizOfficer] = []
    events: list[SunbizEvent] = []
    cut: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None
