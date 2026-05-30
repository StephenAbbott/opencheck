"""Pydantic schema for Sudski registar (Croatia) API responses.

Only the fields the BODS mapper (map_sudreg_croatia) actually reads are
declared.  Everything else passes through via ``extra="allow"``.

The ``/detalji_subjekta`` endpoint returns one structured object per
subject (the ``subject`` field of the bundle below).
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class SudregSubject(_Base):
    """Subject object returned by /detalji_subjekta."""

    mbs: int | None = None
    oib: int | None = None
    potpuni_mbs: str | None = None
    potpuni_oib: str | None = None
    status: int | None = None
    datum_osnivanja: str | None = None
    tvrtka: dict[str, Any] = Field(default_factory=dict)
    skracena_tvrtka: dict[str, Any] = Field(default_factory=dict)
    sjediste: dict[str, Any] = Field(default_factory=dict)
    pravni_oblik: dict[str, Any] = Field(default_factory=dict)
    temeljni_kapitali: list[dict[str, Any]] = Field(default_factory=list)


class SudregBundle(_Base):
    """Top-level shape returned by SudregCroatiaAdapter.fetch."""

    mbs: str  # required — mapper key
    oib: str | None = None
    subject: SudregSubject | None = None
    legal_name: str | None = None
