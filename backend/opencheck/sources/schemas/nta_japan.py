"""Pydantic schema for NTA Corporate Number (Japan) fetch bundles.

Only the fields the BODS mapper (``map_nta_japan``) reads are declared;
everything else passes through via ``extra="allow"``. The bundle is
assembled by ``NtaJapanAdapter.fetch`` from the local SQLite index built by
``scripts/build_nta_japan_index.py`` or from one Web-API row — both carry the
nationwide file's columns, so what this guards is the two staying in step.
"""

from __future__ import annotations

from . import _Base


class NtaJapanBundle(_Base):
    """Top-level shape returned by NtaJapanAdapter.fetch."""

    #: The 13-digit corporate number (法人番号).
    number: str
    #: The 12-digit 会社法人等番号, hyphenated, for a registered body.
    registration_number: str | None = None
    name: str | None = None
    legal_name: str | None = None
    #: 法人種別 — three digits (``301`` a 株式会社).
    kind: str | None = None
    prefecture: str | None = None
    city: str | None = None
    street: str | None = None
    post_code: str | None = None
    address_outside: str | None = None
    #: ISO days, as the register writes them.
    close_date: str | None = None
    close_cause: str | None = None
    successor: str | None = None
    successor_name: str | None = None
    process: str | None = None
    change_date: str | None = None
    change_cause: str | None = None
    update_date: str | None = None
    assignment_date: str | None = None
    en_name: str | None = None
    en_prefecture: str | None = None
    en_city: str | None = None
    en_address_outside: str | None = None
    hihyoji: bool = False
    status_class: str | None = None
    #: ``index`` (the monthly file) or ``api`` (the Web-API).
    tier: str | None = None
    #: The nationwide file's date, for an index read.
    cut: str | None = None
    link: str | None = None
    api_notice: str | None = None
