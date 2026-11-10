"""Pydantic schema for Israel Corporations Authority (data.gov.il) fetch bundles.

Only the fields the BODS mapper (``map_ica_israel``) reads are declared;
everything else passes through via ``extra="allow"``. The bundle is assembled
by ``IcaIsraelAdapter.fetch`` from the local SQLite index built by
``scripts/build_ica_israel_index.py`` — what this guards is the index staying
in step with the adapter after a rebuild.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class IcaIsraelBundle(_Base):
    """Top-level shape returned by IcaIsraelAdapter.fetch."""

    # The 9-digit corporate number, and which register holds it.
    number: str
    kind: str = "company"
    name: str | None = None
    name_en: str | None = None
    legal_name: str | None = None
    type_code: int | None = None
    type_words: str | None = None
    status_code: int | None = None
    status_words: str | None = None
    sub_status: str | None = None
    status_class: str | None = None
    # ISO day, from the register's DD/MM/YYYY.
    incorporated: str | None = None
    government: bool | None = None
    limitation: str | None = None
    violating: str | None = None
    last_report_year: int | None = None
    address: dict[str, Any] | None = None
    # data.gov.il's last-modified day of the files the index was built from.
    cut: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None
