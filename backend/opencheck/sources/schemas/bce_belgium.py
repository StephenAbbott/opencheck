"""Pydantic schema for the BCE / KBO (Belgium) bundle.

Only the fields the BODS mapper reads are declared; everything else passes
through via ``extra="allow"``.
"""

from __future__ import annotations

from . import _Base


class BCEBundle(_Base):
    """Top-level shape returned by ``BceBelgiumAdapter.fetch``."""

    enterprise_number: str  # required — the index key, ten digits
    dotted: str | None = None
    name: str | None = None
    name_nl: str | None = None
    name_fr: str | None = None
    name_de: str | None = None
    name_en: str | None = None
    status: str | None = None
    status_class: str | None = None
    juridical_situation_code: str | None = None
    juridical_situation: str | None = None
    enterprise_type_code: str | None = None
    enterprise_type: str | None = None
    juridical_form_code: str | None = None
    juridical_form: str | None = None
    juridical_form_native: str | None = None
    start_date: str | None = None
    address: str | None = None
    cut: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None
