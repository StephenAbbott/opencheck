"""Pydantic schema for Bulgarian Commercial Register bundles.

Assembled by ``BgTrAdapter.fetch`` from the index that
``scripts/build_bg_tr_index.py`` packs from the Registry Agency's daily files.
Besides the fields the mapper reads, the schema **refuses personal data**:
a party or beneficial owner carrying the register's personal-number field,
a birth date, an address or a passport field fails validation, so a builder
change that let one through cannot reach a card or a BODS statement
(Stephen, 18 Nov 2026).
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import field_validator

from . import _Base

#: Keys that would carry what OpenCheck never republishes about a person.
FORBIDDEN_PARTY_KEYS = frozenset({
    "indent", "Indent", "egn", "lnch", "birth_date", "birthDate", "BirthDate",
    "address", "Address", "passport", "Passport", "hash", "indent_type",
})


#: A personal number or an identity document written into a filed name.
_PERSONAL_IN_NAME = re.compile(r"(?<!\d)\d{10}(?!\d)|ЕГН|ЛНЧ|\bЛК\b|лична карта", re.IGNORECASE)


def _check_parties(parties: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for party in parties:
        bad = FORBIDDEN_PARTY_KEYS & set(party)
        if bad:
            raise ValueError(f"personal field in a party: {sorted(bad)}")
        if _PERSONAL_IN_NAME.search(str(party.get("name") or "")):
            raise ValueError("personal number in a party's name")
    return parties


class BgTrBundle(_Base):
    """Top-level shape returned by ``BgTrAdapter.fetch`` for a company."""

    eik: str
    name: str = ""
    transliteration: str | None = None
    legal_form_code: str | None = None
    legal_form_text: str | None = None
    seat: dict[str, Any] | None = None
    seat_text: str | None = None
    capital: dict[str, Any] | None = None
    parties: list[dict[str, Any]] = []
    beneficial_owners: list[dict[str, Any]] = []
    status_class: str | None = None
    status: str | None = None
    status_since: str | None = None
    registered_on: str | None = None
    first_entry: str | None = None
    last_entry: str | None = None
    cut: str | None = None
    link: str | None = None
    is_stub: bool = False

    @field_validator("parties", "beneficial_owners")
    @classmethod
    def _no_personal_data(cls, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return _check_parties(value)
