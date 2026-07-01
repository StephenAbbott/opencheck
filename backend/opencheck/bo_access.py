"""Beneficial-ownership access status for EU/EEA national registers.

Under the EU's Sixth Anti-Money Laundering Directive (and the 2022 CJEU ruling
that struck down *general* public access), member states are moving beneficial
ownership registers from public access to **legitimate-interest** access. Some
have already done so; others have an announced switch-over date.

OpenCheck surfaces a quiet, per-register note about this so users understand
that the *beneficial ownership* slice of a national register may be restricted —
while the company registration and GLEIF ownership data OpenCheck shows are
unaffected.

Single source of truth: ``data/eu_bo_access.json``, one entry per ISO 3166-1
alpha-2 country code. Edit that file to update the list — this module validates
it at import (fail-fast on a bad date, URL, or a country that isn't EU/EEA) and
computes the user-facing message from ``restricted_from`` and today's date, so
the "currently public but soon…" message flips to "not public" on the day the
restriction takes effect without any code change.

``restricted_from`` semantics:

* a **future** date  → still public; show the "becoming restricted" message with
  the date.
* a **past** date, **today**, or **null** → restricted now; show the "not public"
  message. ``null`` means "already restricted, no announced public-cutoff date".
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator

_DATA_PATH = Path(__file__).parent / "data" / "eu_bo_access.json"

# EU-27 + EEA (Iceland, Liechtenstein, Norway). Norway et al. implement the AML
# directives via the EEA agreement, so they belong here even though they are not
# EU members — the validation set must not reject them.
EU_EEA_COUNTRY_NAMES: dict[str, str] = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DK": "Denmark",
    "EE": "Estonia",
    "FI": "Finland",
    "FR": "France",
    "DE": "Germany",
    "GR": "Greece",
    "HU": "Hungary",
    "IE": "Ireland",
    "IT": "Italy",
    "LV": "Latvia",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MT": "Malta",
    "NL": "the Netherlands",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "ES": "Spain",
    "SE": "Sweden",
    "IS": "Iceland",
    "LI": "Liechtenstein",
    "NO": "Norway",
}


class BoAccessEntry(BaseModel):
    """One country's raw beneficial-ownership access record (as stored)."""

    restricted_from: date | None = Field(
        default=None,
        description=(
            "Date public access ends / ended. Future = still public; "
            "past/today/null = restricted now (null = no announced date)."
        ),
    )
    access_url: str | None = Field(
        default=None,
        description="Where to learn how to apply for legitimate-interest access.",
    )
    note: str = Field(
        default="",
        description="Provenance / source URL for the maintainer. Not shown to users.",
    )

    @field_validator("access_url")
    @classmethod
    def _url_shape(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        if v and not v.startswith(("http://", "https://")):
            raise ValueError(f"access_url must be an http(s) URL: {v!r}")
        return v or None


class BoAccessNotice(BaseModel):
    """Computed, user-facing notice for one national register.

    The frontend picks the sentence template from ``status`` and formats
    ``effective_date`` for the locale; the "Learn how to apply" link is shown
    only when ``access_url`` is present.
    """

    status: Literal["restricted", "becoming_restricted"]
    country_code: str
    country_name: str
    #: ISO date the restriction takes effect — only set for ``becoming_restricted``.
    effective_date: str | None = None
    access_url: str | None = None


def _load(path: Path = _DATA_PATH) -> dict[str, BoAccessEntry]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, BoAccessEntry] = {}
    for code, payload in raw.items():
        cc = code.strip().upper()
        if cc not in EU_EEA_COUNTRY_NAMES:
            raise ValueError(
                f"eu_bo_access.json: {code!r} is not an EU/EEA country code"
            )
        out[cc] = BoAccessEntry.model_validate(payload)
    return out


# Validated at import — a malformed entry fails the process (and CI) loudly.
BO_ACCESS: dict[str, BoAccessEntry] = _load()


def notice_for(
    country: str | None, today: date | None = None
) -> BoAccessNotice | None:
    """The access notice for a national register in ``country``, or ``None``.

    ``None`` when the country has no entry (i.e. its beneficial ownership data
    is still public and unrestricted, e.g. Latvia — deliberately omitted)."""
    if not country:
        return None
    cc = country.strip().upper()
    entry = BO_ACCESS.get(cc)
    if entry is None:
        return None
    today = today or date.today()
    becoming = entry.restricted_from is not None and entry.restricted_from > today
    return BoAccessNotice(
        status="becoming_restricted" if becoming else "restricted",
        country_code=cc,
        country_name=EU_EEA_COUNTRY_NAMES[cc],
        effective_date=entry.restricted_from.isoformat() if becoming else None,
        access_url=entry.access_url,
    )
