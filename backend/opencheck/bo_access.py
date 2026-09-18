"""Beneficial-ownership access status for EU/EEA national registers.

Under the EU's Sixth Anti-Money Laundering Directive (and the 2022 CJEU ruling
that struck down *general* public access), member states are moving beneficial
ownership registers from public access to **legitimate-interest** access. Some
have already done so; others have an announced switch-over date.

OpenCheck surfaces a quiet, per-register note about this so users understand
that the *beneficial ownership* slice of a national register may be restricted —
while the company registration and GLEIF ownership data OpenCheck shows are
unaffected.

**Since Phase 223 this module is a view, not a store.** The facts live in
``data/jurisdictions.json`` — generated from Stephen's Notion table
"Beneficial ownership access status" by ``scripts/sync_jurisdictions.py`` and
read by ``opencheck.knowability`` — so there is one copy of every date. The
old ``data/eu_bo_access.json`` is gone. This module keeps its public names
(``BO_ACCESS``, ``BoAccessEntry``, ``BoAccessNotice``, ``notice_for``) so the
``/sources`` endpoint and the source-card footnote are unchanged.

Mapping from the jurisdiction's access enum to the footnote:

* ``legitimate_interest``, ``restricted_no_lia_route_yet``,
  ``authorities_and_obliged_entities_only`` → **restricted now**;
  ``restricted_from`` is the ``access_since`` date when one is recorded.
* ``public`` / ``public_with_registration_or_justification`` with a
  ``next_change_expected`` in the future → **becoming restricted** on that
  date (the only announced changes to a public register are restrictions).
* ``public`` with no announced change, ``no_register``, ``in_progress`` or
  an unrecorded status → no notice (the register is not restricted, or there
  is nothing to restrict).

``restricted_from`` semantics are unchanged from before:

* a **future** date  → still public; show the "becoming restricted" message with
  the date.
* a **past** date, **today**, or **null** → restricted now; show the "not public"
  message. ``null`` means "already restricted, no announced public-cutoff date".
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .knowability import JURISDICTIONS, Jurisdiction

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

_RESTRICTED_NOW: frozenset[str] = frozenset(
    {
        "legitimate_interest",
        "restricted_no_lia_route_yet",
        "authorities_and_obliged_entities_only",
    }
)
_PUBLIC: frozenset[str] = frozenset({"public", "public_with_registration_or_justification"})


class BoAccessEntry(BaseModel):
    """One country's beneficial-ownership access record, as the footnote reads it."""

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


def _entry_for(j: Jurisdiction) -> BoAccessEntry | None:
    r = j.bo_register
    if r.access in _RESTRICTED_NOW:
        return BoAccessEntry(
            restricted_from=r.access_since,
            access_url=r.access_url,
            note=f"knowability: {r.access}",
        )
    if r.access in _PUBLIC and r.next_change_expected is not None:
        return BoAccessEntry(
            restricted_from=r.next_change_expected,
            access_url=r.access_url,
            note=f"knowability: {r.access}, change announced",
        )
    return None


def _build() -> dict[str, BoAccessEntry]:
    out: dict[str, BoAccessEntry] = {}
    for code, j in JURISDICTIONS.items():
        if code not in EU_EEA_COUNTRY_NAMES:
            continue
        entry = _entry_for(j)
        if entry is not None:
            out[code] = entry
    return out


# Derived at import from the jurisdictions table — a malformed row already
# failed the process in ``knowability``.
BO_ACCESS: dict[str, BoAccessEntry] = _build()


def notice_for(
    country: str | None, today: date | None = None
) -> BoAccessNotice | None:
    """The access notice for a national register in ``country``, or ``None``.

    ``None`` when the country has no entry — its beneficial ownership data is
    public and unrestricted (Latvia, Estonia while its restriction stays
    postponed), or it is outside the EU/EEA."""
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
