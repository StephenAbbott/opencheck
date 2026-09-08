"""Register-scoped hops for the expansion frontier (Phase 182).

FullCheck's frontier (``frontend/src/lib/expand.ts``) used to treat only an
LEI as expandable: an entity that arrived carrying a register-scoped
identifier and no LEI — a ``GB-COH`` company number from a Companies House
PSC filing, the common case in UK chains — was a dead end for
``/expand-layer``, so the depth budget could not drive Companies House hops
and the adapter's own fixed walk was the only recursion the UK got.

This module says which identifier schemes OpenCheck can hop on, and how. A
hop is deliberately **cheap**: it dispatches only the register that owns the
identifier (plus the sanctions screen of the parties it returns, in the
router), not the forty-source lookup a re-anchor on an LEI costs.

The table is derived from what the adapters already declare, so it is
per-scheme rather than UK-only: every GLEIF registration-authority code the
BODS mapper knows an org-id scheme for (``_GLEIF_RA_TO_ORG_ID``) is matched
against the adapters' ``lookup_derivers`` — the same declarations the lookup
pipeline uses to derive a local id from an LEI record — and the adapter that
claims the RA code becomes the hop for that scheme, with its own normaliser.
Companies House is the one explicit entry, for the same reason it is a
special case in ``_build_derived``: it dispatches on jurisdiction, not on a
deriver.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache

from .identifiers import normalise_ch_company_number


@dataclass(frozen=True)
class RegisterHop:
    """How to follow one identifier scheme to the register that owns it."""

    scheme: str
    source_id: str
    #: Canonical local id for the register's ``fetch``; raises ``ValueError``
    #: when the value cannot be one (the hop is then skipped, not guessed).
    normalise: Callable[[str], str]
    #: The adapter's ``fetch`` takes ``legal_name=`` (some registers search
    #: by name behind the number).
    pass_legal_name: bool


def _ch_normalise(value: str) -> str:
    number = normalise_ch_company_number(value)
    if number is None:
        raise ValueError(f"not a Companies House number: {value!r}")
    return number


@cache
def hop_schemes() -> dict[str, RegisterHop]:
    """Scheme → hop, built once from the registry and the mapper's RA table."""
    from .bods.mapper import _GLEIF_RA_TO_ORG_ID
    from .sources import REGISTRY

    hops: dict[str, RegisterHop] = {}
    for ra_code, (scheme, _name) in _GLEIF_RA_TO_ORG_ID.items():
        if scheme in hops:
            continue
        for adapter in REGISTRY.values():
            for deriver in adapter.lookup_derivers:
                if ra_code in deriver.ra_codes:
                    hops[scheme] = RegisterHop(
                        scheme=scheme,
                        source_id=adapter.id,
                        normalise=deriver.normalise,
                        pass_legal_name=adapter.lookup_pass_legal_name,
                    )
                    break
            if scheme in hops:
                break
    if "companies_house" in REGISTRY:
        hops["GB-COH"] = RegisterHop("GB-COH", "companies_house", _ch_normalise, False)
    # ``REG-<country>`` is the mappers' fallback scheme for "a number on that
    # country's company register" when the filing does not name the register
    # — the Companies House mapper emits ``REG-GB`` for a PSC whose
    # ``place_registered`` reads "England And Wales", and OpenAleph's UK
    # records carry ``REG-GB`` too (seen on Vosper Thornycroft, 2026-09-08).
    # Where a country has exactly one register hop, that is the register.
    by_country: dict[str, list[RegisterHop]] = {}
    for scheme, hop in hops.items():
        by_country.setdefault(scheme.split("-", 1)[0], []).append(hop)
    for country, country_hops in by_country.items():
        alias = f"REG-{country}"
        if len(country_hops) == 1 and alias not in hops:
            hops[alias] = country_hops[0]
    return hops


def hop_for(scheme: str | None) -> RegisterHop | None:
    if not scheme:
        return None
    return hop_schemes().get(scheme.strip().upper())


def describe() -> dict[str, dict[str, str]]:
    """The schemes the frontier may treat as expandable, for the client."""
    from .sources import REGISTRY

    return {
        scheme: {"source_id": hop.source_id, "name": REGISTRY[hop.source_id].info.name}
        for scheme, hop in sorted(hop_schemes().items())
    }
