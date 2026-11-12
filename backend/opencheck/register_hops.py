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
from dataclasses import dataclass, replace
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
    #: The lookup pipeline's dispatch key for this register (``gb_coh``,
    #: ``kvk_number``) — what the key would be named in a GLEIF-anchored
    #: lookup's ``derived_identifiers``, so a register-anchored lookup (Phase
    #: 289) builds the same hit the LEI path does.
    derived_key: str = ""
    #: A second register to ask when this one answers that it holds no
    #: record (``not_found``) — FR-INSEE asks INPI first and Sirene on INPI's
    #: 404 (the France Sirene + RNA ticket, decision F): the RNE holds no
    #: association or foundation without a business activity, and Sirene
    #: holds every SIREN.
    fallback: "RegisterHop | None" = None

    @property
    def country(self) -> str:
        """The register's country, from the scheme: ``GB-COH`` → ``GB``,
        ``US-DC`` → ``US`` (the canonical scheme, never a ``REG-`` alias —
        the alias resolves to its register's own hop)."""
        return self.scheme.split("-", 1)[0]


#: Schemes that must never stand in for their whole country in the
#: ``REG-<country>`` alias below. ``US-DC`` is one of fifty-odd US company
#: registers, so "a number on the US register" cannot be taken to mean the
#: District of Columbia's: a Delaware or New York file number carried as
#: ``REG-US`` would otherwise be looked up in DC, where it either misses or —
#: worse — reaches an unrelated company that happens to share the number.
#: ``CA-CC`` (Phase 239 as ``CA-CORP``, when RA000072 joined the mapper's
#: table; renamed to org-id's code in Phase 257) is the
#: same case: Corporations Canada holds federal corporations only, and a
#: provincially incorporated company's number — most Canadian companies — says
#: nothing about the federal register. ``US-NY`` (Phase 263) is the DC case
#: again, for the New York Department of State, and ``US-FL`` (the Florida
#: wiring) for the Florida Division of Corporations.
_NO_COUNTRY_ALIAS: frozenset[str] = frozenset({"US-DC", "US-NY", "US-FL", "CA-CC"})


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
                        derived_key=deriver.derived_key,
                    )
                    break
            if scheme in hops:
                break
    # FR-INSEE: INPI first, Sirene when INPI has no record (decision F).
    if "FR-INSEE" in hops and "sirene" in REGISTRY:
        from .sources.inpi import normalise_siren

        hops["FR-INSEE"] = replace(
            hops["FR-INSEE"],
            fallback=RegisterHop(
                "FR-INSEE", "sirene", normalise_siren,
                REGISTRY["sirene"].lookup_pass_legal_name, derived_key="siren",
            ),
        )
    if "companies_house" in REGISTRY:
        hops["GB-COH"] = RegisterHop(
            "GB-COH", "companies_house", _ch_normalise, False, derived_key="gb_coh"
        )
    # ``CO-RUE`` is value-schemed (the mapper's ``_GLEIF_VALUE_SCHEMED_RA``):
    # GLEIF files a NIT *or* a chamber's matrícula under the same RA codes,
    # and only a check-digit-valid NIT is written ``CO-RUE``, so the RA table
    # above never names it. The NIT is the national key; RUES is the register.
    if "rues_colombia" in REGISTRY:
        from .sources.rues_colombia import CO_RUE_SCHEME, normalise_rues_key

        hops[CO_RUE_SCHEME] = RegisterHop(
            CO_RUE_SCHEME, "rues_colombia", normalise_rues_key, True,
            derived_key="co_rues",
        )
    # ``AR-CUIT`` is value-schemed too (the Argentina wiring): three
    # Argentine authorities file the CUIT, and RA000010 an IGJ number for a
    # few records, so only a check-digit-valid CUIT is written ``AR-CUIT``.
    if "registro_sociedades_argentina" in REGISTRY:
        from .sources.registro_sociedades_argentina import (
            AR_CUIT_SCHEME,
            normalise_cuit_key,
        )

        hops[AR_CUIT_SCHEME] = RegisterHop(
            AR_CUIT_SCHEME, "registro_sociedades_argentina", normalise_cuit_key, True,
            derived_key="ar_cuit",
        )
    # ``PE-RUC`` is value-schemed (the Peru RNP wiring): four Peruvian
    # authorities file the RUC, and SUNARP a partida for some records, so
    # only a check-digit-valid RUC is written ``PE-RUC``.
    if "oece_rnp" in REGISTRY:
        from .sources.oece_rnp import PE_RUC_SCHEME, normalise_ruc

        hops[PE_RUC_SCHEME] = RegisterHop(
            PE_RUC_SCHEME, "oece_rnp", normalise_ruc, True, derived_key="pe_ruc",
        )
    # ``REG-<country>`` is the mappers' fallback scheme for "a number on that
    # country's company register" when the filing does not name the register
    # — the Companies House mapper emits ``REG-GB`` for a PSC whose
    # ``place_registered`` reads "England And Wales", and OpenAleph's UK
    # records carry ``REG-GB`` too (seen on Vosper Thornycroft, 2026-09-08).
    # Where a country has exactly one register hop, that is the register.
    by_country: dict[str, list[RegisterHop]] = {}
    for scheme, hop in hops.items():
        if scheme in _NO_COUNTRY_ALIAS:
            continue
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
