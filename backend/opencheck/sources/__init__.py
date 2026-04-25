"""Source adapters.

Each module in this package exposes an adapter that implements
``SourceAdapter``. Adapters are registered in ``REGISTRY`` for discovery
by the FastAPI app.
"""

from __future__ import annotations

from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .companies_house import CompaniesHouseAdapter
from .everypolitician import EveryPoliticianAdapter
from .gleif import GleifAdapter
from .openaleph import OpenAlephAdapter
from .opensanctions import OpenSanctionsAdapter
from .opentender import OpenTenderAdapter
from .wikidata import WikidataAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "companies_house": CompaniesHouseAdapter(),
    "gleif": GleifAdapter(),
    "opensanctions": OpenSanctionsAdapter(),
    "openaleph": OpenAlephAdapter(),
    "everypolitician": EveryPoliticianAdapter(),
    "wikidata": WikidataAdapter(),
    "opentender": OpenTenderAdapter(),
}

__all__ = [
    "REGISTRY",
    "SearchKind",
    "SourceAdapter",
    "SourceHit",
    "SourceInfo",
]
