"""Source adapters.

Each module in this package exposes an adapter that implements
``SourceAdapter``. Adapters are registered in ``REGISTRY`` for discovery
by the FastAPI app.
"""

from __future__ import annotations

from .ariregister import AriregisterAdapter
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .bolagsverket import BolagsverketAdapter
from .brightquery import BrightQueryAdapter
from .brreg import BrregAdapter
from .companies_house import CompaniesHouseAdapter
from .cro import CroAdapter
from .everypolitician import EveryPoliticianAdapter
from .gleif import GleifAdapter
from .inpi import InpiAdapter
from .kvk import KvKAdapter
from .openaleph import OpenAlephAdapter  # noqa: F401  -- kept for re-enablement
from .opencorporates import OpenCorporatesAdapter
from .opensanctions import OpenSanctionsAdapter
from .opentender import OpenTenderAdapter
from .prh import PrhAdapter
from .sec_edgar import SecEdgarAdapter
from .wikidata import WikidataAdapter
from .zefix import ZefixAdapter

# OpenAleph is intentionally excluded from the registry while the LEI
# flow is the supported entry point — its API is name-keyed rather than
# identifier-keyed, so it doesn't bridge cleanly off an LEI yet.
# Re-enable by adding ``"openaleph": OpenAlephAdapter()`` below once we
# have a curated set of subjects to demo it against.
REGISTRY: dict[str, SourceAdapter] = {
    "ariregister": AriregisterAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "kvk": KvKAdapter(),
    "opencorporates": OpenCorporatesAdapter(),
    "brightquery": BrightQueryAdapter(),
    "opensanctions": OpenSanctionsAdapter(),
    "everypolitician": EveryPoliticianAdapter(),
    "wikidata": WikidataAdapter(),
    "opentender": OpenTenderAdapter(),
    "prh": PrhAdapter(),
    "sec_edgar": SecEdgarAdapter(),
    "zefix": ZefixAdapter(),
}

__all__ = [
    "REGISTRY",
    "SearchKind",
    "SourceAdapter",
    "SourceHit",
    "SourceInfo",
]
