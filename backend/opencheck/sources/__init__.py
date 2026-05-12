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
from .climatetrace import ClimateTRACEAdapter
from .companies_house import CompaniesHouseAdapter
from .cro import CroAdapter
from .everypolitician import EveryPoliticianAdapter
from .gleif import GleifAdapter
from .inpi import InpiAdapter
from .kvk import KvKAdapter
from .openaleph import OpenAlephAdapter
from .opencorporates import OpenCorporatesAdapter
from .opensanctions import OpenSanctionsAdapter
from .opentender import OpenTenderAdapter
from .prh import PrhAdapter
from .sec_edgar import SecEdgarAdapter
from .ur_latvia import UrLatviaAdapter
from .wikidata import WikidataAdapter
from .zefix import ZefixAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "ariregister": AriregisterAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "climatetrace": ClimateTRACEAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "kvk": KvKAdapter(),
    "opencorporates": OpenCorporatesAdapter(),
    "brightquery": BrightQueryAdapter(),
    "openaleph": OpenAlephAdapter(),
    "opensanctions": OpenSanctionsAdapter(),
    "everypolitician": EveryPoliticianAdapter(),
    "wikidata": WikidataAdapter(),
    "opentender": OpenTenderAdapter(),
    "prh": PrhAdapter(),
    "sec_edgar": SecEdgarAdapter(),
    "ur_latvia": UrLatviaAdapter(),
    "zefix": ZefixAdapter(),
}

__all__ = [
    "REGISTRY",
    "SearchKind",
    "SourceAdapter",
    "SourceHit",
    "SourceInfo",
]
