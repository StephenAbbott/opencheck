"""Source adapters.

Each module in this package exposes an adapter that implements
``SourceAdapter``. Adapters are registered in ``REGISTRY`` for discovery
by the FastAPI app.
"""

from __future__ import annotations

from .ariregister import AriregisterAdapter
from .ares import AresAdapter
from .bce_belgium import BceBelgiumAdapter
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .bolagsverket import BolagsverketAdapter
from .brreg import BrregAdapter
from .climatetrace import ClimateTRACEAdapter
from .companies_house import CompaniesHouseAdapter
from .corporations_canada import CorporationsCanadaAdapter
from .cro import CroAdapter
from .everypolitician import EveryPoliticianAdapter
from .firmenbuch import FirmenbuchAdapter
from .gleif import GleifAdapter
from .inpi import InpiAdapter
from .jar_lithuania import JarLithuaniaAdapter
from .krs_poland import KrsPolandAdapter
from .kvk import KvKAdapter
from .openaleph import OpenAlephAdapter
from .opencorporates import OpenCorporatesAdapter
from .opensanctions import OpenSanctionsAdapter
from .prh import PrhAdapter
from .rpo_slovakia import RpoSlovakiaAdapter
from .rpvs_slovakia import RpvsSlovakiaAdapter
from .sec_edgar import SecEdgarAdapter
from .ur_latvia import UrLatviaAdapter
from .wikidata import WikidataAdapter
from .zefix import ZefixAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "ares": AresAdapter(),
    "ariregister": AriregisterAdapter(),
    "bce_belgium": BceBelgiumAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "climatetrace": ClimateTRACEAdapter(),
    "corporations_canada": CorporationsCanadaAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "firmenbuch": FirmenbuchAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "jar_lithuania": JarLithuaniaAdapter(),
    "krs_poland": KrsPolandAdapter(),
    "kvk": KvKAdapter(),
    "opencorporates": OpenCorporatesAdapter(),
    "openaleph": OpenAlephAdapter(),
    "opensanctions": OpenSanctionsAdapter(),
    "everypolitician": EveryPoliticianAdapter(),
    "wikidata": WikidataAdapter(),
    "prh": PrhAdapter(),
    "rpo_slovakia": RpoSlovakiaAdapter(),
    "rpvs_slovakia": RpvsSlovakiaAdapter(),
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
