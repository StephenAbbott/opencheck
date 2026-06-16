"""Source adapters.

Each module in this package exposes an adapter that implements
``SourceAdapter``. Adapters are registered in ``REGISTRY`` for discovery
by the FastAPI app.
"""

from __future__ import annotations

from .abr_australia import AbrAustraliaAdapter
from .ariregister import AriregisterAdapter
from .ares import AresAdapter
from .bce_belgium import BceBelgiumAdapter
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .bolagsverket import BolagsverketAdapter
from .brreg import BrregAdapter
from .cnpj_brazil import CnpjBrazilAdapter
from .cvr_denmark import CvrDenmarkAdapter
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
from .malta_mbr import MaltaMbrAdapter
from .openaleph import OpenAlephAdapter
from .opencorporates import OpenCorporatesAdapter
from .opensanctions import OpenSanctionsAdapter
from .prh import PrhAdapter
from .rpo_slovakia import RpoSlovakiaAdapter
from .rpvs_slovakia import RpvsSlovakiaAdapter
from .sec_edgar import SecEdgarAdapter
from .sudreg_croatia import SudregCroatiaAdapter
from .ur_latvia import UrLatviaAdapter
from .wikidata import WikidataAdapter
from .zefix import ZefixAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "abr_australia": AbrAustraliaAdapter(),
    "ares": AresAdapter(),
    "ariregister": AriregisterAdapter(),
    "bce_belgium": BceBelgiumAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "climatetrace": ClimateTRACEAdapter(),
    "cnpj_brazil": CnpjBrazilAdapter(),
    "corporations_canada": CorporationsCanadaAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "cvr_denmark": CvrDenmarkAdapter(),
    "firmenbuch": FirmenbuchAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "jar_lithuania": JarLithuaniaAdapter(),
    "krs_poland": KrsPolandAdapter(),
    "kvk": KvKAdapter(),
    "malta_mbr": MaltaMbrAdapter(),
    "opencorporates": OpenCorporatesAdapter(),
    "openaleph": OpenAlephAdapter(),
    "opensanctions": OpenSanctionsAdapter(),
    "everypolitician": EveryPoliticianAdapter(),
    "wikidata": WikidataAdapter(),
    "prh": PrhAdapter(),
    "rpo_slovakia": RpoSlovakiaAdapter(),
    "rpvs_slovakia": RpvsSlovakiaAdapter(),
    "sec_edgar": SecEdgarAdapter(),
    "sudreg_croatia": SudregCroatiaAdapter(),
    "ur_latvia": UrLatviaAdapter(),
    "zefix": ZefixAdapter(),
}

# Tell the SourceHit serializer which sources must not have their raw payload
# redistributed (licence permits derived/BODS output, not bulk raw re-publication).
from . import base as _base  # noqa: E402

_base.RAW_SUPPRESSED_SOURCE_IDS = frozenset(
    sid for sid, adapter in REGISTRY.items() if not adapter.republish_raw
)

__all__ = [
    "REGISTRY",
    "SearchKind",
    "SourceAdapter",
    "SourceHit",
    "SourceInfo",
]
