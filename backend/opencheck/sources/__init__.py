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
from .cac_nigeria import CacNigeriaAdapter
from .cnpj_brazil import CnpjBrazilAdapter
from .cvr_denmark import CvrDenmarkAdapter
from .climatetrace import ClimateTRACEAdapter
from .companies_house import CompaniesHouseAdapter
from .corporations_canada import CorporationsCanadaAdapter
from .cro import CroAdapter
from .eiti import EitiAdapter
from .eiti_bo import EitiBoAdapter
from .eiti_soe import EitiSoeAdapter
from .everypolitician import EveryPoliticianAdapter
from .firmenbuch import FirmenbuchAdapter
from .gleif import GleifAdapter
from .inpi import InpiAdapter
from .jar_lithuania import JarLithuaniaAdapter
from .krs_poland import KrsPolandAdapter
from .kvk import KvKAdapter
from .malta_mbr import MaltaMbrAdapter
from .mca_india import McaIndiaAdapter
from .nz_companies import NzCompaniesAdapter
from .openaleph import OpenAlephAdapter
from .opencorporates import OpenCorporatesAdapter
from .opensanctions import OpenSanctionsAdapter
from .prh import PrhAdapter
from .rpo_slovakia import RpoSlovakiaAdapter
from .rpvs_slovakia import RpvsSlovakiaAdapter
from .sec_edgar import SecEdgarAdapter
from .sudreg_croatia import SudregCroatiaAdapter
from .ted_eu import TedEuAdapter
from .ur_latvia import UrLatviaAdapter
from .wikidata import WikidataAdapter
from .wikirate import WikirateAdapter
from .zefix import ZefixAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "abr_australia": AbrAustraliaAdapter(),
    "ares": AresAdapter(),
    "ariregister": AriregisterAdapter(),
    "bce_belgium": BceBelgiumAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "cac_nigeria": CacNigeriaAdapter(),
    "climatetrace": ClimateTRACEAdapter(),
    "cnpj_brazil": CnpjBrazilAdapter(),
    "corporations_canada": CorporationsCanadaAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "cvr_denmark": CvrDenmarkAdapter(),
    "eiti": EitiAdapter(),
    "eiti_bo": EitiBoAdapter(),
    "eiti_soe": EitiSoeAdapter(),
    "firmenbuch": FirmenbuchAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "jar_lithuania": JarLithuaniaAdapter(),
    "krs_poland": KrsPolandAdapter(),
    "kvk": KvKAdapter(),
    "malta_mbr": MaltaMbrAdapter(),
    "mca_india": McaIndiaAdapter(),
    "nz_companies": NzCompaniesAdapter(),
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
    "ted_eu": TedEuAdapter(),
    "ur_latvia": UrLatviaAdapter(),
    "wikirate": WikirateAdapter(),
    "zefix": ZefixAdapter(),
}

# Tell the SourceHit serializer which sources must not have their raw payload
# redistributed (licence permits derived/BODS output, not bulk raw re-publication).
from . import base as _base  # noqa: E402

_base.RAW_SUPPRESSED_SOURCE_IDS = frozenset(
    sid for sid, adapter in REGISTRY.items() if not adapter.republish_raw
)

#: Display names for source ids that are not registered adapters but do reach
#: user-facing prose. ``icij`` is the one that matters: the Offshore Leaks
#: reconciliation endpoint produces signals without being an adapter, so
#: ``REGISTRY`` cannot name it.
_UNREGISTERED_NAMES = {
    "icij": "ICIJ Offshore Leaks",
    "opencheck": "OpenCheck",
}


def source_display_name(source_id: str) -> str:
    """The registry's own name for a source, for a sentence a reader will see.

    Adapter ids are snake_case slugs, and several of them are wrong as brand
    names however they are prettified — the registry says "OpenSanctions",
    "OpenAleph", "Global Energy Monitor / Climate TRACE". A related-party
    signal read "shares a name with a record on openaleph"; the id belongs in
    ``source_id``, which the same signal already carries, not in the sentence.

    Falls back to the id, which is at least true, rather than to a guess.
    """
    info = getattr(REGISTRY.get(source_id), "info", None)
    name = getattr(info, "name", None)
    if isinstance(name, str) and name:
        return name
    return _UNREGISTERED_NAMES.get(source_id, source_id)


__all__ = [
    "REGISTRY",
    "SearchKind",
    "SourceAdapter",
    "SourceHit",
    "SourceInfo",
    "source_display_name",
]
