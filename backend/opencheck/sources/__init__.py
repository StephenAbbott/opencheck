"""Source adapters.

Each module in this package exposes an adapter that implements
``SourceAdapter``. Adapters are registered in ``REGISTRY`` for discovery
by the FastAPI app.
"""

from __future__ import annotations

from .abr_australia import AbrAustraliaAdapter
from .anaf_romania import AnafRomaniaAdapter
from .apr_serbia import AprSerbiaAdapter
from .onrc_romania import OnrcRomaniaAdapter
from .ariregister import AriregisterAdapter
from .asp_moldova import AspMoldovaAdapter
from .ares import AresAdapter
from .bce_belgium import BceBelgiumAdapter
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .bolagsverket import BolagsverketAdapter
from .brreg import BrregAdapter
from .cac_nigeria import CacNigeriaAdapter
from .austender import AusTenderAdapter
from .chilecompra import ChileCompraAdapter
from .compras_gov_br import ComprasGovBrAdapter
from .find_tender_uk import FindTenderUkAdapter
from .cnpj_brazil import CnpjBrazilAdapter
from .acra_singapore import AcraSingaporeAdapter
from .cvr_denmark import CvrDenmarkAdapter
from .dlcp_dc import DlcpDcAdapter
from .ny_dos import NyDosAdapter
from .ct_sots import CtSotsAdapter
from .rues_colombia import RuesColombiaAdapter
from .pa_dos import PaDosAdapter
from .climatetrace import ClimateTRACEAdapter
from .companies_house import CompaniesHouseAdapter
from .corporations_canada import CorporationsCanadaAdapter
from .cr_hongkong import CrHongKongAdapter
from .dart_korea import DartKoreaAdapter
from .gcis_taiwan import GcisTaiwanAdapter
from .cro import CroAdapter
from .eiti import EitiAdapter
from .eiti_assessment import EitiAssessmentAdapter
from .eiti_bo import EitiBoAdapter
from .eiti_soe import EitiSoeAdapter
from .eiti_zambia import EitiZambiaAdapter
from .estma import EstmaAdapter
from .everypolitician import EveryPoliticianAdapter
from .firmenbuch import FirmenbuchAdapter
from .gemi_greece import GemiGreeceAdapter
from .gleif import GleifAdapter
from .inpi import InpiAdapter
from .sirene import SireneAdapter
from .edr_ukraine import EdrUkraineAdapter
from .jar_lithuania import JarLithuaniaAdapter
from .cyprus_drcor import CyprusDrcorAdapter
from .ica_israel import IcaIsraelAdapter
from .si_prs import SiPrsAdapter
from .registro_sociedades_argentina import RegistroSociedadesArgentinaAdapter
from .oece_rnp import OeceRnpAdapter
from .sunbiz_florida import SunbizFloridaAdapter
from .krs_poland import KrsPolandAdapter
from .kvk import KvKAdapter
from .malta_mbr import MaltaMbrAdapter
from .meip import MeipAdapter
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
    "acra_singapore": AcraSingaporeAdapter(),
    "anaf_romania": AnafRomaniaAdapter(),
    "apr_serbia": AprSerbiaAdapter(),
    "onrc_romania": OnrcRomaniaAdapter(),
    "ares": AresAdapter(),
    "ariregister": AriregisterAdapter(),
    "asp_moldova": AspMoldovaAdapter(),
    "austender": AusTenderAdapter(),
    "bce_belgium": BceBelgiumAdapter(),
    "bolagsverket": BolagsverketAdapter(),
    "brreg": BrregAdapter(),
    "cac_nigeria": CacNigeriaAdapter(),
    "chilecompra": ChileCompraAdapter(),
    "find_tender_uk": FindTenderUkAdapter(),
    "climatetrace": ClimateTRACEAdapter(),
    "cnpj_brazil": CnpjBrazilAdapter(),
    "compras_gov_br": ComprasGovBrAdapter(),
    "corporations_canada": CorporationsCanadaAdapter(),
    "cr_hongkong": CrHongKongAdapter(),
    "companies_house": CompaniesHouseAdapter(),
    "cro": CroAdapter(),
    "cvr_denmark": CvrDenmarkAdapter(),
    "dlcp_dc": DlcpDcAdapter(),
    "ny_dos": NyDosAdapter(),
    "ct_sots": CtSotsAdapter(),
    "rues_colombia": RuesColombiaAdapter(),
    "pa_dos": PaDosAdapter(),
    "eiti": EitiAdapter(),
    "eiti_assessment": EitiAssessmentAdapter(),
    "eiti_bo": EitiBoAdapter(),
    "eiti_soe": EitiSoeAdapter(),
    "eiti_zambia": EitiZambiaAdapter(),
    "estma": EstmaAdapter(),
    "firmenbuch": FirmenbuchAdapter(),
    "gemi_greece": GemiGreeceAdapter(),
    "gcis_taiwan": GcisTaiwanAdapter(),
    "dart_korea": DartKoreaAdapter(),
    "gleif": GleifAdapter(),
    "inpi": InpiAdapter(),
    "sirene": SireneAdapter(),
    "jar_lithuania": JarLithuaniaAdapter(),
    "edr_ukraine": EdrUkraineAdapter(),
    "cyprus_drcor": CyprusDrcorAdapter(),
    "ica_israel": IcaIsraelAdapter(),
    "si_prs": SiPrsAdapter(),
    "registro_sociedades_argentina": RegistroSociedadesArgentinaAdapter(),
    "oece_rnp": OeceRnpAdapter(),
    "sunbiz_florida": SunbizFloridaAdapter(),
    "krs_poland": KrsPolandAdapter(),
    "kvk": KvKAdapter(),
    "malta_mbr": MaltaMbrAdapter(),
    "meip": MeipAdapter(),
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
