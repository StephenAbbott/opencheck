"""BODS v0.4 mapping, validation, and helpers.

Every source adapter in OpenCheck feeds through this package before
anything user-facing happens. The design principle is that BODS v0.4
is the internal spine, not an export format — so the UI, risk rules,
and (later) export adapters all work off the mapped statements, not
the raw source payloads.
"""

from .mapper import (
    BODSBundle,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    map_ariregister,
    map_bolagsverket,
    map_brightquery,
    map_brreg,
    map_companies_house,
    map_cro,
    map_everypolitician,
    map_ftm,
    map_gleif,
    map_inpi,
    map_kvk,
    map_openaleph,
    map_opencorporates,
    map_opensanctions,
    map_opentender,
    map_prh,
    map_sec_edgar,
    map_ur_latvia,
    map_wikidata,
    map_zefix,
)
from .validator import validate_shape, ValidationError

__all__ = [
    "BODSBundle",
    "make_entity_statement",
    "make_person_statement",
    "make_relationship_statement",
    "map_ariregister",
    "map_bolagsverket",
    "map_brightquery",
    "map_brreg",
    "map_companies_house",
    "map_cro",
    "map_everypolitician",
    "map_ftm",
    "map_gleif",
    "map_inpi",
    "map_kvk",
    "map_opencorporates",
    "map_opensanctions",
    "map_openaleph",
    "map_opentender",
    "map_prh",
    "map_sec_edgar",
    "map_ur_latvia",
    "map_wikidata",
    "map_zefix",
    "validate_shape",
    "ValidationError",
]
