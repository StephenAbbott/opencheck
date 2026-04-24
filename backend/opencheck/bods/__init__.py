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
    map_companies_house,
)
from .validator import validate_shape, ValidationError

__all__ = [
    "BODSBundle",
    "make_entity_statement",
    "make_person_statement",
    "make_relationship_statement",
    "map_companies_house",
    "validate_shape",
    "ValidationError",
]
