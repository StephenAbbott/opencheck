"""Pydantic schema for Bolagsverket (Sweden) API responses.

Only the fields the BODS mapper (map_bolagsverket) actually reads are
declared.  Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class BVCompany(_Base):
    """Organisation object returned by the Bolagsverket /organisationer endpoint.

    ``organisationsnamn`` and ``organisationsdatum`` are **objects**, not scalars:
    the mapper reads ``organisationsnamn.organisationsnamnLista[].namn`` and
    ``organisationsdatum.registreringsdatum``. They were previously (incorrectly)
    declared as ``str``, which made the API's real object shape fail validation
    once the fields were populated (v4.x). Modelled as dicts so validation
    reflects the mapper's contract; ``extra="allow"`` keeps their inner keys.
    """

    organisationsidentitet: dict[str, Any] = Field(default_factory=dict)  # nested org number
    organisationsnamn: dict[str, Any] = Field(default_factory=dict)  # {organisationsnamnLista: [...]}
    organisationsdatum: dict[str, Any] = Field(default_factory=dict)  # {registreringsdatum: ...}
    organisationsform: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] = Field(default_factory=dict)
    postadressOrganisation: dict[str, Any] = Field(default_factory=dict)
    # styrelseledamoter, revisorer, etc. — leave as passthrough


class BVBundle(_Base):
    """Top-level shape returned by BolagsverketAdapter.fetch."""

    org_number: str  # required — mapper key
    company: BVCompany | None = None
    legal_name: str | None = None
