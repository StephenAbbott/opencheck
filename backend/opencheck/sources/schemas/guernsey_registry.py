"""Pydantic schema for the Guernsey Registry adapter's bundle.

The bundle is OpenCheck's own reduction of the registry's answers (an
allowlist — ``sources.guernsey_registry.entity_summary``), not the registry's
JSON. The one thing it **forbids** is an individual official's identity: only
corporate officials are named (Stephen, 30 Oct 2026), so an official of kind
``person`` must carry no ``name`` and no ``official_key`` (the registry's id
for the person).
"""

from __future__ import annotations

from typing import Literal

from pydantic import model_validator

from . import _Base


class GuernseyRegistryAddress(_Base):
    text: str = ""
    postcode: str = ""
    country: str = ""


class GuernseyRegistryEntity(_Base):
    number: str
    name: str = ""
    registered_on: str | None = None
    type: str = ""
    status: str = ""
    status_detail: str = ""
    status_since: str | None = None
    address: GuernseyRegistryAddress | None = None


class GuernseyRegistryFormerName(_Base):
    name: str
    since: str | None = None


class GuernseyRegistryOfficial(_Base):
    relationship_id: str = ""
    role: str = ""
    appointed: str | None = None
    kind: Literal["guernsey_entity", "body", "person"]
    name: str | None = None
    official_key: str | None = None
    number: str | None = None
    resolved: bool = True

    @model_validator(mode="after")
    def _individuals_are_not_named(self) -> "GuernseyRegistryOfficial":
        if self.kind == "person" and (self.name or self.official_key):
            raise ValueError("an individual official must not be named or keyed")
        if self.kind != "person" and not self.name:
            raise ValueError("a corporate official must be named")
        return self


class GuernseyRegistryBundle(_Base):
    """Top-level shape returned by ``GuernseyRegistryAdapter.fetch``."""

    gg_registry_number: str  # required — mapper key
    entity: GuernseyRegistryEntity | None = None
    former_names: list[GuernseyRegistryFormerName] = []
    names_read: bool = True
    officials: list[GuernseyRegistryOfficial] = []
    officials_read: bool = True
    officials_resolve_capped: bool = False
    legal_name: str | None = None
