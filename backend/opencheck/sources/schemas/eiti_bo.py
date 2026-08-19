"""Pydantic schema for the pooled EITI BO registers adapter bundle.

The bundle is assembled by ``EitiBoAdapter.fetch_by_lei`` / ``fetch`` from the
committed, LEI-keyed pooled index ``opencheck/data/eiti_bo_index.json.gz``
(built by ``scripts/build_eiti_bo_index.py``). The per-register payloads
(``record.drc`` / ``record.armenia`` / ``record.nigeria``) differ in shape by
design — each national register publishes different fields — so only the
common envelope the mapper and frontend read is declared; everything else
passes through via ``extra="allow"`` on ``_Base``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class EitiBoMatch(_Base):
    """How the register company was resolved to this LEI at build time."""

    method: str | None = None
    #: "high" (registration-number equality) | "medium" (name equality).
    confidence: str = "medium"


class EitiBoRecord(_Base):
    """One pooled-register company record (envelope common to all registers)."""

    lei: str
    register_id: str
    country: str | None = None
    company: str | None = None
    company_latin: str | None = None
    local_ids: dict[str, Any] = Field(default_factory=dict)
    #: The register's own date for this record (declaration date / export stamp).
    source_date: str | None = None
    #: When OpenCheck harvested the register (from the raw artifact).
    retrieved: str | None = None
    match: EitiBoMatch | None = None
    lei_registration_status: str | None = None


class EitiBoBundle(_Base):
    """Top-level shape returned by EitiBoAdapter.fetch_by_lei / fetch."""

    lei: str
    record: EitiBoRecord
    identifiers: dict[str, str] = Field(default_factory=dict)
    register_id: str | None = None
    register_name: str | None = None
    register_url: str | None = None
    register_licence: str | None = None
