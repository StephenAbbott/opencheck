"""Pydantic schema for Lithuanian JAR bundles (Phase 325).

The bundle is assembled by ``JarLithuaniaAdapter.fetch`` from the index that
``scripts/build_jar_lithuania_index.py`` packs from Registrų centras' open-data
files. Only the fields the mapper and the finding read are declared;
``extra="allow"`` on ``_Base`` keeps the rest.
"""

from __future__ import annotations

from . import _Base


class JarLithuaniaBundle(_Base):
    """Top-level shape returned by ``JarLithuaniaAdapter.fetch`` for a record."""

    lt_code: str
    name: str = ""
    register_name: str | None = None
    name_withheld: bool = False
    address: str | None = None
    legal_form: str | None = None
    legal_form_code: str | None = None
    registered_on: str | None = None
    status: str | None = None
    status_code: str | None = None
    status_since: str | None = None
    deregistered: bool = False
    deregistered_on: str | None = None
    cut: str | None = None
    link: str | None = None
    is_stub: bool = False
    source_id: str | None = None
