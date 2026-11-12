"""Pydantic schema for Slovenian Business Register (AJPES PRS) fetch bundles.

Only the fields the BODS mapper (``map_si_prs``) reads are declared;
everything else passes through via ``extra="allow"``. The bundle is assembled
by ``SiPrsAdapter.fetch`` from the local SQLite index built by
``scripts/build_si_prs_index.py`` — what this guards is the index staying in
step with the adapter after a rebuild, and a natural person's street never
reaching a bundle.
"""

from __future__ import annotations

from typing import Any

from pydantic import model_validator

from . import _Base


class SiPrsBundle(_Base):
    """Top-level shape returned by SiPrsAdapter.fetch."""

    # The ten-digit registration number (matična številka), the index key.
    number: str
    name: str | None = None
    legal_name: str | None = None
    # The register's legal-form words and registering authority, verbatim.
    form: str | None = None
    organ: str | None = None
    # False for a sole trader and any form not recognised as a legal person.
    legal_person: bool = False
    address: dict[str, Any] | None = None
    # The portal's last-change day of the file the index was built from.
    cut: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None

    @model_validator(mode="after")
    def _no_home_address(self) -> SiPrsBundle:
        if not self.legal_person and self.address and (
            self.address.get("street") or self.address.get("house")
        ):
            raise ValueError("a natural person's street address reached the bundle")
        return self
