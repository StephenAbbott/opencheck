"""Pydantic schema for the CVR Denmark bundle."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class CVRBundle(_Base):
    """Minimal required shape for a CVR Denmark fetch bundle."""

    cvr_number: str = Field(..., min_length=8, max_length=8)
    cvr_enhed_id: str
    name: str
    status: str
    start_date: str | None = None
    end_date: str | None = None
    legal_form_code: str | None = None
    legal_form_text: str | None = None
    branche_code: str | None = None
    address: dict[str, Any] | None = None
    source_url: str
    fully_liable_participant_ids: list[str] = Field(default_factory=list)
