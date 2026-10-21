"""Pydantic schema for the CVR Denmark bundle."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class CVRParticipantPeriod(_Base):
    """One CVR_FuldtAnsvarligDeltagerRelation row: when the role ran."""

    from_: str | None = Field(default=None, alias="from")
    to: str | None = None


class CVRParticipant(_Base):
    """A fully liable participant (I/S, K/S, P/S partner).

    No birth date and no CPR number: Datafordeler's open CVR data carries
    neither for a person, and the adapter reads neither.
    """

    participant_id: str = Field(..., pattern=r"^\d+$")
    kind: str = Field(..., pattern=r"^(person|entity|unknown)$")
    cvr_number: str | None = Field(default=None, min_length=8, max_length=8)
    name: str | None = None
    periods: list[CVRParticipantPeriod] = Field(default_factory=list)
    current: bool = False


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
    participants: list[CVRParticipant] = Field(default_factory=list)
    participants_read: bool = True
