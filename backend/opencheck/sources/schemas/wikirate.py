"""Pydantic schema for the Wikirate adapter bundle.

The bundle is assembled from three Wikirate REST calls (Decko card JSON):

* ``GET /Companies.json?filter[company_identifier[value]]={lei|qid}`` —
  resolves an OpenCheck subject to a Wikirate Company card. The nested
  ``[value]`` param shape is required — the flat
  ``filter[company_identifier]`` documented elsewhere 500s server-side
  (verified 2026-07-07).
* ``GET /~{card_id}+Answer.json?view=count`` — total metric answers.
* ``GET /~{card_id}+Answer.json?filter[year]=latest&limit=N`` — latest
  answer per metric (sample).

Only fields the BODS mapper and the frontend card read are declared;
everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class WikirateAnswer(_Base):
    """One metric answer (latest value for one metric)."""

    metric_designer: str | None = None
    metric_name: str | None = None
    year: int | None = None
    value: Any = None
    answer_url: str | None = None  # HTML page on wikirate.org


class WikirateBundle(_Base):
    """Top-level shape returned by WikirateAdapter.fetch_by_lei/fetch."""

    card_id: int
    name: str
    wikirate_url: str  # HTML page on wikirate.org (stable ~id form)
    matched_by: str  # "lei" | "wikidata_qid" | "card"
    identifiers: dict[str, Any] = Field(default_factory=dict)
    headquarters: str | None = None
    website: str | None = None
    total_answers: int = 0
    latest_answers: list[WikirateAnswer] = Field(default_factory=list)
