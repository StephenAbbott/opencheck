"""Leads for a retired LEI that names no successor (Phase 308).

Three-quarters of INACTIVE LEI records are a dissolution or a liquidation
with no successor named (7 Oct 2026 Golden Copy: 192,528 + 15,344 of
254,762), so for most retired LEIs Phase 307's follow-forward line has
nothing to follow. The reader of Barrick Gold Inc. (``5493002CWGHR03YL8X75``)
still wants to know where to look next. This module offers the two things
GLEIF's own data can say, as **leads, never assertions**:

* **The parent GLEIF last filed** for the record — the Golden Copy mirror's
  relationship row for the LEI, standing or not (a retired record's
  relationships are usually retired with it). A local read; no mirror, no
  parent, said as such.
* **Active LEI records with a similar name** — one GLEIF fulltext search for
  the legal name, filtered to ``entity.status=ACTIVE``, the subject excluded,
  ranked by :mod:`search_rank`'s query-side match tier, fuzzy matches
  dropped, at most :data:`MAX_CANDIDATES`. Each candidate carries its tier in
  words and the ``name_only`` flag that is always true here: only the name
  agrees, which is not an identity match.

Rules, each one a decision (Stephen, 8 Oct 2026; the ticket *Retired LEIs,
successors and former names - October 2026*):

* **Never a redirect, never a successor.** The response's ``note`` says so
  in words, and nothing here reaches the ``subject_profile`` event, the MCP
  profile, the PDF/Markdown report or a saved report. It is a UI-only route,
  like ``/subsidiaries/declared``: the retired page is the record of *that*
  entity, and a saved report of it must stay that (Phase 218).
* **A discretionary GLEIF call** (``gleif_throttle.discretionary``, Phase
  234): refused at once when the reserve for lookups is short, and the
  response says why (``gleif_unavailable_reason``) rather than showing an
  empty list as "nothing similar exists".
* **A name match is OpenCheck's reading, not GLEIF's**, so the tier words
  come from the one place the search ranking already defines them
  (:data:`search_rank.MATCH_TIERS`), and the chip is always "Name only".
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

import httpx

from . import search_rank
from .gleif_throttle import GleifRateLimitedError, unavailable_reason
from .sources import REGISTRY
from .sources.gleif import GleifAdapter

log = logging.getLogger(__name__)

#: How many similar-name candidates the page lists. GLEIF's page is ten;
#: five is what a reader can weigh, and the list is a lead, not a result.
MAX_CANDIDATES = 5

#: Match tiers a candidate may carry. ``fuzzy`` is dropped: with no other
#: evidence a fuzzy name is noise, and the Phase 292 ranking already treats
#: it as the weakest tier.
ACCEPTED_TIERS: frozenset[str] = frozenset({"exact", "same_name", "all_tokens", "distinctive_tokens"})

#: The sentence every response carries, so no consumer can present the list
#: as anything but what it is.
NOTE = (
    "Leads, not a successor: GLEIF names no successor on this LEI record. "
    "A similar name is a name match only, not an identity match; the parent "
    "is what GLEIF last filed for this record, which may itself have ended."
)

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")
_CACHE_NS = "gleif"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:120]


def last_filed_parent(lei: str, store: Any = None) -> dict[str, Any] | None:
    """The direct parent the Golden Copy mirror holds for ``lei``, standing
    or not — ``{"lei", "name", "entity_status", "registration_status",
    "relationship_status", "standing"}`` — or ``None`` when the mirror is
    absent or holds no such row."""
    if store is None:
        from . import entity_pages

        store = entity_pages.get_store()
    if store is None:
        return None
    try:
        rel = store.relationship(lei, "direct")
    except Exception:  # noqa: BLE001 — a mirror read must never fail the route
        log.warning("leads: mirror relationship read failed for %s", lei, exc_info=True)
        return None
    if rel is None or not rel.parent_lei:
        return None
    parent = store.get(rel.parent_lei)
    return {
        "lei": rel.parent_lei,
        "name": parent.name if parent else None,
        "entity_status": parent.entity_status if parent else None,
        "registration_status": parent.registration_status if parent else None,
        "relationship_status": rel.relationship_status,
        "standing": bool(rel.standing),
    }


def _best_name_tier(
    query: str, legal_name: str, entity: dict[str, Any]
) -> tuple[int, str | None, str | None]:
    """The best match tier across the legal name and GLEIF's ``otherNames``
    (and ``transliteratedOtherNames``), with the other name and its type when
    one of those is what matched. The legal name wins a tie."""
    best: tuple[int, str | None, str | None] = (search_rank.match_tier(query, legal_name), None, None)
    for key in ("otherNames", "transliteratedOtherNames"):
        for other in entity.get(key) or []:
            if not isinstance(other, dict):
                continue
            other_name = " ".join(str(other.get("name") or "").split())
            if not other_name:
                continue
            tier = search_rank.match_tier(query, other_name)
            if tier < best[0]:
                best = (tier, other_name, str(other.get("type") or "") or None)
    return best


def rank_candidates(name: str, items: list[dict[str, Any]], *, exclude: str) -> list[dict[str, Any]]:
    """Shape and rank GLEIF ``lei-records`` items as leads. Pure: the route
    does the fetching. Keeps ACTIVE records other than ``exclude`` whose
    name matches at an accepted tier, best tier first, then GLEIF's order."""
    out: list[tuple[int, int, dict[str, Any]]] = []
    seen: set[str] = set()
    for position, item in enumerate(items):
        attrs = (item or {}).get("attributes") or {}
        entity = attrs.get("entity") or {}
        lei = str(attrs.get("lei") or item.get("id") or "").upper()
        if not _LEI_SHAPE.match(lei) or lei == exclude or lei in seen:
            continue
        if str(entity.get("status") or "").upper() != "ACTIVE":
            continue
        candidate_name = (entity.get("legalName") or {}).get("name") or ""
        if not candidate_name:
            continue
        # The legal name first, then every other name GLEIF files — a former
        # legal name above all. GLEIF's fulltext search matched the record on
        # one of them, and the Barrick case is the renamed company: "Barrick
        # Gold Inc." finds BARRICK MINING CORPORATION only through its
        # PREVIOUS_LEGAL_NAME "Barrick Gold Corporation".
        tier, matched, matched_type = _best_name_tier(name, candidate_name, entity)
        tier_word = search_rank.MATCH_TIERS[tier]
        if tier_word not in ACCEPTED_TIERS:
            continue
        seen.add(lei)
        out.append(
            (
                tier,
                position,
                {
                    "lei": lei,
                    "name": candidate_name,
                    "jurisdiction": entity.get("jurisdiction") or None,
                    "registration_status": (attrs.get("registration") or {}).get("status") or None,
                    "match": tier_word,
                    # The other name the match rests on, and GLEIF's type for
                    # it (PREVIOUS_LEGAL_NAME, TRADING_OR_OPERATING_NAME …);
                    # both None when the legal name itself matched.
                    "matched_name": matched,
                    "matched_name_type": matched_type,
                    "name_only": True,
                },
            )
        )
    out.sort(key=lambda t: (t[0], t[1]))
    return [c for _, _, c in out[:MAX_CANDIDATES]]


async def assemble_leads(lei: str, name: str, *, store: Any = None) -> dict[str, Any]:
    """The ``/leads`` response for ``lei``: the last filed parent, the
    similar-name candidates, and the reasons for whatever could not be
    said. Never raises for a GLEIF failure."""
    name = " ".join(str(name or "").split())
    out: dict[str, Any] = {
        "lei": lei,
        "searched_name": name or None,
        "parent": last_filed_parent(lei, store),
        "candidates": [],
        "gleif_unavailable_reason": None,
        "note": NOTE,
    }
    if not name:
        return out
    gleif = REGISTRY.get("gleif")
    if not isinstance(gleif, GleifAdapter) or not gleif.info.live_available:
        out["gleif_unavailable_reason"] = "unreachable"
        return out
    path = (
        f"/lei-records?filter[fulltext]={quote(name)}"
        "&filter[entity.status]=ACTIVE&page[size]=10"
    )
    try:
        payload = await gleif._get(path, cache_key=f"{_CACHE_NS}/leads/{_slug(name)}")
    except (GleifRateLimitedError, httpx.HTTPError) as exc:
        out["gleif_unavailable_reason"] = unavailable_reason(exc)
        return out
    except Exception:  # noqa: BLE001 — an unreadable answer is "unreachable", never a 500
        log.warning("leads: GLEIF search failed for %s", lei, exc_info=True)
        out["gleif_unavailable_reason"] = "unreachable"
        return out
    out["candidates"] = rank_candidates(name, payload.get("data") or [], exclude=lei)
    return out
