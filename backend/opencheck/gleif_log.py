"""GLEIF's own field-modification log for an LEI (Phase 303).

``GET /api/v1/lei-records/{lei}/field-modifications`` is GLEIF's record of
what changed in a LEI record and in the relationship records filed under it,
field by field: the XPath of the field, ``INITIAL`` / ``INSERT`` / ``UPDATE``
/ ``DELETE``, the old and new values, and the date of the Golden Copy publish
that carried the change (day granularity, 00:00 UTC). Relationship changes
are logged under the *child* LEI with ``context.relationshipType`` and
``context.endNode``. GLEIF's DataAlerts notebook (7 Oct 2026) reads it the
same way.

The watchlist uses it twice, never per watched LEI on a schedule:

* on a GLEIF-triggered re-run, one call fetches the log since the oldest
  baseline among the lists watching that LEI, and each list's entry carries
  the lines after *its* baseline — GLEIF's own account beside OpenCheck's
  diff;
* when an LEI is first watched, one call fetches the 30 days before, shown
  with the watch rather than in the feed, because it is not news.

Facts measured against the live API on 7 Oct 2026 shape the code:

* the date filter accepts only an exact timestamp, so the log is read
  newest first (``sort=-date``) in pages of 200 (the API's ceiling) until a
  line is no newer than the cut-off — usually one page;
* the XPath strings are inconsistent (some truncated, some without the
  leading slash), so labels come from the trailing element names;
* most lines are renewal clocks and a re-keyed validation reference, which
  are dropped (:data:`NOISE_ELEMENTS`), as the watchlist's own digest drops
  them.

Every request goes through :func:`opencheck.http.build_client`, so through
the process-wide GLEIF throttle (50/min). Nothing here raises: a failure is
returned as ``{"available": False, "reason": ...}`` and the caller records
it, so a missing log never blocks a watch or an entry.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx

from .config import get_settings
from .gleif_throttle import GleifRateLimitedError
from .http import build_client

log = logging.getLogger(__name__)

API_BASE = "https://api.gleif.org/api/v1"
PAGE_SIZE = 200
MAX_PAGES = 3
#: Lines kept per log. A record whose log runs past this is shown with a
#: count of the rest.
MAX_ITEMS = 50
#: How far back the log is read when a company is first watched.
PREWATCH_DAYS = 30

#: Trailing elements that are renewal clocks or re-keyed paperwork, not
#: changes to the company.
NOISE_ELEMENTS: frozenset[str] = frozenset({"LastUpdateDate", "NextRenewalDate", "ValidationReference"})

#: Container elements whose name the label carries ("legal address city").
_CONTAINERS: dict[str, str] = {
    "LegalAddress": "legal address",
    "HeadquartersAddress": "headquarters address",
    "OtherAddress": "other address",
    "TransliteratedOtherAddress": "transliterated address",
    "SuccessorEntity": "successor",
    "LegalEntityEvent": "legal entity event",
    "EntityExpiration": "expiration",
    "OtherEntityName": "other name",
    "TransliteratedOtherEntityName": "transliterated name",
    "OtherValidationAuthority": "other validation authority",
    "RelationshipPeriod": "relationship period",
    "Period": "period",
    "RelationshipQualifier": "relationship qualifier",
    "RelationshipQuantifier": "relationship quantifier",
}

#: Elements whose plain split-camel-case reading would mislead.
_ELEMENT_WORDS: dict[str, str] = {
    "RegistrationAuthorityID": "registration authority",
    "RegistrationAuthorityEntityID": "register number",
    "OtherRegistrationAuthorityID": "registration authority (other)",
    "ValidationAuthorityID": "validation authority",
    "ValidationAuthorityEntityID": "validation register number",
    "OtherValidationAuthorityID": "validation authority (other)",
    "EntityLegalFormCode": "legal form code",
    "OtherLegalForm": "legal form (other)",
    "ManagingLOU": "managing LOU",
    "LegalJurisdiction": "jurisdiction",
    "LEI": "LEI",
    "SuccessorLEI": "successor LEI",
}


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip().replace("Z", "+00:00")
    if "T" not in raw and " " in raw:
        raw = raw.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _elements(field: str) -> list[str]:
    """``/lei:LEIData/…/lei:Registration/lei:RegistrationStatus`` →
    ``[..., "Registration", "RegistrationStatus"]``. Tolerates the truncated
    and slash-less forms the API serves."""
    out = []
    for part in str(field or "").split("/"):
        name = part.split(":")[-1].strip()
        if name and re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", name):
            out.append(name)
    return out


def _words(element: str) -> str:
    if element in _ELEMENT_WORDS:
        return _ELEMENT_WORDS[element]
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", element).lower()


def label(field: str, record_type: str | None = None, context: dict[str, Any] | None = None) -> str:
    """A readable name for one logged field."""
    elems = _elements(field)
    if not elems:
        return str(field or "field")
    last = elems[-1]
    text = _words(last)
    for parent in reversed(elems[:-1]):
        if parent in _CONTAINERS:
            container = _CONTAINERS[parent]
            if container not in text:
                text = f"{container} {text}"
            break
    if (record_type or "").upper() == "RR":
        ctx = context or {}
        kind = "ultimate" if "ULTIMATELY" in str(ctx.get("relationshipType") or "") else "direct"
        end = ctx.get("endNode")
        text = f"{kind} parent relationship{f' ({end})' if end else ''}: {text}"
    return text


def _is_noise(field: str) -> bool:
    elems = _elements(field)
    return bool(elems) and elems[-1] in NOISE_ELEMENTS


def _clip(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    return text if len(text) <= 200 else text[:197] + "…"


def summarise(raw: list[dict[str, Any]], since: datetime | None) -> dict[str, Any]:
    """Raw API attribute dicts → the stored log: lines newer than ``since``,
    noise dropped, newest first, at most :data:`MAX_ITEMS` with a count of
    the rest."""
    items: list[dict[str, Any]] = []
    for a in raw:
        when = _parse(a.get("date"))
        if since is not None and (when is None or when <= since):
            continue
        field = a.get("field") or ""
        if _is_noise(field):
            continue
        ctx = a.get("context") if isinstance(a.get("context"), dict) else None
        item: dict[str, Any] = {
            "date": when.strftime("%Y-%m-%d") if when else None,
            "record": (a.get("recordType") or "LEI").upper(),
            "label": label(field, a.get("recordType"), ctx),
            "type": (a.get("modificationType") or "UPDATE").upper(),
            "old": _clip(a.get("valueOld")),
            "new": _clip(a.get("valueNew")),
        }
        if ctx and item["record"] == "RR":
            item["relationship"] = {"type": ctx.get("relationshipType"), "end_node": ctx.get("endNode")}
        items.append(item)
    items.sort(key=lambda i: i["date"] or "", reverse=True)
    return {
        "available": True,
        "since": since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None,
        "items": items[:MAX_ITEMS],
        "more": max(0, len(items) - MAX_ITEMS),
    }


def after(log_: dict[str, Any] | None, cutoff: datetime | None) -> dict[str, Any] | None:
    """The part of a fetched log newer than ``cutoff`` — one list's share of
    a log fetched once for every list watching the LEI."""
    if log_ is None or not log_.get("available") or cutoff is None:
        return log_
    kept = [i for i in log_.get("items") or [] if (_parse(i.get("date")) or cutoff) > cutoff]
    return {
        **log_,
        "since": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": kept,
        # Lines past the cap are older than every kept line, so whether any
        # of them is newer than this list's cut-off is not known; keep the
        # count only when nothing was cut by the filter.
        "more": log_.get("more", 0) if len(kept) == len(log_.get("items") or []) else 0,
    }


def unavailable(reason: str, since: datetime | None = None) -> dict[str, Any]:
    return {
        "available": False,
        "reason": reason,
        "since": since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None,
        "items": [],
        "more": 0,
    }


async def fetch_since(lei: str, since: datetime) -> dict[str, Any]:
    """GLEIF's log for ``lei`` newer than ``since``. Never raises."""
    if not get_settings().allow_live:
        return unavailable("live calls are switched off", since)
    url = f"{API_BASE}/lei-records/{quote(lei)}/field-modifications"
    raw: list[dict[str, Any]] = []
    try:
        async with build_client() as client:
            for page in range(1, MAX_PAGES + 1):
                response = await client.get(
                    url,
                    params={"page[size]": PAGE_SIZE, "page[number]": page, "sort": "-date"},
                    headers={"Accept": "application/vnd.api+json"},
                )
                response.raise_for_status()
                body = response.json()
                rows = [x.get("attributes") or {} for x in body.get("data") or []]
                raw.extend(rows)
                last_page = int(((body.get("meta") or {}).get("pagination") or {}).get("lastPage") or 1)
                oldest = _parse(rows[-1].get("date")) if rows else None
                if not rows or page >= last_page or (oldest is not None and oldest <= since):
                    break
    except GleifRateLimitedError:
        return unavailable("GLEIF's API was rate-limited", since)
    except httpx.HTTPStatusError as exc:
        return unavailable(f"GLEIF's API answered HTTP {exc.response.status_code}", since)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("gleif_log: field-modifications for %s failed: %s", lei, type(exc).__name__)
        return unavailable("GLEIF's API could not be reached", since)
    return summarise(raw, since)
