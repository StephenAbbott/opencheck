"""The looked-up company's primary stock-exchange listing, from LSEG PermID.

GLEIF publishes an LEI → ISIN list and nothing about where a security trades
(Shell carries 1,814 ISINs, mostly debt, none marked as the share). PermID
publishes the missing link: an organisation record names the company's LEI
and its **primary quote**, and the quote names the ticker, the RIC and the
venue's ISO 10383 MIC. Three requests take an LEI to a listing:

1. ``GET api-eit.refinitiv.com/permid/search?q=lei:{LEI}`` → organisation id
2. ``GET permid.org/1-{org}?format=json-ld`` → ``tr-org:hasLEI`` (re-checked
   here — the search is a text search) and ``hasOrganizationPrimaryQuote``
3. ``GET permid.org/1-{quote}?format=json-ld`` → ``tr-fin:hasMic``,
   ``tr-fin:hasExchangeTicker``, ``tr-fin:hasRic``

Decisions (Stephen, 24 Sept 2026 — the PermID Notion ticket):

- **One ``listing`` lookup event**, yielded before ``subject_profile`` and
  folded into ``LookupResponse.listing`` *as recorded*: a saved report, the
  exports and MCP read the frozen payload and never re-fetch. Not a registered
  source; it does not count in coverage.
- **"Primary listing", attributed to PermID.** PermID names one primary quote;
  other listings (Shell on NYSE) are not shown, and the wording must not imply
  they do not exist. For Petrobras the primary quote is the *preference* share,
  which is why the security's own name is carried.
- **No key, no line.** Without ``PERMID_API_KEY`` (or with live calls off) the
  event is never emitted — absence is never "not listed".
- **A failure is said on the line itself** (Stephen, 24 Sept 2026): the event
  carries ``status: "unavailable"`` and the line reads "could not be checked".
  It is deliberately *not* a ``DegradedSource``: everything that reads that
  list — the verdict, the MCP caution, the batch chip — treats an entry as a
  screen that did not run, and a listing lookup is not a screen. Never
  cached, so the next lookup tries again.
- **Links only where the pattern was browser-verified** (24 Sept 2026): LSE,
  Nasdaq US, NYSE, TSX, ASX, Nasdaq Copenhagen/Stockholm and NGX, plus
  Euronext's ticker *search* page. Every other venue shows name and ticker
  with no link. These are listing pages, never filings.
- **BODS:** ``publicListing.securitiesListings`` on the subject's GLEIF entity
  statement, with a ``linking`` annotation naming PermID. PermID is not in the
  closed ``securitiesIdentifierSchemes`` codelist (isin, figi, cusip, cins), so
  ``security`` carries the ticker only; ``companyFilingsURLs`` stays empty.
- **Cache:** 30 days for a listing, 7 for "no primary quote", so an IPO shows
  within a week. Listings rarely change and the key allows 5,000 requests a
  day at 1–4 a second.

Licence: PermID's basic fields are CC-BY 4.0 via the API for registered users
("API keys may be used for any purpose, including commercial applications").
The access token travels in the query string, so every error goes through
``secret_scrub`` — never ``str(exc)``.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable
from urllib.parse import quote

import httpx

from .cache import Cache
from .config import get_settings
from .secret_scrub import describe_exception

logger = logging.getLogger(__name__)

SOURCE_ID = "permid"
ATTRIBUTION = "LSEG PermID (CC-BY 4.0)"
LICENCE = "CC-BY-4.0"

_SEARCH_URL = "https://api-eit.refinitiv.com/permid/search"
_RECORD_URL = "https://permid.org/1-{id}"
_PERMID_ID = re.compile(r"^[0-9]{6,15}$")

#: Days a cached answer is trusted.
LISTED_TTL_DAYS = 30.0
UNLISTED_TTL_DAYS = 7.0
_CACHE_NS = "permid/listing"

_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
#: Wall-clock cap on the whole three-request chain.
_BUDGET_S = 25.0


# ---------------------------------------------------------------------------
# Venues — name, the jurisdiction BODS asks for, and the verified link rule
# ---------------------------------------------------------------------------


def _lower(t: str) -> str:
    return quote(t.strip().lower(), safe="")


def _upper(t: str) -> str:
    return quote(t.strip().upper(), safe="")


def _nordic(t: str) -> str:
    # "NOVO B" → "novo-b"; "VOLCAR B" → "volcar-b"
    return quote("-".join(t.strip().lower().split()), safe="-")


_LSE = lambda t: f"https://www.londonstockexchange.com/stock/{_upper(t)}/x/company-page"  # noqa: E731
_NASDAQ_US = lambda t: f"https://www.nasdaq.com/market-activity/stocks/{_lower(t)}"  # noqa: E731
_NYSE = lambda t: f"https://www.nyse.com/quote/XNYS:{_upper(t)}"  # noqa: E731
_TSX = lambda t: f"https://money.tmx.com/en/quote/{_upper(t)}"  # noqa: E731
_ASX = lambda t: f"https://www.asx.com.au/markets/company/{_upper(t)}"  # noqa: E731
_NASDAQ_NORDIC = lambda t: f"https://www.nasdaq.com/european-market-activity/shares/{_nordic(t)}"  # noqa: E731
_NGX = lambda t: (  # noqa: E731
    f"https://ngxgroup.com/exchange/data/company-profile/?symbol={_upper(t)}&directory=companydirectory"
)
_EURONEXT_SEARCH = lambda t: f"https://live.euronext.com/en/search_instruments/{_upper(t)}"  # noqa: E731


@dataclass(frozen=True)
class Venue:
    name: str
    #: ISO 3166-1 alpha-2 of the jurisdiction the venue is regulated under —
    #: BODS ``stockExchangeJurisdiction`` (required).
    country: str
    #: Builds the venue page from a ticker; None = no verified pattern.
    link: Callable[[str], str] | None = None
    #: "listing" (the company's own page) or "search" (a results page).
    link_kind: str = "listing"


_EURONEXT = "search"

#: Keyed on the MIC PermID publishes (``tr-fin:hasMic``). A MIC missing from
#: this table still shows — as its code — with no link and no BODS listing.
VENUES: dict[str, Venue] = {
    # Verified ticker-based pages (24 Sept 2026).
    "XLON": Venue("London Stock Exchange", "GB", _LSE),
    "XNGS": Venue("Nasdaq Global Select Market", "US", _NASDAQ_US),
    "XNMS": Venue("Nasdaq Global Market", "US", _NASDAQ_US),
    "XNCM": Venue("Nasdaq Capital Market", "US", _NASDAQ_US),
    "XNAS": Venue("Nasdaq", "US", _NASDAQ_US),
    "XNYS": Venue("New York Stock Exchange", "US", _NYSE),
    "XTSE": Venue("Toronto Stock Exchange", "CA", _TSX),
    "XASX": Venue("ASX", "AU", _ASX),
    "XCSE": Venue("Nasdaq Copenhagen", "DK", _NASDAQ_NORDIC),
    "XSTO": Venue("Nasdaq Stockholm", "SE", _NASDAQ_NORDIC),
    "XNSA": Venue("Nigerian Exchange", "NG", _NGX),
    # Euronext: the ticker search page (the direct page needs an ISIN,
    # which PermID does not publish).
    "XAMS": Venue("Euronext Amsterdam", "NL", _EURONEXT_SEARCH, _EURONEXT),
    "XPAR": Venue("Euronext Paris", "FR", _EURONEXT_SEARCH, _EURONEXT),
    "XBRU": Venue("Euronext Brussels", "BE", _EURONEXT_SEARCH, _EURONEXT),
    "XLIS": Venue("Euronext Lisbon", "PT", _EURONEXT_SEARCH, _EURONEXT),
    "XMSM": Venue("Euronext Dublin", "IE", _EURONEXT_SEARCH, _EURONEXT),
    "XOSL": Venue("Euronext Oslo Børs", "NO", _EURONEXT_SEARCH, _EURONEXT),
    "XMIL": Venue("Euronext Milan", "IT", _EURONEXT_SEARCH, _EURONEXT),
    # Named, no verified link yet.
    "XETR": Venue("Xetra", "DE"),
    "XFRA": Venue("Frankfurt Stock Exchange", "DE"),
    "XSWX": Venue("SIX Swiss Exchange", "CH"),
    "XHEL": Venue("Nasdaq Helsinki", "FI"),
    "XICE": Venue("Nasdaq Iceland", "IS"),
    "XWBO": Venue("Vienna Stock Exchange", "AT"),
    "XWAR": Venue("Warsaw Stock Exchange", "PL"),
    "XMAD": Venue("BME Spanish Exchanges", "ES"),
    "XASE": Venue("NYSE American", "US"),
    "ARCX": Venue("NYSE Arca", "US"),
    "XTKS": Venue("Tokyo Stock Exchange", "JP"),
    "XNSE": Venue("National Stock Exchange of India", "IN"),
    "XBOM": Venue("BSE", "IN"),
    "XHKG": Venue("Hong Kong Exchanges and Clearing", "HK"),
    "XSES": Venue("Singapore Exchange", "SG"),
    "XKRX": Venue("Korea Exchange", "KR"),
    "XSHG": Venue("Shanghai Stock Exchange", "CN"),
    "XSHE": Venue("Shenzhen Stock Exchange", "CN"),
    "XTAI": Venue("Taiwan Stock Exchange", "TW"),
    "XNZE": Venue("NZX", "NZ"),
    "XJSE": Venue("Johannesburg Stock Exchange", "ZA"),
    "BVMF": Venue("B3", "BR"),
    "XMEX": Venue("Mexican Stock Exchange", "MX"),
    "XSAU": Venue("Saudi Exchange", "SA"),
    "XIST": Venue("Borsa Istanbul", "TR"),
    "XGHA": Venue("Ghana Stock Exchange", "GH"),
    "XNAI": Venue("Nairobi Securities Exchange", "KE"),
}


def venue_link(mic: str | None, ticker: str | None) -> dict[str, str] | None:
    """``{url, kind}`` for a verified venue and a ticker, else None."""
    if not mic or not ticker or not ticker.strip():
        return None
    v = VENUES.get(mic.upper())
    if v is None or v.link is None:
        return None
    return {"url": v.link(ticker), "kind": v.link_kind}


# ---------------------------------------------------------------------------
# PermID
# ---------------------------------------------------------------------------


class _PermIdUnavailable(Exception):
    """A request failed; ``detail`` is safe to show (secret-scrubbed)."""

    def __init__(self, detail: str, reason: str) -> None:
        super().__init__(detail)
        self.detail = detail
        self.reason = reason


def _id_from_uri(uri: Any) -> str | None:
    if not isinstance(uri, str):
        return None
    tail = uri.rstrip("/").rsplit("/", 1)[-1]
    tail = tail[2:] if tail.startswith("1-") else tail
    return tail if _PERMID_ID.match(tail) else None


def _str(v: Any) -> str | None:
    if isinstance(v, list):
        v = next((x for x in v if x), None)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


REASON_RATE_LIMITED = "rate_limited"
REASON_TIMEOUT = "timeout"
REASON_UPSTREAM_ERROR = "upstream_error"


def _reason(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        return REASON_RATE_LIMITED
    if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
        return REASON_TIMEOUT
    return REASON_UPSTREAM_ERROR


async def _get(client: httpx.AsyncClient, url: str, params: dict[str, str], accept: str) -> Any:
    try:
        r = await client.get(url, params=params, headers={"Accept": accept})
        r.raise_for_status()
        return r.json()
    except Exception as exc:  # noqa: BLE001 — described, never stringified
        raise _PermIdUnavailable(describe_exception(exc), _reason(exc)) from None


def _build_client() -> httpx.AsyncClient:
    # permid.org sits behind Cloudflare, which refuses a generic library
    # User-Agent (error 1010) — the named OpenCheck agent is accepted.
    from .http import _USER_AGENT

    return httpx.AsyncClient(
        timeout=_TIMEOUT,
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
    )


async def _fetch_from_permid(lei: str, token: str, today: date) -> dict[str, Any]:
    tok = {"access-token": token}
    async with _build_client() as client:
        found = await _get(
            client, _SEARCH_URL, {"q": f"lei:{lei}", "entityType": "organization", "num": "3", **tok},
            "application/json",
        )
        orgs = (((found or {}).get("result") or {}).get("organizations") or {}).get("entities") or []
        for entity in orgs[:3]:
            org_id = _id_from_uri(entity.get("@id"))
            if not org_id:
                continue
            org = await _get(
                client, _RECORD_URL.format(id=org_id), {"format": "json-ld", **tok},
                "application/ld+json",
            )
            # The search is a text search: the organisation must carry this
            # LEI itself before anything it says is attached to the subject.
            if (_str(org.get("tr-org:hasLEI") or org.get("hasLEI")) or "").upper() != lei:
                continue
            payload = _unlisted(lei, today, org_id)
            quote_id = _id_from_uri(org.get("hasOrganizationPrimaryQuote"))
            if not quote_id:
                return payload
            q = await _get(
                client, _RECORD_URL.format(id=quote_id), {"format": "json-ld", **tok},
                "application/ld+json",
            )
            return _listed(payload, quote_id, q, _id_from_uri(org.get("hasPrimaryInstrument")))
    return _unlisted(lei, today, None)


def _unlisted(lei: str, today: date, org_id: str | None) -> dict[str, Any]:
    return {
        "source_id": SOURCE_ID,
        "attribution": ATTRIBUTION,
        "licence": LICENCE,
        "lei": lei,
        "status": "not_listed",
        "as_of": today.isoformat(),
        "organisation": (
            {"permid": org_id, "url": _RECORD_URL.format(id=org_id)} if org_id else None
        ),
        "quote": None,
        "exchange": None,
        "link": None,
    }


def _listed(
    base: dict[str, Any], quote_id: str, q: dict[str, Any], instrument_id: str | None
) -> dict[str, Any]:
    mic = (_str(q.get("tr-fin:hasMic") or q.get("hasMic")) or "").upper() or None
    ticker = _str(q.get("tr-fin:hasExchangeTicker") or q.get("hasExchangeTicker"))
    venue = VENUES.get(mic or "")
    out = dict(base)
    out.update(
        status="listed",
        quote={
            "permid": quote_id,
            "url": _RECORD_URL.format(id=quote_id),
            "name": _str(q.get("tr-common:hasName") or q.get("hasName")),
            "ticker": ticker,
            "mic": mic,
            "ric": _str(q.get("tr-fin:hasRic") or q.get("hasRic")),
            "exchange_code": _str(q.get("tr-fin:hasExchangeCode") or q.get("hasExchangeCode")),
            "instrument_permid": instrument_id,
        },
        exchange={"name": venue.name, "country": venue.country} if venue else None,
        link=venue_link(mic, ticker),
    )
    return out


def available() -> bool:
    """A key is configured and live calls are on."""
    s = get_settings()
    return bool(s.allow_live and (s.permid_api_key or "").strip())


async def fetch_listing(lei: str, today: date | None = None) -> dict[str, Any] | None:
    """The ``listing`` event payload for *lei*, or None.

    None only when the key is not configured (or live calls are off): there is
    then nothing to say. A PermID failure returns ``status: "unavailable"``
    with the reason, so the line can say the listing could not be checked.
    """
    if not available():
        return None
    lei = lei.strip().upper()
    cache = Cache()
    key = f"{_CACHE_NS}/{lei}"
    hit = cache.get_payload(key, max_age_days=LISTED_TTL_DAYS)
    if hit is not None:
        cached, _tier = hit
        if isinstance(cached, dict) and cached.get("status") == "listed":
            return cached
        if isinstance(cached, dict) and cached.get("status") == "not_listed":
            as_of = cached.get("as_of") or ""
            try:
                age = (date.today() - date.fromisoformat(as_of)).days
            except ValueError:
                age = UNLISTED_TTL_DAYS + 1
            if age <= UNLISTED_TTL_DAYS:
                return cached
    today = today or datetime.now(timezone.utc).date()
    token = (get_settings().permid_api_key or "").strip()
    try:
        payload = await asyncio.wait_for(_fetch_from_permid(lei, token, today), _BUDGET_S)
    except _PermIdUnavailable as exc:
        return unavailable(lei, today, exc.reason, exc.detail)
    except asyncio.TimeoutError:
        return unavailable(lei, today, REASON_TIMEOUT, "PermID did not answer within the time budget.")
    except Exception as exc:  # noqa: BLE001 — a listing is never worth a failed lookup
        logger.warning("permid listing failed for %s: %s", lei, describe_exception(exc))
        return unavailable(lei, today, REASON_UPSTREAM_ERROR, describe_exception(exc))
    try:
        cache.put(key, payload)
    except OSError:  # pragma: no cover — read-only cache dir
        pass
    return payload


def unavailable(lei: str, today: date, reason: str, detail: str) -> dict[str, Any]:
    """The payload for "PermID was asked and did not answer". ``detail`` is
    already secret-scrubbed (``describe_exception``); it names a status and a
    host, never a URL."""
    out = _unlisted(lei, today, None)
    out.update(status="unavailable", reason=reason, detail=detail)
    return out


# ---------------------------------------------------------------------------
# BODS
# ---------------------------------------------------------------------------


def _is_subject(stmt: dict[str, Any], lei: str) -> bool:
    if stmt.get("recordType") != "entity":
        return False
    for ident in (stmt.get("recordDetails") or {}).get("identifiers") or []:
        if (
            isinstance(ident, dict)
            and ident.get("scheme") == "XI-LEI"
            and str(ident.get("id") or "").upper() == lei
        ):
            return True
    return False


def securities_listing(listing: dict[str, Any] | None) -> dict[str, Any] | None:
    """The BODS ``SecuritiesListing`` for a payload, or None when the venue
    or ticker is unknown (``stockExchangeJurisdiction``, ``stockExchangeName``
    and ``security.ticker`` are all required)."""
    if not listing or listing.get("status") != "listed":
        return None
    q = listing.get("quote") or {}
    ex = listing.get("exchange") or {}
    if not (q.get("ticker") and ex.get("name") and ex.get("country")):
        return None
    out: dict[str, Any] = {
        "stockExchangeJurisdiction": ex["country"],
        "stockExchangeName": ex["name"],
        "security": {"ticker": q["ticker"]},
    }
    if q.get("mic"):
        out["marketIdentifierCode"] = q["mic"]
    return out


def apply_to_bods(
    bods: list[dict[str, Any]], lei: str | None, listing: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Return *bods* with the primary listing on the subject's GLEIF entity
    statement — copied, never mutated in place (the statements are also the
    stored event payloads of a saved report).

    One statement only: the GLEIF one when present (the anchor every lookup
    has), else the first entity carrying the LEI. The ``linking`` annotation
    names PermID, so the field is never read as something GLEIF published.
    """
    sl = securities_listing(listing)
    if not sl or not lei:
        return bods
    lei = lei.strip().upper()
    candidates = [i for i, s in enumerate(bods) if _is_subject(s, lei)]
    if not candidates:
        return bods

    def _src(i: int) -> str:
        from .consistency import source_id_of

        try:
            return source_id_of(bods[i]) or ""
        except Exception:  # noqa: BLE001
            return ""

    idx = next((i for i in candidates if _src(i) == "gleif"), candidates[0])
    stmt = copy.deepcopy(bods[idx])
    rd = stmt.setdefault("recordDetails", {})
    rd["publicListing"] = {"hasPublicListing": True, "securitiesListings": [sl]}
    quote_url = ((listing or {}).get("quote") or {}).get("url")
    ann: dict[str, Any] = {
        "statementPointerTarget": "/recordDetails/publicListing",
        "motivation": "linking",
        "description": (
            "Added by OpenCheck from LSEG PermID (CC-BY 4.0): the organisation's "
            "primary quote. Other listings of the same company are not shown."
        ),
        "createdBy": {"name": "OpenCheck", "uri": "https://opencheck.world"},
    }
    if quote_url:
        ann["url"] = quote_url
    if (listing or {}).get("as_of"):
        ann["creationDate"] = listing["as_of"]
    stmt["annotations"] = [*(stmt.get("annotations") or []), ann]
    out = list(bods)
    out[idx] = stmt
    return out


UNAVAILABLE_LINE = "Could not be checked — PermID did not answer"


def describe(listing: dict[str, Any] | None) -> str | None:
    """"London Stock Exchange · SHEL" — the one line every surface shows —
    or the could-not-check sentence, or None when there is nothing to say (no
    payload, or PermID records no primary quote)."""
    if listing and listing.get("status") == "unavailable":
        return UNAVAILABLE_LINE
    if not listing or listing.get("status") != "listed":
        return None
    q = listing.get("quote") or {}
    ex = listing.get("exchange") or {}
    venue = ex.get("name") or (f"MIC {q['mic']}" if q.get("mic") else None)
    bits = [b for b in (venue, q.get("ticker")) if b]
    return " · ".join(bits) or None
