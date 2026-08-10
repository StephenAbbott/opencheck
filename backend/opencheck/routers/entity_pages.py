"""Routes for the SEO entity pages: /entity, /sitemaps, /browse, /robots.txt.

Serving notes (they differ from the JSON API's conventions on purpose):

* **No rate limiting.** Search crawlers are the intended audience; per-IP
  429s to Googlebot collapse the crawl rate. Every handler here is a
  sub-millisecond read over a local SQLite file — the pages are cheap by
  construction, and the expensive endpoints stay behind their own limits.
* **Long-lived caching.** ``Cache-Control: public, max-age=86400`` plus an
  ETag derived from the row's ``last_updated`` and the template version:
  content only changes when the monthly GLEIF refresh lands.
* **503 (not 404) while the DB is missing** — with ``Retry-After``, so a
  crawler that arrives during a cold start backs off instead of recording
  millions of dead URLs.
"""

from __future__ import annotations

import hashlib
import html
import math
import re
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from .. import entity_pages as ep
from .. import identifiers
from ..config import get_settings

router = APIRouter(tags=["entity-pages"])

SITEMAP_PAGE_SIZE = 50_000  # the sitemaps.org per-file URL limit
BROWSE_PAGE_SIZE = 200
_CACHE = "public, max-age=86400, stale-while-revalidate=604800"


def _frontend() -> str:
    frontend = (get_settings().frontend_origin or "").rstrip("/")
    return frontend if frontend.startswith("http") else "https://opencheck.world"


def _api_base() -> str:
    return (get_settings().public_api_base or "https://api.opencheck.world").rstrip("/")


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _publish_date(store: ep.EntityStore) -> str:
    """The Golden Copy publish date (YYYY-MM-DD), or "" when unknown —
    a DB built from local files carries no publish date, and an empty
    value must suppress <lastmod>/footer output rather than leak a
    placeholder string into sitemaps."""
    value = (store.meta().get("source_publish_date") or "")[:10]
    return value if _DATE_RE.match(value) else ""


def _store_or_503() -> ep.EntityStore:
    store = ep.get_store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Entity pages dataset not loaded.",
            headers={"Retry-After": "600"},
        )
    return store


def _html_503() -> HTMLResponse:
    return HTMLResponse(
        ep.render_unavailable(_frontend()),
        status_code=503,
        headers={"Retry-After": "600", "Cache-Control": "no-store"},
    )


def _etag(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]
    return f'W/"{digest}"'


def _conditional(request: Request, response_etag: str) -> bool:
    """True when the client's If-None-Match matches (→ 304)."""
    inm = request.headers.get("if-none-match")
    return inm is not None and response_etag in inm


# ---------------------------------------------------------------------------
# Entity pages
# ---------------------------------------------------------------------------


@router.api_route("/entity/{token}", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def entity_page(request: Request, token: str) -> Response:
    """One page per LEI: ``/entity/{LEI}-{slug}`` (slug optional but canonical).

    Anything after the 20-character LEI is treated as a slug: a stale or
    missing slug 301s to the canonical path, so renamed entities keep their
    old URLs working and search engines converge on one URL per entity.
    """
    store = ep.get_store()
    if store is None:
        return _html_503()

    lei = token[:20].upper()
    if not identifiers.LEI_STRICT_SHAPE.match(lei) or identifiers.lei_check_digit_error(lei):
        return HTMLResponse(
            ep.render_not_found(_frontend(), token),
            status_code=404,
            headers={"Cache-Control": "public, max-age=3600"},
        )

    row = store.get(lei)
    if row is None:
        return HTMLResponse(
            ep.render_not_found(_frontend(), token),
            status_code=404,
            headers={"Cache-Control": "public, max-age=3600"},
        )

    canonical_token = f"{lei}-{row.slug}" if row.slug else lei
    if token != canonical_token:
        return RedirectResponse(
            url=f"/entity/{quote(canonical_token)}",
            status_code=301,
            headers={"Cache-Control": "public, max-age=86400"},
        )

    etag = _etag("entity", lei, row.last_updated or "", ep.TEMPLATE_VERSION)
    if _conditional(request, etag):
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": _CACHE})

    related = store.get_many(
        [row.direct_parent_lei, row.ultimate_parent_lei, row.successor_lei]
    )
    children, children_total = store.children(lei)
    page = ep.render_entity_page(
        row,
        frontend=_frontend(),
        api_base=_api_base(),
        related=related,
        children=children,
        children_total=children_total,
        built=_publish_date(store) or None,
    )
    return HTMLResponse(page, headers={"ETag": etag, "Cache-Control": _CACHE})


# ---------------------------------------------------------------------------
# Sitemaps
# ---------------------------------------------------------------------------


@router.api_route("/sitemaps/sitemap-index.xml", methods=["GET", "HEAD"])
async def sitemap_index(request: Request) -> Response:
    store = _store_or_503()
    total = store.count()
    shards = max(1, math.ceil(total / SITEMAP_PAGE_SIZE))
    frontend = _frontend()
    lastmod = _publish_date(store)
    lastmod_xml = f"<lastmod>{lastmod}</lastmod>" if lastmod else ""
    etag = _etag("smi", str(total), lastmod, ep.TEMPLATE_VERSION)
    if _conditional(request, etag):
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": _CACHE})
    body = "".join(
        f"<sitemap><loc>{frontend}/sitemaps/entities-{i}.xml</loc>{lastmod_xml}</sitemap>"
        for i in range(1, shards + 1)
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</sitemapindex>"
    )
    return Response(
        content=xml,
        media_type="application/xml",
        headers={"ETag": etag, "Cache-Control": _CACHE},
    )


@router.api_route("/sitemaps/entities-{shard}.xml", methods=["GET", "HEAD"])
async def sitemap_shard(request: Request, shard: int) -> Response:
    store = _store_or_503()
    total = store.count()
    shards = max(1, math.ceil(total / SITEMAP_PAGE_SIZE))
    if shard < 1 or shard > shards:
        raise HTTPException(status_code=404, detail="No such sitemap shard.")
    frontend = _frontend()
    rows = store.sitemap_slice((shard - 1) * SITEMAP_PAGE_SIZE, SITEMAP_PAGE_SIZE)
    etag = _etag(
        "shard",
        str(shard),
        str(total),
        _publish_date(store),
        ep.TEMPLATE_VERSION,
    )
    if _conditional(request, etag):
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": _CACHE})

    def loc(lei: str, slug: str) -> str:
        token = f"{lei}-{slug}" if slug else lei
        return f"{frontend}/entity/{quote(token)}"

    body = "".join(
        "<url>"
        f"<loc>{loc(lei, slug)}</loc>"
        + (f"<lastmod>{updated[:10]}</lastmod>" if updated else "")
        + "</url>"
        for lei, slug, updated in rows
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    )
    return Response(
        content=xml,
        media_type="application/xml",
        headers={"ETag": etag, "Cache-Control": _CACHE},
    )


# ---------------------------------------------------------------------------
# Browse hub — crawlable internal-link paths (sitemap-only discovery is weak)
# ---------------------------------------------------------------------------


@router.api_route("/browse", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def browse_index(request: Request) -> Response:
    store = ep.get_store()
    if store is None:
        return _html_503()
    frontend = _frontend()
    countries = store.countries()
    etag = _etag("browse", str(len(countries)), str(store.count()), ep.TEMPLATE_VERSION)
    if _conditional(request, etag):
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": _CACHE})
    items = "".join(
        f'<li><a href="/browse/{code}">{html.escape(ep.country_name(code) or code)}</a>'
        f" ({n:,})</li>"
        for code, n in countries
    )
    head = ep._head(
        analytics_bucket="/browse",
        title="Browse entities by country - OpenCheck",
        description=(
            "Every legal entity with a Legal Entity Identifier, by country of "
            "registration — open-source customer due diligence risk checks "
            "powered by open data and open standards"
        ),
        canonical=f"{frontend}/browse",
    )
    page = f"""{head}
<body>
{ep._header_html(frontend)}
<main>
<h1>Browse entities by country</h1>
<p>Entities carrying a Legal Entity Identifier, grouped by the country of
their registered legal address. Reference data: GLEIF Golden Copy.</p>
<ul class="entities">{items}</ul>
</main>
{ep._footer_html(frontend, _publish_date(store) or None)}
</body>
</html>"""
    return HTMLResponse(page, headers={"ETag": etag, "Cache-Control": _CACHE})


@router.api_route("/browse/{country}", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def browse_country(request: Request, country: str, page: int = 1) -> Response:
    store = ep.get_store()
    if store is None:
        return _html_503()
    country = country.upper()
    if len(country) != 2 or not country.isalpha():
        raise HTTPException(status_code=404, detail="Unknown country code.")
    if page < 1:
        raise HTTPException(status_code=404, detail="No such page.")
    frontend = _frontend()
    rows, total = store.browse(country, (page - 1) * BROWSE_PAGE_SIZE, BROWSE_PAGE_SIZE)
    if total == 0 or (page > 1 and not rows):
        raise HTTPException(status_code=404, detail="No entities for this page.")
    pages = max(1, math.ceil(total / BROWSE_PAGE_SIZE))
    name = ep.country_name(country) or country
    suffix = f" (page {page} of {pages:,})" if pages > 1 else ""
    canonical = f"{frontend}/browse/{country}" + (f"?page={page}" if page > 1 else "")
    etag = _etag("browse", country, str(page), str(total), ep.TEMPLATE_VERSION)
    if _conditional(request, etag):
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": _CACHE})

    items = "".join(
        f'<li><a href="{html.escape(r.path)}">{html.escape(r.name)}</a></li>' for r in rows
    )
    prev_link = (
        f'<a rel="prev" href="/browse/{country}'
        + (f"?page={page - 1}" if page > 2 else "")
        + '">&larr; Previous</a>'
        if page > 1
        else ""
    )
    next_link = (
        f'<a rel="next" href="/browse/{country}?page={page + 1}">Next &rarr;</a>'
        if page < pages
        else ""
    )
    head = ep._head(
        analytics_bucket="/browse",
        title=f"Entities registered in {name}{suffix} - OpenCheck",
        description=(
            f"Legal entities registered in {name} with a Legal Entity Identifier "
            "— open-source customer due diligence risk checks powered by open "
            "data and open standards"
        ),
        canonical=canonical,
    )
    body_page = f"""{head}
<body>
{ep._header_html(frontend)}
<main>
<h1>Entities registered in {html.escape(name)}{html.escape(suffix)}</h1>
<p>{total:,} entities with a Legal Entity Identifier have their registered
legal address in {html.escape(name)}. Each links to an OpenCheck entity page
with GLEIF reference data and one-click live due diligence.</p>
<nav class="pages">{prev_link}{next_link}</nav>
<ul class="entities">{items}</ul>
<nav class="pages">{prev_link}{next_link}</nav>
<p><a href="/browse">All countries</a></p>
</main>
{ep._footer_html(frontend, _publish_date(store) or None)}
</body>
</html>"""
    return HTMLResponse(body_page, headers={"ETag": etag, "Cache-Control": _CACHE})


# ---------------------------------------------------------------------------
# robots.txt — served by the backend so the crawl policy travels with the
# pages it protects, whichever host they're on.
# ---------------------------------------------------------------------------

_DISALLOWED_API_PREFIXES = [
    "/lookup",
    "/lookup-stream",
    "/lookup-source",
    "/search",
    "/stream",
    "/deepen",
    "/expand",
    "/export",
    "/report",
    "/history",
    "/securities",
    "/subsidiaries",
    "/nz-associations",
    "/person-check",
    "/person-appointments",
    "/narrative",
    "/license-matrix",
    "/share",
    "/mcp",
    # NOTE: /memstats is deliberately NOT listed — it's the public aggregate
    # stats endpoint, and the weekly Cowork monitoring routine reads it with a
    # robots.txt-respecting fetcher. Nothing crawlable of value lives there.
]


@router.api_route("/robots.txt", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def robots_txt() -> PlainTextResponse:
    """Allow the reference pages; keep crawlers off the live-lookup API.

    A bot crawling ``/lookup?lei=…`` URLs would trigger the full adapter
    fan-out against rate-limited third-party APIs — the one thing the
    entity-pages design exists to avoid.
    """
    frontend = _frontend()
    lines = ["User-agent: *"]
    lines += [f"Disallow: {p}" for p in _DISALLOWED_API_PREFIXES]
    lines += [
        "Allow: /entity/",
        "Allow: /browse",
        "Allow: /sitemaps/",
        "",
        f"Sitemap: {frontend}/sitemaps/sitemap-index.xml",
        "",
    ]
    return PlainTextResponse(
        "\n".join(lines), headers={"Cache-Control": "public, max-age=86400"}
    )


# ---------------------------------------------------------------------------
# IndexNow key file (Phase 91 — SEO Phase D)
# ---------------------------------------------------------------------------


@router.api_route("/indexnow/{key}.txt", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def indexnow_key(key: str) -> PlainTextResponse:
    """Prove ownership of the host to IndexNow-participating engines.

    The monthly refresh submits changed entity URLs to api.indexnow.org
    (scripts/submit_indexnow.py) with keyLocation pointing here; engines
    fetch it and expect the body to equal the key. 404 for anything but
    the configured key, and for everything while no key is configured.
    """
    configured = (get_settings().indexnow_key or "").strip()
    if not configured or key != configured:
        raise HTTPException(status_code=404, detail="Unknown key.")
    return PlainTextResponse(configured, headers={"Cache-Control": "public, max-age=86400"})
