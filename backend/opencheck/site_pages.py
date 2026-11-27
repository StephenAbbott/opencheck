"""The SPA's own pages, as search engines and link previews read them.

The React app is one ``index.html`` shell for every client-routed view, so
until this module every one of them reached a crawler as the homepage: title
"OpenCheck", the homepage's description and ``<link rel="canonical"
href="https://opencheck.world/">``. Google folded /features, /about, /api,
/sources and /changelog into ``/``, and no sitemap robots.txt names listed
them.

``render_shell`` rewrites the shell's ``<head>`` for a known path (title,
description, canonical, Open Graph and Twitter tags); the browser still runs
the same app, which sets ``document.title`` itself. ``PAGES`` is also the
site sitemap (``/sitemap.xml``) and the page list in ``/llms.txt``.

The titles are the frontend's ``VIEW_DOCUMENT_TITLES`` (``lib/views.ts``);
``tests/test_site_pages.py`` parses that file and fails if they drift.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SitePage:
    path: str
    title: str
    description: str


#: Every client-routed page a crawler should find on its own. A report
#: (``?lei=``, ``/report/{id}``) is not a page here: it runs live lookups.
PAGES: tuple[SitePage, ...] = (
    SitePage(
        "/features",
        "Features — OpenCheck",
        "What OpenCheck can do: ownership and control checks, risk signals, "
        "people screening, subsidiaries, company history, list screening and a "
        "watchlist, all from open registers and datasets.",
    ),
    SitePage(
        "/sources",
        "Sources — OpenCheck",
        "The open registers and datasets OpenCheck reads, with each source's "
        "licence, coverage and latest health check.",
    ),
    SitePage(
        "/api",
        "API — OpenCheck",
        "The OpenCheck API and MCP server: check who owns and controls a "
        "company by LEI or register number, and export the result as BODS v0.4.",
    ),
    SitePage(
        "/about",
        "About — OpenCheck",
        "How OpenCheck works: the open data it reads, the Beneficial Ownership "
        "Data Standard, LEI anchoring, and how every result is dated and "
        "attributed.",
    ),
    SitePage(
        "/changelog",
        "Changelog — OpenCheck",
        "OpenCheck's development history, phase by phase.",
    ),
    SitePage(
        "/batch",
        "Screen a list — OpenCheck",
        "Screen a list of companies by LEI with OpenCheck and download the "
        "results as CSV.",
    ),
    SitePage(
        "/watchlist",
        "Watchlist — OpenCheck",
        "Watch companies with OpenCheck and see when their GLEIF record, "
        "sanctions listings or register entries change.",
    ),
    SitePage(
        "/changes",
        "Daily changes — OpenCheck",
        "What GLEIF and OpenSanctions changed today for companies with a "
        "Legal Entity Identifier, with a daily BODS file to download.",
    ),
)

_BY_PATH = {p.path: p for p in PAGES}


def page_for(path: str) -> SitePage | None:
    """The page served at ``path`` (``/features`` or ``features``), if any."""
    norm = "/" + path.strip("/")
    return _BY_PATH.get(norm)


def _sub(pattern: str, value: str, text: str) -> str:
    """Replace group 2 of ``pattern`` (the attribute value) with ``value``."""
    return re.sub(pattern, lambda m: m.group(1) + value + m.group(3), text, count=1)


#: (regex, field) — each regex captures the text before the value, the
#: value, and the text after it. A tag the shell does not carry is skipped.
_HEAD_TAGS: tuple[tuple[str, str], ...] = (
    (r"(<title>)([^<]*)(</title>)", "title"),
    (r'(<meta name="description" content=")([^"]*)(")', "description"),
    (r'(<link rel="canonical" href=")([^"]*)(")', "url"),
    (r'(<meta property="og:url" content=")([^"]*)(")', "url"),
    (r'(<meta property="og:title" content=")([^"]*)(")', "title"),
    (r'(<meta property="og:description" content=")([^"]*)(")', "description"),
    (r'(<meta name="twitter:title" content=")([^"]*)(")', "title"),
    (r'(<meta name="twitter:description" content=")([^"]*)(")', "description"),
)


def render_shell(index_html: str, path: str, frontend: str) -> str | None:
    """The SPA shell with ``path``'s own head, or None for any other path."""
    page = page_for(path)
    if page is None:
        return None
    values = {
        "title": html.escape(page.title, quote=True),
        "description": html.escape(page.description, quote=True),
        "url": html.escape(frontend.rstrip("/") + page.path, quote=True),
    }
    out = index_html
    for pattern, field in _HEAD_TAGS:
        out = _sub(pattern, values[field], out)
    return out


def sitemap_xml(frontend: str) -> str:
    """The homepage and every page in ``PAGES``, as one urlset."""
    base = frontend.rstrip("/")
    urls = [f"{base}/"] + [base + p.path for p in PAGES] + [f"{base}/blog"]
    body = "".join(f"<url><loc>{html.escape(u)}</loc></url>" for u in urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    )


def llms_txt(frontend: str, api: str) -> str:
    """``/llms.txt``: what OpenCheck is and where an agent should look.

    States no counts (they go stale) and makes no claim about any company.
    """
    base = frontend.rstrip("/")
    api = api.rstrip("/")
    lines = [
        "# OpenCheck",
        "",
        "> OpenCheck is a free, open-source tool that shows who owns and "
        "controls a company, and the risk signals around it, by bringing "
        "together open registers and datasets through its Legal Entity "
        "Identifier (LEI). Results are mapped to the Beneficial Ownership Data "
        "Standard (BODS) v0.4, and every statement names its source, licence "
        "and date.",
        "",
        "A check runs live against the upstream sources, so please use the API "
        "or the MCP server rather than crawling report pages. The live lookup "
        "paths are disallowed in robots.txt and rate-limited per address.",
        "",
        "## Pages",
        "",
        f"- [Home]({base}/): search a company by name, LEI or registration number",
    ]
    lines += [f"- [{p.title.removesuffix(' — OpenCheck')}]({base}{p.path}): {p.description}" for p in PAGES]
    lines += [
        f"- [Blog]({base}/blog): articles on beneficial ownership data and OpenCheck",
        "",
        "## For agents and developers",
        "",
        f"- [MCP server]({api}/mcp): streamable HTTP; descriptor at {api}/.well-known/mcp.json",
        f"- [API reference]({api}/docs): OpenAPI at {api}/openapi.json",
        f"- [Sample /lookup answer]({base}/api-samples/lookup-taqa-bratani.json): "
        "a recorded response, restricted sources' records removed; limits and errors at "
        f"{base}/api#limits",
        f"- [Company reference pages]({base}/browse): one page per LEI, from the GLEIF Golden Copy",
        "- [Source code](https://github.com/StephenAbbott/opencheck)",
        "",
        "## Reading results honestly",
        "",
        "- A source listed under degraded_sources did not answer: the check is "
        "incomplete, not clean.",
        "- A risk signal has a kind: risk or context. Context is never a finding.",
        "- A possible name match is evidence to weigh, not an identity.",
        "- Some sources are licensed CC BY-NC; pass on the licence notices.",
        "",
    ]
    return "\n".join(lines)
