"""Shared httpx.AsyncClient with sensible defaults for OpenCheck adapters.

All live adapters go through ``get_client()``. This keeps timeouts, retries,
and the User-Agent consistent, and makes it easy to add observability or
a circuit breaker later.

Also home to :func:`sanitize_name_query` — outgoing-request hygiene for the
free-text name searches several adapters send to Lucene/Elasticsearch-backed
upstreams.
"""

from __future__ import annotations

import re

import httpx

from . import __version__

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
_DEFAULT_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_USER_AGENT = f"OpenCheck/{__version__} (+https://github.com/StephenAbbott/opencheck)"


# Characters that are query *syntax* to Lucene-style parsers, never part of
# the name as far as screening is concerned. ``"`` opens a phrase query — an
# unbalanced one is a parse error the upstream surfaces as HTTP 400
# (OpenSanctions/yente: ``token_mgr_error: Lexical error … <EOF>``) or a bare
# HTTP 500 (ICIJ Offshore Leaks reconcile). ``\`` is Lucene's escape
# character and can truncate the same way.
_LUCENE_BREAKERS = re.compile(r'["\\]')


def sanitize_name_query(name: str) -> str:
    """Make a free-text name safe for Lucene/Elasticsearch-backed search APIs.

    Israeli company names routinely write the gershayim in ``בע"מ`` ("Ltd")
    as an ASCII double quote — exactly one, so always unbalanced. Every such
    related party deterministically failed screening against OpenSanctions
    (400) and ICIJ Offshore Leaks (500) until sanitised (diagnosed live
    2026-07-22 on Unilever PLC's Israeli subsidiaries).

    OpenCheck never intends phrase-query or escape semantics when screening
    a name, so quote and backslash are replaced with spaces — upstream
    tokenisers split on punctuation anyway, so recall is unaffected — and
    whitespace is collapsed. Names without these characters pass through
    unchanged, so cache keys derived from the sanitised query are stable
    for them. May return ``""`` (e.g. a name that was only quotes); callers
    must skip the search rather than send an empty query.
    """
    if not name:
        return ""
    cleaned = _LUCENE_BREAKERS.sub(" ", name)
    return re.sub(r"\s+", " ", cleaned).strip()


def build_client() -> httpx.AsyncClient:
    """Build a new async client. Callers own the lifecycle (use ``async with``)."""
    transport = httpx.AsyncHTTPTransport(retries=2)
    return httpx.AsyncClient(
        timeout=_DEFAULT_TIMEOUT,
        limits=_DEFAULT_LIMITS,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        transport=transport,
        follow_redirects=True,
    )
