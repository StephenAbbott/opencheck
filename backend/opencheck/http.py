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

from . import __version__, provenance
from .config import get_settings
from .gleif_throttle import GleifThrottledTransport

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
_DEFAULT_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_USER_AGENT = f"OpenCheck/{__version__} (+https://github.com/StephenAbbott/opencheck)"


# Characters that are query *syntax* to Lucene-style parsers, never part of
# the name as far as screening is concerned. ``"`` opens a phrase query — an
# unbalanced one is a parse error the upstream surfaces as HTTP 400
# (OpenSanctions/yente: ``token_mgr_error: Lexical error … <EOF>``) or a bare
# HTTP 500 (ICIJ Offshore Leaks reconcile). ``\`` is Lucene's escape
# character and can truncate the same way. ``/`` opens a *regular expression*
# in Lucene syntax, so Danish/Norwegian company names ending ``A/S`` are an
# unterminated regex — diagnosed live 2026-07-22 on HORNSEA 1 LIMITED
# (LEI 2138002S3XGZ38WN5Q72), whose Ørsted parent chain carries eleven
# ``A/S`` entities: 8 of 25 OpenSanctions probes and all 3 ICIJ batches
# failed. The remaining set (grouping/range/boost/fuzzy/wildcard/field
# syntax, boolean-operator pairs, and a term-leading ``-``/``+``) is
# replaced for the same reason: OpenCheck never intends query syntax when
# screening a name.
# The character class below is Elasticsearch's documented query_string
# reserved set (a superset of classic Lucene): + - = && || > < ! ( ) { }
# [ ] ^ " ~ * ? : \ / — the docs note that < and > "can't be escaped at
# all", so replacement is the only safe handling. Only a - or + with a
# non-space character on BOTH sides is part of the name ("ANNE-MARIE",
# "BP+AMOCO"); anything else is an operator, including a DANGLING one with
# no operand after it — diagnosed live 2026-07-30 on LVMH MOET HENNESSY
# LOUIS VUITTON (LEI IOG4E947OATN0KJYSD45), whose subsidiary is legally
# named "S +": the trailing + survived the earlier term-leading-only rule
# and 500'd ICIJ's parser on every lookup.
_LUCENE_BREAKERS = re.compile(
    r'["\\/:^~\[\]{}()!*?<>=]'  # single-character syntax
    r"|&&|\|\|"                 # boolean operator pairs (single & and | are safe)
    r"|(?<!\S)[-+]"             # - / + leading a term (NOT / MUST)
    r"|[-+](?!\S)"              # - / + with no operand after it ("S +")
)

# Lucene's word-form boolean operators are the same dangling-operand hazard
# as a trailing ``+``: ``AND``/``OR`` at either edge of a query has nothing to
# join, which the ICIJ reconcile parser answers with a bare HTTP 500
# ("AND DIGITAL", a real UK company, and any name ending "… AND"). They are
# case-SENSITIVE to the parser, so lower-casing the edge token disarms the
# operator while keeping the word — upstream analysers lower-case anyway, so
# unlike character replacement this costs no recall at all. ``NOT`` is a
# unary prefix operator and parses fine at the start, so it is left alone.
_BOOL_WORDS = frozenset({"AND", "OR"})


def sanitize_name_query(name: str) -> str:
    """Make a free-text name safe for Lucene/Elasticsearch-backed search APIs.

    Israeli company names routinely write the gershayim in ``בע"מ`` ("Ltd")
    as an ASCII double quote — exactly one, so always unbalanced. Every such
    related party deterministically failed screening against OpenSanctions
    (400) and ICIJ Offshore Leaks (500) until sanitised (diagnosed live
    2026-07-22 on Unilever PLC's Israeli subsidiaries).

    OpenCheck never intends phrase-query, regex, wildcard, fuzzy, field or
    boolean semantics when screening a name, so Lucene syntax characters are
    replaced with spaces — upstream tokenisers split on punctuation anyway,
    so recall is unaffected — and whitespace is collapsed. Mid-word hyphens
    (``ANNE-MARIE``), apostrophes, dots, single ``&`` (``E&P``) and ``|``
    are kept: they are not syntax. A ``AND``/``OR`` first or last token is
    lower-cased rather than dropped (see :data:`_BOOL_WORDS`). Names without
    syntax characters pass through unchanged, so cache keys derived from the
    sanitised query are stable for them. May return ``""`` (e.g. a name that
    was only quotes); callers must skip the search rather than send an empty
    query.
    """
    if not name:
        return ""
    cleaned = _LUCENE_BREAKERS.sub(" ", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    # Disarm a dangling word-form boolean at either edge. Only the edges: a
    # mid-query AND/OR has operands on both sides and parses fine, and
    # "BLACK AND WHITE LTD" should keep its exact casing.
    tokens = cleaned.split(" ")
    for edge in (0, len(tokens) - 1):
        if tokens[edge] in _BOOL_WORDS:
            tokens[edge] = tokens[edge].lower()
    return " ".join(tokens)


def build_client() -> httpx.AsyncClient:
    """Build a new async client. Callers own the lifecycle (use ``async with``).

    Building a client is the moment an adapter commits to going to the network,
    so it is where a ``live`` retrieval is recorded. Adapters that build a
    client and then serve from cache anyway still resolve correctly: provenance
    takes the *worst* liveness and the *oldest* timestamp across a fetch, so the
    cache read wins.
    """
    provenance.record_live()
    transport: httpx.AsyncBaseTransport = httpx.AsyncHTTPTransport(retries=2)
    # Phase 143: GLEIF rate-limits by IP (60 req/min) and every adapter in this
    # process shares that budget, so requests to api.gleif.org pass through a
    # process-wide throttle + 429-retry wrapper. Other hosts are untouched.
    # Installed here — the one place every live adapter already passes through
    # (see the Ariregister provenance regression for why bypassing
    # build_client is a bug class of its own).
    if get_settings().gleif_rate_limit_per_minute > 0:
        transport = GleifThrottledTransport(transport)
    return httpx.AsyncClient(
        timeout=_DEFAULT_TIMEOUT,
        limits=_DEFAULT_LIMITS,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        transport=transport,
        follow_redirects=True,
    )
