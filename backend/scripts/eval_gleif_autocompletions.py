#!/usr/bin/env python3
"""Compare GLEIF's two name-search endpoints for entity resolution (issue #27).

OpenCheck's ``GleifAdapter.search`` resolves a company name via the fulltext
filter on the LEI-records endpoint::

    GET /api/v1/lei-records?filter[fulltext]=<name>

GLEIF also exposes a dedicated *autocompletions* endpoint, which searches the
whole record (legal name, other names, transliterations, previous names) and
returns lightweight suggestion objects that each point at one LEI::

    GET /api/v1/autocompletions?field=fulltext&q=<name>

This harness runs a fixed fixture of real-world queries (legal names, other /
alternative-language names, ASCII transliterations of non-Latin names, previous
/ trading names, and hand-made typos) against several *strategies* and reports
hit@1 / hit@5 for the expected LEI, overall and per category. The fixture and
each expected LEI are grounded in GLEIF's own records — see
``gleif_autocompletions_queries.json`` for provenance.

Strategies (issue #33 adds the last two, to close the typo gap #32 found):

* ``fulltext`` — the current ``GleifAdapter.search`` path (fulltext filter).
* ``autocompletions`` — GLEIF's dedicated typeahead endpoint.
* ``relax_droplast`` — query relaxation: on zero fulltext hits, retry with
  trailing tokens progressively removed, taking the first non-empty result.
* ``relax_loo`` — query relaxation by *leave-one-out*: on zero fulltext hits,
  retry once per token with that token dropped, then rank the union by how many
  leave-one-out variants agreed (consensus), tie-breaking on best rank.

Both relaxation strategies fire ONLY when the plain fulltext query returns zero
results, so on any query fulltext already resolves they return exactly the
fulltext result (this is how the harness proves clean queries are untouched).

Etiquette: requests are sequential with a small delay, and every response is
cached to a scratch JSON (keyed by endpoint+query) so re-runs cost zero API
calls. The relaxation strategies reuse the ``fulltext`` cache for the plain
query and add live calls only for the relaxed sub-queries of zero-result
queries — so the extra API cost is bounded by the number of failing queries.

Usage (from the ``backend`` directory)::

    python scripts/eval_gleif_autocompletions.py                 # run + print table
    python scripts/eval_gleif_autocompletions.py --json out.json # also dump raw results
    python scripts/eval_gleif_autocompletions.py --no-cache      # ignore the cache
    python scripts/eval_gleif_autocompletions.py --delay 1.0     # be extra gentle

The cache lives at ``scripts/.gleif_eval_cache.json`` (git-ignored) by default.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import quote

import httpx

_API_BASE = "https://api.gleif.org/api/v1"
_TOP_K = 5
_PAGE_SIZE = 10  # fetch a few beyond top-5 so ranking past 5 is visible too
_USER_AGENT = "opencheck-eval/1.0 (+https://github.com/StephenAbbott/opencheck issue#27)"

_HERE = Path(__file__).resolve().parent
_DEFAULT_FIXTURE = _HERE / "gleif_autocompletions_queries.json"
_DEFAULT_CACHE = _HERE / ".gleif_eval_cache.json"


# ---------------------------------------------------------------------------
# Response-cache — keyed by (endpoint, query) so re-runs never re-hit the API.
# ---------------------------------------------------------------------------


class ResponseCache:
    def __init__(self, path: Path, enabled: bool = True) -> None:
        self._path = path
        self._enabled = enabled
        self._data: dict[str, Any] = {}
        if enabled and path.exists():
            self._data = json.loads(path.read_text(encoding="utf-8"))

    def get(self, key: str) -> dict[str, Any] | None:
        if not self._enabled:
            return None
        value: dict[str, Any] | None = self._data.get(key)
        return value

    def put(self, key: str, value: Any) -> None:
        self._data[key] = value
        if self._enabled:
            self._path.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=1), encoding="utf-8"
            )


# ---------------------------------------------------------------------------
# Raw endpoints + strategies. A *strategy* turns a typed query into an ORDERED,
# de-duplicated list of LEIs (a single entity can surface via several name
# variants; a user cares about distinct entities, so we rank by first
# occurrence). Simple strategies wrap one raw endpoint; the relaxation
# strategies compose several ``fulltext`` calls behind a zero-result trigger.
# ---------------------------------------------------------------------------


def _dedupe(leis: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for lei in leis:
        if lei and lei not in seen:
            seen.add(lei)
            out.append(lei)
    return out


def _fulltext_leis(payload: dict[str, Any]) -> list[str]:
    leis = [
        (item.get("attributes") or {}).get("lei") or item.get("id") or ""
        for item in payload.get("data", [])
    ]
    return _dedupe(leis)


def _autocompletions_leis(payload: dict[str, Any]) -> list[str]:
    leis: list[str] = []
    for item in payload.get("data", []):
        rel = (item.get("relationships") or {}).get("lei-records") or {}
        lei = (rel.get("data") or {}).get("id") or ""
        leis.append(lei)
    return _dedupe(leis)


def _fulltext_path(q: str) -> str:
    return f"/lei-records?filter[fulltext]={quote(q)}&page[size]={_PAGE_SIZE}"


def _autocompletions_path(q: str) -> str:
    return f"/autocompletions?field=fulltext&q={quote(q)}"


# Raw HTTP endpoints (name -> path builder). These are the only real network
# calls; the strategies below are composed from them via ``FetchFn``.
_PATHS: dict[str, Callable[[str], str]] = {
    "fulltext": _fulltext_path,
    "autocompletions": _autocompletions_path,
}

# A ``FetchFn`` fetches one raw endpoint for one query and returns its JSON
# payload (transparently cached). Strategies call it one or more times.
FetchFn = Callable[[str, str], dict[str, Any]]


# --- query relaxation helpers ----------------------------------------------


def _relaxed_leave_one_out(tokens: list[str]) -> list[str]:
    """Every query with exactly one token dropped (``A B C`` -> AB, AC, BC)."""
    return [" ".join(tokens[:i] + tokens[i + 1 :]) for i in range(len(tokens))]


def _relaxed_drop_last(tokens: list[str]) -> list[str]:
    """Progressively shorter prefixes (``A B C D`` -> ``A B C`` -> ... -> ``A``)."""
    return [" ".join(tokens[:k]) for k in range(len(tokens) - 1, 0, -1)]


def _fulltext(fetch: FetchFn, query: str) -> list[str]:
    return _fulltext_leis(fetch("fulltext", query))


def _autocompletions(fetch: FetchFn, query: str) -> list[str]:
    return _autocompletions_leis(fetch("autocompletions", query))


def _relax_drop_last(fetch: FetchFn, query: str) -> list[str]:
    """Fulltext, then — only on zero hits — retry with trailing tokens removed."""
    primary = _fulltext_leis(fetch("fulltext", query))
    if primary:
        return primary  # trigger fires ONLY on zero primary results
    for relaxed in _relaxed_drop_last(query.split()):
        leis = _fulltext_leis(fetch("fulltext", relaxed))
        if leis:
            return leis
    return primary


def _relax_leave_one_out(fetch: FetchFn, query: str) -> list[str]:
    """Fulltext, then — only on zero hits — leave-one-out consensus retry.

    Runs one fulltext query per token with that token dropped, then ranks the
    union of results by how many leave-one-out variants surfaced each LEI
    (a typo'd token yields empty variants, so the true entity — whose correct
    tokens survive in the variant that drops the typo — still wins), breaking
    ties on the best (lowest) rank any variant gave it.
    """
    primary = _fulltext_leis(fetch("fulltext", query))
    if primary:
        return primary  # trigger fires ONLY on zero primary results
    tokens = query.split()
    if len(tokens) < 2:
        return primary
    votes: dict[str, int] = {}
    best_rank: dict[str, int] = {}
    for relaxed in _relaxed_leave_one_out(tokens):
        for rank, lei in enumerate(_fulltext_leis(fetch("fulltext", relaxed)), start=1):
            votes[lei] = votes.get(lei, 0) + 1
            best_rank[lei] = min(best_rank.get(lei, rank), rank)
    return sorted(votes, key=lambda lei: (-votes[lei], best_rank[lei]))


class Strategy(NamedTuple):
    resolve: Callable[[FetchFn, str], list[str]]


_ENDPOINTS: dict[str, Strategy] = {
    "fulltext": Strategy(_fulltext),
    "autocompletions": Strategy(_autocompletions),
    "relax_droplast": Strategy(_relax_drop_last),
    "relax_loo": Strategy(_relax_leave_one_out),
}


def _fetch(
    client: httpx.Client, endpoint: str, query: str, cache: ResponseCache, delay: float
) -> dict[str, Any]:
    key = f"{endpoint} {query}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    path = _PATHS[endpoint](query)
    resp = client.get(f"{_API_BASE}{path}")
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json()
    cache.put(key, payload)
    time.sleep(delay)  # polite spacing, only on a genuine network hit
    return payload


def _rank_of(expected: str, leis: list[str]) -> int | None:
    """1-indexed rank of ``expected`` in ``leis``, or None if absent."""
    for i, lei in enumerate(leis, start=1):
        if lei == expected:
            return i
    return None


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _pct(n: int, d: int) -> str:
    return f"{100.0 * n / d:5.1f}%" if d else "  n/a"


def _print_table(rows: list[dict[str, Any]]) -> None:
    endpoints = list(_ENDPOINTS)

    def tally(subset: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
        out = {ep: {"h1": 0, "h5": 0, "n": len(subset)} for ep in endpoints}
        for r in subset:
            for ep in endpoints:
                rank = r["ranks"][ep]
                if rank == 1:
                    out[ep]["h1"] += 1
                if rank is not None and rank <= _TOP_K:
                    out[ep]["h5"] += 1
        return out

    def emit(label: str, subset: list[dict[str, Any]]) -> None:
        t = tally(subset)
        n = len(subset)
        cells = []
        for ep in endpoints:
            cells.append(
                f"{ep:>16}: hit@1 {_pct(t[ep]['h1'], n)} ({t[ep]['h1']:>2}/{n:<2})"
                f"  hit@5 {_pct(t[ep]['h5'], n)} ({t[ep]['h5']:>2}/{n:<2})"
            )
        print(f"\n{label}  (n={n})")
        for c in cells:
            print(f"    {c}")

    print("=" * 78)
    print("GLEIF name-search comparison — fulltext vs autocompletions")
    print("=" * 78)
    emit("OVERALL", rows)

    print("\n" + "-" * 78)
    print("By category")
    print("-" * 78)
    by_cat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_cat[r["category"]].append(r)
    for cat in sorted(by_cat):
        emit(cat, by_cat[cat])

    # Per-query misses worth eyeballing (expected LEI found by neither endpoint).
    both_miss = [
        r for r in rows if all(r["ranks"][ep] is None for ep in endpoints)
    ]
    if both_miss:
        print("\n" + "-" * 78)
        print(f"Found by NEITHER endpoint ({len(both_miss)}) — check ground truth:")
        print("-" * 78)
        for r in both_miss:
            print(f"    [{r['category']}] {r['query']!r} -> {r['expected_lei']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, default=_DEFAULT_FIXTURE)
    parser.add_argument("--cache", type=Path, default=_DEFAULT_CACHE)
    parser.add_argument("--no-cache", action="store_true", help="ignore + skip the cache")
    parser.add_argument("--delay", type=float, default=0.5, help="seconds between live hits")
    parser.add_argument("--json", type=Path, default=None, help="dump per-query results here")
    args = parser.parse_args(argv)

    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    queries = fixture["queries"]
    cache = ResponseCache(args.cache, enabled=not args.no_cache)

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    with httpx.Client(
        timeout=30.0, headers={"User-Agent": _USER_AGENT}, follow_redirects=True
    ) as client:

        def fetch_fn(endpoint: str, subquery: str) -> dict[str, Any]:
            return _fetch(client, endpoint, subquery, cache, args.delay)

        for i, q in enumerate(queries, start=1):
            query = q["query"]
            expected = q["expected_lei"]
            ranks: dict[str, int | None] = {}
            results: dict[str, list[str]] = {}
            for ep, strat in _ENDPOINTS.items():
                leis: list[str]
                try:
                    leis = strat.resolve(fetch_fn, query)
                except httpx.HTTPError as exc:  # noqa: PERF203
                    errors.append(f"{ep} {query!r}: {exc}")
                    leis = []
                results[ep] = leis
                ranks[ep] = _rank_of(expected, leis)
            rows.append(
                {
                    "query": query,
                    "expected_lei": expected,
                    "category": q["category"],
                    "provenance": q.get("provenance", ""),
                    "ranks": ranks,
                    "results": results,
                }
            )
            print(
                f"[{i:>2}/{len(queries)}] {q['category']:<20} "
                f"ft={_fmt_rank(ranks['fulltext'])} "
                f"ac={_fmt_rank(ranks['autocompletions'])} "
                f"rx={_fmt_rank(ranks['relax_loo'])}  {query[:48]!r}",
                file=sys.stderr,
            )

    _print_table(rows)

    if errors:
        print("\nERRORS (counted as misses):", file=sys.stderr)
        for e in errors:
            print(f"    {e}", file=sys.stderr)

    if args.json:
        args.json.write_text(
            json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nWrote per-query results to {args.json}")

    return 1 if errors else 0


def _fmt_rank(rank: int | None) -> str:
    return f"#{rank}" if rank is not None else "--"


if __name__ == "__main__":
    raise SystemExit(main())
