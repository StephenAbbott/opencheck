"""Build the recorded examples the OpenAPI document shows (``opencheck/openapi_examples.json``).

    cd backend
    uv run python scripts/build_openapi_examples.py

Each example is a real production answer, cut short so ``/docs`` stays
readable: lists keep their first few items, and the ``raw`` payload of a
search result is left out. The ``/lookup`` example is cut from the sample
answer the /api page serves (``frontend/public/api-samples/``), which already
has every record of a source not free to redistribute removed; the search
example drops those sources' results the same way. ``--from-dir`` reads
saved answers (``search.json``, ``resolve.json``, ``knowability.json``,
``health.json``, ``sources.json``) instead of fetching.

``tests/test_openapi_docs.py`` validates every example against its route's
response model, so rebuild this when a response shape changes.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

_ROOT = Path(__file__).resolve().parents[2]
_OUT = _ROOT / "backend" / "opencheck" / "openapi_examples.json"
_SAMPLE = _ROOT / "frontend" / "public" / "api-samples" / "lookup-taqa-bratani.json"
API = "https://api.opencheck.world"

QUERIES = {
    "search": "/search?q=TAQA%20Bratani",
    "resolve": "/resolve-national-id?country=GB&number=05975475",
    "knowability": "/knowability?jurisdictions=GB",
    "health": "/health",
    "sources": "/sources",
}

#: Lists are cut to this many items.
KEEP = 2


def _sample_builder():
    spec = importlib.util.spec_from_file_location(
        "build_api_sample", Path(__file__).with_name("build_api_sample.py")
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fetch(path: str) -> Any:
    req = Request(f"{API}{path}", headers={"User-Agent": "opencheck-openapi-examples"})
    with urlopen(req, timeout=120) as r:  # noqa: S310 - fixed https host
        return json.load(r)


def _cut(node: Any, keep: int = KEEP) -> Any:
    if isinstance(node, list):
        return [_cut(v, keep) for v in node[:keep]]
    if isinstance(node, dict):
        return {k: _cut(v, keep) for k, v in node.items()}
    return node


def lookup_example() -> dict[str, Any]:
    sample = json.loads(_SAMPLE.read_text(encoding="utf-8"))
    sample.pop("_sample", None)
    # Keep the entity statements first so the cut shows the subject.
    sample["bods"] = sorted(
        sample.get("bods") or [], key=lambda s: s.get("recordType") != "entity"
    )
    return _cut(sample)


def search_example(answer: dict[str, Any], restricted: set[str]) -> dict[str, Any]:
    out = dict(answer)
    hits = [h for h in out.get("hits") or [] if h.get("source_id") not in restricted]
    out["hits"] = [{**h, "raw": {}} for h in hits]
    out["errors"] = {}
    return _cut(out, 3)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from-dir", type=Path, help="saved answers instead of fetches")
    args = ap.parse_args()

    def get(name: str) -> Any:
        if args.from_dir:
            return json.loads((args.from_dir / f"{name}.json").read_text(encoding="utf-8"))
        return _fetch(QUERIES[name])

    restricted = _sample_builder().restricted_sources()
    sources = get("sources")
    examples = {
        "GET /lookup": lookup_example(),
        "GET /search": search_example(get("search"), restricted),
        "GET /resolve-national-id": get("resolve"),
        "GET /knowability": get("knowability"),
        "GET /health": get("health"),
        "GET /sources": {**sources, "sources": sources["sources"][:KEEP]},
    }
    _OUT.write_text(json.dumps(examples, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {_OUT} ({_OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
