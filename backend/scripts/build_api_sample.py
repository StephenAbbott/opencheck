"""Build the sample `/lookup` response the /api page links to.

    cd backend
    uv run python scripts/build_api_sample.py \\
        --lei 213800E11LI1SCETU492 \\
        --out ../frontend/public/api-samples/lookup-taqa-bratani.json

It records one real `/lookup` answer from production (or reads one saved with
`--from-file`) and keeps it as returned, with one change: the records of
every source whose licence does not allow free redistribution are taken out
(`licensing.classify(...).color != "green"` — OpenSanctions, EveryPolitician,
OpenCorporates, OpenAleph and the other conditional licences). Their hits,
their BODS statements, the relationships that named one of those statements,
their signals and their rows in the cross-source tables go; what is left is
data a reader may reuse under the licences the sample names.

A `_sample` block at the top says what this is, when it was recorded, which
sources were removed and where the licences are. Rebuild it when the response
shape changes; `tests/test_api_reference.py` checks it still has every key the
/api page describes and no restricted source.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck import licensing  # noqa: E402
from opencheck.bods.refs import party_ref  # noqa: E402
from opencheck.bods.source_ids import source_id_of  # noqa: E402
from opencheck.sources import REGISTRY  # noqa: E402

API = "https://api.opencheck.world"


def restricted_sources() -> set[str]:
    """Every registered source whose licence is not free to redistribute."""
    return {
        sid for sid in REGISTRY
        if (lic := licensing.source_licensing(sid)) is not None and lic.terms.color != "green"
    }


def _source_names(ids: set[str]) -> set[str]:
    return {REGISTRY[s].info.name for s in ids if s in REGISTRY}


def strip_restricted(payload: dict[str, Any], restricted: set[str]) -> dict[str, Any]:
    """The payload without any record from a restricted source."""
    out = dict(payload)

    # BODS: drop the statements, then any relationship naming one of them.
    dropped_refs: set[str] = set()
    kept: list[dict] = []
    for stmt in payload.get("bods") or []:
        if source_id_of(stmt) in restricted:
            dropped_refs.update(
                v for v in (stmt.get("statementId"), stmt.get("recordId")) if v
            )
        else:
            kept.append(stmt)
    bods: list[dict] = []
    for stmt in kept:
        if stmt.get("recordType") == "relationship":
            rd = stmt.get("recordDetails") or {}
            ends = {party_ref(rd.get("subject")), party_ref(rd.get("interestedParty"))}
            if ends & dropped_refs:
                continue
        bods.append(stmt)
    out["bods"] = bods

    out["hits"] = [h for h in payload.get("hits") or [] if h.get("source_id") not in restricted]
    # A signal whose evidence compares a restricted source's record (Records
    # compared, a cross-source name match) goes whole: its sentence counts it.
    out["risk_signals"] = [
        s for s in payload.get("risk_signals") or []
        if s.get("source_id") not in restricted
        and not any(f'"{r}"' in json.dumps(s.get("evidence") or {}) for r in restricted)
    ]
    out["openaleph_screening"] = []

    links = []
    for link in payload.get("cross_source_links") or []:
        hits = [h for h in link.get("hits") or [] if h.get("source_id") not in restricted]
        if len({h.get("source_id") for h in hits}) >= 2:
            links.append({**link, "hits": hits, "independent_source_count": len(
                {h.get("source_id") for h in hits}
            )})
    out["cross_source_links"] = links

    names = _source_names(restricted)
    out["possibly_same_entities"] = [
        p for p in payload.get("possibly_same_entities") or []
        if p.get("a_source") not in names and p.get("b_source") not in names
        and p.get("a") not in dropped_refs and p.get("b") not in dropped_refs
    ]

    profile = payload.get("subject_profile")
    if isinstance(profile, dict):
        clean: dict[str, Any] = {}
        for key, field in profile.items():
            if isinstance(field, dict) and ("sources" in field or "source_id" in field):
                sources = [s for s in field.get("sources") or [] if s not in restricted]
                others = [
                    o for o in field.get("other_values") or [] if o.get("source_id") not in restricted
                ]
                if field.get("source_id") in restricted or ("sources" in field and not sources):
                    clean[key] = None
                    continue
                field = {**field}
                if "sources" in field:
                    field["sources"] = sources
                if "other_values" in field:
                    field["other_values"] = others
            clean[key] = field
        out["subject_profile"] = clean

    io = payload.get("integrated_ownership")
    if isinstance(io, dict) and isinstance(io.get("holders"), list):
        holders = []
        for h in io["holders"]:
            figures = [f for f in h.get("figures") or [] if f.get("source_id") not in restricted]
            if figures or not h.get("figures"):
                holders.append({**h, "figures": figures} if "figures" in h else h)
        out["integrated_ownership"] = {**io, "holders": holders}
    return out


def leaked(payload: dict[str, Any], restricted: set[str]) -> list[str]:
    """Paths of any `source_id` value naming a restricted source with data."""
    found: list[str] = []
    # Per-source status maps say what was asked, not what was answered.
    skip = {"errors", "source_liveness", "disclosure_status", "sources_applicable",
            "degraded_sources", "_sample"}

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            if node.get("source_id") in restricted:
                found.append(path)
            if isinstance(node.get("sources"), list) and {s for s in node["sources"] if isinstance(s, str)} & restricted:
                found.append(f"{path}/sources")
            for k, v in node.items():
                if path == "" and k in skip:
                    continue
                walk(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}/{i}")

    walk(payload, "")
    found += [
        f"/bods/{i}" for i, s in enumerate(payload.get("bods") or [])
        if source_id_of(s) in restricted
    ]
    return found


def fetch(lei: str, refresh: bool) -> dict[str, Any]:
    q = {"lei": lei}
    if refresh:
        q["refresh"] = "true"
    req = Request(f"{API}/lookup?{urlencode(q)}", headers={"User-Agent": "opencheck-api-sample"})
    with urlopen(req, timeout=180) as r:  # noqa: S310 - fixed https host
        return json.load(r)


def build(payload: dict[str, Any], lei: str) -> dict[str, Any]:
    restricted = restricted_sources()
    present = {h.get("source_id") for h in payload.get("hits") or []}
    present |= {source_id_of(s) for s in payload.get("bods") or []}
    removed = sorted(restricted & present)
    sample = strip_restricted(payload, restricted)
    problems = leaked(sample, restricted)
    if problems:
        raise SystemExit(f"restricted records left in the sample: {problems[:10]}")
    kept_sources = sorted(
        {h.get("source_id") for h in sample["hits"]} | {source_id_of(s) for s in sample["bods"]}
        - {None}
    )
    return {
        "_sample": {
            "about": (
                "A recorded answer of GET /lookup, kept as returned except that the records "
                "of sources whose licence does not allow free redistribution were removed. "
                "It is an example of the response's shape, not a current check: run the "
                "request yourself for today's answer."
            ),
            "request": f"{API}/lookup?lei={lei}",
            "recorded_at": payload.get("run_completed_at")
            or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "removed_sources": removed,
            "licences": {
                s: licensing.source_licensing(s).license for s in kept_sources
                if licensing.source_licensing(s) is not None
            },
            "licence_matrix": f"{API}/license-matrix?sources={','.join(kept_sources)}",
        },
        **sample,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--lei", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--from-file", type=Path, help="a saved /lookup answer instead of a fetch")
    ap.add_argument("--refresh", action="store_true", help="ask for a fresh run (costs a lookup)")
    args = ap.parse_args()
    payload = json.loads(args.from_file.read_text()) if args.from_file else fetch(args.lei, args.refresh)
    if payload.get("lei") != args.lei:
        raise SystemExit(f"the answer is for {payload.get('lei')!r}, not {args.lei!r}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(build(payload, args.lei), indent=1, ensure_ascii=False) + "\n")
    print(f"wrote {args.out} ({args.out.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
