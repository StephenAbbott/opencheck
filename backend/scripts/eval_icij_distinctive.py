"""Eval harness for the Phase 120 distinctive-token gate (ICIJ screening).

The PR #86 measurement (14 subjects / ~350 chain names / ~3,600 ICIJ
results) was run from an uncommitted scratch script, so its corpus and
adjudication survived only in the commit message. This harness rebuilds the
measurement reproducibly and commits alongside its results, so the next
threshold change starts from artifacts rather than archaeology.

Subcommands (run from ``backend/``, live network required for ``pull``):

    uv run python scripts/eval_icij_distinctive.py pull    [--out DIR] [--api BASE]
    uv run python scripts/eval_icij_distinctive.py score   [--in DIR] [--min-sim 0.87]
    uv run python scripts/eval_icij_distinctive.py report  [--in DIR] [--labels CSV]
    uv run python scripts/eval_icij_distinctive.py tokens  [--in DIR]
    uv run python scripts/eval_icij_distinctive.py surfaces [--in DIR]

* ``pull`` fetches ``/lookup`` for each subject from a running OpenCheck API
  (default: production), extracts the ICIJ screening targets with the real
  ``icij_check._collect_targets``, then queries the ICIJ reconciliation API
  at ``limit=10`` per node type — a superset of production's 2; ICIJ result
  lists are order-stable prefixes, so the production pool is recovered by
  truncation (the PR #86 lesson: measure on the pool you ship).
* ``score`` classifies every (target, result) pair under the OLD gate
  (similarity ≥ 0.93, no token gate) and the NEW gate (similarity ≥
  ``--min-sim`` plus ``names.distinctive_token_agreement`` for entities),
  and writes ``worksheet.csv`` for every pair either gate accepts or that
  falls in the adjudication band (similarity ≥ 0.80). Fill in the ``label``
  column by hand: ``tp`` (same real-world organisation/person) or ``fp``.
* ``report`` reads the labelled worksheet and prints precision/recall per
  gate and a threshold sweep, on both the production pool (2/type) and the
  wide pool (10/type).
* ``tokens`` prints the token-frequency table over chain names + result
  names — the evidence base for ``names._GENERIC_ORG_TOKENS``.
* ``surfaces`` replays the gate over the RELATED_* signal evidence pairs
  (cross_check / OpenAleph) already present in the pulled lookups — sizing
  data for extending the gate to those surfaces; no behaviour change here.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck import names  # noqa: E402
from opencheck.icij_check import (  # noqa: E402
    _MIN_NAME_SIM,
    _RECONCILE_URL,
    _SCREENED_TYPES,
    _collect_targets,
    _screenable,
)
from opencheck.http import sanitize_name_query  # noqa: E402

DEFAULT_API = "https://opencheck-api.onrender.com"
DEFAULT_DIR = Path(__file__).resolve().parents[1] / "data" / "eval" / "icij_distinctive"

#: The 14 production subjects from the PR #86 measurement, re-resolved
#: against GLEIF 2026-08-20.
SUBJECTS: dict[str, str] = {
    "BP p.l.c.": "213800LH1BZH3DI6G760",
    "Rosneft": "253400JT3MQWNDKMJE44",
    "Bank Saderat PLC": "2138008KTNTDICZU8L25",
    "Hornsea 1 Limited": "2138002S3XGZ38WN5Q72",
    "TAQA Bratani Limited": "213800E11LI1SCETU492",
    "Newcastle United FC": "213800AG2V6YE68H5N63",
    "Biffa plc": "2138008RB4WDK7HYYS91",
    "Care UK Social Care": "213800DBE5Y9ZM58PN63",
    "DMGT": "4OFD47D73QFJ1T1MOF29",
    "LVMH": "IOG4E947OATN0KJYSD45",
    "Unilever PLC": "549300MKFYEKVRWML317",
    "A.P. Moller-Maersk": "549300D2K6PKKKXVNN73",
    "Glencore plc": "2138002658CPO9NBH955",
    "Nordea Bank Abp": "529900ODI3047E2LIV03",
}

_PULL_LIMIT = 10  # per node type; production uses _RESULTS_PER_TYPE = 2
_BAND_LOW = 0.80  # worksheet inclusion floor


def _slug(name: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in name.lower()).strip("-")


def pull(out_dir: Path, api_base: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=300.0) as client:
        for subject, lei in SUBJECTS.items():
            path = out_dir / f"{_slug(subject)}.json"
            if path.exists():
                print(f"cached  {subject}")
                continue
            print(f"lookup  {subject} ({lei}) …", flush=True)
            resp = client.get(f"{api_base}/lookup", params={"lei": lei})
            resp.raise_for_status()
            lookup = resp.json()
            targets = _collect_targets(lookup.get("bods") or [])
            print(f"        {len(targets)} screening target(s)")
            results: dict[str, dict] = {}
            for start in range(0, len(targets), 8):
                batch = targets[start : start + 8]
                queries: dict[str, dict] = {}
                for i, t in enumerate(batch, start=start):
                    q = sanitize_name_query(t["name"])
                    if not _screenable(q):
                        continue
                    for type_name, type_uri in _SCREENED_TYPES.items():
                        queries[f"q{i}-{type_name.lower()}"] = {
                            "query": q,
                            "limit": _PULL_LIMIT,
                            "type": type_uri,
                        }
                if not queries:
                    continue
                r = client.post(
                    _RECONCILE_URL, data={"queries": json.dumps(queries)}
                )
                r.raise_for_status()
                results.update(r.json())
            payload = {
                "subject": subject,
                "lei": lei,
                "targets": targets,
                "icij": results,
                "risk_signals": lookup.get("risk_signals") or [],
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
            n_results = sum(len(v.get("result") or []) for v in results.values())
            print(f"        {n_results} ICIJ result(s) → {path.name}")


def _iter_pairs(in_dir: Path):
    """Yield (subject, target, node_type, rank, result) for every pulled pair."""
    for path in sorted(in_dir.glob("*.json")):
        data = json.loads(path.read_text())
        targets = {i: t for i, t in enumerate(data["targets"])}
        for key, qr in (data.get("icij") or {}).items():
            # key shape: q{index}-{typename}
            idx_part, _, type_name = key.partition("-")
            idx = int(idx_part[1:])
            target = targets.get(idx)
            if target is None:
                continue
            for rank, result in enumerate(qr.get("result") or []):
                yield data["subject"], idx, target, type_name, rank, result


def _pair_row(subject, target_index, target, type_name, rank, result, min_sim):
    matched = (result.get("name") or "").strip()
    sim = names.name_similarity(target["name"], matched)
    # Mirror the production rule in icij_check._signal_from_match exactly:
    # entities always gated; person-kind targets gated when either name
    # carries a legal form (corporate officers filed as persons).
    entityish = (
        target["kind"] != "person"
        or names.has_org_form_tokens(target["name"])
        or names.has_org_form_tokens(matched)
    )
    agree = names.distinctive_token_agreement(target["name"], matched) if entityish else True
    icij_match = bool(result.get("match"))
    icij_score = int(result.get("score") or 0)
    passes_score = icij_match or icij_score >= 70
    old_gate = passes_score and sim >= _MIN_NAME_SIM
    new_gate = passes_score and sim >= min_sim and agree
    return {
        "subject": subject,
        "target_index": target_index,
        "kind": target["kind"],
        "search_name": target["name"],
        "matched_name": matched,
        "node_type": type_name,
        "rank": rank,
        "icij_score": icij_score,
        "icij_match": icij_match,
        "sim": round(sim, 4),
        "distinctive_agree": agree,
        "old_gate": old_gate,
        "new_gate": new_gate,
        "label": "",
    }


def score(in_dir: Path, min_sim: float) -> None:
    rows = []
    total = 0
    for subject, idx, target, type_name, rank, result in _iter_pairs(in_dir):
        total += 1
        row = _pair_row(subject, idx, target, type_name, rank, result, min_sim)
        if row["old_gate"] or row["new_gate"] or row["sim"] >= _BAND_LOW:
            rows.append(row)
    rows.sort(key=lambda r: (-r["sim"], r["subject"]))
    out = in_dir / "worksheet.csv"
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{total} pairs scored; {len(rows)} in worksheet → {out}")
    old_n = sum(r["old_gate"] for r in rows)
    new_n = sum(r["new_gate"] for r in rows)
    print(f"old gate (sim≥{_MIN_NAME_SIM}, no token gate): {old_n} signals")
    print(f"new gate (sim≥{min_sim} + distinctive agreement): {new_n} signals")
    flipped = [r for r in rows if r["old_gate"] != r["new_gate"]]
    for r in flipped:
        arrow = "DROPPED" if r["old_gate"] else "GAINED "
        print(
            f"  {arrow} [{r['kind'][:1]}] {r['search_name'][:40]!r} vs "
            f"{r['matched_name'][:40]!r} sim={r['sim']} agree={r['distinctive_agree']}"
        )


def report(in_dir: Path, labels_path: Path, min_sim: float) -> None:
    with labels_path.open() as fh:
        rows = list(csv.DictReader(fh))
    labelled = [r for r in rows if r["label"] in ("tp", "fp")]
    print(f"{len(labelled)} labelled pairs of {len(rows)}")

    def stats(pred) -> str:
        picked = [r for r in labelled if pred(r)]
        tp = sum(r["label"] == "tp" for r in picked)
        fp = len(picked) - tp
        all_tp = sum(r["label"] == "tp" for r in labelled)
        prec = tp / len(picked) if picked else float("nan")
        rec = tp / all_tp if all_tp else float("nan")
        return f"signals={len(picked):3d}  tp={tp:3d}  fp={fp:3d}  precision={prec:.2f}  recall={rec:.2f}"

    def truthy(v) -> bool:
        return str(v) == "True"

    for pool, keep in (
        (
            "production pool (2/type, first 30 targets)",
            lambda r: int(r["rank"]) < 2 and int(r["target_index"]) < 30,
        ),
        (f"wide pool ({_PULL_LIMIT}/type, all targets)", lambda r: True),
    ):
        print(f"\n== {pool}")
        sub = [r for r in labelled if keep(r)]

        def in_sub(pred):
            return lambda r: keep(r) and pred(r)

        print("  old gate:", stats(in_sub(lambda r: truthy(r["old_gate"]))))
        print("  new gate:", stats(in_sub(lambda r: truthy(r["new_gate"]))))
        print("  threshold sweep (with distinctive gate):")
        for t in (0.85, 0.86, 0.87, 0.88, 0.90, 0.93):
            print(
                f"    sim≥{t:.2f}:",
                stats(
                    in_sub(
                        lambda r, t=t: (
                            (str(r["icij_match"]) == "True" or int(r["icij_score"]) >= 70)
                            and float(r["sim"]) >= t
                            and truthy(r["distinctive_agree"])
                        )
                    )
                ),
            )


def tokens(in_dir: Path) -> None:
    counter: collections.Counter[str] = collections.Counter()
    for path in sorted(in_dir.glob("*.json")):
        data = json.loads(path.read_text())
        seen: set[str] = set()
        for t in data["targets"]:
            seen.add(t["name"])
        for qr in (data.get("icij") or {}).values():
            for r in qr.get("result") or []:
                if r.get("name"):
                    seen.add(r["name"])
        for name in seen:
            counter.update(names.org_name_residue(name).split())
    print("token frequency over chain + result names (top 40):")
    for tok, n in counter.most_common(40):
        flag = "GENERIC" if tok in names._GENERIC_ORG_TOKENS else ""
        print(f"  {n:5d}  {tok}  {flag}")


def surfaces(in_dir: Path) -> None:
    """What would the gate do to existing RELATED_* signals (sizing only)."""
    total = 0
    would_drop = []
    for path in sorted(in_dir.glob("*.json")):
        data = json.loads(path.read_text())
        for sig in data.get("risk_signals") or []:
            code = sig.get("code") or ""
            ev = sig.get("evidence") or {}
            if not code.startswith("RELATED_"):
                continue
            search, matched = ev.get("search_name"), ev.get("matched_name")
            if not search or not matched or ev.get("kind") == "person":
                continue
            total += 1
            if not names.distinctive_token_agreement(search, matched):
                would_drop.append((data["subject"], sig["source_id"], code, search, matched))
    print(f"{total} entity RELATED_* signals in the pulled lookups")
    print(f"{len(would_drop)} would fail the distinctive-token gate:")
    for subject, source, code, search, matched in would_drop:
        print(f"  {subject} | {source} | {code}: {search[:38]!r} vs {matched[:38]!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["pull", "score", "report", "tokens", "surfaces"])
    parser.add_argument("--out", "--in", dest="dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--min-sim", type=float, default=0.87)
    parser.add_argument("--labels", type=Path, default=None)
    args = parser.parse_args()
    if args.command == "pull":
        pull(args.dir, args.api)
    elif args.command == "score":
        score(args.dir, args.min_sim)
    elif args.command == "report":
        report(args.dir, args.labels or (args.dir / "worksheet.csv"), args.min_sim)
    elif args.command == "tokens":
        tokens(args.dir)
    elif args.command == "surfaces":
        surfaces(args.dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
