"""Benchmark BigQuery Graph for OpenCheck's bulk ownership-traversal hot path.

Evidence-first test for the "Work out how to deal with bulk data sources"
brainstorm: can BigQuery Graph return a rooted multi-hop ownership subgraph for
each demo anchor within the "few seconds" budget, and at what bytes-scanned
(= cost) per query?

Query shape matters more than the engine. An early run with an *undirected*
``TRAIL`` traversal took >7 minutes (path-enumeration explosion); the same
traversal expressed as **directed, bounded** queries runs in ~seconds warm. So
this benchmark roots each anchor and runs two directed bounded queries per
anchor (owners upstream + owned downstream, no TRAIL) and sums them, which is
what the service would render. A warm-up query is run first so the table shows
steady-state latency; the one-time cold-start (slot spin-up) is reported
separately.

In the public bodsdata GLEIF dataset an entity's recordId is ``XI-LEI-<LEI>``,
so queries root directly with no identifier lookup.

Prerequisite — a BigQuery property graph over the bodsdata GLEIF tables (needs
Enterprise edition). Minimal graph that this script needs:

    CREATE OR REPLACE PROPERTY GRAPH `YOUR_PROJECT.bods_graph`.OwnershipGraph
      NODE TABLES ( `bodsdata.gleif_version_0_4`.entity_statement AS Entity
        KEY (recordId) LABEL Entity PROPERTIES (recordId) )
      EDGE TABLES ( `bodsdata.gleif_version_0_4`.relationship_statement AS HAS_INTEREST
        KEY (statementId)
        SOURCE KEY (recordDetails_interestedParty) REFERENCES Entity (recordId)
        DESTINATION KEY (recordDetails_subject) REFERENCES Entity (recordId)
        LABEL HAS_INTEREST PROPERTIES (statementId) );

Usage:

    pip install google-cloud-bigquery
    gcloud auth application-default login
    python scripts/benchmark_bigquery_graph.py \\
        --graph 'YOUR_PROJECT.bods_graph.OwnershipGraph' \\
        --project YOUR_PROJECT --max-hops 3

Re-run with --max-hops 5 to see how latency/cost scale with depth.
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
import time

# Demo set (Phase 0). GLEIF recordId = rid-prefix + LEI.
ANCHORS: dict[str, str] = {
    "DMGT": "4OFD47D73QFJ1T1MOF29",
    "BP": "213800LH1BZH3DI6G760",
    "Rosneft": "253400JT3MQWNDKMJE44",
    "Bank Saderat": "2138008KTNTDICZU8L25",
    "Biffa": "2138008RB4WDK7HYYS91",
    "Hornsea 1": "2138002S3XGZ38WN5Q72",
    "Care UK": "213800DBE5Y9ZM58PN63",
    "Taqa Bratani": "213800E11LI1SCETU492",
    "Newcastle United": "213800AG2V6YE68H5N63",
}

ON_DEMAND_USD_PER_TIB = 6.25  # cost *proxy*; graph queries run on slot-based Enterprise edition
_RID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def build_query(graph: str, rid: str, max_hops: int, direction: str) -> str:
    """Directed, bounded traversal of the ownership network around ``rid``.

    direction="owners" -> upstream: entities that own rid (directly/indirectly).
    direction="owned"  -> downstream: entities rid owns.

    Directed + bounded (no TRAIL): undirected TRAIL explodes on hub nodes
    (path enumeration); directed bounded stays in the seconds range.
    """
    rid_node = f"(root:Entity {{recordId: '{rid}'}})"
    other = "(other:Entity)"
    edge = f"-[:HAS_INTEREST]->{{1,{max_hops}}}"
    pattern = f"{other}{edge}{rid_node}" if direction == "owners" else f"{rid_node}{edge}{other}"
    return f"GRAPH `{graph}`\nMATCH {pattern}\nRETURN COUNT(DISTINCT other.recordId) AS reached"


def _run(client, job_config, sql: str) -> tuple[float, int, int]:
    """Run a query; return (wall_seconds, nodes_reached, bytes_billed)."""
    t0 = time.perf_counter()
    job = client.query(sql, job_config=job_config)
    rows = list(job.result())
    wall = time.perf_counter() - t0
    nodes = rows[0]["reached"] if rows else 0
    return wall, nodes, (job.total_bytes_billed or 0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--graph", required=True, help="Property graph: project.dataset.GraphName")
    ap.add_argument("--project", default=None, help="Billing project (default: ADC default)")
    ap.add_argument("--max-hops", type=int, default=3)
    ap.add_argument("--rid-prefix", default="XI-LEI-")
    args = ap.parse_args(argv)

    try:
        from google.cloud import bigquery
    except ImportError:
        print("ERROR: pip install google-cloud-bigquery", file=sys.stderr)
        return 1

    client = bigquery.Client(project=args.project)
    job_config = bigquery.QueryJobConfig(use_query_cache=False)  # measure true compute, not cache hits
    anchors = list(ANCHORS.items())

    # Warm-up: the first query pays slot spin-up. Report it as cold-start, exclude from the table.
    first_rid = f"{args.rid_prefix}{anchors[0][1]}"
    try:
        cold_wall, _, _ = _run(client, job_config, build_query(args.graph, first_rid, args.max_hops, "owners"))
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).splitlines()[0]
        print(f"Warm-up query failed: {msg[:140]}", file=sys.stderr)
        print("If this mentions editions/reservations, graph queries need BigQuery Enterprise edition.", file=sys.stderr)
        return 1
    print(f"Graph: {args.graph}   max_hops={args.max_hops}   (directed owners+owned, no TRAIL, cache off)")
    print(f"Cold-start (first query, slot spin-up): {cold_wall:.1f}s  [one-time / after idle]\n")

    header = f"{'anchor':<18}{'wall':>8}{'owners':>8}{'owned':>8}{'GB bill':>9}{'est $':>9}"
    print(header)
    print("-" * len(header))

    walls: list[float] = []
    billed_total = 0
    cost_total = 0.0
    failures = 0

    for name, lei in anchors:
        rid = f"{args.rid_prefix}{lei}"
        if not _RID_RE.match(rid):
            print(f"{name:<18}  skipped (bad recordId: {rid!r})")
            continue
        try:
            w_up, n_up, b_up = _run(client, job_config, build_query(args.graph, rid, args.max_hops, "owners"))
            w_dn, n_dn, b_dn = _run(client, job_config, build_query(args.graph, rid, args.max_hops, "owned"))
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"{name:<18}  ERROR: {str(exc).splitlines()[0][:70]}")
            continue
        wall = w_up + w_dn
        billed = b_up + b_dn
        cost = billed / (1024 ** 4) * ON_DEMAND_USD_PER_TIB
        walls.append(wall)
        billed_total += billed
        cost_total += cost
        print(f"{name:<18}{wall:>7.2f}s{n_up:>8}{n_dn:>8}{billed/1e9:>9.2f}{cost:>9.4f}")

    if walls:
        print("-" * len(header))
        med = statistics.median(walls)
        worst = max(walls)
        print(f"{'median / total':<18}{med:>7.2f}s{'':>8}{'':>8}{billed_total/1e9:>9.2f}{cost_total:>9.4f}")
        verdict = (
            "PASS - warm directed traversals clear the few-seconds bar"
            if med <= 5
            else "SLOW - reconsider pre-materialisation or Spanner Graph"
        )
        print(f"\nMedian round-trip (owners+owned) wall: {med:.2f}s   worst: {worst:.2f}s   ->  {verdict}")
        print("Cold-start is the one-time figure above; cached repeat lookups are ~0s.")
        print(
            f"Total bytes billed across {2*len(walls)} queries: {billed_total/1e9:.2f} GB "
            f"(~${cost_total:.4f} at on-demand proxy rate; Enterprise edition is slot-based)."
        )
    if failures:
        print(f"\n{failures} anchor(s) failed.", file=sys.stderr)
    return 0 if walls and not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
