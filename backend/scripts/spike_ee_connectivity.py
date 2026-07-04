#!/usr/bin/env python3
"""Phase 1 spike: does Estonia's LEI population form a connected graph?

Runs entirely offline against the Open Ownership ``gleif_version_0_4``
parquet files downloaded by ``setup_bods_data.py`` (default location:
``backend/data/bods/gleif/parquet``). No API calls.

Method
------
1. Latest entity statement per recordId; keep jurisdiction=EE, non-closed.
2. Latest relationship statement per recordId; keep real edges
   (``interestedParty_reason IS NULL``, non-closed) touching the EE set.
3. networkx connected components over EE nodes + foreign neighbours.
4. Reporting-exception breakdown for EE subjects (GLEIF exception category
   is recoverable from ``interestedParty_description``).

Usage::

    python3 scripts/spike_ee_connectivity.py \
        [--parquet-dir data/bods/gleif/parquet] [--out ee_spike_summary.json]

Requires ``duckdb`` and ``networkx`` (pip install duckdb networkx).

Result recorded 2026-07-03 (snapshot: statements to 2025-02-09): archipelago.
23,392 active EE records; 483 (2.1%) have any L2 edge; 247 multi-node
components, largest 16 (Swedbank fund-management star); 338 cross-border
edges (FI, LV, SE, LT top partners); 10,256 EE entities declare the
NATURAL_PERSONS exception — the gap the ariregister layer fills in Phase 2.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import networkx as nx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", default="data/bods/gleif/parquet")
    ap.add_argument("--out", default="ee_spike_summary.json")
    ap.add_argument("--jurisdiction", default="EE")
    args = ap.parse_args()
    pq = Path(args.parquet_dir)
    con = duckdb.connect()

    con.execute(f"""
        CREATE TEMP TABLE ent AS
        SELECT recordId, recordDetails_name AS name,
               recordDetails_jurisdiction_code AS juris, recordStatus
        FROM (SELECT *, row_number() OVER (PARTITION BY recordId ORDER BY statementDate DESC) rn
              FROM '{pq}/entity_statement.parquet') WHERE rn = 1""")
    con.execute(
        "CREATE TEMP TABLE cc AS SELECT * FROM ent WHERE juris = ? AND recordStatus != 'closed'",
        [args.jurisdiction],
    )
    n_cc = con.execute("SELECT count(*) FROM cc").fetchone()[0]

    con.execute(f"""
        CREATE TEMP TABLE rel AS
        SELECT recordId, recordDetails_subject s, recordDetails_interestedParty ip, recordStatus
        FROM (SELECT *, row_number() OVER (PARTITION BY recordId ORDER BY statementDate DESC) rn
              FROM '{pq}/relationship_statement.parquet'
              WHERE recordDetails_interestedParty_reason IS NULL) WHERE rn = 1""")
    edges = con.execute("""
        SELECT r.s, r.ip, es.juris, ei.juris FROM rel r
        JOIN ent es ON es.recordId = r.s
        LEFT JOIN ent ei ON ei.recordId = r.ip
        WHERE r.recordStatus != 'closed'
          AND (es.juris = ? OR ei.juris = ?)""", [args.jurisdiction, args.jurisdiction]).fetchall()

    exc = con.execute(f"""
        SELECT regexp_extract(recordDetails_interestedParty_description,
                              'Exception Reason: ([A-Z_ ]+)\\.', 1) reason,
               count(DISTINCT rs.recordDetails_subject) n_entities
        FROM '{pq}/relationship_statement.parquet' rs
        JOIN cc ON cc.recordId = rs.recordDetails_subject
        WHERE recordDetails_interestedParty_reason IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC""").fetchall()

    cc_set = {r[0] for r in con.execute("SELECT recordId FROM cc").fetchall()}
    g = nx.Graph()
    g.add_nodes_from(cc_set)
    xborder = 0
    partners: dict[str, int] = {}
    j = args.jurisdiction
    for s, ip, js, ji in edges:
        if ip is None:
            continue
        g.add_edge(s, ip)
        if (js == j) != (ji == j):
            xborder += 1
            p = ji if js == j else js
            partners[p] = partners.get(p, 0) + 1

    comps = [c for c in nx.connected_components(g) if any(n in cc_set for n in c)]
    sizes = sorted((len(c) for c in comps), reverse=True)
    summary = {
        "jurisdiction": j,
        "active_records": n_cc,
        "edges_touching": len(edges),
        "nodes_with_any_edge": sum(1 for n in cc_set if g.degree(n) > 0),
        "isolated_nodes": sum(1 for s in sizes if s == 1),
        "multi_node_components": sum(1 for s in sizes if s > 1),
        "largest_components": sizes[:12],
        "cross_border_edges": xborder,
        "top_partner_jurisdictions": sorted(partners.items(), key=lambda x: -x[1])[:10],
        "exception_entities_by_gleif_reason": dict(exc),
    }
    Path(args.out).write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
