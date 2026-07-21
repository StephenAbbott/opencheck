"""OpenCheck Estonia RDF demo queries — SQL over triples in DuckDB (duck_rdf).

The RDF twin of ``data/estonia/neo4j/demo_queries.cypher``: the same demo
questions answered over the NQuads produced by ``scripts/export_rdf_bulk.py``,
plus the two queries only the RDF projection can answer — per-statement
licensing and named-graph provenance. Also the benchmark harness for the
parked AuraDB/graph-DB question (CLAUDE.md Phase 8): a multi-hop ownership
traversal over 6.6M quads in plain DuckDB, no graph database running.

Usage (from backend/):

    python scripts/export_rdf_bulk.py --in ../data/estonia/estonia-2026-07-04.jsonl.gz \
        --out ../data/estonia/estonia-2026-07-04.nq --fallback-license \
        "https://creativecommons.org/publicdomain/zero/1.0/"
    python scripts/rdf_demo_queries.py [--nq ../data/estonia/estonia-2026-07-04.nq[.gz]]

A ``.gz`` argument (e.g. the release asset ``estonia-2026-07-04.nq.gz``) is
transparently decompressed to a temporary file first — duck_rdf cannot read
gzipped NQuads itself (it refuses the double extension, and with a
``file_type`` hint it parses the compressed bytes raw).

Expected numbers (precomputed from estonia-2026-07-04) are asserted, so this
doubles as an integrity check of the JSONL → RDF conversion.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import tempfile
import time
from contextlib import contextmanager

import duckdb

B = "https://vocab.openownership.org/terms#"
RDFT = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


@contextmanager
def _maybe_gunzip(path: str):
    """Yield a path duck_rdf can read: ``.gz`` inputs are decompressed to a
    temporary ``.nq`` (deleted afterwards); anything else passes through."""
    if not path.endswith(".gz"):
        yield path
        return
    with tempfile.NamedTemporaryFile(suffix=".nq", delete=False) as tmp:
        tmp_path = tmp.name
        print(f"decompressing {path} → {tmp_path} …")
        with gzip.open(path, "rb") as src:
            shutil.copyfileobj(src, tmp)
    try:
        yield tmp_path
    finally:
        os.unlink(tmp_path)


def timed(con, title, sql, expect=None, check=None):
    t0 = time.time()
    rows = con.execute(sql).fetchall()
    ms = (time.time() - t0) * 1000
    print(f"\n── {title}  ({ms:,.0f} ms)")
    for row in rows[:10]:
        print("  ", row)
    if expect is not None:
        assert rows == expect, f"expected {expect}, got {rows}"
    if check is not None:
        assert check(rows), f"check failed for {title}: {rows[:3]}"
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nq", default="../data/estonia/estonia-2026-07-04.nq",
                        help="NQuads file; .gz accepted (decompressed to a temp file)")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL rdf FROM community; LOAD rdf;")
    t0 = time.time()
    with _maybe_gunzip(args.nq) as nq_path:
        con.execute(f"CREATE TABLE t AS SELECT * FROM read_rdf('{nq_path}')")
    n = con.execute("SELECT count(*) FROM t").fetchone()[0]
    print(f"loaded {n:,} quads in {time.time() - t0:.1f}s")

    # Materialise the views the queries share.
    con.execute(f"""
        CREATE VIEW typed AS
        SELECT subject, object AS cls FROM t WHERE predicate = '{RDFT}';
        CREATE VIEW names AS                      -- record → fullName
        SELECT nm.subject AS rec, min(fn.object) AS name
        FROM t nm JOIN t fn ON fn.subject = nm.object AND fn.predicate = '{B}fullName'
        WHERE nm.predicate = '{B}name' GROUP BY nm.subject;
        CREATE VIEW idents AS                     -- record → (scheme, id)
        SELECT i.subject AS rec,
               max(CASE WHEN d.predicate = '{B}scheme'   THEN d.object END) AS scheme,
               max(CASE WHEN d.predicate = '{B}idString' THEN d.object END) AS id
        FROM t i JOIN t d ON d.subject = i.object
        WHERE i.predicate = '{B}identifier' GROUP BY i.subject, i.object;
        CREATE VIEW edges AS                      -- interest-typed ownership edges
        SELECT s.object AS child, p.object AS parent, ty.object AS interest_cls,
               s.graph AS stmt_graph
        FROM t s
        JOIN t p  ON p.subject = s.subject AND p.predicate = '{B}interestedParty'
        JOIN t ix ON ix.subject = s.subject AND ix.predicate = '{B}interest'
        JOIN t ty ON ty.subject = ix.object AND ty.predicate = '{RDFT}'
        WHERE s.predicate = '{B}subject' AND ty.object LIKE '{B}%';
    """)

    # 0. Sanity counts — must match the Neo4j release exactly.
    timed(con, "0. record counts (Entity/Person/Relationship/Unspecified)", f"""
        SELECT replace(cls, '{B}', '') AS label, count(*) AS n FROM typed
        WHERE cls IN ('{B}Entity', '{B}Person', '{B}Relationship', '{B}Unspecified')
        GROUP BY 1 ORDER BY 2 DESC""",
        expect=[("Relationship", 132260), ("Entity", 47579),
                ("Unspecified", 46071), ("Person", 29690)])

    # 1. The headline join: GLEIF record ↔ register twin via shared registry code.
    timed(con, "1. LEI companies joined to a register twin (expect 22,568)", """
        SELECT count(DISTINCT g.rec) FROM idents g
        JOIN idents a ON a.id = g.id AND a.rec <> g.rec
        -- registry code carries either label in this snapshot (see release notes)
        WHERE g.scheme IN ('EE-KMKR', 'EE-ARIREGISTER')
          AND a.scheme IN ('EE-KMKR', 'EE-ARIREGISTER')
          AND g.rec LIKE '%records/XI-LEI-%' AND a.rec NOT LIKE '%records/XI-LEI-%'""",
        expect=[(22568,)])

    # 2. Deepest entity shareholding chain (expect the 5-company Kaamos chain,
    #    i.e. 4 OWNS edges — matching the Cypher query's Entity-only roots).
    timed(con, "2. deepest entity shareholding chains (expect Kaamos, 5 companies)", f"""
        WITH RECURSIVE down(top, node, depth, chain) AS (
            SELECT e.parent, e.child, 1, [e.parent, e.child]
            FROM edges e
            JOIN typed pt ON pt.subject = e.parent AND pt.cls = '{B}Entity'
            WHERE e.interest_cls = '{B}Shareholding'
              AND NOT EXISTS (SELECT 1 FROM edges up
                              WHERE up.child = e.parent
                                AND up.interest_cls = '{B}Shareholding')
            UNION ALL
            SELECT d.top, e.child, d.depth + 1, list_append(d.chain, e.child)
            FROM edges e JOIN down d ON e.parent = d.node
            WHERE e.interest_cls = '{B}Shareholding'
              AND d.depth < 8 AND NOT list_contains(d.chain, e.child)
        )
        , deepest AS (SELECT row_number() OVER (ORDER BY depth DESC) AS rank, chain, depth
                      FROM down ORDER BY depth DESC LIMIT 5)
        SELECT list(coalesce(n.name, u.x) ORDER BY u.i) AS chain_names, any_value(depth) AS depth
        FROM deepest, unnest(chain) WITH ORDINALITY AS u(x, i)
        LEFT JOIN names n ON n.rec = u.x
        GROUP BY deepest.rank ORDER BY deepest.rank""",
        check=lambda rows: any(
            r[1] == 4 and any("KAAMOS" in str(x).upper() for x in r[0]) for r in rows))

    # 3. Busiest people (expect the top person connected to ~31 companies).
    timed(con, "3. busiest people by connected companies (expect top ≈ 31)", f"""
        SELECT coalesce(n.name, e.parent) AS person, count(DISTINCT e.child) AS companies
        FROM edges e
        JOIN typed ty ON ty.subject = e.parent AND ty.cls = '{B}Person'
        LEFT JOIN names n ON n.rec = e.parent
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5""",
        check=lambda rows: rows and rows[0][1] >= 25)

    # 4. Where GLEIF's knowledge ends: reporting exceptions by reason.
    timed(con, "4. reporting exceptions by reason (expect ≈31.7k / ≈15.1k)", f"""
        SELECT lbl.object AS reason, count(*) AS n
        FROM typed u JOIN t lbl ON lbl.subject = u.subject AND lbl.predicate = '{LABEL}'
        WHERE u.cls = '{B}Unspecified'
        GROUP BY 1 ORDER BY 2 DESC""",
        check=lambda rows: rows and rows[0][1] > 30000)

    # 5. RDF-only: the licence matrix travelling with the data.
    timed(con, "5. statements per licence (RDF-only query)", f"""
        SELECT object AS licence, count(*) AS statements
        FROM t WHERE predicate = '{B}license' GROUP BY 1 ORDER BY 2 DESC""",
        check=lambda rows: sum(r[1] for r in rows) == 209529)

    # 6. RDF-only: named-graph provenance on one edge — who asserted the
    #    ownership edge, under what licence, retrieved when.
    timed(con, "6. provenance of one deep-chain edge from its named graph", f"""
        SELECT
          max(CASE WHEN g.predicate = '{B}description' THEN g.object END) AS source,
          max(CASE WHEN g.predicate = '{B}license'     THEN g.object END) AS licence,
          max(CASE WHEN g.predicate = '{B}retrievedAt' THEN g.object END) AS retrieved
        FROM edges e JOIN t g ON g.graph = e.stmt_graph
        JOIN names cn ON cn.rec = e.child JOIN names pn ON pn.rec = e.parent
        WHERE pn.name LIKE 'KAAMOS HOLDING%' AND e.interest_cls = '{B}Shareholding'
        GROUP BY e.stmt_graph LIMIT 3""",
        check=lambda rows: rows and rows[0][1] is not None)

    # 7. Benchmark for the parked AuraDB question: full multi-hop UBO-style
    #    traversal upward from every LEI company simultaneously.
    timed(con, "7. AuraDB benchmark: 3-hop upward traversal from ALL LEI companies", f"""
        WITH RECURSIVE up(start, node, depth) AS (
            SELECT ty.subject, ty.subject, 0 FROM typed ty
            WHERE ty.cls = '{B}Entity' AND ty.subject LIKE '%records/XI-LEI-%'
            UNION ALL
            SELECT up.start, e.parent, up.depth + 1
            FROM edges e JOIN up ON e.child = up.node
            WHERE up.depth < 3
        )
        SELECT max(depth) AS max_depth, count(*) AS visited FROM up""",
        check=lambda rows: rows[0][0] == 3)

    print("\nall checks passed")


if __name__ == "__main__":
    main()
