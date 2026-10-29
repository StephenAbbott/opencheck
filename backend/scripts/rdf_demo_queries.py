"""OpenCheck Estonia RDF demo queries — SQL over triples in DuckDB (duck_rdf).

The RDF twin of ``scripts/estonia_demo_queries.cypher`` (shipped as
``demo_queries.cypher`` in the dataset release's Neo4j zip): the same demo
questions answered over the NQuads produced by ``scripts/export_rdf_bulk.py``,
plus the two queries only the RDF projection can answer — per-statement
licensing and named-graph provenance. Also the benchmark harness for the
parked AuraDB/graph-DB question (CLAUDE.md Phase 8): a multi-hop ownership
traversal over 13.9M quads in plain DuckDB, no graph database running.

Usage (from backend/):

    python3 scripts/export_rdf_bulk.py --in ../data/estonia/estonia-2026-10-25.jsonl.gz \
        --out ../data/estonia/estonia-2026-10-25.nq
    python3 scripts/rdf_demo_queries.py [--nq ../data/estonia/estonia-2026-10-25.nq[.gz]]

A ``.gz`` argument (e.g. the release asset ``estonia-2026-10-25.nq.gz``) is
transparently decompressed to a temporary file first — duck_rdf cannot read
gzipped NQuads itself (it refuses the double extension, and with a
``file_type`` hint it parses the compressed bytes raw).

Expected numbers are the ones in the Cypher file's ``expect`` comments —
measured from ``estonia-2026-10-25.jsonl.gz`` loaded with bods-neo4j 1775533
into Neo4j 5.26 on 26 Oct 2026 — and are asserted here, so the script doubles
as an integrity check of the JSONL → RDF conversion and as proof that the two
projections answer the same questions the same way. Where Neo4j and RDF count
differently the query says so: a Neo4j ``OWNS`` / ``CONTROLS`` / ``MANAGES``
edge is one *interest* of a relationship (bods-neo4j's family table in
``bods_schema.BODS_INTEREST_TO_FAMILY``), so the SQL counts interest nodes,
never relationship records.

Keeping the two in step: when the dataset is re-issued, re-measure the Cypher
in Neo4j, update its comments, and change the ``expect=`` values here to the
same numbers in the same commit.
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

#: bods-neo4j's five-family taxonomy, as the vocabulary classes the RDF export
#: writes for each interest type (``bods/rdf.py::_INTEREST_CLASS``). Only the
#: three families the Estonia dataset carries are listed; an interest class
#: outside them is counted under its own name.
OWNS = ("Shareholding", "RightsToProfitOrIncome", "RightsToSurplusAssetsOnDissolution",
        "RightToProfitOrIncomeFromAssets", "EnjoymentAndUseOfAssets",
        "RightsGrantedByContract", "ConditionalRightsGrantedByContract")
CONTROLS = ("VotingRights", "ControlViaCompanyRulesOrArticles", "ControlByLegalFramework",
            "OtherInfluenceOrControl", "AppointmentOfBoard")
MANAGES = ("SeniorManagingOfficial", "BoardMember", "BoardChair")

REGISTER = "Estonian e-Business Register (e-Äriregister)"
EXCEPTION_SUFFIX = "(GLEIF reporting exception)"


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


def _sql_list(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


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
    parser.add_argument("--nq", default="../data/estonia/estonia-2026-10-25.nq",
                        help="NQuads file; .gz accepted (decompressed to a temp file)")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL rdf FROM community; LOAD rdf;")
    t0 = time.time()
    with _maybe_gunzip(args.nq) as nq_path:
        con.execute(f"CREATE TABLE t AS SELECT * FROM read_rdf('{nq_path}')")
    (n,) = con.execute("SELECT count(*) FROM t").fetchone() or (0,)
    print(f"loaded {n:,} quads in {time.time() - t0:.1f}s")

    # Materialise the tables the queries share (as views they would re-run their
    # self-joins on every query). Every record's triples sit in
    # its statement's named graph, so a record's source and status are read
    # off the statement that carries it.
    con.execute(f"""
        CREATE VIEW typed AS
        SELECT subject, object AS cls FROM t WHERE predicate = '{RDFT}';
        CREATE VIEW stmt_rec AS                   -- statement → record
        SELECT subject AS stmt, object AS rec FROM t WHERE predicate = '{B}recordDetails';
        CREATE VIEW stmt_source AS                -- statement → source description
        SELECT s.subject AS stmt, d.object AS source
        FROM t s JOIN t d ON d.subject = s.object AND d.predicate = '{B}description'
        WHERE s.predicate = '{B}source';
        CREATE VIEW stmt_status AS                -- statement → new | updated | closed
        SELECT subject AS stmt, lower(replace(object, '{B}', '')) AS status
        FROM t WHERE predicate = '{RDFT}'
          AND object IN ('{B}New', '{B}Updated', '{B}Closed');
        CREATE TABLE records AS                    -- record, class, source, status
        SELECT sr.rec, replace(ty.cls, '{B}', '') AS cls, so.source, st.status
        FROM stmt_rec sr
        JOIN typed ty ON ty.subject = sr.rec
             AND ty.cls IN ('{B}Entity', '{B}Person', '{B}Relationship')
        LEFT JOIN stmt_source so ON so.stmt = sr.stmt
        LEFT JOIN stmt_status st ON st.stmt = sr.stmt;
        CREATE TABLE names AS                      -- record → fullName
        SELECT nm.subject AS rec, min(fn.object) AS name
        FROM t nm JOIN t fn ON fn.subject = nm.object AND fn.predicate = '{B}fullName'
        WHERE nm.predicate = '{B}name' GROUP BY nm.subject;
        CREATE TABLE idents AS                     -- record → (scheme, id)
        SELECT i.subject AS rec,
               max(CASE WHEN d.predicate = '{B}scheme'   THEN d.object END) AS scheme,
               max(CASE WHEN d.predicate = '{B}idString' THEN d.object END) AS id
        FROM t i JOIN t d ON d.subject = i.object
        WHERE i.predicate = '{B}identifier' GROUP BY i.subject, i.object;
        CREATE TABLE juris AS                      -- entity → jurisdiction code
        SELECT j.subject AS rec, c.object AS code
        FROM t j JOIN t c ON c.subject = j.object AND c.predicate = '{B}code'
        WHERE j.predicate = '{B}jurisdiction';
        CREATE TABLE interests AS                  -- one row per interest = one Neo4j edge
        SELECT s.subject AS rel, p.object AS party, s.object AS child,
               replace(ty.object, '{B}', '') AS kind,
               CASE WHEN replace(ty.object, '{B}', '') IN ({_sql_list(OWNS)}) THEN 'OWNS'
                    WHEN replace(ty.object, '{B}', '') IN ({_sql_list(CONTROLS)}) THEN 'CONTROLS'
                    WHEN replace(ty.object, '{B}', '') IN ({_sql_list(MANAGES)}) THEN 'MANAGES'
                    ELSE replace(ty.object, '{B}', '') END AS family,
               max(CASE WHEN f.predicate = '{B}beneficialOwnershipOrControl'
                        THEN f.object END) AS bo,
               max(CASE WHEN f.predicate = '{B}shareExact' THEN f.object END) AS share_exact,
               max(CASE WHEN f.predicate = '{B}shareMinimum' THEN f.object END) AS share_min,
               max(CASE WHEN f.predicate = '{B}shareMaximum' THEN f.object END) AS share_max,
               max(CASE WHEN f.predicate = '{B}directOrIndirect'
                        THEN lower(split_part(f.object, '#', 2)) END) AS direct_or_indirect,
               r.status, r.source, s.graph AS stmt_graph
        FROM t s
        JOIN t p  ON p.subject = s.subject AND p.predicate = '{B}interestedParty'
        JOIN t ix ON ix.subject = s.subject AND ix.predicate = '{B}interest'
        JOIN t ty ON ty.subject = ix.object AND ty.predicate = '{RDFT}'
        LEFT JOIN t f ON f.subject = ix.object AND f.predicate IN (
             '{B}beneficialOwnershipOrControl', '{B}shareExact', '{B}shareMinimum',
             '{B}shareMaximum', '{B}directOrIndirect')
        JOIN records r ON r.rec = s.subject
        WHERE s.predicate = '{B}subject' AND ty.object LIKE '{B}%'
        GROUP BY s.subject, p.object, s.object, ty.object, r.status, r.source, s.graph;
        CREATE TABLE owns_current AS               -- current shareholdings into entities
        SELECT i.*,
               TRY_CAST(coalesce(i.share_exact, i.share_min) AS DOUBLE) AS lower,
               TRY_CAST(coalesce(i.share_exact, i.share_max) AS DOUBLE) AS upper
        FROM interests i JOIN records c ON c.rec = i.child AND c.cls = 'Entity'
        WHERE i.family = 'OWNS' AND i.status <> 'closed';
        CREATE TABLE joined AS                     -- GLEIF record ↔ register twin
        SELECT DISTINCT g.rec AS gleif_rec, a.rec AS register_rec
        FROM idents gi JOIN records g ON g.rec = gi.rec AND g.cls = 'Entity' AND g.source = 'GLEIF'
        JOIN idents ai ON ai.scheme = gi.scheme AND ai.id = gi.id
        JOIN records a ON a.rec = ai.rec AND a.cls = 'Entity' AND a.source = '{REGISTER}'
        WHERE gi.scheme = 'EE-ARIREGISTER';
        CREATE TABLE named_bo AS                   -- register entity → a named beneficial owner
        SELECT DISTINCT i.child AS rec, i.party
        FROM interests i JOIN records p ON p.rec = i.party AND p.cls = 'Person'
        WHERE i.family IN ('OWNS', 'CONTROLS') AND i.bo = 'true';
    """)

    # 0. Sanity counts — the records, and the edges as Neo4j counts them.
    timed(con, "0a. record counts (expect Entity 74,069; Person 88,349; Relationship 186,376)",
          """
        SELECT cls, count(*) AS n FROM records GROUP BY 1 ORDER BY 2 DESC""",
        expect=[("Relationship", 186376), ("Person", 88349), ("Entity", 74069)])
    timed(con, "0b. edges by family (expect OWNS 81,910; CONTROLS 59,007; MANAGES 45,459)",
          """
        SELECT family, count(*) AS n FROM interests GROUP BY 1 ORDER BY 2 DESC""",
        expect=[("OWNS", 81910), ("CONTROLS", 59007), ("MANAGES", 45459)])

    # 1. The join: GLEIF record ↔ the register's own record, on the registry code.
    timed(con, "1. LEI companies joined to a register twin (expect 28,729 of 29,246)", """
        SELECT (SELECT count(DISTINCT gleif_rec) FROM joined) AS joined_lei_companies,
               (SELECT count(*) FROM records r JOIN juris j ON j.rec = r.rec
                WHERE r.cls = 'Entity' AND r.source = 'GLEIF' AND j.code = 'EE')
               AS ee_gleif_companies""",
        expect=[(28729, 29246)])

    # 2. Beneficial-owner coverage: a register twin naming at least one BO.
    timed(con, "2. LEI companies whose register record names a beneficial owner (expect 25,370)",
          """
        SELECT count(DISTINCT j.gleif_rec) FROM joined j
        JOIN named_bo b ON b.rec = j.register_rec""",
        expect=[(25370,)])

    # 3. NATURAL_PERSONS reporting exceptions, and how many the register names an owner for.
    timed(con, "3. natural-person exceptions joined to the register / with a named BO "
          "(expect 15,497 / 13,662)", f"""
        WITH np AS (
            SELECT DISTINCT i.child AS g FROM interests i
            JOIN names n ON n.rec = i.party
            WHERE n.name = 'Natural person(s) {EXCEPTION_SUFFIX}'
        ), per_company AS (
            SELECT np.g, count(DISTINCT b.party) AS bos
            FROM np JOIN joined j ON j.gleif_rec = np.g
            LEFT JOIN named_bo b ON b.rec = j.register_rec
            GROUP BY np.g
        )
        SELECT count(*) AS natural_person_exceptions,
               count(CASE WHEN bos > 0 THEN 1 END) AS with_named_beneficial_owner
        FROM per_company""",
        expect=[(15497, 13662)])

    # 4. Where GLEIF's knowledge ends: reporting exceptions by reason. Each is its
    #    own unknown party, named for the reason, one per company and kind.
    timed(con, "4. GLEIF reporting exceptions by reason "
          "(expect 31,350 / 14,469 / 11,115 / 527 / 278)", f"""
        SELECT n.name AS reason, count(*) AS n
        FROM interests i JOIN names n ON n.rec = i.party
        JOIN records p ON p.rec = i.party AND p.source = 'GLEIF'
        WHERE n.name LIKE '%{EXCEPTION_SUFFIX}'
        GROUP BY 1 ORDER BY 2 DESC""",
        expect=[(f"Natural person(s) {EXCEPTION_SUFFIX}", 31350),
                (f"No known controlling person {EXCEPTION_SUFFIX}", 14469),
                (f"Non-consolidating parent {EXCEPTION_SUFFIX}", 11115),
                (f"Parent without an LEI {EXCEPTION_SUFFIX}", 527),
                (f"Undisclosed parent {EXCEPTION_SUFFIX}", 278)])

    # 5. Deepest current shareholding chains between companies (the Cypher's
    #    OWNS*3..10 from a top with no owner; closed records and self-loops out).
    timed(con, "5. deepest current shareholding chains "
          "(expect several of 7 levels, e.g. … → aktsiaselts TOFTAN)", """
        WITH RECURSIVE owns AS (
            SELECT o.party AS parent, o.child FROM owns_current o
            JOIN records p ON p.rec = o.party AND p.cls = 'Entity'
            WHERE o.party <> o.child
        ), any_owns AS (          -- the Cypher's NOT (:Entity)-[:OWNS]->(top): any status
            SELECT i.child FROM interests i JOIN records p ON p.rec = i.party AND p.cls = 'Entity'
            WHERE i.family = 'OWNS'
        ), down(node, depth, chain) AS (
            SELECT e.child, 1, [e.parent, e.child] FROM owns e
            WHERE NOT EXISTS (SELECT 1 FROM any_owns a WHERE a.child = e.parent)
            UNION ALL
            SELECT e.child, d.depth + 1, list_append(d.chain, e.child)
            FROM owns e JOIN down d ON e.parent = d.node
            WHERE d.depth < 10 AND NOT list_contains(d.chain, e.child)
        ), deepest AS (
            SELECT row_number() OVER (ORDER BY depth DESC) AS rank, chain, depth
            FROM down WHERE depth >= 3 ORDER BY depth DESC LIMIT 5
        )
        SELECT list(coalesce(n.name, u.x) ORDER BY u.i) AS chain_names, any_value(depth) AS depth
        FROM deepest, unnest(chain) WITH ORDINALITY AS u(x, i)
        LEFT JOIN names n ON n.rec = u.x
        GROUP BY deepest.rank ORDER BY deepest.rank""",
        check=lambda rows: rows and rows[0][1] == 7
        and any("TOFTAN" in str(r[0][-1]).upper() for r in rows))

    # 6. Cross-border GLEIF consolidation edges. A reporting-exception bridge has
    #    no jurisdiction; like the Cypher's null comparison, it is left out.
    timed(con, "6. cross-border GLEIF CONTROLS edges by kind (expect direct 156, indirect 169)", """
        SELECT i.direct_or_indirect AS kind, count(*) AS edges
        FROM interests i
        JOIN records p ON p.rec = i.party AND p.cls = 'Entity' AND p.source = 'GLEIF'
        JOIN records c ON c.rec = i.child AND c.cls = 'Entity' AND c.source = 'GLEIF'
        JOIN juris pj ON pj.rec = i.party JOIN juris cj ON cj.rec = i.child
        WHERE i.family = 'CONTROLS' AND (pj.code = 'EE') <> (cj.code = 'EE')
        GROUP BY 1 ORDER BY 1""",
        expect=[("direct", 156), ("indirect", 169)])

    # 7. Busiest people on the register's current lists.
    timed(con, "7. busiest register people (expect De Silvestri 50, Madissoon 27, Saarepere 21)",
          f"""
        SELECT n.name, count(DISTINCT i.child) AS companies
        FROM interests i
        JOIN records p ON p.rec = i.party AND p.cls = 'Person' AND p.source = '{REGISTER}'
        JOIN records c ON c.rec = i.child AND c.cls = 'Entity'
        JOIN names n ON n.rec = i.party
        WHERE i.status <> 'closed'
        GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 3""",
        expect=[("Roberto De Silvestri", 50), ("Annika Madissoon", 27), ("Rando Saarepere", 21)])

    # 9. Share bands → one weight (the Estonian register files exact figures).
    timed(con, "9. share kinds on current OWNS edges "
          "(expect exact 41,228, mean ≈ 73.4; no stated share 32,113)", """
        SELECT CASE WHEN lower IS NULL AND upper IS NULL THEN 'no stated share'
                    WHEN lower = upper THEN 'exact' ELSE 'band' END AS kind,
               count(*) AS edges,
               round(avg(CASE WHEN lower IS NOT NULL AND upper IS NOT NULL
                              THEN (lower + upper) / 2 END), 1) AS mean_midpoint
        FROM owns_current GROUP BY 1 ORDER BY 2 DESC""",
        expect=[("exact", 41228, 73.4), ("no stated share", 32113, None)])

    # 10. The same holders, two answers — lower and upper bound at 25% and 50%.
    timed(con, "10. holders at a threshold, lower / upper bound "
          "(expect 25%: 35,042; 50%: 32,364; no gap)", """
        SELECT threshold,
               count(CASE WHEN lower >= threshold THEN 1 END) AS at_least_on_lower_bound,
               count(CASE WHEN upper >= threshold THEN 1 END) AS possibly_on_upper_bound,
               count(CASE WHEN lower IS NULL AND upper IS NULL THEN 1 END) AS no_stated_share
        FROM owns_current, (SELECT unnest([25, 50]) AS threshold)
        GROUP BY 1 ORDER BY 1""",
        expect=[(25, 35042, 35042, 32113), (50, 32364, 32364, 32113)])

    # 11. Ownership history the files do carry: founders no longer holding shares.
    timed(con, "11. former founders — closed OWNS edges (expect 8,569 on 6,843 companies)", """
        SELECT count(*) AS former_founders, count(DISTINCT child) AS companies
        FROM interests WHERE family = 'OWNS' AND status = 'closed'""",
        expect=[(8569, 6843)])

    # RDF-only: the licence travelling with every statement.
    timed(con, "R1. statements per licence "
          "(RDF-only; expect CC-BY 203,175 + CC0 145,619 = every statement)", f"""
        SELECT object AS licence, count(*) AS statements
        FROM t WHERE predicate = '{B}license' GROUP BY 1 ORDER BY 2 DESC""",
        expect=[("https://creativecommons.org/licenses/by/4.0/", 203175),      # the register layer
                ("https://creativecommons.org/publicdomain/zero/1.0/", 145619)])  # the GLEIF layer

    # RDF-only: named-graph provenance on one deep-chain edge — who asserted it,
    # under what licence, retrieved when.
    timed(con, "R2. provenance of the TOFTAN edge from its named graph (RDF-only)", f"""
        SELECT
          max(CASE WHEN g.predicate = '{B}description' THEN g.object END) AS source,
          max(CASE WHEN g.predicate = '{B}license'     THEN g.object END) AS licence,
          max(CASE WHEN g.predicate = '{B}retrievedAt' THEN g.object END) AS retrieved
        FROM owns_current e JOIN t g ON g.graph = e.stmt_graph
        JOIN names cn ON cn.rec = e.child
        WHERE cn.name = 'aktsiaselts TOFTAN'
        GROUP BY e.stmt_graph LIMIT 3""",
        check=lambda rows: rows and all(r[0] and r[1] and r[2] for r in rows))

    # Benchmark for the parked AuraDB question: a 3-hop upward traversal from
    # every LEI company at once, over ownership and control edges.
    timed(con, "B. AuraDB benchmark: 3-hop upward traversal from ALL LEI companies", """
        WITH RECURSIVE edges AS (
            SELECT child, party AS parent FROM interests WHERE family IN ('OWNS', 'CONTROLS')
        ), up(start, node, depth) AS (
            SELECT DISTINCT i.rec, i.rec, 0 FROM idents i
            JOIN records r ON r.rec = i.rec AND r.cls = 'Entity' AND r.source = 'GLEIF'
            WHERE i.scheme = 'XI-LEI'
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
