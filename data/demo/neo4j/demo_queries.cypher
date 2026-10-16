// OpenCheck demo graph — queries to run after import.cypher against Neo4j 5.x.
// Graph model (bods-neo4j): (:Entity), (:Person), (:Identifier), edges OWNS
// and CONTROLS carrying shareMinimum / shareMaximum (/ shareExact) as numbers.
// The demo set is UK PSC (OGL v3.0) + GLEIF (CC0) BODS — see LICENCES.md.
// Expected numbers were computed from this directory's CSVs on 16 Oct 2026.

// ── 1. Share bands → one weight: keep both ends, choose per question.
//       Shares load as numbers (bods-neo4j ≥ 0e01cf1). A register that
//       publishes bands (UK PSC: 25–50, 50–75, 75–100) gives a range, not
//       a figure, so no single number is "the" share. Keep the minimum and
//       maximum on the edge and let each query say which it used:
//         • lower bound  — conservative tests ("does anyone hold ≥ 25%?")
//         • midpoint     — weights for centrality / PageRank-style scores
//         • upper bound  — "could this party reach the threshold?"
//       An exact figure is all three. An edge with no stated share has no
//       weight: leave it null, never 0 — 0 says "holds nothing".
MATCH (o)-[r:OWNS]->(c:Entity)
WITH r,
     coalesce(r.shareExact, r.shareMinimum) AS lower,
     coalesce(r.shareExact, r.shareMaximum) AS upper
RETURN CASE WHEN lower IS NULL AND upper IS NULL THEN 'no stated share'
            WHEN lower = upper THEN 'exact'
            ELSE 'band ' + toString(lower) + '–' + toString(upper) END AS kind,
       count(*) AS edges,
       avg(CASE WHEN lower IS NOT NULL AND upper IS NOT NULL
                THEN (lower + upper) / 2 END) AS mean_midpoint
ORDER BY edges DESC;
// expect band 75–100: 297 (midpoint 87.5); 25–50: 51 (37.5); 50–75: 7 (62.5).

// ── 2. The same holders, two answers — say which one you used.
//        Lower bound = "holds at least X%"; upper bound = "may hold X% or
//        more". The gap between them is the holders whose band straddles X.
UNWIND [25, 50] AS threshold
MATCH (o)-[r:OWNS]->(c:Entity)
WITH threshold,
     coalesce(r.shareExact, r.shareMinimum) AS lower,
     coalesce(r.shareExact, r.shareMaximum) AS upper
RETURN threshold,
       count(CASE WHEN lower >= threshold THEN 1 END) AS at_least_on_lower_bound,
       count(CASE WHEN upper >= threshold THEN 1 END) AS possibly_on_upper_bound,
       count(CASE WHEN lower IS NULL AND upper IS NULL THEN 1 END) AS no_stated_share
ORDER BY threshold;
// expect 25%: 355 / 355; 50%: 304 / 355 — the 51 holders in the 25–50 band
// may or may not hold half, and only the upper bound counts them.
