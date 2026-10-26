// OpenCheck Estonia dataset — queries to run after import.cypher against
// Neo4j 5.x (APOC Core installed, default CSV settings).
// Graph model (bods-neo4j): (:Entity), (:Person), (:Identifier {scheme, id}),
// (:Address), (:Country); edges OWNS (shareholding), CONTROLS (control and
// GLEIF consolidation), MANAGES (officer and board roles), HAS_IDENTIFIER.
// Every node and edge carries sourceDescription: 'GLEIF' for the LEI layer,
// 'Estonian e-Business Register (e-Äriregister)' for the register layer. The
// layers join through shared Identifier nodes, scheme 'EE-ARIREGISTER' (the
// registry code). A founder no longer holding shares is an OWNS edge with
// recordStatus 'closed' — ownership history, filtered out below where a
// query asks about now.
// Expected numbers were computed from estonia-2026-10-25.jsonl.gz loaded
// with bods-neo4j 1775533 into Neo4j 5.26 + APOC 5.26 on 26 Oct 2026.

// ── 0. Sanity: node counts (expect Person 88,349; Identifier 77,500;
//       Entity 74,069; Address 47,958; Country 113) and edge counts
//       (OWNS 81,910; CONTROLS 59,007; MANAGES 45,459).
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n ORDER BY n DESC;
MATCH ()-[r]->() WHERE type(r) IN ['OWNS', 'CONTROLS', 'MANAGES']
RETURN type(r) AS type, count(*) AS n ORDER BY n DESC;

// ── 1. The join: Estonian LEI companies whose GLEIF record meets the
//       register's own record on the registry code (expect 28,729 of the
//       29,246 EE companies in the GLEIF layer).
MATCH (g:Entity {sourceDescription: 'GLEIF'})-[:HAS_IDENTIFIER]->
      (:Identifier {scheme: 'EE-ARIREGISTER'})<-[:HAS_IDENTIFIER]-(a:Entity)
WHERE a.sourceDescription STARTS WITH 'Estonian'
RETURN count(DISTINCT g) AS joined_lei_companies;

// ── 2. Beneficial-owner coverage: LEI companies whose register record names
//       at least one beneficial owner (expect 25,370).
MATCH (p:Person)-[c:CONTROLS|OWNS]->(a:Entity)-[:HAS_IDENTIFIER]->
      (:Identifier {scheme: 'EE-ARIREGISTER'})<-[:HAS_IDENTIFIER]-(g:Entity {sourceDescription: 'GLEIF'})
WHERE c.beneficialOwnershipOrControl = true AND a.sourceDescription STARTS WITH 'Estonian'
RETURN count(DISTINCT g) AS lei_companies_with_bo;

// ── 3. Where GLEIF's knowledge ends and the register's begins: companies
//       that filed a NATURAL_PERSONS reporting exception with GLEIF, and how
//       many of them the register names an owner for (expect 15,497 / 13,662).
MATCH (:Person {name: 'Natural person(s) (GLEIF reporting exception)'})-->(g:Entity)
WITH DISTINCT g
MATCH (g)-[:HAS_IDENTIFIER]->(:Identifier {scheme: 'EE-ARIREGISTER'})<-[:HAS_IDENTIFIER]-(a:Entity)
WHERE a.sourceDescription STARTS WITH 'Estonian'
OPTIONAL MATCH (p:Person)-[c:CONTROLS|OWNS]->(a)
WHERE c.beneficialOwnershipOrControl = true
WITH g, count(DISTINCT p) AS bos
RETURN count(g) AS natural_person_exceptions,
       count(CASE WHEN bos > 0 THEN 1 END) AS with_named_beneficial_owner;

// ── 4. GLEIF reporting exceptions by reason (expect natural persons 31,350;
//       no known controlling person 14,469; non-consolidating parent 11,115;
//       parent without an LEI 527; undisclosed parent 278). Each exception is
//       its own unknown party, one per company and kind (direct / ultimate).
MATCH (u {sourceDescription: 'GLEIF'})-[]->(:Entity)
WHERE u.name ENDS WITH '(GLEIF reporting exception)'
RETURN u.name AS reason, count(*) AS n ORDER BY n DESC;

// ── 5. Deepest current shareholding chains (expect several of 7 levels, e.g.
//       … → Combiwood Grupp OÜ → Combimill Sakala OÜ → Combimill Reopalu OÜ →
//       aktsiaselts TOFTAN, and … → Scandium Energia AS → … → Bolt Holdings OÜ
//       → Diamond Mobility Services EE OÜ). A company holding its own shares
//       is a self-loop in the register's lists, left out here.
MATCH path = (top:Entity)-[:OWNS*3..10]->(bottom:Entity)
WHERE NOT (:Entity)-[:OWNS]->(top)
  AND all(r IN relationships(path) WHERE r.recordStatus <> 'closed' AND startNode(r) <> endNode(r))
RETURN [n IN nodes(path) | n.name] AS chain, length(path) AS depth
ORDER BY depth DESC LIMIT 5;

// ── 6. Cross-border GLEIF edges: Estonian companies and their foreign
//       consolidating parents, or foreign companies consolidated by an
//       Estonian one (expect 156 direct, 169 indirect; FI, SE, DE, LV lead).
MATCH (parent:Entity {sourceDescription: 'GLEIF'})-[r:CONTROLS]->(child:Entity {sourceDescription: 'GLEIF'})
WHERE (parent.jurisdictionCode = 'EE') <> (child.jurisdictionCode = 'EE')
RETURN r.directOrIndirect AS kind, count(*) AS edges;

// ── 7. Busiest people on the register's current lists (expect Roberto De
//       Silvestri 50, Annika Madissoon 27, Rando Saarepere 21). A name the
//       register files with a Nasdaq CSD account prefix ("Omanikukonto:") is
//       shown as filed.
MATCH (p:Person)-[r:OWNS|CONTROLS|MANAGES]->(e:Entity)
WHERE p.sourceDescription STARTS WITH 'Estonian' AND r.recordStatus <> 'closed'
RETURN p.name AS name, count(DISTINCT e) AS companies
ORDER BY companies DESC LIMIT 10;

// ── 8. One group in full: the Swedbank cluster (GLEIF consolidation,
//       register shareholders, boards and owners in one subgraph).
MATCH (e:Entity) WHERE e.name CONTAINS 'Swedbank'
OPTIONAL MATCH (e)-[r]-(n)
RETURN e, r, n LIMIT 200;

// ── 9. Share bands → one weight: keep both ends, choose per question.
//       Shares load as numbers. A register that publishes bands gives a
//       range, not a figure; the Estonian register files exact figures, so
//       here all three readings coincide. An edge with no stated share has
//       no weight: leave it null, never 0 — 0 says "holds nothing".
MATCH (o)-[r:OWNS]->(c:Entity)
WHERE r.recordStatus <> 'closed'
WITH coalesce(r.shareExact, r.shareMinimum) AS lower,
     coalesce(r.shareExact, r.shareMaximum) AS upper
RETURN CASE WHEN lower IS NULL AND upper IS NULL THEN 'no stated share'
            WHEN lower = upper THEN 'exact'
            ELSE 'band' END AS kind,
       count(*) AS edges,
       avg(CASE WHEN lower IS NOT NULL AND upper IS NOT NULL THEN (lower + upper) / 2 END) AS mean_midpoint
ORDER BY edges DESC;
// expect exact 41,228 (mean ≈ 73.4); no stated share 32,113.

// ── 10. The same holders, two answers — say which one you used.
UNWIND [25, 50] AS threshold
MATCH (o)-[r:OWNS]->(c:Entity)
WHERE r.recordStatus <> 'closed'
WITH threshold,
     coalesce(r.shareExact, r.shareMinimum) AS lower,
     coalesce(r.shareExact, r.shareMaximum) AS upper
RETURN threshold,
       count(CASE WHEN lower >= threshold THEN 1 END) AS at_least_on_lower_bound,
       count(CASE WHEN upper >= threshold THEN 1 END) AS possibly_on_upper_bound,
       count(CASE WHEN lower IS NULL AND upper IS NULL THEN 1 END) AS no_stated_share
ORDER BY threshold;
// expect 25%: 35,042 / 35,042; 50%: 32,364 / 32,364 — exact figures, no gap.

// ── 11. Ownership history the files do carry: founders no longer holding
//        shares (closed OWNS edges, no end date published; expect 8,569).
MATCH (f)-[r:OWNS]->(c:Entity)
WHERE r.recordStatus = 'closed'
RETURN count(*) AS former_founders, count(DISTINCT c) AS companies;
