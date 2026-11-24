# Querying an OpenCheck graph in Neo4j, Memgraph or Relate

Read this before you write Cypher against an OpenCheck graph. A graph query
answers in confident rows, and the rows are only as honest as the `WHERE`
clause. Every rule in the main skill (per source, never added; consolidation
is not ownership; ended is not current; a name is not an identity) has to be
written into the query, because the database will not apply it for you.

There are two graphs, and they are shaped differently.

| | OpenCheck `cypher` export | The bulk datasets' `neo4j` zip |
|---|---|---|
| Where | `GET /export?format=cypher`, the FullCheck network zip | the Estonia and Azerbaijani Laundromat releases |
| Built by | `opencheck/bods/neo4j.py` | `bods-neo4j` (`to-csv`, or `to-cypher` for an APOC-free script) |
| Nodes | `:Entity`, `:Person`, `:UnspecifiedParty`, keyed on `id` (a statementId) | `:Entity`, `:Person`, `:UnspecifiedParty`, `:Identifier`, `:Address`, `:Country`, keyed on `recordId` |
| Edges | one `OWNS_OR_CONTROLS` per relationship **statement** | one edge per **interest**: `OWNS`, `CONTROLS`, `MANAGES`, `IS_PARTY_TO`, `HAS_OTHER_INTEREST` |
| Source on the edge | `r.sourceId`, `r.source`, `r.license` | `r.sourceDescription`, `r.publisherName` |
| Ended | `r.ended` (judged on `r.endedAsOf`), `r.endDate` | `r.endDate`, `r.recordStatus = 'closed'` |
| Not ownership | `r.notOwnership = true`, `r.basis` | read `r.sourceDescription` (GLEIF, OECD-UNSD MEIP) |

The edge properties of the export are listed in `docs/data-dictionary.md`
under "Cypher edge properties".

## Loading

- The export is plain Cypher: no APOC, no `LOAD CSV`. On Neo4j 5 run it with
  `cypher-shell -f network.cypher`; Relate (1.2.0) runs it as written. Its
  three `CREATE INDEX` lines are Neo4j syntax: on Memgraph delete them first.
- The bulk zips' `import.cypher` needs APOC (`apoc.convert.fromJsonList`) and
  fails on a Neo4j without it, and on Relate. Rebuild the graph from the
  release's `.bods.jsonl.gz` with `bods-neo4j to-cypher` instead: same graph,
  core Cypher only (`--dialect memgraph` for Memgraph). For a large graph on
  Neo4j, `bods-neo4j to-neo4j` (the driver) is faster and lighter. For Relate
  the releases carry a ready-made `.rl.gz` (gunzip, then open); Estonia's
  takes about 13 s to open, because Relate holds the whole graph in memory.
- In the bulk graphs a GLEIF reporting exception ("Natural person(s)", "No
  known controlling person") is a `:Person` or `:Entity` node, not
  `:UnspecifiedParty`, and it is the widest hub in the Estonia graph. Leave
  it out of "who are the people" queries by its `name`, and report it as a
  filing.
- **Relate** (Neo4j Labs' embedded engine) sends usage telemetry by default.
  Turn it off before you open anything a user would not want reported, and
  say that you did. It is a Labs project: fine for exploring a published
  dataset, not a system of record.

## Rules, as `WHERE` clauses

**1. Current only, unless the question is history.**

```cypher
// export
MATCH (o)-[r:OWNS_OR_CONTROLS]->(c:Entity) WHERE NOT r.ended ...
// bods-neo4j
MATCH (o)-[r:OWNS|CONTROLS]->(c:Entity)
WHERE r.endDate IS NULL AND coalesce(r.recordStatus, '') <> 'closed' ...
```

When you do include ended edges, return `r.endDate` and say "formerly".

**2. GLEIF Level 2 and MEIP are not ownership.** Leave them out of any
ownership question, and when you show them, call them consolidation or group
membership.

```cypher
MATCH (p)-[r:OWNS_OR_CONTROLS]->(c) WHERE NOT coalesce(r.notOwnership, false) ...
```

**3. Officers are not owners.** In the export, `r.category = 'role'` (and
`r.officerPosition`) is a director, secretary or similar. In `bods-neo4j`,
`MANAGES` is a role. Neither belongs in an ownership answer.

**4. Never add shares across sources, or across one statement's interests.**
A `sum()` over edges from two sources adds two readings of the same holding.
In `bods-neo4j` one statement with a shareholding and voting rights is two
edges with the same `statementId`, so a `sum()` counts it twice. Group by
source and show the sources side by side:

```cypher
MATCH (o)-[r:OWNS_OR_CONTROLS]->(c:Entity {id: $subject})
WHERE NOT r.ended AND NOT coalesce(r.notOwnership, false) AND r.category <> 'role'
RETURN o.name AS holder, r.sourceId AS source,
       r.shareExact AS exact, r.shareMin AS min, r.shareExclusiveMin AS exclusiveMin,
       r.shareMax AS max, r.shareInterest AS interest
ORDER BY holder, source
```

A figure through a chain is OpenCheck's integrated ownership
(`opencheck_owners`), computed per source; do not rebuild it by multiplying
along a Cypher path.

**5. Bands are bands.** `shareMin`/`shareMax` (or the exclusive bounds) are a
range from the register: say "more than 25% and up to 50%", never the
midpoint. A missing share is unknown, not zero.

**6. An unspecified party is a finding, not a person.** `:UnspecifiedParty`
is an owner the source did not disclose, with its reason. Count it and report
the reason; never join through it, and leave it out of "who are the people"
queries with `WHERE NOT n:UnspecifiedParty`.

**7. Same name, two records.** Two `:Person` nodes named alike are two
records. Join on an identifier (the `:Identifier` nodes in `bods-neo4j`, the
`lei` property in the export), never on `name`. A name match is a lead to
check with `opencheck_person_check`, not an identity.

**8. `undated` means no source gave a date.** `r.undated = true` is not
"current since forever"; say the dates are unknown.

**9. Licences travel with the rows.** Some sources are CC BY-NC
(OpenSanctions). Return `r.license` (export) or check the release's
`LICENSES.md`, and pass the notice on.

## Bounded paths

Ownership graphs have cycles and very wide hubs (a registered agent, a
nominee, a state). Always bound a variable-length path and cap the rows:

```cypher
MATCH path = (o)-[:OWNS_OR_CONTROLS*1..4]->(c:Entity {id: $subject})
WHERE all(r IN relationships(path)
          WHERE NOT r.ended AND NOT coalesce(r.notOwnership, false)
                AND r.category <> 'role')
RETURN [n IN nodes(path) | n.name] AS chain,
       [r IN relationships(path) | r.sourceId] AS sources
LIMIT 50
```

A chain whose edges come from different sources is a joined reading, not one
source's statement: say which source each link is from.

## Hubs are a finding to explain, not a conclusion

A node with hundreds of edges is often a formation agent, a nominee service
or a register placeholder. Report the degree and what the node is; do not
call it an ultimate owner on degree alone.

```cypher
MATCH (n)-[r:OWNS|CONTROLS]->()
WHERE r.endDate IS NULL
RETURN labels(n) AS kind, n.name AS name, count(r) AS companies
ORDER BY companies DESC LIMIT 20
```
