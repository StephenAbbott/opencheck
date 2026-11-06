# Loading the Azerbaijani Laundromat companies into Neo4j

I wrote `laundromat_to_neo4j.py`. It has two stages. `fetch` calls the OpenCheck MCP server for each LEI and caches the responses to disk. `load` reads only that cache and writes the graph to Neo4j, so you can re-run it without spending lookups.

**I have not run the fetch stage against the live server.** The script's MCP handshake and response parsing are written from the protocol spec, not tested against OpenCheck. Run it on one LEI first (`--limit 1`) to confirm it works. The load stage was tested offline against the recorded `opencheck_lookup` and `opencheck_export_bods` responses in `/home/claude/fixtures`. Those fixtures cover a few companies (Bentcard, Persia, Shell and others), not your 200, so I couldn't check the full set.

## Before you run it

1. **Budget.** Each LEI costs two fresh checks (`opencheck_lookup` and `opencheck_export_bods`). At ten fresh checks a minute, 200 companies take about **40 minutes**. Use `--skip-bods` for a lookup-only pass (about 20 minutes), but that gives you companies with no ownership edges. The cache means a crash or a stop loses nothing.
2. **Use the bulk release for the full graph.** OpenCheck's guidance for hundreds of companies is the published bulk datasets, not live loops. The Azerbaijani Laundromat release is at https://github.com/StephenAbbott/opencheck/releases. For a live demo, the script works on a sample of 20–30 LEIs.
3. **No LEI, no lookup.** `opencheck_lookup` needs an LEI. Small companies often have none, and I haven't checked how many of your 200 do. The script writes those rows to `rejected_rows.json`, and they need `opencheck_register_lookup` by register number. I didn't add that, since it's a different call with its own shape.
4. **You need the LEI list.** I don't have it. Put it in a CSV with a `lei` column, or one LEI per line.

## Run it

```bash
pip install neo4j                       # only for the load stage

# 1. Fetch (live; resumable; ~40 min for 200 LEIs with BODS)
python3 laundromat_to_neo4j.py fetch --leis leis.csv --cache ./cache --limit 1   # check one first
python3 laundromat_to_neo4j.py fetch --leis leis.csv --cache ./cache

# 2. Check the counts without Neo4j
python3 laundromat_to_neo4j.py load --cache ./cache --dry-run

# 3. Load
export NEO4J_PASSWORD=...
python3 laundromat_to_neo4j.py load --cache ./cache --uri bolt://localhost:7687 --user neo4j
```

## Graph model

| Node | Key | Source |
|---|---|---|
| `Company` | `lei` | `opencheck_lookup` (name, status, verdict, licensing) and the BODS subject record |
| `Signal` | `lei` + `code` + `kind` | `risk_signals` (`kind = risk` is a finding; `context` is not) |
| `Entity` (also `Party`) | `record_id` | BODS entity statements, e.g. owning companies |
| `Person` (also `Party`) | `record_id` | BODS person statements |

| Relationship | Properties |
|---|---|
| `(Company or Party)-[:HAS_INTEREST]->(Company or Party)` | `types`, `direct_or_indirect`, `beneficial`, `details`, `share`, `source`, `retrieved_at` |
| `(Company)-[:HAS_SIGNAL]->(Signal)` | |

Things to keep in mind when you read the graph:

- **LEI status is not company status.** `lei_status` is the LEI record. `register_liveness = "declared"` means the last value GLEIF holds, not a current register reading. Many Azerbaijani LEIs are lapsed, as Bentcard's is (renewal due 2015). Its `register_sentence` property says so.
- **Azerbaijan publishes little.** OpenCheck holds no register notes for Azerbaijan. An owner missing from the graph means nothing about what Azerbaijan publishes. Don't read an empty graph as a clean result.
- **Degraded screens are incomplete.** `degraded` and `degraded_sources` are on each company. A company with `degraded = true` was not fully screened.
- **Placeholders.** Persons with `placeholder = true` (e.g. "Natural person(s) (GLEIF reporting exception)") are not individuals. GLEIF reports that the company has natural-person controllers that are not named.
- **Duplicate entities across sources.** In the Persia export, the same company appears as two entity records: one from GLEIF and one from OpenSanctions, with different `record_id`s. Count companies by `lei`, not by entity nodes, and consider merging duplicates by name and jurisdiction before you analyse ownership.
- **Shared owners.** The script merges parties on `record_id`. I have not checked whether OpenCheck gives the same owner the same `record_id` across different companies' exports. Test this with two companies that share an owner before you trust cross-company paths.
- **Skipped edges.** `load` prints how many relationships were dropped because an endpoint was missing from the export. That is normally zero for a complete export.

## Licensing

The graph inherits the licences of its sources, and some are non-commercial:

- **OpenSanctions** and **EveryPolitician** are CC-BY-NC-4.0. Non-commercial only.
- **OpenCorporates** is share-alike: derivative works need the same licence.
- **OpenAleph** is per-collection. **EITI** has its own content-use policy.
- **GLEIF** is CC0. **Climate TRACE** and **Wikirate** are CC-BY-4.0, and **PermID** is CC-BY 4.0. Both need attribution.

Each `Company` node carries `licensing_headline`, `commercial_use`, `licensing_warnings` and `license_notices`. Keep those when you show the graph. For a masterclass, teaching use is fine. Don't publish or sell the graph without removing the non-commercial sources and giving attribution.

## Useful queries

```cypher
// Who owns what, directly, with the figures we have
MATCH (a)-[h:HAS_INTEREST]->(c:Company)
RETURN c.name, a.name, labels(a), h.types, h.share, h.beneficial
ORDER BY c.name;

// Companies with findings (risk only, not context)
MATCH (c:Company)-[:HAS_SIGNAL]->(s:Signal {kind: "risk"})
RETURN c.name, c.lei, collect(s.code) AS risks ORDER BY size(risks) DESC;

// Companies whose LEI has lapsed, so their data may be stale
MATCH (c:Company) WHERE c.lei_status = "LAPSED"
RETURN count(c) AS lapsed_lei_companies;

// Companies not fully screened
MATCH (c:Company) WHERE c.degraded = true
RETURN c.name, c.degraded_sources;

// Owners shared by more than one company in the set
MATCH (p)-[:HAS_INTEREST]->(c:Company)
WITH p, collect(DISTINCT c.name) AS companies WHERE size(companies) > 1
RETURN p.name, companies ORDER BY size(companies) DESC;
```

## Files

- `/home/claude/ws/iteration-2-haiku/bulk-exploration-goes-to-datasets/without_skill/outputs/laundromat_to_neo4j.py`: the script
- `/home/claude/ws/iteration-2-haiku/bulk-exploration-goes-to-datasets/without_skill/outputs/answer.md`: this note

The script sends `User-Agent: opencheck-bulk-loader/0.1` and no personal contact details.
