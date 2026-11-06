I'd advise against calling `opencheck_lookup` 200 times in the room. Instead, load OpenCheck's published Azerbaijani Laundromat dataset straight into Neo4j. I've written the loader for that: **`load_bods_neo4j.py`**, which is next to this answer.

## Why not loop over live lookups

- **It's slow, and the whole room would share one limit.** Each `opencheck_lookup` counts against a lookup budget of 10 fresh checks a minute per IP address. 200 companies would take at least 20 minutes. Everyone on the venue wifi shares one public address, so if a few attendees run the script together, they all hit 429s.
- **Most of these companies can't be looked up this way.** `opencheck_lookup` needs an LEI. Most Laundromat companies are UK LLPs and LPs that never had one. Bentcard Import LLP shows the problem. Its LEI (`54930007FGRO3F0RZ382`) is lapsed, has been unrenewed since 2015, and is registered in Azerbaijan. The UK company itself is a different record: Companies House `OC369315`, which has no LEI. A loop over LEIs would quietly skip or misattribute most of the network.
- **The lookup result isn't a graph.** It's a summary: a verdict, risk signals and counts. For Bentcard it says "5 BODS statements (2 relationships)" but doesn't contain them. You would need a second call per company (`opencheck_export_bods`) to get the edges, which doubles the budget.
- **OpenCheck already publishes this dataset for this purpose.** Its own guidance says: "To explore hundreds, use the published bulk datasets (Estonia, the Azerbaijani Laundromat: https://github.com/StephenAbbott/opencheck/releases) rather than looping over live lookups." The dataset is BODS v0.4, the same format `opencheck_export_bods` returns, so the loader handles both.

## Run it

```bash
pip install neo4j

# 1. Get the Laundromat dataset from the releases page (check the exact asset name there)
gh release list -R StephenAbbott/opencheck
gh release download <laundromat-tag> -R StephenAbbott/opencheck -D data/

# 2. Check what's in it, without touching Neo4j
python load_bods_neo4j.py data/<asset> --dry-run

# 3. Load it
NEO4J_URI=bolt://localhost:7687 NEO4J_USER=neo4j NEO4J_PASSWORD=... \
  python load_bods_neo4j.py data/<asset>
```

It reads a JSON array, `{"statements": [...]}`, or JSON Lines, and can be gzipped. It also reads a saved `opencheck_export_bods` response. So if you want to add a few non-Laundromat companies live, export them one at a time (each export uses the same budget) and pass those files as extra arguments.

**What the graph looks like:**
- Each BODS record becomes one node, labelled `:Record` plus `:Entity`, `:Person` or `:Unspecified`.
- Identifiers become shared `:Identifier {key:"GB-COH:OC369315"}` nodes. That's how the same company found by two sources gets joined.
- Ownership and control are stored as `(party)-[:INTERESTED_IN]->(subject)` edges. Each edge carries the interest types, share figures, direct/beneficial flags, `source` and `licence_nc`.
- Parties that are referenced but never described get the `:Stub` label.

**Some queries to start the session with:**
```cypher
// Most-connected parties (typical nominee/agent hubs)
MATCH (p:Record)-[:INTERESTED_IN]->(c) RETURN p.name, count(c) AS n ORDER BY n DESC LIMIT 20;

// Companies that share a registered address
MATCH (a:Entity),(b:Entity) WHERE a.address = b.address AND id(a) < id(b)
RETURN a.address, collect(a.name)+collect(b.name) LIMIT 25;

// Ownership chains up to 4 hops from one company
MATCH path=(:Record)-[:INTERESTED_IN*1..4]->(c:Entity {name:'BENTCARD IMPORT LLP'}) RETURN path;
```

## Things to say in the room

- **Edges are one source's claim, not a finding.** Every edge carries `source`. Figures from two sources stay as separate edges, so don't add up shares across sources. A name that appears twice is a possible match, not a confirmed identity. Only `:Identifier` nodes join records.
- **An empty profile doesn't mean the company is clean.** OpenCheck holds no register notes for Azerbaijan, so a missing owner there tells you nothing.
- **Licences.** OpenSanctions and EveryPolitician data is CC-BY-NC-4.0. A masterclass is fine. For any commercial reuse, load with `--exclude-nc`, or filter on `licence_nc = false`.

## What I tested

I couldn't reach the network, so I haven't seen the Laundromat release asset itself. I've assumed it's BODS v0.4 in one of the layouts above, so run `--dry-run` on it first.

I ran the parser against a recorded `opencheck_export_bods` for Persia International Bank. It read 28 statements and produced 9 entities, 7 people, 12 relationships and 20 identifiers. With `--exclude-nc`, the 3 OpenSanctions relationships drop out.

The Neo4j write path hasn't been run: there's no Neo4j or `neo4j` driver here. Load into a scratch database first, before the session.
