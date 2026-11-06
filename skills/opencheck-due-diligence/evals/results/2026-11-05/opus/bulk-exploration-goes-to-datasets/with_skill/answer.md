I wouldn't loop `opencheck_lookup` over 200 companies. The work you want has already been done and published as a dataset that loads straight into Neo4j, so the script below downloads that instead.

**Why not 200 live lookups**
- **The budget is shared.** OpenCheck allows ten fresh lookups a minute per IP address, and everyone on the masterclass Wi-Fi shares that address. 200 lookups would take at least 20 minutes for one person even with the room to yourself. With other people running checks, you'd mostly get 429s.
- **You'd end up with less data.** `opencheck_lookup` returns a summary: identity, owners, signals. Getting a graph would also take an `opencheck_export_bods` call per company, which is another 200 calls from the budget.
- **The dataset is already built for this.** The release `dataset-azerbaijani-laundromat-2026-10-06` ran every company and bank in the OCCRP Laundromat wire transfers through OpenCheck's full pipeline. That covers LEI lookups plus each company's GLEIF subsidiary network, and a Companies House walk for the UK companies. It ships with a `.neo4j.zip` (node/edge CSVs plus `import.sh` / `import.cypher`). It's also dated, so everyone in the room explores the same graph.

**Files**
- `load_laundromat.sh` downloads the release from https://github.com/StephenAbbott/opencheck/releases using `gh`, or `curl` + `jq` if `gh` isn't installed. It shows the licence, unzips the Neo4j bundle, runs the bundle's own importer and prints node and relationship counts.
  ```bash
  NEO4J_PASSWORD=... bash load_laundromat.sh
  # if the bundle uses LOAD CSV file:/// paths, also set NEO4J_IMPORT_DIR=/path/to/neo4j/import
  ```
- `starter_queries.cypher` has queries to start exploring: schema, the most connected nodes, shareholding chains, people linked to several companies, shared addresses, and withheld/unknown parties. It also notes how to read what you find.
- `refresh_live.py` (optional) gets today's view of companies you pick out in Neo4j. It makes one `opencheck_batch_lookup` call for up to 20 LEIs and waits `retry_after_s` before retrying once on a 429. Run it only on a shortlist, never on the whole set.

**What I haven't tested.** I had no network access, so none of this has run against the real release. I wrote it from the release's documented layout. I didn't check the exact file names inside the zip or the label and property casing in Neo4j. The loader finds `import.sh` / `import.cypher` wherever they are, but run query 0 in `starter_queries.cypher` first and adjust label and property names if they differ. The Python script parses cleanly. I couldn't syntax-check the shell script here.

**Check first: not every company may be in it**
- **Some companies were skipped.** Companies the source data only identifies by a Russian tax ID or an Azerbaijani name have no register OpenCheck can read. They aren't in the bundle; `manifest.json` gives the count. If your list of ~200 includes them, you'll have fewer nodes than companies.
- **Some coverage is partial.** `subjects.csv` has one row per company. A row flagged as degraded, or with `subsidiaries_partial=true`, wasn't fully covered, so its zero counts are a floor, not a fact.

**How to read the graph**
- **GLEIF parent/child edges aren't ownership.** They mean one company consolidates another in its accounts, and they carry no percentages.
- **Officer roles aren't control.** Positions like "Secretary" or "Authorised person" are stored as control-type interests, but they're roles.
- **Don't add percentages together.** Figures from different sources, or PSC bands (25–50% is a range), never sum.
- **Ended links are former links.** A record that is closed or has a past `endDate` describes a former owner or role.
- **Context isn't a finding.** In `signals.jsonl`, `kind: context` (e.g. `NON_EU_JURISDICTION`) is description. Only `kind: risk` is a finding.
- **A name match isn't an identity.** "Possible name match only" is a lead to check.
- **No grading.** Present what the records show rather than calling companies "high-risk" or "clean".

**Licence.** The dataset is non-commercial only. It includes OpenSanctions and EveryPolitician data (CC-BY-NC-4.0). Tell attendees, and point them to `LICENSES.md` before they reuse anything.
