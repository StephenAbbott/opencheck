# Bundle archiving & graph-tools evaluation

*2026-07-03 — feeds the Notion ticket "Demo: Query all LEIs issued in a country and convert to Neo4J" and the QuickCheck/FullCheck thinking.*

## 1. Should every OpenCheck BODS bundle be timestamped and archived?

**Yes, but as a private-first "bundle ledger", not a public accumulating dataset.** The distinction matters for licensing, GDPR, and dataset quality.

### The case for

- **Near-zero marginal cost.** Completed pipeline runs already exist as a single object (the thing `_REPLAY_CACHE` holds). Persisting each `done` run as gzipped BODS JSON-Lines is one hook in `_lookup_pipeline()`. A bundle is tens of KB gzipped; even thousands of lookups/month is megabytes.
- **Statement IDs make it diff-friendly.** `_stable_id()` is deterministic, so two snapshots of the same entity dedup naturally and `diff` shows real change — a free change-detection layer that complements the Time Machine (which only covers registers exposing history APIs).
- **Provenance/audit.** For a CDD tool, "what did OpenCheck say about entity X on date Y, from which source versions" is valuable in itself.

### The caveats

1. **Licensing forbids archiving everything publicly.** Per the licence matrix: OpenSanctions and EveryPolitician are CC-BY-NC; OpenCorporates is conditional + share-alike; OpenAleph is per-collection; BCE/KBO and CNPJ are conditional. A public archive must be filtered to Commercial=yes sources — reuse `opencheck.licensing.assess` at write time, exactly as exports do. INPI BO-flagged data is already never emitted, so no new risk there.
2. **GDPR.** Person statements are personal data. Accumulating them into a persistent, growing, downloadable corpus makes you a de facto BO register operator, with accuracy/erasure obligations that clash with keeping stale snapshots forever. Open Ownership does publish person data in bulk, so it's not unprecedented — but they have a legal framework behind it. Mitigation for now: keep the ledger private; anything published (e.g. the country demo dataset) is a curated, licence-filtered, dated release, not a rolling feed.
3. **Passive accumulation gives a biased dataset.** Bundles arrive in whatever order users happen to look things up — good for provenance and change detection, poor as an analytical dataset. Coherent datasets should come from deliberate sweeps (§2), with the ledger as the storage substrate.
4. **Freshness framing.** OpenCheck's pitch is up-to-date data. Every archived bundle must carry an envelope (`lei`, `retrievedAtUtc`, `sources` + per-source status, licence verdict, OpenCheck version) and be presented as a snapshot, never as current truth. BODS `statementDate` / `publicationDetails.publicationDate` already carry the in-band dates.

### Recommended mechanism

- **Storage: Cloudflare R2 over S3 or GitHub** for the ledger — 10 GB free, zero egress fees (relevant on no budget), S3-compatible API. Key scheme: `bundles/{lei}/{retrievedAtUtc}.jsonl.gz` + `bundles/{lei}/{retrievedAtUtc}.manifest.json`. Skip a write when the licence-filtered statement set is identical to the previous snapshot (compare sorted statementId hashes).
- **GitHub for curated public releases only** — the country demo dataset, licence-filtered, with `LICENCES.md`, as dated releases. Git history on a rolling raw ledger would bloat fast and makes GDPR erasure awkward (history rewrite); don't put the ledger itself in git.
- Ship it behind an env flag (`OPENCHECK_BUNDLE_LEDGER_URL` unset = off) so Render free tier and local dev are unaffected.

## 2. Country-sweep demo feasibility (Notion ticket)

Active LEI counts, April 2026 (GLEIF statistics API), for small(ish) jurisdictions:

| Jurisdiction | Active LEIs | OpenCheck adapter? |
|---|---|---|
| Faroe Islands | 477 | no |
| Croatia | 2,657 | yes (sudreg) |
| Latvia | 3,795 | yes (ur_latvia) |
| Slovenia | 4,588 | no |
| Lithuania | 4,761 | yes (jar) |
| Iceland | 5,304 | no |
| Malta | 5,896 | yes (mbr) |
| Slovakia | 6,866 | yes (rpo/rpvs) |
| Liechtenstein | 12,212 | no |
| Estonia | 27,529 | yes (ariregister) |

Two things make this cheaper than it first looks:

- **The GLEIF layer needs no API calls at all.** GLEIF Golden Copy (L1 + L2) is a free CC0 bulk download, and Open Ownership already republishes it as BODS — the same source as `data/demo/`. The spike question ("does a country's LEI population form a connected graph?") can be answered entirely from bulk files: filter by jurisdiction, load L2 edges, run connected components. Zero API budget.
- **Don't run the full 17-source pipeline per entity.** For a sweep, restrict to GLEIF (bulk) + the national register adapter. Croatia: 2,657 entities × 1 register call at 1 req/s ≈ 45 min. Even Estonia (27.5k, public scraper — be polite, ~0.5 req/s) is a weekend job. The full pipeline (sanctions cross-checks, OpenAleph, etc.) at 2,657 entities would burn keyed-API quotas and produce mostly-NC-licensed data you couldn't publish anyway.

**Recommended target: Croatia or Latvia** — smallest populations with adapters. Estonia is tempting (richest adapter, shareholder history) but is 10× Croatia's size and its BO access changes 10 July 2026. Output lands in the ledger (§1), then the existing `bods-neo4j` CSV path produces the Neo4j demo — no new graph infrastructure needed.

## 3. Graph tools: what to learn or borrow

None of the three changes the parked AuraDB/embedded-DB decision (2026-06-07) — the revisit triggers haven't fired. But each validates or sharpens a direction.

### Omnigraph (Modern Relay) — omnigraph.dev

Rust typed property-graph engine on **Lance/Arrow columnar files + DataFusion**, stored on local disk or any S3 bucket ("lakehouse graph", no DB server). Git-style branches, atomic commits, three-way merge, time-travel snapshot reads; hybrid graph+BM25+vector retrieval fused with RRF; own DSL (not Cypher); MIT. **Extremely early: 5 stars, 33 commits, version numbering inconsistent between repo and site.**

*Borrow:* the **commit/snapshot mental model is exactly the bundle ledger** — timestamped immutable snapshots, diffable, time-travel reads. Its graph-on-object-storage architecture also validates the DuckDB-not-Neo4j stance: columnar files on cheap storage, query engine on top, no server. The RRF hybrid-ranking idea is relevant to OpenAleph match scoring someday. *Don't adopt:* pre-adoption maturity, custom query language, new Rust binary in the stack.

### MemGQL (Memgraph Zero)

**Not an embedded database** — a federated ISO-GQL query engine (Rust server, Bolt protocol) that translates GQL to backend-native Cypher/SQL across Memgraph, Neo4j, PostgreSQL, **DuckDB**, ClickHouse, etc. Relational backends are mapped via a JSON file (node labels → tables, edge types → association tables; quantified paths → recursive CTEs). Community tier free but capped at 4 connectors, no support; v0.1→v0.5 between March and May 2026, visibly pre-1.0.

*Borrow:* the **mapping-file pattern** — a declarative graph view over relational storage. If the Phase 8 revisit trigger ever fires (2 s median multi-hop latency on DuckDB), the answer may be "BODS statements → DuckDB tables + recursive CTEs behind a small mapping layer", imitated in Python, rather than adopting either a graph DB or MemGQL itself. Its DuckDB connector is also a concrete way users could run GQL over an OpenCheck export without Neo4j — worth a docs mention once it stabilises. *Don't adopt* as a dependency: another v0.x server container, exactly what the 2026-06-07 decision parked.

### RushDB

Source-available (platform Elastic License 2.0, SDKs Apache 2.0) "push any JSON, get a graph" layer **on top of Neo4j** (requires Neo4j + APOC to self-host). Auto-infers schema on write via its LMPG model (properties become first-class nodes), one JSON `SearchQuery` shape for filter+traversal+aggregation+vector, TS/Python SDKs, MCP server. ~309 stars, active, small team.

*Borrow:* the **takeaway experience, not the engine**. RushDB's core insight is that JSON→queryable-graph should be one command with zero schema work. OpenCheck exports are BODS JSON-Lines that currently require the user to know the `bods-neo4j` pipeline. Shipping each export with a tiny self-contained loader (script + docker-compose, or a documented RushDB/Neo4j one-liner) would close the "take away and analyse in other tools" gap cheaply. Caveat: RushDB's automatic edge inference is containment-based (nested JSON); BODS statements are reference-linked by statementId, so its magic ingestion would *not* reconstruct ownership edges — explicit `attach` calls or the existing CSV path are still needed. Its schema-as-data idea is worth copying into export manifests (statement counts, interest types present, sources, licence verdict) so datasets are self-describing.

### Cross-cutting observation

All three ship an MCP server and pitch at AI agents. OpenCheck already has one — that's the right bet; the differentiator to press is that OpenCheck's graph is **standards-based (BODS/BOVS) with provenance and licensing built in**, which none of these general-purpose tools offer.

## 4. Suggested next actions

1. Spike the connected-graph question from GLEIF/Open Ownership bulk files for Croatia (no API cost, answers the Notion sub-ticket).
2. Add the bundle ledger hook behind an env flag, writing licence-filtered JSON-Lines + manifest to R2.
3. Run the Croatia sweep (GLEIF bulk + sudreg only), publish as a dated, licence-filtered GitHub release, convert via `bods-neo4j` for the demo.
4. Add an "analyse this export" docs page: bods-neo4j path, DuckDB recipe, and (when mature) MemGQL-over-DuckDB as options.
