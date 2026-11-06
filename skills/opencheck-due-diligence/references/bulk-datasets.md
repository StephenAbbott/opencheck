# OpenCheck bulk datasets — for anything bigger than a batch

Live lookups are for one company at a time (or 20 with
`opencheck_batch_lookup`). The server's lookup budget is ten fresh checks a
minute per address, shared by everyone behind that address. To explore
hundreds or thousands of companies, download a published dataset and work
offline. It is faster, it does not depend on the server, and the result is
reproducible because the file is dated.

All datasets are GitHub release assets:
https://github.com/StephenAbbott/opencheck/releases

| Release | What it is | Files |
|---|---|---|
| `dataset-azerbaijani-laundromat-2026-10-06` | The companies and banks named in the OCCRP *Azerbaijani Laundromat* wire transfers (curated in DerwenAI/azeri_laverie), each run through OpenCheck's full pipeline: LEI subjects by lookup plus their GLEIF subsidiary network, UK companies by a Companies House register walk | `.bods.jsonl.gz` (BODS v0.4) · `.nq.gz` (RDF, licence per statement) · `.ftm.jsonl.gz` (FollowTheMoney) · `.senzing.jsonl.gz` (Senzing records) · `.neo4j.zip` (CSV + Cypher) · `.signals.jsonl` (risk signals per subject) · `.subjects.csv` (one row per subject, incl. degraded and partial-network flags) · `manifest.json` · `LICENSES.md` |
| `dataset-estonia-2026-10-25` | Every Estonian LEI holder's GLEIF record joined to the e-Business Register's own record — 348,794 statements, with shareholders, beneficial owners, officers and upward ownership chains | `.jsonl.gz` (BODS) · `.nq.gz` (RDF) · `-neo4j.zip` · `manifest.json` · `register-missing.csv` · `LICENCES.md` |
| `meip-bods-2024` | The OECD-UNSD Multinational Enterprise Information Platform Global Register (31 Dec 2024) as the OECD's own BODS v0.4, packed as SQLite | `meip.sqlite.gz` |

Older dated releases of the same datasets exist; use the newest.

## Licences

- **Laundromat: non-commercial.** It includes OpenSanctions and
  EveryPolitician data (CC-BY-NC-4.0). Read its `LICENSES.md` before any reuse;
  the NQuads carry a licence per statement so you can filter.
- **Estonia:** CC-BY-4.0 (register) and CC0 (GLEIF) — attribute the e-Business
  Register.
- **MEIP:** OECD terms, attribution.

## Reading the laundromat bundle

- `subjects.csv` is the place to start: one row per seed subject, with how it
  was reached (LEI lookup or register hop), counts, and flags. A subject with a
  degraded screen or `subsidiaries_partial=true` was not fully covered — a `0`
  there is a floor, not a fact.
- `signals.jsonl` holds every risk and context signal per subject, with the
  same `kind`, `confidence` and evidence as a live lookup. The reading rules in
  SKILL.md apply unchanged: name-only matches are possible matches, context is
  not a finding, nothing is summed across sources.
- Entities the source thesaurus identified only by a Russian tax id or an
  Azerbaijani name have no register OpenCheck reads; they are counted as
  skipped in `manifest.json` and are not in the bundle.

## Loading

```bash
# Python: stream BODS statements
python3 - <<'EOF'
import gzip, json
by_record = {}
edges = []
with gzip.open("azerbaijani-laundromat-2026-10-06.bods.jsonl.gz", "rt") as f:
    for line in f:
        s = json.loads(line)
        if s["recordType"] == "relationship":
            edges.append(s)
        else:
            by_record[s["recordId"]] = s   # relationships reference recordId
print(len(by_record), "parties,", len(edges), "relationships")
EOF
```

- Neo4j: unzip the `neo4j` archive; it holds the node and edge CSVs
  (`entity.csv`, `person.csv`, `owns.csv`, `controls.csv`, `manages.csv`,
  `unspecified_party.csv`, …) with `import.sh` / `import.cypher` to load them
  (the `bods-neo4j` layout). The Estonia release also ships
  `demo_queries.cypher`.
- Senzing: load the `.senzing.jsonl.gz` records directly; each carries
  `DATA_SOURCE` and its licence.
- FollowTheMoney: `ftm` CLI / OpenAleph / followthemoney tooling.
- RDF: any triple store; the BODS RDF vocabulary is Open Ownership's.

## When a live lookup is still right

After the offline pass narrows the set, a live `opencheck_lookup` on a handful
of companies gives today's view (the datasets are dated). Spread them out; a
repeat of the same lookup within 15 minutes is free.
