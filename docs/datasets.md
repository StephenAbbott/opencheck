# Published datasets

OpenCheck is a lookup tool, but some of what it assembles is worth having in
bulk: a whole register, a whole reference database, or every company in one
investigation, run through the same pipeline and written out in every format
the `/export` route offers. Those live as GitHub release assets on this
repository, not on the API, so a conference room, a notebook or a graph
database can use them without a live server behind them.

| Release | What it is | Formats | Licence |
|---|---|---|---|
| [`dataset-estonia-2026-07-04`](https://github.com/StephenAbbott/opencheck/releases/tag/dataset-estonia-2026-07-04) | The Estonian e-Business Register's BODS output joined to GLEIF — 209,529 statements | BODS JSONL · NQuads (6.6M quads, licence per statement) · Neo4j CSV | CC-BY-4.0 (register) + CC0 (GLEIF) |
| [`meip-bods-2024`](https://github.com/StephenAbbott/opencheck/releases/tag/meip-bods-2024) | The OECD-UNSD MEIP Global Register (31 Dec 2024) as the OECD's own BODS v0.4, packed as the `meip` source's SQLite (Phase 208) | SQLite | OECD terms (attribution) |
| `dataset-azerbaijani-laundromat-<stamp>` | The companies and banks named in the OCCRP *Azerbaijani Laundromat* wire transfers, as curated in [DerwenAI/azeri_laverie](https://github.com/DerwenAI/azeri_laverie), run through OpenCheck's pipeline (Phase 288) | BODS JSONL · NQuads · FollowTheMoney · Senzing · Neo4j CSV · subjects table · signals | **Non-commercial** — the bundle carries CC-BY-NC sources (OpenSanctions, EveryPolitician); see its `LICENSES.md` |

Operational assets (`psc-graph-latest`, `entity-pages-latest`, `securities-index`,
`chilecompra-index`, `source-health-latest`, `findings-regression-latest`) are
also release assets, but they are the service's own working files, refreshed by
workflows, not datasets for reuse.

## The Azerbaijani Laundromat dataset (Phase 288)

`backend/scripts/build_laundromat_dataset.py` builds it. Two steps:

```
cd backend

# 1. Derive the seed (identifiers, names and Paco's class label — nothing else)
#    from the thesaurus. The result is committed as data/laundromat/seed.json so
#    the build is reproducible without his repository.
uv run python scripts/build_laundromat_dataset.py seed \
    --thesaurus /path/to/azeri_laverie/data/thesaurus.json \
    --thesaurus-commit <sha> --out ../data/laundromat/seed.json

# 2. Run the pipeline over the seed (needs the usual API keys in .env) and
#    write every artefact to the output directory.
OPENCHECK_ALLOW_LIVE=true uv run python scripts/build_laundromat_dataset.py build \
    --seed ../data/laundromat/seed.json --out /tmp/laundromat
```

What the build does, and what the bundle therefore is:

- **LEI subjects** run the full lookup pipeline in-process —
  `routers.lookup._lookup_impl`, the call the MCP `opencheck_lookup` tool and
  `/export` make — and then their GLEIF subsidiary network is folded in, deduplicated
  by `statementId` as `GET /export?subsidiaries=true` does.
- **Register subjects** (`GB-COH` company numbers here) run the Phase 182 register
  hop, `routers.expand._register_one_layer`: the one register that owns the scheme,
  its PSC / officer / related-company walk, and the sanctions and PEP name screen
  over everything it returned. An entity with both an LEI and a company number is
  both kinds of subject, and the hop is anchored on its GLEIF node so the register's
  record stitches onto the same node as FullCheck's "+1 layer" would.
- Entities the thesaurus identifies only by a Russian tax id or an Azerbaijani name
  have no register OpenCheck reads, so they are counted in the seed's `skipped` and
  are not in the bundle. The release notes say so.
- The build runs in **three paced passes**: LEI lookups one at a time
  (`--lei-concurrency 1`, `--lei-pause 5`), then each LEI's subsidiary network, then
  register hops. Everything that touches GLEIF shares one process-wide throttle of
  50 calls a minute (GLEIF allows 60 per address); a lookup's anchor costs about
  eight and a bank's subsidiary network one call per child. The first keyed run
  (5 Oct 2026) fetched subsidiaries inline, two lookups at a time, and 21 of 26 LEI
  subjects failed on the resulting 429s while every register hop succeeded. A
  lookup refused as momentary (GLEIF 429/503, lookup budget) is retried after
  `--retry-wait` (70 s), up to `--retries` (2) times; `--skip-subsidiaries` leaves
  the costliest GLEIF calls out altogether.
- Raw results are cached per subject under `<out>/raw/`, so an interrupted run
  resumes, `--retry-degraded` refetches only the subjects that failed, returned
  nothing or had a screen that did not fully run, the subsidiaries pass covers any
  cached lookup that does not have a network yet, and `--assemble-only` rewrites the
  artefacts without fetching. Register hops run one at a time with
  `--register-pause` (default 3 s) because Companies House allows 600 calls per
  five minutes per key and a hop costs about four plus its PSC walk.
- **Partial subsidiary networks are named, not hidden (Phase 293).** The first
  keyed release showed HSBC Holdings and Deutsche Bank with 0 subsidiary children
  and nothing said why (one a pipeline bug, Phase 289; one the GLEIF throttle). The
  subjects table now carries `subsidiaries_partial` and `subsidiaries_note`: `true`
  when GLEIF refused the direct or the ultimate children list or the fetch errored,
  with the error or the list that was refused. A `0` in `subsidiary_children` with
  `subsidiaries_partial=false` means GLEIF lists no children; with `true` it means
  the network was not obtained, and the count is a floor. A partial network adds
  `subsidiaries:partial` to `degraded_checks` and makes the row `degraded`, so the
  manifest's `degraded` count includes it; the manifest also carries
  `subjects.networks` (`fetched`, `partial`, `errored`, `children_total`) and
  `subjects.partial_networks`, and the release notes name them. A list the Golden
  Copy snapshot stood in for counts as obtained (its rows are real; they are not
  live).
- `--retry-subsidiaries` refetches only the cached networks that came back partial
  or errored, logged as `[subsidiaries retry i/n]`, keeping the statements an
  earlier partial fetch added; `--retry-degraded` includes them too. Inside a
  network the per-child direct-parent calls run one at a time
  (`--subsidiary-concurrency 1`; the Subsidiaries tab keeps four), because the
  throttle's bounded wait is what turns a large network into a partial one. After
  a run, `build … --retry-subsidiaries` and re-reading `manifest.json` should show
  `partial: 0`, or name the networks that are still partial.
- Work started by the script has no HTTP client, so it is never charged to the
  Phase 234 lookup budget; the process-wide GLEIF throttle and every adapter's
  outbound-rate scope still apply, and a capped register degrades rather than fails.

The output directory holds, for stamp `YYYY-MM-DD`:

| File | Contents |
|---|---|
| `azerbaijani-laundromat-<stamp>.bods.jsonl.gz` | The merged BODS v0.4 bundle, LEI subjects first, deduplicated by `statementId` |
| `….nq.gz` | The same as NQuads via `bods/rdf.py` — one named graph per statement, `bods:license` on every statement, OpenCheck's signals as `bods:Annotation` in a separate graph |
| `….ftm.jsonl.gz`, `….senzing.jsonl.gz` | FollowTheMoney entities and Senzing JSON records (`bods/ftm.py`, `bods/senzing.py`) |
| `….neo4j.zip` | `bods-neo4j to-csv` output — CSVs plus `import.cypher`; skipped with a warning when the CLI is not installed |
| `….subjects.csv` | One row per seed subject: kind, identifiers, Paco's class, status (`done` / `stub` / `failed` / `missing`), legal name, jurisdiction, register status, verdict sentence, risk and context codes, statement count, subsidiary counts, which sources answered, and whether any screening check did not fully run |
| `….signals.jsonl` | Every risk and context signal, with the subject key that raised it |
| `manifest.json` | Counts, per-artefact SHA-256, subject status, seed provenance, BODS shape issues, the licence verdict |
| `LICENSES.md` | The same licence notes `/batch-export` writes, over the union of contributing sources |
| `RELEASE_NOTES.md` | A release description with the numbers filled in, for editing before publishing |

Read the manifest before trusting the bundle: a subject with status `failed` or
`missing` contributed nothing, one with `degraded=true` in the subjects table had a
check that did not fully run, and neither is a clean result. Register subjects carry
a register's record and a name screen, not the full source fan-out an LEI subject
gets.

Publishing follows the Estonia recipe: upload the artefacts to a `dataset-…` release,
paste the edited `RELEASE_NOTES.md` as its description, and verify the checksums in
`manifest.json` against the uploaded files. A reassembled multi-part file goes to a
temporary name, is checksummed, and only then renamed over the destination.
