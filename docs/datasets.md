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
- Raw results are cached per subject under `<out>/raw/`, so an interrupted run
  resumes, `--retry-degraded` refetches only the subjects that failed, returned
  nothing or had a screen that did not fully run, and `--assemble-only` rewrites the
  artefacts without fetching. Register hops run one at a time with
  `--register-pause` (default 3 s) because Companies House allows 600 calls per
  five minutes per key and a hop costs about four plus its PSC walk.
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
