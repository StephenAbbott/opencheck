# Loading the Azerbaijani Laundromat companies into Neo4j

I'd skip a loop of live `opencheck_lookup` calls for about 200 companies. OpenCheck has a published bulk dataset for exactly this set, and it loads into Neo4j without using the live lookup budget.

## Why not live lookups

- **Budget.** The live lookup budget is ten fresh checks a minute per address, shared by everyone on the room's Wi-Fi. Two hundred lookups take at least twenty minutes and will hit 429s. `opencheck_batch_lookup` takes 20 LEIs per call, which cuts the call count, but it still spends the same budget and returns one row per company, not a graph.
- **Coverage.** A live lookup gives one company's view. The Laundromat release already runs each subject through the full pipeline: the LEI lookup, the GLEIF subsidiary network, and a Companies House walk for UK companies. A live loop would miss the network.
- **Reproducibility.** The release is dated (`dataset-azerbaijani-laundromat-2026-10-06`), so everyone in the room works from the same snapshot.

## What to load

The release `dataset-azerbaijani-laundromat-2026-10-06` at https://github.com/StephenAbbott/opencheck/releases includes:

- `*.neo4j.zip`: CSVs plus an import script in the bods-neo4j layout (`entity.csv`, `person.csv`, `owns.csv`, `controls.csv`, `manages.csv`, `unspecified_party.csv`, …).
- `*.subjects.csv`: one row per seed company, with how it was reached, counts, and flags for degraded screens and partial networks.
- `*.signals.jsonl`: every risk and context signal per subject.
- `LICENSES.md`, `manifest.json`.

Script: `/home/claude/ws/iteration-2-haiku/bulk-exploration-goes-to-datasets/with_skill/outputs/load_laundromat_neo4j.sh`

```bash
bash load_laundromat_neo4j.sh /path/to/new/empty/dir
```

It downloads the release with `gh`, unpacks the bundle, lists the CSVs, and prints the next steps. It does not run the import, so you can read the bundle's import script first. Then load into an empty Neo4j database. The import script may clear existing data, and `neo4j-admin import` needs the database stopped.

## Caveats

- **Not run.** I wrote the script but did not execute it. Listing the release assets needed a shell approval I couldn't get here, so the asset names come from the release description, not from the release itself. The script stops with a clear error if the patterns don't match.
- **Not inspected.** I have not seen the bundle's `import.sh` or the node labels. Run `CALL db.labels();` after loading and adjust your queries to match.
- **The 200-company list.** The release covers the whole Laundromat set. To narrow it to your list, filter `*.subjects.csv` by LEI or name.
- **Fixtures.** The recorded output files in `/home/claude/fixtures` do not contain your list. They hold unrelated batches (Shell, Eli Lilly, Ørsted and others), so I did not use them. You didn't name the files you meant, so tell me if there are others.
- **Licence.** The bundle includes OpenSanctions and EveryPolitician data under CC-BY-NC-4.0. Non-commercial use only. Pass the notice on with any export or slide.
- **Not a verdict.** `subjects.csv` flags degraded screens and partial networks. A zero in a flagged row is a floor, not a finding. Treat "context" signals as description, not as risk findings, and don't add shares across sources.

## When a live lookup is still worth it

After the offline pass narrows the set, a few live `opencheck_lookup` calls give today's view, since the dataset is dated. Space them out. A repeat of the same lookup within 15 minutes is free.
