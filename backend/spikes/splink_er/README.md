# Splink entity-resolution spike

Offline experiment: would **Splink** (probabilistic Fellegi–Sunter record
linkage) improve OpenCheck's matching of the *same entity across sources when
no strong identifier is shared*, and give a more transparent per-decision
confidence than today's `difflib`/label approach?

- Background + comparison: Notion → *Examining Splink*.
- Plan: Notion → *Examining Splink → Implementation plan*.

**Not a runtime dependency.** Splink lives in the `spike` dependency group:

```bash
cd backend
uv sync --group spike      # installs splink (DuckDB backend)
```

`corpus/`, `model.json`, `charts/` are git-ignored (throwaway; may carry source
data we don't commit — OpenSanctions is excluded from the corpus by design).

## Phase 1 — corpus (done)

`build_corpus.py` pulls BODS **entity statements** per LEI from the OpenCheck
`/export` endpoint and flattens each to one row:

- **Soft features (model inputs):** `name_norm`, `jurisdiction`, `inc_date`, `address_norm`
- **Identifier labels (held out, ground truth):** `lei`, `nat_reg`

Records sharing an `lei` or `nat_reg` are the same entity — free labels to train
`m` and to score whether a soft-feature-only model recovers the matches.

```bash
# resumable + incremental; re-run with a bigger --n to grow the corpus
uv run python spikes/splink_er/build_corpus.py --n 400 --batch 8
```

Notes:
- **Snowball:** LEIs discovered in a bundle (parents/subsidiaries) are queued and
  fetched as subjects too, so a handful of seeds grows to hundreds of rows.
- **OpenSanctions excluded** (CC-BY-NC).
- Resumable: progress in `corpus/_done_leis.txt`, rows in `corpus/entities.csv`
  (deduped by `record_id`). Safe to Ctrl-C / re-run.
- Render's free tier is slow (~7–18 s/lookup, cold starts) — build the corpus up
  over several runs to reach a few hundred lookups.

### Phase 1 findings so far (8 lookups, 457 rows)
- Feature coverage is strong for **company** entities: name 100%, jurisdiction
  99%, incorporation date 95%, address 98%; **99% of rows carry ≥3 soft
  features** — so the "Splink needs several low-correlation columns" risk looks
  *low* for entities (re-confirm at full corpus size).
- Label coverage: only 6/457 rows have neither `lei` nor `nat_reg`.
- Matched-pair density grows with lookups: ~103 positive pairs / 124 linked rows
  at 8 lookups (positives via shared `lei` **or** `nat_reg`). Build to a few
  hundred lookups before evaluating so there are enough positives.
- Gotcha fixed: GLEIF entity statements use `incorporatedInJurisdiction`;
  OpenSanctions uses `jurisdiction`. The extractor reads both.

## Phase 2–3 (next)
`train_model.py` — Splink `SettingsCreator` (DuckDB), comparisons on name (+TF),
jurisdiction, inc_date, address; estimate prior + u-sampling + `m` from the `lei`
label + EM; save `model.json`. Then predict/cluster, evaluate vs the
`reconcile.py`/`cross_check.py` baseline, and render waterfall charts.
