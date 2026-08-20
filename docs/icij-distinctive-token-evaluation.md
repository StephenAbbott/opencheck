# Distinctive-token gate for ICIJ screening — evaluation (Phase 120)

**Question.** PR #86 took ICIJ offshore-leaks screening from ~45% to ~85% adjudicated precision by raising the name-similarity cut to 0.93 — but the surviving false positives score *as high as the true matches* (generic tokens match, distinctive token differs), and the cut cost two named true matches just under it (`NICHOLAS PAUL RATCLIFFE`→`NICHOLAS RATCLIFFE` at 0.878; `MOET HENNESSY INTERNATIONAL`→`HENNESSY INTERNATIONAL LIMITED` at 0.877 — leaving LVMH with no offshore-leaks signal at all). Can a distinctive-token requirement kill the collisions *by shape*, so the threshold can come back down and recover both?

**Answer.** Yes. `names.distinctive_token_agreement` + threshold 0.87 kills every named collision — including two previously unknown ones sitting *above* the old 0.93 cut — and recovers both named true matches. On the production candidate pool, precision moves 69% → ≥80% and recall of adjudicated true matches 75% → 100%, under the worst polarity of every judgement call.

## Corpus

The PR #86 corpus was never committed (its numbers survive only in commit `1255868`'s message), so it was rebuilt reproducibly: `backend/scripts/eval_icij_distinctive.py pull` fetches the same 14 production subjects (BP, Rosneft, Bank Saderat, Hornsea 1, TAQA Bratani, Newcastle United, Biffa, Care UK, DMGT, LVMH, Unilever, Maersk, Glencore, Nordea — LEIs re-resolved against GLEIF 2026-08-20 and embedded in the script), extracts the screening targets with the real `icij_check._collect_targets` (1,140 targets), and queries the ICIJ reconciliation API at `limit=10` per node type (29,337 scored pairs). ICIJ result lists are order-stable prefixes, so the **production pool** — 2 results/type, first 30 targets per subject — is recovered by truncation. Thresholds are only ever read off the production pool (the PR #86 lesson: a threshold measured on a different candidate pool did not transfer).

The raw pull lives under `backend/data/eval/icij_distinctive/` (gitignored, ~11 MB, regenerable); the scored worksheet with adjudication labels is committed as [`icij-distinctive-adjudication.csv`](icij-distinctive-adjudication.csv).

## The gate

`names.distinctive_token_agreement(a, b)` (entities only — person targets bypass unless either name carries a legal form, which marks a corporate officer filed as a person):

1. Strip legal forms with rigour's `remove_org_types` (`org_name_residue`); an empty residue on either side never agrees ("PJLS HOLDINGS LIMITED" vs "HOLDINGS LIMITED").
2. Join runs of ≥2 single-letter tokens ("B S C" → "bsc"), drop isolated single letters (legal-form stripping tears acronyms: "B.S.C." loses its "S.C." and strands a "b").
3. Drop filler (`_GENERIC_ORG_TOKENS`: HOLDINGS, INTERNATIONAL, GROUP, … — seeded from the collision table, extended by the `tokens` frequency measurement; "ENERGY" and "BANK" deliberately kept distinctive, since dropping them would readmit measured collisions).
4. Numeric/single-char discriminators must be identical on both sides ("HORNSEA 1" ≠ "HORNSEA" ≠ "HORNSEA 2").
5. Every distinctive token of the shorter residue must agree with one of the longer — exact, or one edit for tokens of ≥4 characters ("GAZPRM"≈"GAZPROM"; "U.K." vs "S.K." is a different acronym, not a typo; "ENERGEN"≁"ENERGY" at 2 edits).

Applied in `icij_check._signal_from_match` **even to ICIJ `match: true` results** — ICIJ's own scorer rated the ENERGEN/BIOGAS collision 90/100. `_MIN_NAME_SIM` drops 0.93 → 0.87 (and becomes an injectable kwarg for threshold sweeps).

## Results — production pool, unique pairs, hand-adjudicated

19 unique pairs pass either gate. Labels: 12 clear true matches, 4 clear false positives, 3 judgement calls (JC).

| outcome | pairs |
|---|---|
| Kept by both gates (all true) | BRITANNIC TRADING, ROTHERMERE CONTINUATION, GLENCORE PLC, GLENCORE INTERNATIONAL AG, NJORD, Gulf International Bank B.S.C†, DMGT plc, Viscount Rothermere (person), BIFFA CORPORATE HOLDINGS |
| **Killed by the new gate** (old gate passed all four) | HORNSEA 1↔HORNSEA (0.9375), ENERGEN BIOGAS↔BIOGAS ENERGY (0.9302), **WIGMORE 1↔WIGMORE (0.9375)**, **PRACTICE PLUS↔PRACTICE PLAN (0.9444)** |
| **Recovered by the new gate** | NICHOLAS PAUL RATCLIFFE (0.878, person), MOET HENNESSY INTERNATIONAL (0.877 — LVMH regains its signal), GLENCORE GROUP FUNDING AG↔Ltd (0.902) |
| Admitted by the new gate, judgement calls | RMS↔NorAm Risk Management Solutions (0.906), ASSOCIATED PRINT HOLDINGS↔ASSOCIATED HOLDINGS (0.900), BRITANNIC ENERGY TRADING↔BRITANNIC TRADING (0.877) — each a same-corporate-family name where identity is uncertain; all surface at `medium` confidence |

† Recovered by the acronym rules — the gate's own first-pass bug, caught by this harness before it shipped.

WIGMORE 1 and PRACTICE PLUS/PLAN are the headline: the same shapes as the known collisions, sitting **above** the old 0.93 threshold, found only because the corpus was rebuilt. The old gate was still shipping false positives; no threshold could have removed them.

### Sensitivity (judgement calls flipped both ways)

| gate | JC = fp | JC = tp |
|---|---|---|
| old (0.93, no token gate) | precision 0.69, recall 0.75 | precision 0.69, recall 0.60 |
| **new (0.87 + gate)** | **precision 0.80, recall 1.00** | **precision 1.00, recall 1.00** |

The new gate dominates the old on both axes under either polarity. Threshold sweep: 0.88 loses both named recoveries (0.877/0.878); nothing new enters between 0.86 and 0.87; 0.87 is the chosen cut.

## Other surfaces (sizing only, no behaviour change this phase)

`eval_icij_distinctive.py surfaces` replays the gate over the entity `RELATED_*` signal evidence in the same 14 lookups (cross_check + OpenAleph): of 122 signals, exactly 20 would fail the gate — **all of them the Rosneft «Общество с ограниченной ответственностью» legal-form matches** independently found in Phase 119 (19 via OpenAleph, 1 via OpenSanctions `RELATED_SANCTIONS_LINKED`, which Phase 119's OpenAleph-side guard does not cover). Zero legitimate signals dropped. That is the follow-up ticket, sized: extending the gate to `cross_check` closes the one remaining boilerplate signal, and nothing else moves.

## Reproducing

```
cd backend
uv run python scripts/eval_icij_distinctive.py pull       # live network
uv run python scripts/eval_icij_distinctive.py score --min-sim 0.87
uv run python scripts/eval_icij_distinctive.py report --labels ../docs/icij-distinctive-adjudication.csv
uv run python scripts/eval_icij_distinctive.py tokens     # filler-list evidence
uv run python scripts/eval_icij_distinctive.py surfaces   # follow-up sizing
```

Re-run `pull` + `score` and re-adjudicate whenever the candidate pool changes shape — a different `_RESULTS_PER_TYPE`, node-type set, or target cap invalidates the threshold, not just the numbers.
