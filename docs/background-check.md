# BackgroundCheck — screening the people connected to an entity (SPIKE)

> **Status:** spike, on branch `feat/background-check`. Full initial thoughts,
> design critique and the phased implementation plan live on the Notion ticket
> *"Feature: Risk checks on people linked to an entity (BackgroundCheck)"*.

OpenCheck's headline flow screens the **entity**. The people connected to it —
officers, PSCs, beneficial owners — were only screened indirectly (the
`cross_check` RELATED_PEP / RELATED_SANCTIONED pass) and surfaced quietly.
BackgroundCheck brings them to the fore, returning to the original project
plan's Phase 3 ("person lookups", `docs/plan.md` §3).

## What this spike adds

**Backend** — `GET /person-check?name=&birth_year=`
(`opencheck/routers/person_check.py`): fans a person query out across every
person-capable adapter (`SearchKind.PERSON` in `info.supports`: Companies
House officers, OpenSanctions, EveryPolitician, Wikidata, OpenAleph, …),
scores every hit against the queried name with the cross-check module's
`_name_score` / `_birth_year_compatible` helpers (same 0.88 threshold, so
"strong match" means one thing product-wide), and derives risk signals via
`assess_hits` **from strong matches only**. Every signal carries a
`evidence.match` block (query name, similarity, birth-year corroboration);
the response lists every checked source with attribution/licence/hit-count/
error so "no hit" renders honestly. Tests: `tests/test_person_check.py`.

**Frontend** — a third check mode alongside QuickCheck / FullCheck:

- `src/lib/backgroundCheck.ts` — extracts connected people from the assembled
  BODS bundle (person statements + their relationship statements), merging the
  same person across sources by normalised name + birth year while keeping
  every statementId for traceability. Tested in `backgroundCheck.test.ts`.
- `src/components/cdd/BackgroundCheckPanel.tsx` — lists connected people with
  their roles/sources; per-person "Run background check" (plus a capped
  "check all") calls `/person-check` on demand and renders risk chips,
  strong/weak matches with similarity percentages, failed-source warnings and
  a checked-sources attribution footer.

## Deliberate constraints (evidence discipline)

- Person screening is **name-based** — UK PSC/officer records expose no stable
  public person identifier. Everything is framed as a *potential match* with
  its evidence; risk chips never derive from weak matches; birth-year
  mismatch blocks a strong match.
- Checks run **on demand**, not during the main lookup — bounded upstream API
  load (OpenSanctions free tier) and no unsolicited claims about people.
- "No signals" is always qualified: which sources were checked, which failed,
  and that absence is not proof of absence (plan.md §4.5 coverage caveat).

## Extending beyond UK (e.g. Estonia)

The extraction is source-agnostic: it walks BODS statements, not adapter
payloads, so Estonian officers/BOs/shareholders (mapped by `map_ariregister`)
appear automatically with their role labels. Any future source that emits
person + relationship statements joins for free.

## Phase B hardening (2026-07-22)

- Vite dev proxy entry for `/person-check` (missing entries serve index.html →
  "Unexpected token '<'"); `origin/main` merged in so name queries go through
  `sanitize_name_query()` (quote-safe, `e458677`).
- First-review UX changes: entity-scoped panels (AI summary, risk signals,
  cross-source identifiers, possibly-same) hidden while BackgroundCheck is
  active; mode-card copy avoids the PSC acronym; per-person Hide/Show for
  completed results; "N of M checked…" progress on Check all; checked-at
  timestamps; weak matches labelled "below threshold".
- Docs: `docs/status.md` Phase 82 row (+ regenerated changelog JSON),
  `okf/api/person-check.md` (+ index, viz) — `generate_okf.py --check` clean.

## Phase C — person identity enrichment (2026-07-22)

- **`GET /person-appointments?officer_id=`** — every CH appointment under one
  officer id (register-asserted same-person, stronger than a name match);
  "View appointments across companies" on strong CH matches. The BODS output
  carries the `GB-COH-OFFICER` identifier.
- **Q-ID bridging** — `/person-check` reconciles strong matches only into
  `cross_source_links`; "Same person across sources" panel. Weak matches
  excluded by design.
- **Wikidata human filter** — person searches post-filter to P31→Q5 via one
  batched SPARQL query, failing open on errors (fixes the painting/song noise
  found in the Phase B live smoke; benefits `/search?kind=person` too).
- **Possibly-same-person review** — same-name pairs with a missing birth year
  flagged for human review in the people list, never auto-merged.

## Known gaps (for the de-spike ticket)

- CH PSC/director person statements (from the entity bundle) still carry no
  officer identifier — the people *list* is name-keyed; only screen matches
  gain the officer-id-backed appointments view. Wiring the bundle's persons
  to officer ids remains open.
- EveryPolitician remains cross-check-only as a PEP source in the entity flow;
  its promotion to a first-class source card is planned, not built.
- No shareable person-report URL and no MCP tool exposure yet.
- Live `/person-check` + `/person-appointments` runs against real keys are
  Stephen's local step — the adapters are the same code paths
  `/search?kind=person` exercises in production.
