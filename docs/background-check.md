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

## Known gaps (for the de-spike ticket)

- CH PSC/director person statements carry no officer identifier — a later
  phase should enrich via `/search/officers` + appointments to link a person
  to their full cross-company appointment history.
- EveryPolitician remains cross-check-only as a PEP source in the entity flow;
  its promotion to a first-class source card is planned, not built.
- No shareable person-report URL, no MCP tool exposure, no OKF/api concept
  for the new endpoint yet.
