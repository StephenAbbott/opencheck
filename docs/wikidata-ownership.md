# Wikidata controlling-owner extraction — design proposal

**Status:** proposal + prototype parser (not yet wired into the live lookup).

## Why

A review of the community **Wikidata MCP** (`wd-mcp.wmcloud.org`) concluded the
MCP is a *transport / agent-ergonomics layer over the same Wikidata* OpenCheck
already queries directly (WDQS + `wbsearchentities`) — it unlocks no new data and
would only add a third-party dependency to the deterministic backend. The real
opportunity it surfaced is **what** we query, not how: OpenCheck currently reads
only a thin slice of Wikidata ownership (`P749` parent, `P127` owned-by, label
only). Two seams are worth more.

## What the probe found

Live probes (run through WDQS, with the wd-mcp `execute_sparql` as a fallback
while WDQS was mid-outage) over ~30 entities:

- **State-owned enterprises** — a viable, high-precision-when-present signal:
  Gazprom, Rosneft, Equinor, Deutsche Bahn, EDF, CNPC, plus SWFs (QIA, Temasek)
  all detectable. But **minority / indirect state ownership is a blind spot**
  (Norsk Hydro's ~34% state stake absent; Saudi Aramco's PIF ownership not
  classed as government), and the private long tail is — correctly — silent.
- **Foundation / family ownership** — the *richer* seam, lighting up exactly
  where the SOE signal goes dark. Robert Bosch → **Robert Bosch Stiftung 92%**
  plus family fiduciary vehicles (Industrietreuhand, Familientreuhand); Koch →
  Charles & David Koch (named persons); Cargill → "Cargill family"; Heineken →
  Heineken Holding 50% + Charlene de Carvalho-Heineken. `P1107` proportions are
  populated (as ratios).

### Caveats (these shape the design)

1. **Proportions are indicative, not authoritative.** Bosch shows Stiftung 92%
   *and* Industrietreuhand 93% — Wikidata conflates capital share, voting rights
   and points-in-time with no rank/as-of discrimination in a truthy pull. Never
   present the number as clean.
2. **Coverage is famous-names-only.** The SME long tail is absent. This is a
   **presence-only, corroborating** enrichment — its silence means nothing,
   exactly like the Offshore Leaks signal.
3. **Name resolution is fragile** (3/21 no-match, 1 flat-wrong on suffixed
   names) — but irrelevant here, because OpenCheck resolves via `P1278` (LEI),
   not name.
4. **WDQS is currently unreliable** (active outage, 1 req/min). The adapter's
   existing defensive `_sparql()` (treats non-200 / error-JSON as empty, caches
   only genuine result sets) already covers this; the wd-mcp proxy is noted as a
   *possible fallback runner*, not adopted on the critical path.

## The unified extractor

Foundation/family and SOE are the **same extraction** — `P127`/`P749` owners +
owner-type classification + `P1107` proportion + references — differing only in
how the owner maps into BODS v0.4:

| Wikidata owner type | BODS modelling |
|---|---|
| named person (Koch, de Carvalho-Heineken) | `personStatement` (`knownPerson`) + OOC; `beneficialOwnershipOrControl: true` |
| foundation / Stiftung (Robert Bosch Stiftung) | `entityStatement` `entityType.type = registeredEntity` + OOC |
| trust / Treuhand / fiduciary arrangement | `entityType.type = arrangement` (per BODS *Representing trusts*) |
| company / holding (Heineken Holding) | `entityStatement` `registeredEntity` + OOC |
| **"family"** (e.g. "Cargill family") | **DROPPED** — not a legal entity or a single person; we do not fabricate a person or invent a group |
| **state / ministry / agency** | `entityType.type = state` or `stateBody` (+ `subtype`, `jurisdiction`), per BODS *Representing state-owned enterprises* |
| **sovereign wealth fund / GLIE** | `registeredEntity` intermediary connected *up* to a `state` entity — the spec's **government-linked investment entity** pattern (the clean fix for the Temasek / PIF / Rosneftegaz indirection) |

Decisions:

- **Family owners are dropped** (decided). A "family" is neither a legal entity
  nor a single natural person; emitting an `unknownEntity` would add noise and
  fabricating a person would be wrong. We skip them rather than guess.
- **Proportion** (`P1107`) → `interests[].share.exact`, flagged **indicative**,
  and only surfaced alongside provenance + an as-of date where available.
- **Provenance** → BODS `source.type: "thirdParty"` (Wikidata is crowd-sourced;
  **never** `verified`), carrying the statement's reference (`P248` stated-in /
  `P854` reference URL / `P813` retrieved) where present.

## SOE branch and BODS compliance

Per the BODS [Representing state-owned enterprises](https://standard.openownership.org/en/0.4.0/standard/modelling/repr-state-owned-enterprises.html)
requirement, an SOE's entity statement MUST connect (directly or indirectly) to
an entity statement whose `entityType.type` is `state` or `stateBody`. So the SOE
**risk signal falls out of the same extraction for free**: when an owner
classifies as state / government / ministry / SWF, we model that owner as a
`state`/`stateBody` entity (or a `registeredEntity` GLIE linked up to a `state`),
which both satisfies the spec and drives a `STATE_CONTROLLED` / SOE risk signal.
The signal is corroborating and presence-only (see caveat 2); it must never imply
"not state-owned" from absence, and must not render the `P1107` number as fact.

## Rollout plan

1. **Prototype parser** (this change): a new `_OWNERSHIP_QUERY` + `_parse_ownership()`
   in `sources/wikidata.py` that classifies owners, captures proportion +
   references, and drops family — with unit tests over synthetic bindings. Not
   yet wired into `fetch()`.
2. **Wire into `fetch()` + `map_wikidata()`**: emit the owner statements with the
   correct `entityType`, the `thirdParty` source block, and indicative share.
3. **SOE risk signal**: `STATE_CONTROLLED` off the state/stateBody/GLIE branch,
   evidence-linked to the BODS node, with the presence-only caveat in the UI.
4. **Provenance density** (open question): measure how many Wikidata ownership
   statements actually carry references before deciding whether to publish share
   percentages at all. Tracked below.

### Provenance density check — result

Measured over the foundation/family + SOE sample (28 ownership statements across
9 entities): **23/28 (82%) carry a `prov:wasDerivedFrom` reference**, and the
references are frequently **authoritative**, not just any URL:

- official company sources — `assets.bosch.com` (Bosch), `gazprom.com/investors`
  (Gazprom), Heineken/FEMSA investor pages;
- government sources — `economie.gouv.fr` / `edf.fr` for EDF's *French State* owner;
- **an official company register** — `datacvr.virk.dk` (Danish CVR) for Carlsberg;
- journalism — Washington Post / Popular Mechanics for the Koch brothers.

The five unreferenced edges include the dropped "Cargill family" and cases where
a *duplicate* edge for the same owner is referenced — so owner-level coverage is
even higher than 82%.

**Decision:** share percentages **are publishable**, but only as `share.exact`
**carried with their reference** in the BODS `source` block (`type: thirdParty`,
`url`, `retrievedAt`). An ownership edge with no reference is still emitted, but
its share is marked indicative and the UI must show "source: Wikipedia/Wikidata,
unreferenced". Never present a bare percentage. This makes the foundation/family
enrichment defensible enough to ship.
