# Time Machine — change-over-time spec (draft)

**Status:** draft for discussion · **Owner:** Stephen · **Last updated:** 2026-06-22

A "Time Machine" / History view for OpenCheck that surfaces a simplified
timeline of **notable** changes to an entity's ownership, control and identity,
drawn from sources that publish historical data — starting with **GLEIF** and
**Companies House (UK PSC)**, and designed to extend to any future source.

The point is not the timeline widget (Structuriser and Stephen's own
[bods-timeline](https://github.com/StephenAbbott/bods-timeline) already do
single-source reconstruction). OpenCheck's distinct claim is **multi-source
change on one axis** — GLEIF's corporate-tree moves *and* Companies House PSC
moves for the same entity, reconciled through one model — and a live demo of the
**temporal half of [BODS v0.4](https://standard.openownership.org/en/0.4.0/)**
that almost nobody shows: `statementDate`, the stable `recordId` +
`recordStatus` (new/updated/closed) lifecycle, and interest
`startDate`/`endDate`. (`replacesStatements` was removed in 0.4 — versioning is
carried by the shared `recordId`.)

## Goals

- A per-entity timeline of notable ownership / control / identity changes.
- Live, general capability — works for any looked-up entity, not a curated demo
  subject (v1 decision).
- Source-agnostic core: a third source that publishes history maps into the same
  model with no changes to the renderer or the notability logic.
- Honest provenance — never present a change as more precise (in date or
  certainty) than the source actually supports.

## Non-goals (v1)

- Inferred narratives ("Owner A *replaced* Owner B"). We show **raw events**
  first and earn the right to infer later (Fork 2).
- A full BODS temporal *publishing* pipeline. We synthesise a timeline view; we
  do not (yet) republish a versioned BODS dataset.
- Reconstructing history for sources that only expose a current snapshot.

## Key finding that shapes the design

Both launch sources expose a **typed modification stream** per entity, queryable
live — they are *not* two different problems:

- **Companies House** — filing-history API returns discrete, pre-categorised
  events (CS01, PSC07, TM01, NM01, …) with real filing/effective dates.
- **GLEIF** — the modification-history endpoint
  (`/lei-records/{lei}/.../history`, exposed via the GLEIF MCP
  `gleif_get_modification_history_by_lei`) returns **field-level**
  modifications: `modificationType` (UPDATE/INSERT/DELETE), the exact `field`
  path, a `date`, and `valueOld` / `valueNew`. Query `record_type=LEI` for the
  entity record and `record_type=RR` for relationship (parent) changes.

So GLEIF needs **no snapshot-hoarding and no diffing** — it already hands us
old→new field transitions. Detection therefore reduces to **a per-source
allowlist** over a shared event model.

### The noise is the product

Heineken N.V. (`724500K5PTPSST86UQ23`) over 3+ years returned 15 GLEIF
modifications and **not one** was substantive: 5× `NextRenewalDate`, 7×
`LastUpdateDate`, an `EntityCreationDate` timezone-precision fix on an 1873 date,
and an `EntityCategory` backfill. The teaching parallel writes itself:

> A GLEIF `NextRenewalDate` update with nothing else == a Companies House CS01
> "confirmed, no change." Same administrative noise, different registry.

Filtering that noise *is* the feature. The allowlist is not a refinement bolted
on later — it is the core.

## Core model: `ChangeEvent` (raw-first)

**Decision:** a `ChangeEvent` carries the **raw registry change**, faithful to
the source, one row per modification (including the noise). Notability is a
*derived classification* (`tier`) computed on top, and the renderer suppresses /
shows by tier. This keeps the full audit trail and lets us offer "show
everything, including the noise" without re-querying or re-deriving.

```jsonc
{
  "source_id": "gleif",              // emitter: "gleif" | "companies_house" | …
  "subject_id": "<recordId>",        // OpenCheck stable recordId of the entity/relationship the change is about
  "record_type": "entity",           // "entity" | "relationship"

  // --- raw, source-faithful ---
  "raw_change_type": "UPDATE",       // GLEIF: UPDATE/INSERT/DELETE; CH: filing category code
  "raw_field": "/lei:.../lei:LegalName", // GLEIF field path; CH: filing type/description
  "value_old": "OLD CO LTD",         // null on INSERT / first observation
  "value_new": "NEW CO PLC",         // null on DELETE
  "raw_payload_ref": "<id/url>",     // link back to the source record / filing

  // --- derived classification ---
  "change_type": "LEGAL_NAME_CHANGE",// controlled codelist (see below); null if unmapped/noise
  "tier": 3,                         // 1 = ownership/control moved, 2 = identity/status, 3 = admin noise
  "boosted": false,                  // true when a boost rule lifts an otherwise-Tier-3 event
  "boost_reason": null,              // e.g. "co-occurs with SANCTIONED within 30d"

  // --- temporal + provenance ---
  "event_date": "2025-12-09",        // best available date for the change
  "date_basis": "recorded",          // "effective" (CH filing) | "recorded" (GLEIF publish date) | "snapshot_window"
  "date_confidence": "medium",       // high | medium | low
  "date_range": null                 // [from, to] when date_basis == "snapshot_window"
}
```

Notes:

- `raw_*` fields are never dropped — a Tier-3 / unmapped event is still a valid
  `ChangeEvent`; it just won't render by default.
- `change_type` is the **source-agnostic codelist** (below). Each source maps
  its raw vocabulary into it. A third source only has to write that mapping.
- `date_basis` is how we stay honest (see Provenance).

## Notability — the allowlists

`tier` and `change_type` are assigned by a per-source allowlist. Everything not
on an allowlist defaults to `tier: 3, change_type: null` (kept, hidden).

### Tier 1 — ownership / control actually moved

Maps to **relationship** `recordStatus` + interest `startDate` / `endDate`.

| `change_type` | GLEIF (`record_type=RR`) | Companies House |
|---|---|---|
| `OWNER_ADDED` | RR INSERT / RelationshipStatus→ACTIVE | psc01–05 (PSC notified) |
| `OWNER_REMOVED` | RR DELETE / RelationshipStatus→INACTIVE | psc07 (PSC ceased) |
| `CONTROL_BAND_CHANGED` | — (GLEIF has no %) | natures-of-control band crossing **25 / 50 / 75 / 100%** only |
| `CONTROL_NATURE_CHANGED` | — | ownership-of-shares ↔ voting-rights ↔ right-to-appoint |
| `PARENT_CHANGED` | StartNode/EndNode change; direct vs ultimate parent | — |
| `REPORTING_EXCEPTION_CHANGED` | ReportingException ↔ has-parent (NO_KNOWN_PERSON, NON_CONSOLIDATING, …) | — |

### Tier 2 — entity identity / status changed

Maps to **entity** `recordStatus: updated` + `statementDate`.

| `change_type` | GLEIF field (`record_type=LEI`) | Companies House |
|---|---|---|
| `STATUS_CHANGED` | `EntityStatus` (ACTIVE→INACTIVE) | company_status (Active→Dissolved/Liquidation) |
| `SUCCESSION` | `SuccessorEntity` / `SuccessorLEI`; `EntityExpirationReason` | merger / reconstruction filings |
| `LEGAL_NAME_CHANGE` | `LegalName` | NM01 / change of name |
| `LEGAL_FORM_CHANGE` | `LegalForm` | re-registration (Ltd↔PLC) |
| `JURISDICTION_CHANGE` | `LegalJurisdiction` | — |
| `REGISTRATION_RETIRED` | `RegistrationStatus` → RETIRED/MERGED/ANNULLED | dissolution / gazette |
| `ADDRESS_CHANGE` *(judgement)* | `LegalAddress` / `HeadquartersAddress` | AD01 |

### Tier 3 — administrative noise (kept, suppressed by default)

- **GLEIF:** `LastUpdateDate`, `NextRenewalDate`, `InitialRegistrationDate`,
  `ManagingLOU`, `ValidationSources`, `EntityCreationDate` precision fixes,
  `EntityCategory` backfills, `RegistrationStatus → LAPSED` (see boost rule).
- **Companies House:** CS01 confirmation statement with no delta, accounts
  filings, PSC statement housekeeping, address re-formatting.

## Boost rule (Fork 1 — start static, this is the next step)

Notability has a **static base tier** plus an optional **boost** when an
otherwise-Tier-3 event co-occurs (within a time window) with another signal —
notably one of OpenCheck's existing **risk signals**. Examples:

- `RegistrationStatus → LAPSED` (Tier 3 alone) **+** a `SANCTIONED` /
  `RELATED_SANCTIONED` signal within ~30 days → surface as boosted.
- A cluster of ≥N changes in a short window → surface the cluster.

A boosted event keeps its base `tier` but sets `boosted: true` +
`boost_reason`, so the renderer can lift it without losing the classification.
This couples Time Machine to the risk engine — a deliberate, opt-in dependency.

## BODS v0.4 mapping

GLEIF's change log is **already update-structured**, so the mapping is close to
mechanical, and it answers "how do we structure the entity record so changes are
recorded as updates":

| ChangeEvent | BODS v0.4 |
|---|---|
| `raw_change_type` INSERT / UPDATE / DELETE | statement `recordStatus` = `new` / `updated` / `closed` |
| `event_date` | `statementDate` (= when GLEIF *recorded* it; provenance only) |
| `subject_id` | the shared, stable `recordId` (same value across the whole series) |
| Tier-1 add / remove | relationship statement + interest `startDate` / `endDate` |

**Critical rule — relationship interest dates ≠ the modification date.** For a
Level-2 relationship, the BODS interest `startDate` / `endDate` must come from
the GLEIF **`RelationshipPeriod`** (ACCOUNTING_PERIOD / RELATIONSHIP_PERIOD),
**not** from the modification `date`. The Morrisons dry-run proves why: GLEIF
first *published* the Market Bidco parent on **2023-11-25**, but the relationship
period shows consolidation began **2021-11-01** — a ~2-year reporting lag. Put
the economic date (2021-11-01) on the interest and keep the recording date
(2023-11-25) as `event_date` / provenance. (Pin down RELATIONSHIP_PERIOD vs
ACCOUNTING_PERIOD precedence — see open questions.)

**OpenCheck already has half the spine.** `_stable_id(source_id, local_id)` is
deterministic, so the same entity already gets a **stable `recordId` across
observations**. What's missing today is (a) letting `statementDate` reflect each
modification's date rather than "now", and (b) computing `recordStatus` from the
change instead of always emitting `new`.

**Interop check before we encode this ourselves:** Open Ownership's
[`bodspipelines` GLEIF mapping](https://github.com/openownership/bodspipelines/tree/main/bodspipelines/pipelines/gleif)
already encodes the GLEIF lifecycle into `recordId` / `recordStatus` for the
published [GLEIF-in-BODS datasets](https://bods-data.openownership.org/source/gleif_version_0_4/).
We should match its conventions so OpenCheck's timeline output stays
interoperable with those datasets rather than inventing a parallel encoding.

## Provenance & date honesty (Fork 3)

Be explicit that the two sources differ in date meaning and certainty; never
render them as the same precision:

- **Companies House** → `date_basis: "effective"`, `date_confidence: "high"` —
  real filing / effective dates.
- **GLEIF** → `date_basis: "recorded"`, `date_confidence: "medium"` — the `date`
  is *when GLEIF published the change*, which can lag the real-world event
  (e.g. a name change recorded months later). Show what GLEIF says, labelled as
  GLEIF-recorded.
- **Future snapshot-only source** → `date_basis: "snapshot_window"`, populate
  `date_range`, render as a band, not a dot.

## Extensibility contract

To add a source to Time Machine:

1. Emit `ChangeEvent`s from the source's history endpoint (raw fields always
   populated).
2. Provide a mapping from the source's raw vocabulary → the `change_type`
   codelist + `tier`.
3. Set `date_basis` / `date_confidence` honestly.

The renderer, the tier suppression, the boost engine and the BODS mapping are
**unchanged**. The codelist is the contract.

## Worked example — Wm Morrison Supermarkets (recommended demo subject)

`213800IN6LSRGTZSOS29` — the 2021 Clayton, Dubilier & Rice take-private. The
single best demo entity found: a real ownership change visible **live** in both
GLEIF record history and GLEIF relationship history, all key-free.

**What the allowlist correctly surfaces (notable):**

| Date (GLEIF-recorded) | `change_type` | Tier | Detail |
|---|---|---|---|
| 2021-12-09 | `LEGAL_NAME_CHANGE` | 2 | "WM MORRISON SUPERMARKETS **P L C**" → "… **LIMITED**" — the take-private fingerprint |
| 2022-01-11 | `LEGAL_FORM_CHANGE` | 2 | ELF `B6ES` (PLC) → `H0PO` (private limited) |
| period from 2021-11-01 | `OWNER_ADDED` | 1 | new direct parent **MARKET BIDCO LIMITED** (`549300RKU7UEPSC42U63`, CD&R vehicle), `IS_DIRECTLY_CONSOLIDATED_BY` |

The parent's own direct/ultimate parent is a **reporting exception** (CD&R funds
have no LEI) — the timeline ends honestly at "top of the public tree."

**What it correctly suppresses (noise):** of ~55 LEI-record modifications, only
the three above are notable. The rest: `NextRenewalDate` ×7, `LastUpdateDate`
×9, `@xml:lang` inserts, `EntityCreationDate` (1940) backfill, `EntityCategory`
backfill, a `LAPSED`→`ISSUED` renewal cycle (Jan 2022), and address `Region`
recodes (GB-UKM→GB-ENG→GB-BRD). Noise ratio ≈ 95%.

**Two gotchas the dry-run surfaced — now folded into the rules above:**

1. **LegalForm encoding changes are false positives.** A 2018 modification
   moved the form from free-text `8888` + OtherLegalForm "PUBLIC LIMITED
   COMPANY" to ELF code `B6ES` — *still PLC*, just better encoded. A naive
   "LegalForm changed → notable" rule fires wrongly here. **Guard:** map ELF
   codes to a form *class* and only flag when the class actually changes (the
   real PLC→Ltd is the *2022* `B6ES`→`H0PO` event, not the 2018 recode).

2. **Relationship dates lag by years** (see the critical rule under BODS
   mapping). Use the period start, not the publish date.

**Cross-source bonus:** the relationship's `ValidationReference` fields are
literal Companies House filing-history PDF URLs
(`find-and-update.company-information.service.gov.uk/company/00358949/…`) — GLEIF
Level-2 is corroborated *by* CH filings. As a *change* it's noise, but it's a
ready-made provenance link between the GLEIF and CH halves of the same timeline.

## Third emitter — New Zealand (dated-record reconstruction)

New Zealand is a third *shape* of source, which is exactly what the
source-agnostic model was built to absorb. GLEIF is a field-diff stream
(`recorded` dates), Companies House a typed filing stream (`effective` dates);
**New Zealand is dated current-and-historic records** that we reconstruct events
from — and the dates are real effective dates, so NZ events are
`DateBasis.EFFECTIVE` / `DateConfidence.HIGH` (no snapshot-window guessing).

The NZBN `FullEntity` (already fetched for the NZ source) plus three dated
history endpoints feed `timeline/nz_companies.py` → `nz_change_events()`, which
emits `ChangeEvent`s directly (passed to the assembler via `extra_events`, since
there's no raw stream to classify):

| NZ record | ChangeEvent |
|---|---|
| `shareAllocation[].shareholder[]` `appointmentDate` / `vacationDate` (current) | `OWNER_ADDED` (+ `OWNER_REMOVED`), interest start/end; share % in `counterparty` |
| `historicShareholder[]` (`appointmentDate` + `vacationDate`) | `OWNER_ADDED` + `OWNER_REMOVED` |
| `roles[]` directors (`startDate` / `endDate`) | `OWNER_ADDED` / `OWNER_REMOVED` (control; role in `counterparty`) |
| `/history/entity-names` | `LEGAL_NAME_CHANGE` (transition from prior name) |
| `/history/entity-statuses` | `STATUS_CHANGED` |
| `/history/addresses` (per address type) | `ADDRESS_CHANGE` |

Notes: shareholders are ownership and directors are control, but the codelist
has no director-specific type — both use `OWNER_ADDED`/`OWNER_REMOVED` (Tier 1)
with the role + share % carried in `counterparty` (e.g. *"John Doe — director"*,
*"Jane Smith — shareholder (60.0%)"*). Identity history endpoints give dated
transitions (the earliest entry is the original state, not a change). NZ is
gated on `NZBN_API_KEY` and entered from the NZ source card's "See timeline"
button — which is what makes the timeline useful for NZ entities, since for them
GLEIF is mostly admin noise and Companies House is empty. NZ ownership events
stay their own entries (different identifiers from GLEIF parents / CH PSCs);
name/status changes can corroborate GLEIF's via the existing entity de-dup.

Follow-on: a per-entity deep link on the NZ source chip (v1 shows the label
only), and promoting directors to a distinct control change type if the generic
"Owner / parent" label proves confusing in testing.

## Open questions / next steps

1. **Codelist freeze.** Lock the `change_type` values and the two allowlists
   above (Stephen to edit). This is the editable artifact.
2. **CH band-crossing logic.** Confirm we map *only* statutory band crossings
   (25/50/75/100%), not raw percentage wobble.
3. **Boost engine scope.** Decide the co-occurrence window and which risk
   signals are eligible to boost.
4. **OO `bodspipelines` reconciliation.** Confirm the recordId / recordStatus
   conventions to match for interop.
5. **API shape.** Likely `GET /history?lei=` (and `?company_number=`),
   returning `ChangeEvent[]` + a `tier`-filtered default view; lazy, never on
   the main lookup — same pattern as `/securities`.
6. **Cheapest test.** Run GLEIF + CH history for one entity that *did* change
   ownership (not Heineken) and hand-verify the allowlist catches the real
   moves and drops the noise, before building the renderer.
