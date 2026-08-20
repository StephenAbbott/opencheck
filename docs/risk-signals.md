# OpenCheck — Risk Signals

All deterministic — every firing is documented with a `summary`, `confidence` (`high` / `medium` / `low`), and an `evidence` payload citing the underlying topic / collection / BODS statement IDs that triggered it.

Risk signals fall into three groups:

1. **Source-derived** — read straight off a single source's payload at search time.
2. **AMLA CDD RTS** — derived from the assembled BODS v0.4 bundle, mirroring the objective conditions in [the EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) for "complex corporate structures".
3. **Cross-source name match** — for every related person and entity inside the BODS bundle, search OpenSanctions and EveryPolitician by name (with optional birth-year compatibility) and surface a scoped signal on the matching node.

## Source-derived signals

- `PEP` — OpenSanctions `role.pep`-family topic, every EveryPolitician hit, or a Wikidata person with a currently-held position (P39 with no P582 end qualifier).
- `SANCTIONED` — OpenSanctions `sanction` topic: the record is itself the subject of a designation. Never fires on the adjacency subtopics below, and — since Phase 105 — never on `sanction.counter`.
- `COUNTER_SANCTIONED` — OpenSanctions `sanction.counter`: the record is designated under a counter-sanctions regime, which upstream defines as ["designations imposed by countries with weak democratic institutions … often punitive responses to foreign sanctions, or … used to suppress domestic political opposition"](https://www.opensanctions.org/docs/topics/). Structurally a direct listing of the record, and so reported at `high` confidence — the listing is a fact. What it is *not* is a designation by an authority the reader is likely to owe an obligation to, and appearing on such a list is frequently a consequence of journalism, sanctions enforcement or human-rights work. Rendered in slate at graph severity **2** — below plain adjacency, and deliberately outside the rose/amber sanctions colour ramp. Fires alongside `SANCTIONED` when a record carries both topics; neither substitutes for the other.
- `SANCTIONS_CONTROLLED` — OpenSanctions `sanction.control`: the record is a direct or **indirect** subsidiary, asset or vessel of a designated party. No percentage threshold and no depth limit are applied upstream (an end-dated shareholding stops the chain), so this is the starting point for an ownership-and-control test such as OFAC's 50 Percent Rule, not the answer to one. High confidence — the ownership assertion is deterministic; what is uncertain is the legal threshold, which the signal's summary states rather than discounting into the confidence level.
- `SANCTIONS_LINKED` — OpenSanctions `sanction.linked`: one-hop adjacency to a designated party (ownership, directorship, membership, employment, association, family, succession, or the company↔securities relationship). Upstream declares this topic a **superset** of `sanction.control`, so a controlled entity carries both and only `SANCTIONS_CONTROLLED` is surfaced. Any unrecognised `sanction.*` subtopic degrades to this signal — conservatively, and with a logged warning; `tests/test_opensanctions_live.py::test_sanction_family_is_fully_classified` fails the build when upstream adds one.
- `DEBARMENT` — OpenSanctions `debarment`: excluded from public contracts or procurement. A confirmed adverse listing, distinct from financial sanctions.
- `EXPORT_CONTROLLED` — OpenSanctions `export.control`: the record is itself subject to export-control restrictions (e.g. a BIS Entity List or Military End-User designation). *high* confidence. Added in Phase 118, after the `us_bis_mieu` dataset — all 13 of whose topic-bearing entities carry only this topic — was found to be fetched by search but classified into nothing.
- `EXPORT_CONTROL_LINKED` — OpenSanctions `export.control.linked`: adjacent to an export-controlled party; the record is not itself restricted. *medium* confidence. Unlike `sanction.linked` over `sanction.control`, upstream declares **no superset relationship** within the export family (verified against `model.json`, 2026-08-20), so nothing is suppressed when `EXPORT_CONTROLLED` also fires. Any unrecognised `export.*` subtopic degrades to this signal — conservatively, with a logged warning; `tests/test_opensanctions_live.py::test_export_family_is_fully_classified` fails the build when upstream adds one.
- `EXPORT_RISK` — OpenSanctions `export.risk` (upstream label "Trade risk"): flagged for trade-diversion risk, softer than a listing. *medium* confidence.

  Beyond the three families above, every topic in the adapter's `_RISK_TOPICS` must either map to a signal or appear on the explicit informational allowlist in `risk.py` (`_INFORMATIONAL_TOPICS` — 18 topics today: `poi`, `wanted`, `role.oligarch`, the `crime.*` family, `invest.*`, `mare.*`, `reg.*`, `corp.disqual`). `tests/test_opensanctions_live.py::test_every_risk_topic_is_classified_or_allowlisted` enforces the partition, so "we fetch it but don't understand it" is a build failure rather than a discovery — promoting an allowlisted topic to a signal is a deliberate edit with its own ticket.
- `OFFSHORE_LEAKS` — a name in the BODS bundle matches a record in the ICIJ Offshore Leaks database (Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, Offshore Leaks) via the ICIJ reconciliation API; or an OpenAleph hit in an ICIJ-family collection (OpenAleph is currently disabled in `REGISTRY` but this signal also fires via the ICIJ name cross-check, which requires no API key).
- `OPAQUE_OWNERSHIP` — a party exists whose identity is deliberately withheld or could not be obtained; every firing is a claim the register itself makes (*high* confidence). Three families:
  - **GLEIF `NON_PUBLIC`-family reporting exceptions** — the entity declines to name a known accounting-consolidation parent (`NON_PUBLIC` plus the five deprecated pre-2022 reasons `BINDING_LEGAL_COMMITMENTS`, `LEGAL_OBSTACLES`, `DISCLOSURE_DETRIMENTAL`, `DETRIMENT_NOT_EXCLUDED`, `CONSENT_NOT_OBTAINED`). Classified from the raw exception record's reason code, per the [GLEIF Level 2 Reporting Exceptions 2.1 format](https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-reporting-exceptions-2-1-format).
  - **`anonymousPerson` / `anonymousEntity` statements** — identifying details deliberately withheld, e.g. a Companies House super-secure PSC protected by court order.
  - **Unspecified-`interestedParty` relationships** whose reason is `subjectUnableToConfirmOrIdentifyBeneficialOwner` or `interestedPartyHasNotProvidedInformation` — the register says an owner exists but is unidentified or non-cooperative (`noBeneficialOwners` and `interestedPartyExemptFromDisclosure` deliberately do not fire).

  Deliberately does **not** fire on `unknownPerson` / `unknownEntity` statements: unknown-to-this-source is a different claim from deliberately-withheld, and the GLEIF reporting-exception bridges use exactly those types for the benign exception reasons below. GLEIF Level 2 only ever names *entities* (LEI holders), so no wording anywhere claims an "unknown person in the ownership chain" on GLEIF evidence.
- `GLEIF_REPORTING_EXCEPTION` — **context, not risk** (`kind: "context"`, *high* confidence). A GLEIF Level 2 reporting exception from the benign/structural family, i.e. the [permitted reasons under the LEI ROC policy](https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-reporting-exceptions-2-1-format) for having no accounting-consolidation parent record:
  - `NATURAL_PERSONS` — controlled directly by natural person(s) with no intermediate consolidating legal entity (common for founder-, family-owned **and** widely held companies — Eli Lilly reports this for both categories);
  - `NO_KNOWN_PERSON` — no known controlling person (diversified shareholding);
  - `NON_CONSOLIDATING` — controlling entities not subject to preparing consolidated financial statements;
  - `NO_LEI` — a parent exists but does not consent to have an LEI, so it is not identified in GLEIF (distinct wording in the summary: a real transparency gap in GLEIF coverage, but national registers may still identify the parent — not evidence of opacity).

  Unrecognised future reason codes fall into this context signal rather than the risk one. The evidence carries each exception's `category` (`DIRECT_ACCOUNTING_CONSOLIDATION_PARENT` / `ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT`), `reason`, `reference` (the legal basis, when supplied) and the bridging statement's `statement_id` for graph badges.

## Geographic risk — the standing rule

**OpenCheck emits a geographic *risk* signal only where an authoritative, externally maintained, dated list says so.** Today that means the FATF lists and the EU's Article 29 list, below. Everything else about where an ownership chain reaches is reported as **context** (`kind: "context"`), not risk.

This is not conservatism for its own sake — it follows the framework. AMLR **Annex III(3)** enumerates higher-risk geography as *qualified* categories of third country (FATF-listed, no effective AML/CFT system, significant corruption, sanctioned, terrorist financing, financial secrecy), and **Annex II** places third countries with effective AML/CFT systems in the **lower**-risk column. "Being a third country" is on neither list.

Two things were considered and deliberately **not** built:

- **A jurisdiction-level signal for Annex III(3)(f)(iv)** — "not requiring beneficial ownership information to be recorded or held in a central database or register". Three problems: the ground truth moves faster than any list we could ship (FinCEN removed US domestic BO reporting in August 2026); the criterion turns on whether information is *recorded*, not published, so a register open to law enforcement but closed to the public **satisfies** it while being invisible to a public-data tool; and the only observation actually available to us — "we found no BO data" — conflates *no register exists*, *the register is closed*, and *OpenCheck has no adapter for it yet*. The third case is our coverage gap, and shipping it as jurisdiction risk would turn that gap into an accusation against a country. Where OpenCheck genuinely observes an absence, that is entity-scoped and `OPAQUE_OWNERSHIP` already models it.
- **An Annex II lower-risk allowlist** — deciding which third countries have "effective AML/CFT systems" means adjudicating FATF mutual-evaluation outcomes that are 5–10 years old and rated across 40 Recommendations and 11 Immediate Outcomes. That is an editorial position to defend and maintain, in order to suppress a chip that is no longer emitted.

### Signal kinds

Every signal carries `kind`: `"risk"` (an adverse finding) or `"context"` (a structural observation). The classification lives on the signal, not in per-surface exclusion lists, because the risk **count** is rendered independently by the results page, the OG share card and the share-page meta description. A missing `kind` is read as `"risk"`, so payloads cached before the field existed behave as before.

## Jurisdiction list signals

- `FATF_BLACK_LIST` — entity registered in a FATF High-Risk Jurisdiction subject to a Call for Action. *high* confidence. Update at each plenary (typically February, June, October).
- `FATF_GREY_LIST` — entity registered in a FATF Jurisdiction under Increased Monitoring. *medium* confidence.
- `EU_HIGH_RISK_THIRD_COUNTRY` — entity registered in a jurisdiction on the Annex to **Commission Delegated Regulation (EU) 2016/1675**, as amended. *high* confidence, because unlike a FATF listing an EU designation is a binding trigger for mandatory enhanced due diligence rather than an international assessment.

  Kept as a **separate code** from the FATF signals, not a widening of them. The two instruments diverge in practice, because the Commission adopts its updates months after the FATF plenary that prompted them — as of the current lists, Algeria and Namibia are EU-listed but were removed from the FATF grey list in June 2026, Myanmar is FATF black but sits in the EU's Section I, and Bosnia and Herzegovina, Bulgaria, Iraq, Kuwait and Papua New Guinea are FATF grey but not EU-listed. Folding them together would leave the summary unable to say which instrument applies, and a delisting from one would silently move a signal attributed to the other.

## AMLA CDD RTS signals (BODS v0.4 derived)

These mirror the objective conditions in the EU AMLA draft customer due diligence regulatory technical standards for "complex corporate structures".

- `TRUST_OR_ARRANGEMENT` — entity with `entityType=arrangement` or a legal-form keyword (`trust`, `Stiftung`, `Anstalt`, `fideicomiso`, `Treuhand`, `foundation`). AMLA condition (a).
- `NON_EU_JURISDICTION` — any entity statement's `recordDetails.jurisdiction.code` (BODS v0.4; falls back to a top-level `incorporatedInJurisdiction` dict for v0.3-shaped pass-through fixtures) outside the EU+EEA. Configurable via `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` (additive, e.g. `GB,CH`) or `OPENCHECK_AMLA_EU_EEA_OVERRIDE` (full replace).

  **This is a structural observation, not an adverse finding.** Neither the AMLA draft RTS nor AMLR Annex III treats non-EU status as a risk factor in its own right. Annex III(3) lists only *qualified* categories of third country — FATF-listed, no effective AML/CFT system, significant corruption, sanctioned, terrorist financing, financial secrecy — and AMLR Annex II puts third countries with effective AML/CFT systems in the **lower**-risk column. This signal reports where the ownership chain reaches and feeds AMLA condition (b) in the composite below; it does not assert that a jurisdiction is higher risk. For that, see `FATF_GREY_LIST` / `FATF_BLACK_LIST`.

  Dedups on `(code, source_id, hit_id)` like the FATF jurisdiction signals, so every non-EU node found by every source keeps its graph badge.

### Dedup: which codes collapse globally

`_merge_signals` in `routers/lookup.py` collapses "structural" codes on `(code,)` alone — and because it assigns rather than combines, only the last-processed source's evidence survives. Membership of that set is therefore **not** "is this a structural claim?" but a narrower test: **does the signal's evidence identify particular nodes?**

`TRUST_OR_ARRANGEMENT`, `NOMINEE` and `NON_EU_JURISDICTION` all carry per-node `statement_id`s (in `evidence.matches[]` / `evidence.jurisdictions[]`) that the graph reads to draw badges, so they dedup **per source**. `COMPLEX_OWNERSHIP_LAYERS`, `COMPLEX_CORPORATE_STRUCTURE`, `POSSIBLE_OBFUSCATION` and `SANCTIONED_SECURITY` are whole-structure claims and still collapse globally.

`COMPLEX_OWNERSHIP_LAYERS` must **not** be moved out — per source it would report conflicting layer counts, and the chip strip picks its winner by confidence rather than depth, so the number shown would be arbitrary. It stays globally collapsed and resolves the conflict with a **depth resolver**: the deepest chain wins, so the reported depth no longer depends on which source happened to be processed last, and the surviving `longest_path` is the chain that justifies the number.

That resolver is exactly equivalent to computing the depth over the merged bundle, because statement ids are namespaced per source (`_stable_id(source_id, …)`) — no ownership edge ever bridges two sources, so `bods_all` is a concatenation of disjoint subgraphs and its longest path *is* the maximum of the per-source longest paths.
- `NOMINEE` — a nominee shareholder or director arrangement. AMLA condition (c). Two grades of evidence, reported distinctly:
  - **structured** (*high* confidence) — the register filed a nominee code. Companies House / Register of Overseas Entities `natures_of_control` codes matching `registered-owner-as-nominee-*` (six of them; see `bods/psc_natures.NOMINEE_NATURE_CODES`). The code travels in the signal's evidence so a reviewer can check the filing rather than our reading of it. Ceased PSCs are excluded — that arrangement has ended.
  - **textual** (*medium* confidence) — the word "nominee" (or `prête-nom` / `fiduciaire` / camelCase variants) appears in an interest type, an interest's details, or a person record. Real evidence, but weaker than a filed code: "Nominee Services Ltd" is a company name, not a declaration. The summary says outright that it matched on descriptive text.

  `evidence.basis` is `"structured"` or `"textual"`. A filed code reports as structured even though the mapper also renders it into `interest.details` — the same fact must not be reported by its weaker trace.
- `COMPLEX_OWNERSHIP_LAYERS` — DFS over the BODS relationship graph finds an entity-only chain ≥3 nodes (cycle-safe). Made meaningfully detectable by the Phase 10 Open Ownership bundles, which carry full multi-layer chains.
- `COMPLEX_CORPORATE_STRUCTURE` — composite (high confidence), fires when `COMPLEX_OWNERSHIP_LAYERS` fires **and ≥2** of AMLA conditions (a)–(c) {trust, non-EU on the layered path, nominee} are met.

  Article 12(1) of the draft RTS requires three or more layers "and, in addition, **more than one** of the following conditions is met" — i.e. at least two. Conditions **(a)** and **(b)** are evaluated over the layered ownership path only, because both are worded "in any of the(se) layers": a trust or a non-EU entity on a side branch raises its standalone signal but does not count here. Condition **(c)** is deliberately left bundle-wide — it reads "nominee shareholders or nominee directors involved **in the structure**", which is looser than the other two.

  Condition (d) is deliberately excluded — its "no legitimate economic rationale" limb cannot be judged from data, so it is surfaced advisorily as `POSSIBLE_OBFUSCATION` instead of being allowed to push a structure over this threshold. That makes the rule able to under-fire relative to the RTS text, which is the right direction for a claim of this kind.

  Tracks a **draft** RTS (consultation closed 8 May 2026; final text not yet adopted, and AMLA has not yet published all responses). Revisit on adoption.
- `POSSIBLE_OBFUSCATION` — advisory (low confidence) mirror of AMLA's subjective condition; explicitly notes the legitimate-economic-rationale caveat.

## Ownership structure (BODS v0.4 derived)

- `STATE_CONTROLLED` — `medium` — a controlling owner is modelled as a `state` or `stateBody` entity, i.e. the subject connects (directly or indirectly) to a state per the BODS [Representing state-owned enterprises](https://standard.openownership.org/en/0.4.0/standard/modelling/repr-state-owned-enterprises.html) requirement — a possible state-owned enterprise. Source-agnostic (any source whose BODS carries a `state`/`stateBody` owner), but currently fed by the Wikidata controlling-owner extraction (`P127`/`P749`). **Presence-only and corroborating**: Wikidata is crowd-sourced and famous-names-only, so the signal's *absence is not evidence* an entity is privately owned, and it is never a determination. Not part of the AMLA composite. Evidence carries the `state`/`stateBody` node (`statement_id`) and the controlled entity (`subject_statement_id`). See [docs/wikidata-ownership.md](wikidata-ownership.md).

## FATF jurisdiction signals (BODS v0.4 derived)

For every `entityStatement` in the assembled BODS bundle, OpenCheck checks `incorporatedInJurisdiction.code` against the FATF lists current as of February 2026 (refreshed each FATF plenary: typically February, June, and October). Two independent signals, with different confidence levels reflecting FATF's own severity distinction:

- `FATF_BLACK_LIST` — `high` — entity in the FATF High-Risk Jurisdictions (Call for Action) list: **Democratic People's Republic of Korea (KP), Iran (IR), Myanmar (MM)**.
- `FATF_GREY_LIST` — `medium` — entity in the FATF Jurisdictions under Increased Monitoring list (June 2026 plenary): Angola, Bolivia, Bosnia and Herzegovina, Bulgaria, Cameroon, Côte d'Ivoire, Democratic Republic of Congo, Haiti, Iraq, Kenya, Kuwait, Laos, Lebanon, Monaco, Nepal, Papua New Guinea, South Sudan, Syria, Venezuela, Vietnam, British Virgin Islands, Yemen.

Both signals are derived purely from the BODS jurisdiction codes — they fire independently of the AMLA CDD RTS composite rule and require no additional source calls. The country code sets live in `risk.py` (`FATF_BLACK_LIST_CODES` / `FATF_GREY_LIST_CODES`) and should be updated after each FATF plenary.

## Cross-source name match

For every `personStatement` and `entityStatement` in the assembled BODS bundle, OpenCheck searches OpenSanctions (and EveryPolitician for persons) by name. Matches above a similarity threshold of 0.88 — with optional birth-year compatibility (±1 year, only when both sides supply a DOB) — produce **scoped** signals attached to the matching related-party's `statementId` (in `evidence.subject_statement_id`), not the subject. That means a sanctioned PSC behind an otherwise clean shell company surfaces on the right node in the graph.

### Corroboration: what a person match rests on

A name match is weak evidence about a *person*, and the scorer cannot be tuned out of it. `Michael R. Gordon` and `Michael E. Gordon` — definitively different people — score **0.9375**, above the 0.9333 of a genuine abbreviation pair (`Michael Gordon` / `Michael R. Gordon`). A middle initial is two characters, so it barely moves a character-similarity score; raising the threshold drops true positives before it drops that pair. `test_conflicting_middle_initials_still_score_above_the_gate` pins the inversion, and fails if it ever reverses — at which point a threshold fix becomes viable and this rule can be revisited.

So discrimination comes from a second attribute. `corroborating_attributes()` reports which of **birth year** (±1) and **nationality** agree, requiring *both* sides to supply the attribute. A hit that publishes no birth date is not corroborated by our knowing one — it is unchecked, and reporting silence as agreement is exactly how the Liverpool FC false positive read as a finding.

Person signals are then capped when nothing but the name agrees — never `high`, since "high confidence this is the same person" is the one claim a name alone cannot support:

| | corroborated | name only |
|---|---|---|
| score ≥ 0.95 | `high` | `medium` |
| 0.88 ≤ score < 0.95 | `medium` | `low` |

The cap is two-tier rather than a flat `low` on purpose. Sanctions screening *is* name-based: an exact match against a designated person is something a reviewer must adjudicate, and filing every such hit under `low` would misreport standard practice as a weak lead. Either way the summary opens with **"Possible name match only"** and names what is missing, so the prose never asserts identity regardless of the rung. `evidence.corroboration` lists the agreeing attributes and `evidence.name_match_only` is the boolean.

**Entities keep the score-only ladder.** An organisation name is distinctive in a way a personal name is not — "Gazprom PJSC" matching "Gazprom" is a finding on its own.

The OpenAleph percolation screen shares all of this: it matches on names for the same reasons, so it imports `match_confidence()` / `match_summary()` rather than keeping a second copy. BackgroundCheck (`/person-check`) is unaffected — it is an explicit person search presenting candidate matches with its own strong/weak split, not an assertion about a node in an ownership graph.

- `RELATED_PEP` — a related person matches an OpenSanctions PEP record or appears in EveryPolitician.
- `RELATED_SANCTIONED` — a related person or entity matches an OpenSanctions record carrying `sanction`.
- `RELATED_COUNTER_SANCTIONED` — the match carries `sanction.counter`: a related party designated under a counter-sanctions regime. Emitted **last** in the ladder despite being a direct listing, so it is never the headline finding a consumer takes from `[0]`.
- `RELATED_SANCTIONS_CONTROLLED` — the match carries `sanction.control`: a related party inside a designated party's ownership chain.
- `RELATED_SANCTIONS_LINKED` — the match carries `sanction.linked` only: plain adjacency.
- `RELATED_DEBARMENT` — the match carries `debarment`.
- `RELATED_EXPORT_CONTROLLED` — the match carries `export.control`: a related party itself subject to export-control restrictions.
- `RELATED_EXPORT_CONTROL_LINKED` — the match carries `export.control.linked`: adjacent to an export-controlled party.
- `RELATED_EXPORT_RISK` — the match carries `export.risk`: flagged for trade risk.

These follow the **same reporting rule as the subject-level signals**: every fact a matched record asserts is reported, so a related party that is designated *and* inside another designated party's ownership chain surfaces both `RELATED_SANCTIONED` and `RELATED_SANCTIONS_CONTROLLED`. The one suppression is `RELATED_SANCTIONS_LINKED` when `RELATED_SANCTIONS_CONTROLLED` fires — upstream declares `sanction.linked` a superset of `sanction.control`, so the weaker code is the same fact stated less precisely rather than an additional one. Signals are ordered most-severe-first (direct listing → ownership chain → debarment → export-control listing → sanction adjacency → export adjacency → trade risk → PEP → counter-designation), so a consumer taking the first still gets the headline finding. The export-control rung sits above plain sanction adjacency because it is a restriction on the related party itself, and nothing within the export family is suppressed (no superset relationship upstream). `RELATED_COUNTER_SANCTIONED` sits at the end by design: it is structurally a direct listing, but by a regime the reader almost certainly owes no obligation to, so promoting it would misreport the finding. The OpenAleph percolation screen shares this rule, so the two cannot diverge.

The normaliser folds standalone non-ASCII letters (Polish `ł`, Norwegian `ø`, German `ß`, Icelandic `ð`/`þ`, French `œ`) so transliterated and native spellings match. Bounded at `max_targets=25` per lookup to keep the OpenSanctions request volume sane on large PSC chains. The cross-check is a no-op when live mode is off or no OpenSanctions API key is configured.

## ICIJ Offshore Leaks name cross-check

For every `personStatement` and `entityStatement` in the assembled BODS bundle, OpenCheck posts each name to the [ICIJ Offshore Leaks reconciliation API](https://offshoreleaks.icij.org/docs/reconciliation) in batches of 10. The API covers roughly 800,000 offshore entities and associated individuals across the Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, and the original Offshore Leaks dataset.

- `OFFSHORE_LEAKS` — a name matches an ICIJ Offshore Leaks record. Confidence is `high` when ICIJ's own `match: true` flag is set; `medium` when the score ≥ 70 without the ICIJ match flag.

A secondary token-overlap similarity check (≥ 0.45 Jaccard) guards against false positives when the ICIJ index blends multiple transliterations of the same name. Signals are scoped to the matching BODS `statementId` (in `evidence.subject_statement_id`) — the same deduplication logic as `RELATED_PEP` / `RELATED_SANCTIONED`. No API key is required; the check runs in live mode automatically. Bounded at `max_targets=30`.
