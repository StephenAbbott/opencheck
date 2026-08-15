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
- `OFFSHORE_LEAKS` — a name in the BODS bundle matches a record in the ICIJ Offshore Leaks database (Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, Offshore Leaks) via the ICIJ reconciliation API; or an OpenAleph hit in an ICIJ-family collection (OpenAleph is currently disabled in `REGISTRY` but this signal also fires via the ICIJ name cross-check, which requires no API key).
- `OPAQUE_OWNERSHIP` — BODS bundle contains a `personStatement` with `personType=unknownPerson` or an `entityStatement` with `entityType=anonymousEntity`.

## AMLA CDD RTS signals (BODS v0.4 derived)

These mirror the objective conditions in the EU AMLA draft customer due diligence regulatory technical standards for "complex corporate structures".

- `TRUST_OR_ARRANGEMENT` — entity with `entityType=arrangement` or a legal-form keyword (`trust`, `Stiftung`, `Anstalt`, `fideicomiso`, `Treuhand`, `foundation`). AMLA condition (a).
- `NON_EU_JURISDICTION` — any entity statement's `incorporatedInJurisdiction.code` outside the EU+EEA. AMLA condition (b). Configurable via `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` (additive, e.g. `GB,CH`) or `OPENCHECK_AMLA_EU_EEA_OVERRIDE` (full replace).
- `NOMINEE` — a nominee shareholder or director arrangement. AMLA condition (c). Two grades of evidence, reported distinctly:
  - **structured** (*high* confidence) — the register filed a nominee code. Companies House / Register of Overseas Entities `natures_of_control` codes matching `registered-owner-as-nominee-*` (six of them; see `bods/psc_natures.NOMINEE_NATURE_CODES`). The code travels in the signal's evidence so a reviewer can check the filing rather than our reading of it. Ceased PSCs are excluded — that arrangement has ended.
  - **textual** (*medium* confidence) — the word "nominee" (or `prête-nom` / `fiduciaire` / camelCase variants) appears in an interest type, an interest's details, or a person record. Real evidence, but weaker than a filed code: "Nominee Services Ltd" is a company name, not a declaration. The summary says outright that it matched on descriptive text.

  `evidence.basis` is `"structured"` or `"textual"`. A filed code reports as structured even though the mapper also renders it into `interest.details` — the same fact must not be reported by its weaker trace.
- `COMPLEX_OWNERSHIP_LAYERS` — DFS over the BODS relationship graph finds an entity-only chain ≥3 nodes (cycle-safe). Made meaningfully detectable by the Phase 10 Open Ownership bundles, which carry full multi-layer chains.
- `COMPLEX_CORPORATE_STRUCTURE` — composite (high confidence), fires when `COMPLEX_OWNERSHIP_LAYERS` AND ≥1 of {trust, non-EU, nominee} both fire — the AMLA threshold rule end-to-end.
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

- `RELATED_PEP` — a related person matches an OpenSanctions PEP record or appears in EveryPolitician.
- `RELATED_SANCTIONED` — a related person or entity matches an OpenSanctions record carrying `sanction`.
- `RELATED_COUNTER_SANCTIONED` — the match carries `sanction.counter`: a related party designated under a counter-sanctions regime. Emitted **last** in the ladder despite being a direct listing, so it is never the headline finding a consumer takes from `[0]`.
- `RELATED_SANCTIONS_CONTROLLED` — the match carries `sanction.control`: a related party inside a designated party's ownership chain.
- `RELATED_SANCTIONS_LINKED` — the match carries `sanction.linked` only: plain adjacency.
- `RELATED_DEBARMENT` — the match carries `debarment`.

These follow the **same reporting rule as the subject-level signals**: every fact a matched record asserts is reported, so a related party that is designated *and* inside another designated party's ownership chain surfaces both `RELATED_SANCTIONED` and `RELATED_SANCTIONS_CONTROLLED`. The one suppression is `RELATED_SANCTIONS_LINKED` when `RELATED_SANCTIONS_CONTROLLED` fires — upstream declares `sanction.linked` a superset of `sanction.control`, so the weaker code is the same fact stated less precisely rather than an additional one. Signals are ordered most-severe-first (direct listing → ownership chain → debarment → adjacency → PEP → counter-designation), so a consumer taking the first still gets the headline finding. `RELATED_COUNTER_SANCTIONED` sits at the end by design: it is structurally a direct listing, but by a regime the reader almost certainly owes no obligation to, so promoting it would misreport the finding. The OpenAleph percolation screen shares this rule, so the two cannot diverge.

The normaliser folds standalone non-ASCII letters (Polish `ł`, Norwegian `ø`, German `ß`, Icelandic `ð`/`þ`, French `œ`) so transliterated and native spellings match. Bounded at `max_targets=25` per lookup to keep the OpenSanctions request volume sane on large PSC chains. The cross-check is a no-op when live mode is off or no OpenSanctions API key is configured.

## ICIJ Offshore Leaks name cross-check

For every `personStatement` and `entityStatement` in the assembled BODS bundle, OpenCheck posts each name to the [ICIJ Offshore Leaks reconciliation API](https://offshoreleaks.icij.org/docs/reconciliation) in batches of 10. The API covers roughly 800,000 offshore entities and associated individuals across the Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, and the original Offshore Leaks dataset.

- `OFFSHORE_LEAKS` — a name matches an ICIJ Offshore Leaks record. Confidence is `high` when ICIJ's own `match: true` flag is set; `medium` when the score ≥ 70 without the ICIJ match flag.

A secondary token-overlap similarity check (≥ 0.45 Jaccard) guards against false positives when the ICIJ index blends multiple transliterations of the same name. Signals are scoped to the matching BODS `statementId` (in `evidence.subject_statement_id`) — the same deduplication logic as `RELATED_PEP` / `RELATED_SANCTIONED`. No API key is required; the check runs in live mode automatically. Bounded at `max_targets=30`.
