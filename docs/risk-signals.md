# OpenCheck — Risk Signals

All deterministic — every firing is documented with a `summary`, `confidence` (`high` / `medium` / `low`), and an `evidence` payload citing the underlying topic / collection / BODS statement IDs that triggered it.

Risk signals fall into three groups:

1. **Source-derived** — read straight off a single source's payload at search time.
2. **AMLA CDD RTS** — derived from the assembled BODS v0.4 bundle, mirroring the objective conditions in [the EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) for "complex corporate structures".
3. **Cross-source name match** — for every related person and entity inside the BODS bundle, search OpenSanctions and EveryPolitician by name (with optional birth-year compatibility) and surface a scoped signal on the matching node.

## Source-derived signals

- `PEP` — OpenSanctions `role.pep`-family topic, every EveryPolitician hit, or a Wikidata person with a currently-held position (P39 with no P582 end qualifier).
- `SANCTIONED` — OpenSanctions topic starting with `sanction`.
- `OFFSHORE_LEAKS` — a name in the BODS bundle matches a record in the ICIJ Offshore Leaks database (Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, Offshore Leaks) via the ICIJ reconciliation API; or an OpenAleph hit in an ICIJ-family collection (OpenAleph is currently disabled in `REGISTRY` but this signal also fires via the ICIJ name cross-check, which requires no API key).
- `OPAQUE_OWNERSHIP` — BODS bundle contains a `personStatement` with `personType=unknownPerson` or an `entityStatement` with `entityType=anonymousEntity`.

## AMLA CDD RTS signals (BODS v0.4 derived)

These mirror the objective conditions in the EU AMLA draft customer due diligence regulatory technical standards for "complex corporate structures".

- `TRUST_OR_ARRANGEMENT` — entity with `entityType=arrangement` or a legal-form keyword (`trust`, `Stiftung`, `Anstalt`, `fideicomiso`, `Treuhand`, `foundation`). AMLA condition (a).
- `NON_EU_JURISDICTION` — any entity statement's `incorporatedInJurisdiction.code` outside the EU+EEA. AMLA condition (b). Configurable via `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` (additive, e.g. `GB,CH`) or `OPENCHECK_AMLA_EU_EEA_OVERRIDE` (full replace).
- `NOMINEE` — relationship interest type/details mentions nominee (English / French / camelCase variants), or person record mentions nominee. AMLA condition (c).
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
- `RELATED_SANCTIONED` — a related person or entity matches an OpenSanctions `sanction*` record.

The normaliser folds standalone non-ASCII letters (Polish `ł`, Norwegian `ø`, German `ß`, Icelandic `ð`/`þ`, French `œ`) so transliterated and native spellings match. Bounded at `max_targets=25` per lookup to keep the OpenSanctions request volume sane on large PSC chains. The cross-check is a no-op when live mode is off or no OpenSanctions API key is configured.

## ICIJ Offshore Leaks name cross-check

For every `personStatement` and `entityStatement` in the assembled BODS bundle, OpenCheck posts each name to the [ICIJ Offshore Leaks reconciliation API](https://offshoreleaks.icij.org/docs/reconciliation) in batches of 10. The API covers roughly 800,000 offshore entities and associated individuals across the Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, and the original Offshore Leaks dataset.

- `OFFSHORE_LEAKS` — a name matches an ICIJ Offshore Leaks record. Confidence is `high` when ICIJ's own `match: true` flag is set; `medium` when the score ≥ 70 without the ICIJ match flag.

A secondary token-overlap similarity check (≥ 0.45 Jaccard) guards against false positives when the ICIJ index blends multiple transliterations of the same name. Signals are scoped to the matching BODS `statementId` (in `evidence.subject_statement_id`) — the same deduplication logic as `RELATED_PEP` / `RELATED_SANCTIONED`. No API key is required; the check runs in live mode automatically. Bounded at `max_targets=30`.
