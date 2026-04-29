# OpenCheck — Project Plan

A corporate intelligence tool that checks entities against open data sources, surfaces risk signals and turns any ownership information it finds into BODS so users can download it, visualise it, and take it into other tools.

> **Status:** draft project plan. All design decisions here are provisional and up for revision once prototyping begins.
> **Owner:** Stephen Abbott Pugh (Understand Beneficial Ownership; GODIN co-organiser).
> **Audience for this doc:** primarily the author, as a working spec and roadmap. Sections 1–3 are also usable as a pitch when talking about the project externally.

---

## 1. Vision & positioning

You paste in a Legal Entity Identifier. OpenCheck queries GLEIF first, derives every cross-source identifier it can (UK Companies House number, Wikidata Q-ID, etc.), and uses those bridges to fan out across UK Companies House, OpenSanctions, OpenAleph, EveryPolitician, Wikidata, and OpenTender.

Everything maps into version 0.4 of the Beneficial Ownership Data Standard (BODS), the cross-source links + risk signals are computed deterministically, and the whole bundle is one click away from a downloadable shareable export.

The risk-signal layer mirrors the draft customer due diligence regulatory technical standards from the EU's Anti-Money Laundering Authority (AMLA) draft conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

### Why it earns its keep

- **Shows the GODIN thesis in action.** OpenCheck is a working demonstration of *why* GODIN exists: disparate open datasets become much more valuable when connected via shared identifiers. Every query surfaces cross-source links.
- **Normalises BODS as the interchange format.** Rather than being abstract, BODS becomes the invisible plumbing that makes cross-source analysis possible, and the download format users take away.
- **Promotes the BODS adapter ecosystem.** The export options map one-to-one to the author's adapter repos, giving them concrete, visible use cases.
- **Surfaces licensing in a way that most commercial tools don't.** Instead of burying attribution, OpenCheck foregrounds it — which is both ethically correct and a competitive differentiator.

### Positioning vs. inspiration projects

| Project | What it does | What OpenCheck takes from it | What OpenCheck does differently |
|---|---|---|---|
| [Linkurious OpenScreening](https://resources.linkurious.com/openscreening) | Name screening against sanctions/PEPs/ICIJ via a graph UI | The idea of combining open datasets for screening; the graph-first presentation of results | Chatbot-first UX, more sources, licensing attribution foregrounded, BODS as the export spine |
| [GLEIF Transparency Fabric](https://transparencyfabric.gleif.org/) | Visualises how open datasets connect via the LEI and other identifiers | The identifier-first mental model; showing where an entity appears across datasets | Entity/person-centric rather than dataset-centric; produces a report, not just a visualisation |
| [BO Explorer](https://github.com/openownership/beneficial-ownership-explorer) | Explores BODS-structured beneficial ownership data | Visualisation approach and BODS-native data model | Not limited to BODS registers — treats BODS as an output format, with heterogeneous inputs |

---

## 2. Users and use cases

**Primary user:** a non-developer doing due diligence — a journalist, civil society researcher, compliance officer, or policy analyst who wants a quick, credible first look at who a company or person is and whether there are red flags worth investigating.

**Secondary user:** a developer or data integrator evaluating BODS who wants to see a working example of how heterogeneous open data can be normalised into BODS and used downstream.

**Tertiary user:** the author themselves, using it as a reference implementation and a demo surface for consultancy conversations, conference talks and workshops.

### Explicit non-goals

- Not a replacement for Sayari, Kyckr, Dow Jones, Refinitiv or paid screening tools.
- Not a KYC system of record — it explicitly shows that "no hit" in these sources does not mean "clean."
- Not a universal search over all open data in the world — scope is fixed to the named sources for v1.
- No automated risk scoring or machine-generated guilt inference. It surfaces signals; the human decides.

---

## 3. Scope

### In scope for v1

- Search interface accepting an LEI which is used to look up information on the entity in question
- Entity lookups against: **Companies House (UK)**, **GLEIF**, **OpenSanctions**, **OpenAleph**.
- Individual lookups against: **Companies House (UK)**, **OpenSanctions**, **EveryPolitician**, **Wikidata**, **OpenAleph**.
- Hybrid data model: curated cached demo records for a set of known-interesting entities/people, with live API calls as a fallback.
- An intelligence report per query: summary of hits, risk signals, ownership graph (where data supports it), and per-source cards with explicit attribution.
- "Go deeper" on any source card — expand the full record from that source, and show mappings/links to equivalent records in the other sources.
- Convert any ownership/control data encountered into BODS 0.4.
- Visualise ownership structures using `@openownership/bods-dagre`.
- Download the BODS data for any report. Offer conversion to other formats via the author's BODS adapter repos.

### Out of scope for v1 (candidates for v1.1 / v2)

- Alerts / monitoring / "notify me when something changes."
- User accounts, saved reports, collaboration.
- Uploading one's own data for screening against the open sources.
- Full-text search over OpenAleph document collections beyond what its API returns for a given entity.
- Non-UK corporate registers (Norway, France, Nigeria, Latvia, Slovakia, etc.) — acknowledged as high-value v2 additions; entry point is via Open Ownership's BODS data explorer and national APIs.
- AI-generated narrative risk summaries beyond deterministic templating. (Optional v1.1 addition; see §7.)

---

## 4. Data sources, licensing and attribution

For each source, the plan records: what we query it for, how we query it, what license applies and how attribution is surfaced in-product. Where a source requires API keys, those are held server-side and never exposed to the client.

### 4.1 UK Companies House

- **What we query.** Company search, company profile, officers, Persons with Significant Control (PSC), filing history metadata. For individuals: officer appointments and PSC records across companies.
- **API.** [Companies House Public Data API](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference); PSC endpoint [here](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/persons-with-significant-control). Authenticated API key required.
- **Bulk option.** [Snapshot download product](https://download.companieshouse.gov.uk/en_output.html) plus [PSC data snapshots](http://download.companieshouse.gov.uk/en_pscdata.html) for seeding the cache.
- **License.** [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). Attribution required.
- **Attribution string.** "Contains public sector information licensed under the Open Government Licence v3.0 (Companies House)."
- **BODS mapping.** PSC → BODS person/entity + ownership-or-control statements. The PSC `natures_of_control` codelist maps to BODS `interests[].type` (see BODS skill reference). UK PSC is the canonical reference implementation for BODS.

### 4.2 GLEIF (Global Legal Entity Identifier Foundation)

- **What we query.** LEI-CDF (Level 1) entity records and RR-CDF (Level 2) relationship records and reporting exceptions, for any entity that holds an LEI.
- **API.** [GLEIF API](https://api.gleif.org/api/v1/), public and free.
- **License.** [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) for GLEIF Level 1 and Level 2. Attribution not required but appropriate as a matter of practice.
- **Attribution string.** "Contains LEI data from GLEIF, available under CC0 1.0."
- **BODS mapping.** Use the [published GLEIF → BODS 0.4 pipeline](https://github.com/openownership/bods-gleif-pipeline) and its dataset for offline seeding; for live queries, reuse the mapping logic. GLEIF was the first dataset published in BODS 0.4 — it is the reference.

### 4.3 OpenSanctions

- **What we query.** Matching API for name queries; entity endpoint for a specific canonical entity. Returns a rich FollowTheMoney (FtM) object with sanctions/PEP/crime/debarment context, cross-references to other datasets (including the [OpenOwnership dataset on OpenSanctions](https://www.opensanctions.org/datasets/openownership/) and [GEM energy ownership dataset](https://www.opensanctions.org/datasets/gem_energy_ownership/)).
- **API.** [OpenSanctions API](https://api.opensanctions.org/). Free tier confirmed suitable for the hosted public demo. Intent is to share OpenCheck with the OpenSanctions founder via GODIN once the project is public — early engagement will help surface any issues.
- **License.** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) for the open data release. OpenCheck is a non-commercial open-source project, so this fits directly; the per-export `LICENSE_NOTICE.md` (§4.7) warns any user who downloads data that OpenSanctions-derived content is non-commercial.
- **Attribution string.** "Data from OpenSanctions.org, licensed CC BY-NC 4.0." Link to [https://www.opensanctions.org](https://www.opensanctions.org) and to the specific source dataset(s) flagged in the hit.
- **BODS mapping.** OpenSanctions serves FtM — round-trip via the author's [`bods-ftm`](https://github.com/StephenAbbott/bods-ftm) repo. Ownership-bearing entities (companies, beneficial owners) map cleanly; sanctions/PEP/crime entries attach as risk flags rather than as BODS statements.

### 4.4 OpenAleph

- **What we query.** Entity search and entity profile. OpenAleph aggregates investigative datasets — company registries, leaks (ICIJ Offshore Leaks, FinCEN Files), sanctions lists, court records — and supports cross-collection search.
- **Endpoint.** [search.openaleph.org](https://search.openaleph.org/). Uses the Aleph REST API.
- **API.** Aleph REST API (v2). Authenticated for some collections; public collections accessible without auth.
- **License.** Per-collection. Many collections are CC BY or CC BY-SA; some are restricted. The API surfaces collection metadata including license and source — **OpenCheck must respect and display per-collection licenses, not apply a single blanket license to all OpenAleph results.**
- **Attribution string.** Per collection, surfaced on the source card alongside a link back to the collection page on OpenAleph.
- **BODS mapping.** OpenAleph uses FollowTheMoney internally — via the author's [`bods-ftm`](https://github.com/StephenAbbott/bods-ftm) repo. ICIJ Offshore Leaks content can additionally be processed via [`bods-icij-offshoreleaks`](https://github.com/StephenAbbott/bods-icij-offshoreleaks) against bulk data for the cached demos.

### 4.5 EveryPolitician (persons only)

- **What we query.** Name matching against current legislator and public-office-holder records.
- **Status.** [EveryPolitician](https://everypolitician.org) is a living project again. Originally built by mySociety and paused in 2019, it has been revived by OpenSanctions and is now actively maintained as part of the OpenSanctions ecosystem. As of the March 2026 relaunch, EveryPolitician covers **665,938 politicians in 175,076 positions across 258 countries and territories** — presidents, legislators, judges, senior officials, military commanders, anyone holding public authority.
- **How it's kept current.** OpenSanctions launched [Poliloom](https://www.opensanctions.org/articles/2026-03-24-poliloom/) on 24 March 2026, a crowdsourcing tool that breaks politician-data maintenance into short tasks (Wikidata login required). Poliloom uses LLMs to extract candidate politician data from Wikipedia and route it through human review. Verified data flows into EveryPolitician's PEP dataset and back into Wikidata, keeping all three in sync.
- **Data source / access.** Database browsable at [everypolitician.org](https://everypolitician.org); data integrated into OpenSanctions' [PEPs dataset](https://www.opensanctions.org/datasets/peps/) and accessible via the OpenSanctions API. Bulk data available via OpenSanctions dataset downloads. Every record is keyed to a Wikidata Q-ID.
- **License.** CC BY-NC 4.0 (via OpenSanctions).
- **Attribution string.** "EveryPolitician data, maintained by OpenSanctions. Licensed CC BY-NC 4.0."
- **Role in OpenCheck.** A first-class PEP source, used alongside OpenSanctions' sanctions/crime datasets and Wikidata's "position held" data. For persons, OpenCheck can query EveryPolitician via OpenSanctions' API and get structured current-position data keyed to Wikidata Q-IDs — the same identifier that bridges to Companies House directors, OpenAleph entities, and GLEIF LEI-holding entities. This materially strengthens the cross-source linking story: a PEP matched in EveryPolitician resolves to a Q-ID which we can then check against every other source in the stack.
- **Coverage caveat — honest framing.** Coverage is stronger for well-digitised polities (US, Europe) than for smaller or less documented national/regional legislatures. OpenCheck surfaces this by noting, on any report where EveryPolitician returned no hit, that non-hits are not proof of non-PEP status, just as with any other source.

### 4.6 Wikidata (persons, and entities where relevant)

- **What we query.** SPARQL or the Wikidata API for persons (Q5) matching a name, with their occupations, positions held, political party, citizenships, and identifiers (Q-ID, LEI, Companies House number, ISIN, etc.). Wikidata is the connective tissue that lets OpenCheck detect when a person mentioned in one source is the same as a person in another.
- **API.** [Wikidata Query Service (SPARQL)](https://query.wikidata.org/), [MediaWiki API](https://www.wikidata.org/w/api.php), and [wbsearchentities](https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities). Free and public.
- **License.** [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) for structured data.
- **Attribution string.** "Wikidata structured data, CC0 1.0." Always link the Q-ID.
- **Role in OpenCheck.** Primary identifier bridge for cross-source linking (§5.3) and for enriching person records with human context (DOB, occupations, positions held, aliases in other scripts). Wikidata is also a GODIN supporter — that relationship is noted in-product.

### 4.7 Licensing-aware download behaviour

A downloadable report or export must carry its composite license, per source:

- On download, OpenCheck writes an accompanying `LICENSE_NOTICE.md` listing each source that contributed data, its license, and its attribution string.
- For the BODS JSON export, each statement's `source` field is populated with a `source.type`, `description`, `url`, and `assertedBy` reflecting the origin — aligning with BODS 0.4's native source-recording mechanism.
- Any source with a non-commercial license (e.g. OpenSanctions free tier, Aleph CC BY-NC collections) is flagged in the export bundle, and the end user is warned in-UI if they select a commercial-looking export path.

---

## 5. Hybrid data strategy

Chosen approach: **curated cached demo corpus + live API fallback**.

### 5.1 Cache layout

A local filesystem cache (`data/cache/`) structured as:

```
data/
  cache/
    demos/                          # hand-curated, shipped with the repo
      entities/
        {canonical-id}.json         # one file per demo entity (BODS + metadata)
      persons/
        {canonical-id}.json
      README.md                     # notes on each demo and why it was chosen
    live/                           # runtime-populated, gitignored
      {source}/
        {query-hash}.json           # raw API responses, TTL-keyed
```

### 5.2 The demo corpus

8–12 hand-picked demos that together exhibit the full feature surface. Draft list (to be refined during prototyping):

- **Entity: a Russian-linked sanctioned entity** (exercises OpenSanctions, Aleph, and GLEIF LEI hit). E.g. Rosneft or a sanctioned Sberbank subsidiary.
- **Entity: a UK Scottish Limited Partnership with minimal PSC disclosure** (exercises Companies House, missing-info patterns).
- **Entity: an entity from the GEM Global Energy Ownership Tracker** (exercises OpenSanctions GEOT dataset + BODS-aligned energy data).
- **Entity: an entity with an ICIJ Offshore Leaks presence** (exercises `bods-icij-offshoreleaks` mapping and Aleph).
- **Entity: an EITI-listed SOE** (exercises state-body BODS mapping, EITI SOE database).
- **Entity: a publicly listed company with an LEI** (exercises GLEIF Level 1+2).

Each demo is stored as a pre-computed bundle: raw per-source responses (so the source cards render exactly as they would live), a pre-built BODS dataset, and a pre-rendered graph layout. This means the demo UX is reliable and fast, and demo day doesn't depend on any live service.

### 5.3 Cross-source identifier bridging

For an entity:

- **LEI** (from GLEIF) is the primary bridge for corporate entities.
- **UK company number** (GB-COH from [org-id.guide](https://org-id.guide)) for UK-registered companies.
- **Wikidata Q-ID** as the secondary bridge, especially for entities without an LEI.


### 5.4 Live fallback behaviour

When a user queries something not in the demo corpus:

- Each source adapter runs concurrently with a short timeout (5–8s) and a circuit breaker.
- Results are cached in `data/cache/live/{source}/{query-hash}.json` with a short TTL (1–24h depending on source) so repeated queries during a demo session are instantaneous.
- Any source that fails, rate-limits, or errors is shown with its specific error state rather than being silently hidden ("OpenSanctions did not respond — click to retry").

### 5.5 "Live vs cached" badging

The UI always shows, per source card:

- **Live** — fetched from the source's API in this session.
- **Cached** — served from our cache (with a "fetched {timestamp}" note and a refresh button).
- **Demo** — a hand-curated, versioned snapshot (with a visible "demo data" flag).

This is a commitment to the honesty the project requires: we never let a cached result masquerade as a live one.

---

## 6. Report format

### 6.1 Report structure

A report is composed of these sections:

1. **Subject header.** Canonical name, one-line disambiguation ("UK private company, dissolved 2021" or "Russian businessman, b. 1957"), primary identifiers (LEI, CH number, Wikidata Q-ID), jurisdictions, key dates.
2. **Risk banner.** A deterministic chip strip: *Sanctioned*, *PEP*, *Appears in Offshore Leaks*, *Dissolved / Inactive*, *Incorporated in high-risk jurisdiction (FATF grey/black list)*, *Opaque ownership*, *No risk signals found*. Each chip is clickable to reveal the evidence. No AI-generated risk score.
3. **Source cards.** One card per source that returned a hit. Each card shows: source name + logo, attribution/license line, a 2–3 line summary of what that source knows, a "View full record" button (drills into the raw record), and a link back to the source's canonical URL.
4. **Ownership graph.** Rendered via `@openownership/bods-dagre` from the assembled BODS dataset. Shown only if any source returned ownership/control data.
5. **Cross-source links.** A small panel showing which sources we believe describe the same real-world entity/person, and why (shared LEI, shared Q-ID, name+DOB match, etc.).
6. **Export & download.** Buttons for: BODS JSON, BODS JSONL, FtM JSON (via `bods-ftm`), Neo4j CSV (via `bods-neo4j`), XML (via `bods-xml`), BigQuery CSV + GQL schema (via `bods-gql`), AML AI NDJSON (via `bods-aml-ai`), plus a shareable/printable intelligence report (Markdown and PDF).

### 6.2 Risk signals — deterministic rules

Signals are derived by rules, not inference. Examples:

- *Sanctioned* fires if OpenSanctions or Aleph surfaces a sanctions-list dataset hit.
- *PEP* fires if OpenSanctions PEP, Wikidata "position held" (current or recent), or EveryPolitician hit is present.
- *Appears in Offshore Leaks* fires if Aleph's ICIJ collections or direct ICIJ data return a hit.
- *Incorporated in high-risk jurisdiction* fires if the incorporation jurisdiction is on the FATF [Jurisdictions under Increased Monitoring](https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/) list or the [High-Risk Jurisdictions](https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/Call-for-action-June-2021.html) call-for-action list. The FATF list is refreshed on a known cadence; OpenCheck tracks it server-side.
- *Opaque ownership* fires if BODS output contains `unknownPersons`, `anonymousEntity`, or component statements with missing leaves.
- *No risk signals found* fires only when every check ran successfully and none fired — this is a meaningful UX state, distinct from "we didn't check."

### 6.3 "Go deeper" progressive disclosure

Clicking *View full record* on a source card switches the card into a full-screen pane showing:

- The raw record from the source, rendered in a human-readable form (not JSON — unless the user toggles a "Show raw JSON" control).
- A map of how each field in that record maps to BODS (where applicable) — this is where the BODS-as-spine narrative becomes visible.
- Cross-source mappings: "this OpenSanctions entity is also matched to …" with a confidence band and the matching key.
- Per-record export buttons, scoped to just that source's data.

---

## 7. BODS as the central data spine

BODS is not a feature of OpenCheck. It is the internal data model that every source adapter writes into, and the export format that every other format converts from.

### 7.1 The data flow

```
      ┌──────────────┐
      │  User query  │
      └──────┬───────┘
             ▼
     ┌───────────────────┐
     │  Intent / entity  │   (LLM + rules)
     │  classification   │
     └────────┬──────────┘
              │
   ┌──────────┴───────────┐
   │  Fan-out to source   │
   │      adapters        │
   └──┬─────┬────┬───┬────┘
      ▼     ▼    ▼   ▼   ... (CH, GLEIF, OS, Aleph, EP, Wikidata)
   ┌──────────────────────┐
   │  Per-source raw      │
   │  responses (cached)  │
   └──────────┬───────────┘
              ▼
     ┌───────────────────┐
     │  BODS 0.4 mapper  │   (one mapping module per source)
     └────────┬──────────┘
              ▼
     ┌───────────────────┐
     │  Reconcile across │   (share IDs, dedupe, flag same-as)
     │  sources          │
     └────────┬──────────┘
              ▼
     ┌───────────────────┐
     │  Report compose   │   (risk rules, graph layout, narrative)
     └────────┬──────────┘
              ▼
     ┌───────────────────┐
     │  Export adapters  │   (JSON, FtM, Neo4j, XML, GQL, AML AI)
     └───────────────────┘
```

### 7.2 Source-to-BODS mappings

| Source | Mapping strategy |
|---|---|
| Companies House PSC | Direct map — UK PSC is the canonical BODS reference implementation. `natures_of_control` → `interests[].type`. Company data → `entityStatement`; PSCs → `personStatement` (or `entityStatement` for corporate RLEs); link via `ownershipOrControlStatement`. |
| GLEIF | Reuse [bods-gleif-pipeline](https://github.com/openownership/bods-gleif-pipeline) mapping logic: LEI-CDF → entity statements, RR-CDF → ownership-or-control statements, reporting exceptions → statements with appropriate missing-info fields. |
| OpenSanctions | Via [`bods-ftm`](https://github.com/StephenAbbott/bods-ftm) (bidirectional FtM ↔ BODS). Ownership-bearing entities round-trip; sanctions/PEP signals attach as metadata, not as BODS interests. |
| OpenAleph | Via `bods-ftm` (Aleph uses FtM). For ICIJ Offshore Leaks content, alternative path is [`bods-icij-offshoreleaks`](https://github.com/StephenAbbott/bods-icij-offshoreleaks) against bulk data (used for cached demos). |
| EveryPolitician | Maps to `personStatement` with `politicalExposure` context, BUT not surfaced as BODS ownership data (no ownership relationships in EP). Person identity only. |
| Wikidata | Maps to `personStatement` / `entityStatement`. The Q-ID is stored in `identifiers[]` as a linked identifier (scheme `wikidata`). Positions held → PEP signal metadata rather than BODS interests. |

### 7.3 Reconciliation across sources

When adapters produce BODS statements describing the same real-world entity, OpenCheck:

- Merges by matching LEI

### 7.4 Validation

Every BODS dataset produced by OpenCheck is validated against [`lib-cove-bods`](https://github.com/openownership/lib-cove-bods) before being offered for download. Validation failures block export, with errors shown in-product. Users can also validate the downloaded file via the [BODS validator web tool](https://datareview.openownership.org/) (the author's [alternative validator](https://github.com/StephenAbbott/bods-validator) is another option).

### 7.5 Visualisation

Ownership graphs rendered via [`@openownership/bods-dagre`](https://www.npmjs.com/package/@openownership/bods-dagre) — the BODS Visualisation Library that implements the [BOVS specification](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/). This means OpenCheck's graphs follow the same visual grammar as the Armenia, Bermuda, and Botswana official registers — reinforcing the standard.

### 7.6 Exports — mapping to the author's adapter repos

| Export format | Adapter | Use case |
|---|---|---|
| BODS 0.4 JSON / JSONL | (native) | Default download. |
| FtM JSON | [`bods-ftm`](https://github.com/StephenAbbott/bods-ftm) | Take data into OpenSanctions / Aleph investigative workflows. |
| Neo4j CSV + Cypher | [`bods-neo4j`](https://github.com/StephenAbbott/bods-neo4j) | Load into a graph database for UBO traversal analysis. |
| Canonical XML / MRAS XML | [`bods-xml`](https://github.com/StephenAbbott/bods-xml) | XML ingestion pipelines; Canada MRAS profile. |
| BigQuery CSV + GQL schema | [`bods-gql`](https://github.com/StephenAbbott/bods-gql) | Cloud-scale analysis with GQL queries. |
| AML AI NDJSON | [`bods-aml-ai`](https://github.com/StephenAbbott/bods-aml-ai) | Feed Google AML AI pipelines. |
| OpenCorporates / Kyckr / BrightQuery / ICIJ | [`bods-opencorporates`](https://github.com/StephenAbbott/bods-opencorporates), [`bods-kyckr`](https://github.com/StephenAbbott/bods-kyckr), [`bods-brightquery`](https://github.com/StephenAbbott/bods-brightquery), [`bods-icij-offshoreleaks`](https://github.com/StephenAbbott/bods-icij-offshoreleaks) | These are *input* adapters, used server-side for cache seeding (not exposed as export buttons). |

Each export button on the report page is a one-line call into the corresponding adapter. The export UI itself is a thin shell; the adapters do the work.

---

## 8. Architecture and tech stack

### 8.1 Component diagram

```
┌──────────────────────────────────────────────────────┐
│                     React / TS                       │
│   chat UI · report widgets · bods-dagre graph        │
└──────────────────────────┬───────────────────────────┘
                           │ HTTP/JSON + SSE
┌──────────────────────────▼───────────────────────────┐
│                    FastAPI (Python)                  │
│                                                      │
│  /chat (SSE)   /entity/{id}   /person/{id}           │
│  /export/{fmt}/{report_id}                           │
│                                                      │
│   ┌─────────────┐  ┌─────────────┐  ┌────────────┐   │
│   │ Conversation│  │ Source      │  │ Cache      │   │
│   │ layer       │  │ adapters    │  │ (fs/sqlite)│   │
│   └──────┬──────┘  └──────┬──────┘  └─────┬──────┘   │
│          │                │                │         │
│          ▼                ▼                │         │
│   ┌─────────────────────────────┐          │         │
│   │ BODS mapper + reconciler    │ ◀────────┘         │
│   └────────────┬────────────────┘                    │
│                ▼                                     │
│   ┌─────────────────────────────┐                    │
│   │ Report composer +           │                    │
│   │ risk rules engine           │                    │
│   └────────────┬────────────────┘                    │
│                ▼                                     │
│   ┌─────────────────────────────┐                    │
│   │ Export adapters (Stephen's  │                    │
│   │ BODS adapter repos)         │                    │
│   └─────────────────────────────┘                    │
└──────────────────────────────────────────────────────┘
```

### 8.2 Backend: Python / FastAPI

- **Language:** Python 3.11+.
- **Framework:** FastAPI. Async source adapters, streaming (SSE) for chat responses.
- **Source adapters:** one Python module per source under `opencheck/sources/{companies_house,gleif,opensanctions,aleph,everypolitician,wikidata}.py`. Each exposes `search(query, kind)` and `fetch(id)` coroutines. All HTTP calls go through a shared `httpx.AsyncClient` with retries, timeouts, and structured logging.
- **BODS mapper:** `opencheck/bods/{mapper.py, reconcile.py, validate.py}`. Depends on the author's adapter repos (listed above) as library dependencies, pinned in `pyproject.toml`.
- **Cache:** SQLite for structured cache indexes + filesystem for raw responses. TTLs per source. On cold start, load demo bundles into the index.
- **LLM:** Anthropic Claude via API for intent extraction, disambiguation, and narrative phrasing. Kept strictly bounded — see §6.5.
- **Packaging:** `uv` (or Poetry); `pyproject.toml` declares adapter dependencies from their GitHub URLs until they are published to PyPI.
- **Tests:** `pytest`, with [`pytest-bods-v04-fixtures`](https://pypi.org/project/pytest-bods-v04-fixtures/) for BODS conformance and the canonical [`bods-v04-fixtures`](https://pypi.org/project/bods-v04-fixtures/) pack for mapper edge-case coverage. Each source adapter has an `httpx_mock`-driven adapter test.

### 8.3 Frontend: React + TypeScript

- **Framework:** Vite + React 18 + TypeScript.
- **Styling:** Tailwind.
- **Chat UI:** Custom, not a library. Message stream via SSE.
- **Report widgets:** A `Report` component composed of `SubjectHeader`, `RiskBanner`, `SourceCards`, `OwnershipGraph`, `CrossSourceLinks`, `ExportTray`.
- **Graph:** [`@openownership/bods-dagre`](https://www.npmjs.com/package/@openownership/bods-dagre) mounted in a dedicated pane; accepts BODS JSON directly.
- **State:** Minimal — React Query for server state, Zustand for conversation state. No global client router beyond report IDs.
- **Accessibility:** WCAG AA target. Keyboard-first. Source cards and risk chips fully accessible. Graph has a tabular fallback view.

### 8.4 Hosting and deployment

- **Local dev:** `docker compose up` brings up API + frontend + SQLite. `.env.example` with placeholders for API keys.
- **Public demo:** Hosted backend on Fly.io or Render (autosleep-friendly for cost); frontend on Vercel or the same service. TLS, HTTP/2. One-click redeploy on main push. Public demo uses the cached demo corpus by default; live API calls require an explicit toggle and a server-side rate limiter.
- **Secrets:** API keys in hosting-platform secrets only; never committed, never sent to the client.
- **Observability:** Structured JSON logs, optional OpenTelemetry. Usage metrics that respect privacy (no query strings logged in plain text on the public demo).

### 8.5 Repository layout (one monorepo)

```
opencheck/
  backend/
    opencheck/
      __init__.py
      app.py               # FastAPI app
      conversation/        # LLM + intent extraction
      sources/             # one module per data source
      bods/                # mapper, reconciler, validator
      reports/             # composer, risk rules
      exports/             # thin wrappers around Stephen's adapters
      cache/
    tests/
    pyproject.toml
  frontend/
    src/
      components/
        chat/
        report/
        graph/
        exports/
      pages/
      lib/
    package.json
    vite.config.ts
  data/
    cache/
      demos/               # checked in
      live/                # gitignored
  docker-compose.yml
  .env.example
  README.md
  LICENSE
  ATTRIBUTIONS.md         # per-source license notices baked into every build
```

---

## 9. Promoting GODIN

GODIN is a core audience, not an afterthought. Specific, concrete hooks:

- **A GODIN ribbon in the UI.** A small but permanent banner explaining: "OpenCheck is built on open data from GODIN members — GLEIF, Open Ownership (BODS), OpenSanctions — and others, and demonstrates the kind of interoperability GODIN exists to enable." Links to [godin.gleif.org](https://godin.gleif.org/).
- **Identifier-bridge visualisation.** The "cross-source links" panel (§6.2) visualises exactly what GODIN's Transparency Fabric does — each bridge is attributed, with a hover card explaining which identifier made the match. This makes GODIN's value proposition concrete every time a user runs a query.
- **A dedicated "Behind the scenes" page.** Explains the architecture, the sources, BODS as the spine, and links to GODIN, the Transparency Fabric, the BODS standard, and the author's adapter repos.
- **Source cards that name GODIN members.** Where a source is a GODIN member, the card carries a small GODIN chip linking to that member's GODIN profile.
- **Structured data for GODIN showcases.** The OpenCheck public demo is linkable with URL-encoded query state (e.g. `/?q=rosneft`) so it can be embedded or cited from GODIN talks and blog posts.

---

## 10. Phased delivery plan

### Phase 0 — Foundations (≈1 week)

- Set up the monorepo, pyproject, package.json, Docker Compose.
- Decide on hosting provider and provision staging.
- Capture API keys for Companies House, OpenSanctions, Aleph, Anthropic (LLM). Verify license terms for each (especially OpenSanctions for this use case).
- Acceptance: `docker compose up` starts both services; an HTTP GET returns a stub response from each source adapter.

### Phase 1 — One source, end-to-end (≈2 weeks)

Pick the narrowest possible end-to-end slice: **Companies House entity lookup → BODS → graph → download**.

- Entity lookup adapter for Companies House with PSC data.
- BODS mapper for PSC.
- Report composer with subject header, one source card, an ownership graph, BODS export.
- Basic chat UI + SSE stream.
- Acceptance: a user types a UK company name or number, sees a structured report with PSC-derived ownership graph, and can download valid BODS.

### Phase 2 — All entity sources (≈2 weeks)

- Add GLEIF, OpenSanctions, OpenAleph adapters for entities.
- Reconciliation across sources (LEI / Q-ID / CH number matching).
- Full risk banner for entities (sanctions, high-risk jurisdiction, opaque ownership, offshore leaks presence).
- Export adapters for FtM, Neo4j.
- Acceptance: demo entities from the curated corpus each return a rich multi-source report with a coherent ownership graph and at least two export formats.

### Phase 3 — Person lookups (≈1.5 weeks)

- Person intent classification.
- Companies House (officer appointments), OpenSanctions (PEP/sanctions), EveryPolitician, Wikidata, OpenAleph adapters for persons.
- PEP risk signal logic; offshore-leaks signal for persons.
- Cross-source links panel, anchored on Wikidata Q-ID.
- Acceptance: demo persons each return a report with PEP/sanctions/offshore signals correctly attributed across sources.

### Phase 4 — Go-deeper drill-downs + remaining exports (≈1 week)

- Full-record view per source card.
- Field-level BODS mapping view (the "see how this record becomes BODS" pane).
- Remaining exports: XML (`bods-xml`), GQL (`bods-gql`), AML AI (`bods-aml-ai`), Markdown/PDF report.
- Validation gate — exports blocked if `lib-cove-bods` fails.
- Acceptance: from any report, a user can drill into any source and export in all declared formats, with every export passing validation where applicable.

### Phase 5 — Public demo polish (≈1 week)

- Accessibility pass (keyboard, screen reader, colour contrast).
- Rate limiting + API-key protection for live calls on the public demo.
- Analytics (privacy-respecting).
- `ATTRIBUTIONS.md`, `LICENSE_NOTICE.md` per export, in-product attribution audit.
- GODIN promotional hooks (ribbon, Behind the Scenes page).
- Acceptance: hosted public demo live and linkable; README and ATTRIBUTIONS complete; at least one blog post or LinkedIn launch post drafted.

### Total time estimate (part-time): ~7–9 working weeks

This is achievable solo part-time over 3 calendar months. If the author delegates the frontend polish in Phase 5 to a contractor, the critical path compresses.

---

## 11. Risks, unknowns, and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| OpenAleph collection licenses vary and are easy to misattribute | High | Medium | Per-collection license metadata is read from the OpenAleph API and surfaced in the source card; no composite "OpenAleph license" is ever displayed |
| API rate limits break demos | Medium | Medium | Cache aggressively; demo corpus is pre-baked; circuit breakers on live adapters; visible graceful degradation in UI |
| BODS mapping from FtM loses fidelity for non-ownership relationships | Low (known) | Low | `bods-ftm` already handles this (ownership-bearing links map; others pass through as metadata); don't try to force all FtM into BODS |
| LLM hallucinates a source or a risk signal | Medium | High | Architecture pins the LLM to phrasing only; risk signals and source cards are deterministic; every claim in the chat message is trace-linked to an entry in the report |
| User assumes "no hit" means "clean" | High | Medium | Every report explicitly lists which sources were checked and which weren't applicable; "no risk signals found" is phrased carefully and paired with a "this does not constitute due diligence" caveat; individual source cards note known coverage gaps (e.g. EveryPolitician non-hits on under-documented legislatures) |
| Non-commercial licenses (OpenSanctions, some OpenAleph collections, EveryPolitician) propagate into downloads | Medium | Medium | Per-export `LICENSE_NOTICE.md` lists every source and its license; downloads that include NC data are labelled as such; the in-UI export dialog warns users before they commit to a download |
| Scope creep into general-purpose corporate intelligence | High | Medium | Non-goals (§2) are enforced; v2 wishlist lives in a separate file, not in the main backlog |
| Companies House PSC is of variable quality | Confirmed | Low | Surface data quality as a feature — if a company's PSC data is missing or marked as "unable to identify," say so plainly; that's a signal not a bug |

### Open questions to resolve during Phase 0

- Is Wikidata's SPARQL query service sufficient under its rate limits for live use, or do we need a local mirror or QLever endpoint for entity-heavy lookups?
- Does the public demo default to UK/EU jurisdictions with an "expand scope" toggle, or start global from day one?
- For live queries on the public demo, do we ship with the project's own API keys (rate-limited server-side) or require users to supply their own? First option is friendlier; second is more sustainable.

The three earlier open questions — OpenAleph endpoint, OpenSanctions license fit, and EveryPolitician status — are now resolved: OpenAleph is [search.openaleph.org](https://search.openaleph.org/); the OpenSanctions free/CC BY-NC tier is confirmed as fitting the hosted demo; EveryPolitician is actively maintained by OpenSanctions via Poliloom (§4.5).

---

## 12. Immediate next actions

1. Resolve the three remaining Phase-0 questions (Wikidata query strategy, default public demo scope, API-key model for live queries).
2. Spike the end-to-end Phase 1 slice (Companies House → BODS → graph → download) as a 2-day walking skeleton to validate the architecture before committing to the full monorepo structure.
3. Draft the curated demo corpus list (§5.2) concretely — pick the actual 8–12 entities and persons — so Phase 2 work can proceed in parallel with cache seeding.
4. Create the OpenCheck GitHub repo (public, MIT / Apache 2.0) with this plan as `docs/plan.md`, the phased roadmap as GitHub issues, and an initial `ATTRIBUTIONS.md` skeleton.
5. Once something demoable exists, share with the OpenSanctions founder via the GODIN connection for early feedback, and flag to GLEIF and OpenOwnership the same way. The GODIN framing (§9) benefits from being validated by the named member organisations early.

---

## Appendix A — Named inspiration projects

- **Linkurious OpenScreening.** <https://resources.linkurious.com/openscreening> — graph-based sanctions/PEP/leaks screening.
- **GLEIF Transparency Fabric.** <https://transparencyfabric.gleif.org/> — identifier-bridge visualisation across open datasets; GODIN-adjacent.
- **OpenOwnership Beneficial Ownership Explorer.** <https://github.com/openownership/beneficial-ownership-explorer> — BODS-native data exploration.

## Appendix B — Core external dependencies

- BODS standard 0.4: <https://standard.openownership.org/en/0.4.0/>
- BODS visualisation library (`bods-dagre`): <https://www.npmjs.com/package/@openownership/bods-dagre>
- BODS validator (`lib-cove-bods`): <https://github.com/openownership/lib-cove-bods>
- BODS test fixtures: <https://pypi.org/project/bods-v04-fixtures/> and <https://pypi.org/project/pytest-bods-v04-fixtures/>
- Author's BODS adapter repos: `bods-ftm`, `bods-neo4j`, `bods-xml`, `bods-gql`, `bods-aml-ai`, `bods-icij-offshoreleaks`, `bods-opencorporates`, `bods-kyckr`, `bods-brightquery` — all at <https://github.com/StephenAbbott>.

## Appendix C — Attribution strings (consolidated)

These go in `ATTRIBUTIONS.md` and in the per-export `LICENSE_NOTICE.md`:

- **Companies House** — "Contains public sector information licensed under the Open Government Licence v3.0 (Companies House)."
- **GLEIF** — "Contains LEI data from GLEIF, available under CC0 1.0."
- **OpenSanctions** — "Data from OpenSanctions.org, licensed CC BY-NC 4.0."
- **OpenAleph** — per-collection, read from the API; e.g. "Data from [collection name] in OpenAleph, licensed [CC BY 4.0 / CC BY-SA 4.0 / restricted]."
- **EveryPolitician** — "EveryPolitician data, maintained by OpenSanctions. Licensed CC BY-NC 4.0."
- **Wikidata** — "Wikidata structured data, CC0 1.0."
- **BODS** — "Output conforms to the Beneficial Ownership Data Standard (BODS) v0.4, a project of Open Ownership."
