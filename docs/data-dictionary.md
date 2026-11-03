# Output data dictionary

What every field OpenCheck writes means — the API responses, the BODS statements and the keys OpenCheck adds to them, the annotations, the risk-signal and degradation objects, the export tables and the MCP tool responses. Inputs and concepts have their own pages: [How it works](how-it-works.md), [Dates](dates.md), [Identifiers](identifiers.md), [Risk signals](risk-signals.md), [Record consistency](record-consistency.md), [MCP server](mcp-server.md).

**Generated from the code** by `backend/scripts/generate_data_dictionary.py` (`opencheck/data_dictionary.py`): the field tables are read from the Pydantic models and dataclasses, the vocabularies from their constants, the export columns from the writers, and the MCP keys from the tool bodies. CI fails when this file is stale (`--check`), when a column or `opencheck*` key appears in output without a description, and when a mapper emits an unregistered extension key. Do not edit it by hand.

Contents: [API responses](#api-responses) · [BODS statements](#bods-statements) · [Risk signals and degraded sources](#risk-signals-and-degraded-sources) · [Exports](#exports) · [MCP tools](#mcp-tools)

## API responses

Every response is JSON. `Type` is the Python annotation; `list[dict[str, Any]]` fields hold the objects described elsewhere on this page — `risk_signals[]` is a [RiskSignal](#risksignal), `degraded_sources[]` a [DegradedSource](#degradedsource), `bods[]` a [BODS statement](#bods-statements). A `Default` of `required` means the field is always present; any other value is what a payload recorded before the field existed carries.

### `GET /lookup` and `GET /lookup-register`

The folded form of a run. `/lookup-stream` and `/lookup-register-stream` carry the same payload as server-sent events (one event per field group); `LookupResponse` is the fold of those events, and a saved report replays exactly this.

#### `LookupResponse`

Extends `ReportResponse`; only its own fields are listed.

Same shape as /report, with the LEI echoed back and the GLEIF bundle surfaced separately so the UI doesn't have to dig for it.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | The LEI the run was anchored on. |
| `legal_name` | `str \| None` | `None` | GLEIF's legal name for it. |
| `jurisdiction` | `str \| None` | `None` | GLEIF's jurisdiction code — ISO 3166-1 alpha-2, or 3166-2 for a US state (`US-DE`). |
| `derived_identifiers` | `dict[str, str]` | `{}` | The register numbers derived from the LEI record for dispatching the national adapters, keyed by the adapter's lookup key (`gb_coh`, `no_orgnr` …). Dispatch keys, not corroborated identifiers. |
| `replayed` | `bool` | `False` | True when served from the short-lived replay cache rather than a fresh run. `?refresh=true` always yields a fresh run. |
| `fetched_at` | `str \| None` | `None` | The wall-clock completion time of the original run when `replayed`; None on a live run, so a live run is never badged as cached. |
| `sources_applicable` | `list[str]` | `[]` | Phase 164: the source ids the pipeline announced as applicable to this company (the `sources_applicable` stream event; the GLEIF anchor is never in it — Phase 126/156). Lets a JSON consumer build the Phase 156 coverage sentence without the stream. |
| `run_completed_at` | `str \| None` | `None` | Phase 216: the wall-clock UTC completion time of the run this response was folded from — set for live and replayed runs alike (`fetched_at` stays replay-only, so a live run is never badged as cached). It names the run: a saved report is taken from the replay cache only when the caller's `run_completed_at` matches the held run's, and the narrative cache records which run a summary was written from. |

#### `ReportResponse`

Aggregate post-search synthesis for a single subject.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `query` | `str` | `required` | What was looked up: the LEI, the `SCHEME:id` register reference or the search string. |
| `kind` | `SearchKind` | `required` | `entity` or `person` — what the query names. |
| `hits` | `list[SourceHit]` | `required` | One `SourceHit` per source that answered with a record or a note card. |
| `errors` | `dict[str, str]` | `required` | Sources that failed, keyed by source id, each a secret-scrubbed one-line error. A source here is also a `degraded_sources` entry. |
| `cross_source_links` | `list[dict[str, Any]]` | `required` | Confirmed bridges between hits that describe the same subject — one `CrossSourceLink` per shared identifier (see *Objects inside lists*). |
| `risk_signals` | `list[dict[str, Any]]` | `required` | Every `RiskSignal` the run produced, after merging — see *Risk signals and degraded sources*. |
| `bods` | `list[dict[str, Any]]` | `required` | The merged BODS v0.4 bundle: every deepened source's statements, one per id, the subject's own records folded onto one node. |
| `bods_issues` | `list[str]` | `required` | Shape problems the BODS validator found in `bods` — empty when clean. |
| `license_notices` | `list[dict[str, str]]` | `required` | Per-record licence notices adapters attached, each `source_id` / `hit_id` / `notice` — a non-commercial source's wording to carry with the data. |
| `possibly_same_entities` | `list[dict[str, Any]]` | `[]` | Name-only "likely same" entity candidates (same name + jurisdiction, no shared identifier) — human-review suggestions, never auto-merges. |
| `degraded_sources` | `list[dict[str, Any]]` | `[]` | Derived risk checks that did not fully run for this result (issue #50) — empty when every screen completed. Each record carries source_id / check / affected_signals / detail / reason (closed vocabulary: upstream_error, timeout, not_configured, rate_limited, truncated). An empty risk_signals list with a non-empty degraded_sources list is NOT a clean screen. Never contains related-party names. |
| `openaleph_screening` | `list[dict[str, Any]]` | `[]` | Informational related-party matches from OpenAleph percolation (Phase 96): attributed, similarity-gated hits whose topics map to no RELATED_* code — leak/court collections, poi, corp.disqual. Each entry carries statement_id / matched_name / collection / url / surface_form. Name-derived — never identifier corroboration. |
| `source_liveness` | `dict[str, dict[str, Any]]` | `{}` | How current each source's payload is, keyed by source_id: liveness ('live' / 'cached' / 'snapshot' / 'curated' / 'stub'), a display label, the retrieval time OpenCheck actually observed (null when nothing was fetched) and a short detail string. Sibling to degraded_sources: data that is not current must not read as live, just as a check that could not run must not read as clean. Per-hit values ride on each SourceHit. |
| `graph_shape` | `dict[str, Any]` | `{}` | How big the mapped graph is: companies / people / relationships across the sources that answered (deduplicated by statementId), plus the longest ownership chain the risk layer measured, or null when it did not. Counts what this check holds — never what a deeper one might find. |
| `verdict` | `str \| None` | `None` | One deterministic sentence stating what the check found — see `opencheck.verdict`. Rendered at the top of the report, above the evidence and above the AI summary. Template-built, never a model call, and defaulted so replayed payloads recorded before Phase 122 still validate. |
| `subject_profile` | `dict[str, Any] \| None` | `None` | What the registers say the subject *is* (Phase 154): legal form, register status, founding date and registered address, each with the sources stating it and their independent count — read from the subject's own entity statements only (`opencheck.subject_profile`). Facts, never findings. None for a name search or before any source has been deepened. |
| `knowability` | `dict[str, Any] \| None` | `None` | What is knowable about a company in the subject's jurisdiction (Phase 224): the dated statement from `opencheck.knowability` — who may see beneficial owners there, what the register records, and what OpenCheck reads of it — as the `knowability` stream event carried it, plus `as_of`. Describes, never judges; nothing here is a signal. Frozen at run time so a saved report replays the sentence that was true that day. None for a name search, for a payload recorded before this field existed, or when the subject's jurisdiction is unknown. |
| `knowability_chain` | `dict[str, Any] \| None` | `None` | What is knowable along the ownership path (Phase 226): `subject` (the subject's code), `codes` (every jurisdiction on the upward walk from the subject, subject first then path order, ended links included) and one `statements[]` entry per code, plus `as_of` — as the `knowability_chain` stream event carried it, frozen at run time. The chain is the run's own depth (the deepened sources); FullCheck extends it client-side as it expands. Describes, never judges. None for a payload recorded before this field existed. |
| `integrated_ownership` | `dict[str, Any] \| None` | `None` | Integrated ownership along the path (Phase 339): for every party above the subject, how much of it that party holds directly and through companies it holds, one figure per source, never added across sources (`opencheck.integrated_ownership`). `holders[]` with `status` (current / voting_rights_only / formerly) and `figures[]`, plus `method`, `description`, `credit` and `as_of` — frozen at run time. Context only: no threshold, never a signal. None for a payload recorded before this field existed. |
| `listing` | `dict[str, Any] \| None` | `None` | The subject's primary stock-exchange listing (Phase 236), from LSEG PermID: `status` ("listed" / "not_listed" / "unavailable"), `quote` (ticker, MIC, RIC, security name, PermID ids), `exchange` (name + country, or None for a venue OpenCheck has no name for), `link` (a verified venue page, or None) and `as_of` — as the `listing` stream event carried it, frozen at run time (`opencheck.listing`). "unavailable" carries `reason` and a secret-scrubbed `detail` and is never "not listed" nor a degraded source. None when no PermID key is configured, or for a payload recorded before this field existed. |
| `form13f` | `dict[str, Any] \| None` | `None` | Institutional shareholders from SEC Form 13F (Phase 334): the positions investment managers reported in the company's securities, as the `form13f` stream event carried them, frozen at run time (`opencheck.form13f`). `status` "found" (`securities[]`, each with its top twenty `positions`) or "out_of_date". **Not beneficial ownership**: never in `bods`, never read by a signal or the verdict, never totalled. None when there is no index or no matched security. |
| `register_record` | `dict[str, Any] \| None` | `None` | The subject's own page on its home register (Phase 296): `source_id`, `register`, `identifier`, `url` and, on an LEI run, `ra_code` — as the anchor event carried it, built from the LEI record's registration authority and number alone (`opencheck.register_links`), so it is present even when that register's adapter did not answer. None when OpenCheck has no per-company address for the register, or for a payload recorded before this field existed. |

#### `RegisterLookupResponse`

Extends `LookupResponse`; only its own fields are listed.

``/lookup-register``: the ``/lookup`` shape for a subject anchored on a register number (Phase 290). ``lei`` is ``None`` — the whole point is that there is none — and ``scheme`` / ``id`` say what the anchor was. Everything else is as ``/lookup`` returns it, so every reader of a ``LookupResponse`` (exports, the MCP shaping, saved reports) reads this one unchanged.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str \| None` | `None` | Always None: a register-anchored subject has no LEI. |
| `scheme` | `str` | `required` | The register scheme the subject was anchored on (`GB-COH`, `LT-JAR`). |
| `id` | `str` | `required` | The subject's number on that register, in the register's canonical form. |

### `hits[]` and `GET /sources`

Every source that answered contributes one `SourceHit` to `hits[]`; `SourceInfo` is the static metadata `/sources` lists per adapter.

#### `SourceHit`

A single search result from a source.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | `required` | Adapter id (matches SourceInfo.id). |
| `hit_id` | `str` | `required` | Adapter-local identifier for this hit. |
| `kind` | `SearchKind` | `required` | Entity or person. |
| `name` | `str` | `required` | Primary display name. |
| `summary` | `str` | `required` | Short human-readable line used in chat. |
| `finding` | `str \| None` | `None` | One plain-English SENTENCE saying what this source actually said about the subject — 'Active since 12 March 1998, 6 officers on file.' Deliberately distinct from `summary`, which is an identifier fragment ('GB · registered entity', 'SG-UEN 12345 · live') consumed by search-result rows, the share card and `og_image.py`; `summary` must not be repurposed, a dozen call sites depend on its current shape. Optional: a source with no template leaves this None and its `summary` renders as before. Templates live in `opencheck.findings`, which carries the rules they follow. |
| `identifiers` | `dict[str, str]` | `dict()` | Cross-source identifiers (e.g. {'lei': '...', 'gb_coh': '...', 'wikidata_qid': '...'}). |
| `raw` | `dict[str, Any]` | `dict()` | Raw payload (stub-shaped in Phase 0). |
| `is_stub` | `bool` | `True` | True when this hit was produced by the Phase 0 stub path. |
| `liveness` | `str` | `'stub'` | How current this payload is: 'live' (fetched now), 'cached' (OpenCheck response cache), 'snapshot' (bulk dataset), 'curated' (committed fixture) or 'stub' (placeholder). Defaults to 'stub' so a source that declares nothing under-claims rather than over-claims. |
| `retrieved_at` | `datetime \| None` | `None` | When OpenCheck actually obtained this payload from the source, or None when nothing was fetched (stub and curated data). Feeds BODS `source.retrievedAt`, which the dates guidance requires a republisher to state. |

#### `SourceInfo`

Static metadata about a source — surfaced on the /sources endpoint.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | `str` | `required` | Stable adapter identifier, e.g. 'companies_house'. |
| `name` | `str` | `required` | Human-readable source name. |
| `homepage` | `str` | `required` | Canonical source homepage URL. |
| `description` | `str` | `''` | Short user-facing one-liner describing what the source is. Distinct from `attribution`, which is the credit line. |
| `license` | `str` | `required` | Short license identifier (e.g. 'OGL-3.0', 'CC0', 'CC-BY-NC-4.0'). |
| `attribution` | `str` | `required` | Attribution string to display alongside data. |
| `supports` | `list[SearchKind]` | `required` | Which search kinds this adapter supports (entity / person / both). |
| `requires_api_key` | `bool` | `required` | Whether live mode needs an API key configured. |
| `live_available` | `bool` | `required` | Whether live mode is available right now (key present AND allow_live=true). |
| `category` | `Literal['cdd', 'esg']` | `'cdd'` | Broad purpose category. 'cdd' = customer due diligence / compliance; 'esg' = environmental, social and governance data. |
| `is_national_register` | `bool` | `False` | True when this source is an official national company or beneficial ownership register (e.g. Companies House, Bolagsverket). False for aggregators, cross-border databases, and ESG sources. |
| `country` | `str \| None` | `None` | ISO 3166-1 alpha-2 country code for national registers (e.g. 'FR', 'DK'). Used to join per-country notices such as the EU beneficial ownership access status. None for cross-border / global sources. |
| `bo_access` | `'BoAccessNotice \| None'` | `None` | Computed EU/EEA beneficial-ownership access notice for this register's country, or None. Populated by the /sources endpoint; adapters do not set it (see opencheck.bo_access). |
| `derived_from` | `list[str]` | `list()` | Source ids this source republishes (e.g. OpenCorporates mirrors the national registers). Agreement between a source and one it derives from is one observation, not corroboration. Empty for original data. Populated by the /sources endpoint from the adapter's declaration (see opencheck.sources.lineage). |

### `GET /deepen` and `GET /lookup-source`

One source's full record (the Data drawer) and the per-source retry.

#### `DeepenResponse`

One source's full record for one hit — the Data drawer's payload.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | `required` | Adapter id of the source read. |
| `hit_id` | `str` | `required` | The adapter-local id of the record read (`SourceHit.hit_id`). |
| `raw` | `dict[str, Any]` | `required` | The source's own payload, as fetched — redacted to a notice for a source whose licence forbids republishing it (OpenCorporates). |
| `bods` | `list[dict[str, Any]]` | `required` | The BODS v0.4 statements mapped from this one source's record. |
| `bods_issues` | `list[str]` | `required` | Shape problems the BODS validator found in `bods` — empty when clean. |
| `license` | `str` | `required` | The source's licence identifier (`OGL-3.0`, `CC-BY-NC-4.0` …). |
| `license_notice` | `str \| None` | `None` | A per-record licence notice the adapter attached (a non-commercial source's wording), or None. |
| `risk_signals` | `list[dict[str, Any]]` | `[]` | The signals this one source's record produced on its own — never the cross-source RELATED_* screens, which need the merged graph. |

#### `LookupSourceResponse`

Result of re-running a single source within an existing lookup.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | The LEI of the run the source was retried in. |
| `source_id` | `str` | `required` | The adapter id retried. |
| `hits` | `list[SourceHit]` | `required` | The hits the retry produced — the same shape the run's `hits[]` carry. |
| `error` | `str \| None` | `None` | A secret-scrubbed one-line error when the retry failed, else None. |
| `error_type` | `str \| None` | `None` | `schema_changed` / `timeout` / `fetch_error` when `error` is set — the same words a `source_error` event carries (Phase 331). |
| `source_liveness` | `dict[str, dict[str, Any]]` | `dict()` | What the retry actually read, per source it dispatched — the entry the run's own `source_liveness` would carry (Phase 331). A client merges it over the run's map so the freshness chip describes the retry, not the attempt that failed. |

### `GET /search`

A ranked name search across sources.

#### `SearchResponse`

A name search across every source that supports it, ranked.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `query` | `str` | `required` | The search string as given. |
| `kind` | `SearchKind` | `required` | `entity` or `person`. |
| `hits` | `list[SourceHit]` | `required` | Every candidate, ranked across sources by match tier, register status and LEI (`opencheck.search_rank`), one `SourceHit` each. |
| `errors` | `dict[str, str]` | `required` | Sources that failed, keyed by source id, each a secret-scrubbed one-line error. |
| `cross_source_links` | `list[dict[str, Any]]` | `required` | Confirmed bridges between candidates that share an identifier — one `CrossSourceLink` each. |
| `risk_signals` | `list[dict[str, Any]]` | `required` | Signals the candidates' own records carry (a sanctioned hit); the cross-source screens need a lookup. |

### `GET /subsidiaries` and `GET /subsidiaries/declared`

What the company consolidates (GLEIF Level 2) and the declared lists other sources hold.

#### `SubsidiariesResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `available` | `bool` | `required` | — |
| `reason` | `str \| None` | `None` | — |
| `children_available` | `bool` | `True` | Phase 146. False = GLEIF refused both relation calls and no snapshot stood in, so an empty `children` list says nothing about the entity. The frontend must render the degraded notice, not "no network published". |
| `direct_available` | `bool` | `True` | — |
| `ultimate_available` | `bool` | `True` | — |
| `snapshot_fallback` | `bool` | `False` | Children served from the entity-pages Golden Copy, not live. |
| `snapshot_date` | `str \| None` | `None` | — |
| `snapshot_source` | `str \| None` | `None` | Phase 179: why the snapshot answered — "mirror" (the mirror-first order chose it; nothing was refused) or "fallback" (GLEIF refused and the snapshot stood in); None when the network came live. |
| `degraded_detail` | `str \| None` | `None` | One sentence naming what GLEIF did not answer (None when it answered). |
| `unavailable_reason` | `str \| None` | `None` | Phase 258: why GLEIF gave no answer — "held_for_lookups" (OpenCheck kept its last GLEIF requests for lookups; nothing was sent), "rate_limited" or "unreachable". None when it answered or the mirror served the network. |
| `direct_total` | `int` | `0` | — |
| `ultimate_total` | `int` | `0` | — |
| `distinct_fetched` | `int` | `0` | — |
| `indirect_only` | `int` | `0` | — |
| `node_estimate` | `int` | `0` | — |
| `render_mode` | `str` | `'graph'` | — |
| `truncated` | `bool` | `False` | — |
| `jurisdictions` | `list[SubJurisdiction]` | `[]` | — |
| `countries` | `list[SubJurisdiction]` | `[]` | Phase 255: `jurisdictions` rolled up to ISO 3166-1 alpha-2 (a `US-DE` or `CA-AB` subsidiary counts under `US` / `CA`). |
| `enriched` | `bool` | `True` | Phase 261: False when GLEIF would not give the relationship records or the direct parents — the dates and paths on the rows could not be read. |
| `relationships_read` | `bool` | `True` | Which half of the enrichment answered: the relationship records (the rows' dates) and the direct-parent lookups (the "via" paths). `enriched` is true only when both are. |
| `parents_read` | `bool` | `True` | — |
| `children` | `list[SubChild]` | `[]` | — |
| `bods` | `list[dict] \| None` | `None` | — |

#### `SubChild`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `name` | `str \| None` | `None` | — |
| `jurisdiction` | `str \| None` | `None` | — |
| `status` | `str \| None` | `None` | — |
| `relation` | `str` | `required` | — |
| `link` | `str \| None` | `None` | — |
| `lei_registration` | `dict \| None` | `None` | Phase 261: the child LEI's own registration status — `status` above is the entity's. `{status, label, flag, since, next_renewal_date}`, the `lei_registration` shape minus its sentence; None when GLEIF's record carries no registration block. |
| `direct_parent_lei` | `str \| None` | `None` | Phase 261: for an ultimate-only child, the LEI GLEIF names as its direct consolidating parent, and whether that LEI is in this network. False is often a lapsed LEI of a merged holding company, so the path through it is not shown. None when GLEIF names no parent. |
| `direct_parent_in_network` | `bool \| None` | `None` | — |
| `relationship_start` | `str \| None` | `None` | Phase 261: the relationship record's RELATIONSHIP_PERIOD (YYYY-MM-DD). |
| `relationship_end` | `str \| None` | `None` | — |

#### `SubJurisdiction`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `code` | `str` | `required` | — |
| `count` | `int` | `required` | — |

#### `DeclaredSubsidiariesResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `sources` | `list[DeclaredSource]` | `required` | — |
| `covered` | `int` | `required` | — |
| `listed` | `int` | `required` | — |
| `with_lei` | `int` | `required` | — |

#### `DeclaredSource`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | `str` | `required` | — |
| `label` | `str` | `required` | — |
| `measures` | `str` | `required` | — |
| `homepage` | `str` | `required` | — |
| `available` | `bool` | `True` | False = the data could not be read (not a finding about the company). |
| `covered` | `bool` | `False` | False = the subject is not in this source's universe at all. |
| `reason` | `str \| None` | `None` | — |
| `total` | `int \| None` | `None` | The source's own count where it publishes one larger than `rows` (MEIP's committed fallback holds only the LEI-carrying subset; the SQLite store lists the whole group and `total` equals `listed`). |
| `listed` | `int` | `0` | — |
| `with_lei` | `int` | `0` | — |
| `context` | `dict \| None` | `None` | — |
| `rows` | `list[DeclaredRow]` | `[]` | — |

#### `DeclaredRow`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | `required` | — |
| `lei` | `str \| None` | `None` | Only where the source's own data carries one — never name-derived here. |
| `country` | `str \| None` | `None` | ISO 3166-1 alpha-3, as MEIP, EITI and GEM all publish it. |
| `relation` | `str \| None` | `None` | "direct" \| "in group" (MEIP, via an intermediate) \| "declared" (EITI). |
| `percent` | `float \| None` | `None` | — |
| `years` | `list[str]` | `[]` | — |
| `via` | `str \| None` | `None` | MEIP: the immediate parent's name. GEM: the GEM entity id. |
| `register_id` | `dict[str, str] \| None` | `None` | A register number the source itself publishes for the row, as `{"scheme", "id"}` (the Estonian register's registry code) — a register-lookup key where the row has no LEI. |

### `GET /history`

The Time Machine: how the company's records changed, merged across every register that keeps a change log.

#### `HistoryResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `company_number` | `str \| None` | `required` | — |
| `available` | `bool` | `required` | — |
| `sources` | `list[str]` | `required` | — |
| `notable_count` | `int` | `required` | — |
| `notable` | `list[HistoryEntry]` | `required` | — |
| `events` | `list[RawChange]` | `[]` | — |
| `gleif_record_available` | `bool` | `True` | Phase 146 — "checked, no history" and "could not check" are different answers, and `available: false` said both. False here means GLEIF refused that call (rate-limited or down), so an empty timeline is not evidence about the entity. |
| `gleif_events_available` | `bool` | `True` | — |
| `registry_sources_blocked` | `bool` | `False` | The GLEIF record failed and no cached one stood in, so the registry history sources (Companies House / NZ / Estonia / Denmark) could not be attempted — they gate on a registry number only that record carries. |
| `company_number_basis` | `str \| None` | `None` | "live" \| "cached" \| None — where `company_number` came from. |
| `registry_numbers` | `dict[str, str]` | `{}` | Phase 190 — `{source_id: registry number}`, how each register that publishes a change log addresses this company. Present means that register knows the company by that number, not that its history was fetched; `sources` says what actually answered. |
| `filings_truncated` | `bool` | `False` | Phase 194 — Companies House holds more filings than this fetch read. The register answers newest-first, so the missing end is the oldest. |
| `officers_available` | `bool` | `False` | Phase 198 — the officers list, which the board stream is built from, was read. False means it was not (no key, not a GB company, or the register refused), so an empty board stream is unchecked, not empty. |

#### `HistoryEntry`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `change_type` | `str` | `required` | — |
| `label` | `str` | `required` | — |
| `tier` | `int` | `required` | — |
| `record_type` | `str` | `required` | — |
| `date` | `str \| None` | `required` | — |
| `date_basis` | `str` | `required` | — |
| `date_confidence` | `str` | `required` | — |
| `value_old` | `str \| None` | `None` | — |
| `value_new` | `str \| None` | `None` | — |
| `sources` | `list[str]` | `required` | — |
| `corroborating_sources` | `list[str]` | `[]` | — |
| `counterparty` | `str \| None` | `None` | — |
| `interest_start_date` | `str \| None` | `None` | — |
| `interest_end_date` | `str \| None` | `None` | — |
| `boosted` | `bool` | `False` | — |

#### `RawChange`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | `required` | — |
| `record_type` | `str` | `required` | — |
| `raw_change_type` | `str` | `required` | — |
| `raw_field` | `str \| None` | `None` | — |
| `value_old` | `str \| None` | `None` | — |
| `value_new` | `str \| None` | `None` | — |
| `change_type` | `str \| None` | `None` | — |
| `label` | `str \| None` | `None` | The codelist's own label for `change_type`. Sent so a client never has to keep a second copy of the vocabulary — the failure mode `lib/historyMode.ts` warns about, where a label map drifts from the thing it labels. None where the change is untyped. |
| `tier` | `int` | `required` | — |
| `event_date` | `str \| None` | `None` | — |
| `date_basis` | `str` | `required` | — |
| `counterparty` | `str \| None` | `None` | The other end of the change. For a Companies House officer filing this is the officer's name — the filing publishes no officer id, so a board row names a person without claiming to identify them. |
| `party_id` | `str \| None` | `None` | Phase 198 — who that other end IS, where the source publishes a key. `party_id` is the register's own officer id; `party_statement_id` is the BODS person statement OpenCheck builds from it, which is what lets a board row link to the person node the graph draws. Both null on every filing-history row, which publishes a name and nothing more. |
| `party_statement_id` | `str \| None` | `None` | — |

### `GET /securities`

The ISINs GLEIF maps to the LEI.

#### `SecuritiesResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `available` | `bool` | `required` | — |
| `total` | `int` | `required` | — |
| `page` | `int` | `required` | — |
| `page_size` | `int` | `required` | — |
| `securities` | `list[Security]` | `required` | — |
| `sanctioned` | `list[Security]` | `required` | — |
| `isin_list_available` | `bool` | `True` | Phase 145: false when GLEIF could not be queried (rate-limited or down). The sanctioned overlay is a local index and still runs, so the endpoint answers 200 with the overlay applied and `total`/`securities` empty — the reader is told which part is missing instead of losing the section. |
| `isin_list_stale` | `bool` | `False` | Phase 253: the ISIN page is cached a day; an older page stands in when GLEIF cannot be asked, flagged `isin_list_stale` and dated by `isin_list_as_of` (when GLEIF was asked, ISO 8601 UTC). |
| `isin_list_as_of` | `str \| None` | `None` | — |
| `isin_list_unavailable_reason` | `str \| None` | `None` | Why GLEIF could not be asked: "held_for_lookups" (OpenCheck kept its last GLEIF slots for lookups — nothing was sent), "rate_limited" or "unreachable". None when it answered or a fresh cached page was used. |
| `isin_list_source` | `str \| None` | `None` | Phase 258: "gleif_file" when GLEIF's daily ISIN-to-LEI file answered (the list is in ISIN order and dated by `isin_list_as_of`), "gleif_api" when the live endpoint or its cache did; None when there is no list. |
| `sources` | `list[str]` | `required` | — |
| `license_notices` | `list[dict[str, str]]` | `required` | — |

#### `Security`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `isin` | `str` | `required` | — |
| `checksum_valid` | `bool` | `True` | ISO 6166 shape + Luhn check digit (advisory data-quality flag; the security is surfaced either way). See opencheck/identifiers.py. |
| `type` | `str \| None` | `None` | — |
| `name` | `str \| None` | `None` | — |
| `ticker` | `str \| None` | `None` | — |
| `exchange` | `str \| None` | `None` | — |
| `sanctioned` | `bool` | `False` | — |
| `regimes` | `list[str]` | `[]` | — |
| `opensanctions_id` | `str \| None` | `None` | — |

### `GET /knowability`

What is knowable about beneficial owners in a jurisdiction — a dated statement, never a signal.

#### `KnowabilityResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `statements` | `list[KnowabilityStatement]` | `required` | — |
| `known_codes` | `list[str]` | `required` | Every code the table holds — so a client can tell "not in the table" from "not asked for". |
| `generated_at` | `str \| None` | `required` | — |
| `as_of` | `str` | `required` | — |

### `GET /person-check`, `/person-appointments`, `/person-positions`, `/legal-person-relations`

BackgroundCheck: a person screened by name across the watchlist sources, and the other companies a register names them on.

#### `PersonCheckResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `query` | `str` | `required` | — |
| `birth_year` | `int \| None` | `required` | — |
| `matches` | `list[PersonMatch]` | `required` | — |
| `risk_signals` | `list[dict[str, Any]]` | `required` | — |
| `weak_match_count` | `int` | `required` | — |
| `sources` | `list[CheckedSource]` | `required` | — |
| `caveats` | `list[str]` | `required` | — |
| `cross_source_links` | `list[dict[str, Any]]` | `[]` | Identifier-backed links between STRONG matches only (shared wikidata_qid / opensanctions_id / …) — the person-world version of the entity report's cross-source panel. Weak matches are excluded: bridging two below-threshold hits would manufacture a same-person claim the name evidence doesn't support. |

#### `CheckedSource`

Per-source outcome record — powers honest 'what was checked' UI.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | `required` | — |
| `name` | `str` | `required` | — |
| `license` | `str` | `required` | — |
| `attribution` | `str` | `required` | — |
| `homepage` | `str` | `required` | — |
| `live` | `bool` | `required` | — |
| `hit_count` | `int` | `required` | — |
| `error` | `str \| None` | `None` | — |

#### `PersonMatch`

One hit, scored against the queried person.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `hit` | `SourceHit` | `required` | — |
| `name_score` | `float` | `required` | — |
| `birth_year_compatible` | `bool` | `required` | — |
| `strong` | `bool` | `required` | — |

#### `PersonAppointmentsResponse`

A person's appointments across companies (Phase C enrichment).

| Field | Type | Default | Meaning |
|---|---|---|---|
| `officer_id` | `str` | `required` | — |
| `name` | `str \| None` | `required` | — |
| `birth_date` | `str \| None` | `required` | — |
| `is_stub` | `bool` | `required` | — |
| `scheme` | `str` | `'GB-COH-OFFICER'` | Which key the list hangs off: `GB-COH-OFFICER` (a Companies House officer id) or `EE-ARIREGISTER-HASH` (the Estonian register's pseudonymous person key), and the source that answered. |
| `source_id` | `str` | `'companies_house'` | — |
| `as_of` | `str \| None` | `None` | The Estonian index's cut — the day the register's files describe. |
| `company_count` | `int \| None` | `None` | Distinct companies in the list (Estonia; one company can carry the person on several lists), and whether the rows were cut short. |
| `truncated` | `bool` | `False` | — |
| `total_results` | `int \| None` | `required` | — |
| `active_count` | `int` | `required` | — |
| `appointments` | `list[AppointmentItem]` | `required` | — |
| `bods` | `list[dict[str, Any]]` | `required` | BODS statements for the officer + appointments (personStatement carries the Companies House officer id) — traceable evidence. |
| `attribution` | `str` | `required` | — |
| `caveat` | `str` | `required` | — |

#### `AppointmentItem`

One appointment or position held — a Companies House appointment, or a row on one of an Estonian company's lists.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `company_name` | `str` | `required` | — |
| `company_number` | `str \| None` | `None` | — |
| `company_status` | `str \| None` | `None` | — |
| `role` | `str \| None` | `None` | — |
| `appointed_on` | `str \| None` | `None` | — |
| `resigned_on` | `str \| None` | `None` | — |
| `company_legal_form` | `str \| None` | `None` | Estonia: the company's legal form, which of the register's lists the person is on (`registry_card` / `shareholders` / `beneficial_owners` / `off_card`), the register's role code, and the share the list states (an exact percentage where it gives one). |
| `register_list` | `str \| None` | `None` | — |
| `role_code` | `str \| None` | `None` | — |
| `share_percent` | `str \| None` | `None` | — |
| `share_size` | `str \| None` | `None` | — |
| `share_currency` | `str \| None` | `None` | — |

#### `PersonPositionsResponse`

Positions held by one EveryPolitician / OpenSanctions PEP record.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `entity_id` | `str` | `required` | — |
| `name` | `str \| None` | `required` | — |
| `is_stub` | `bool` | `required` | — |
| `positions` | `list[PositionItem]` | `required` | — |
| `wikidata_qid` | `str \| None` | `required` | — |
| `countries` | `list[str]` | `required` | — |
| `source_url` | `str` | `required` | Canonical record URL on OpenSanctions for onward verification. |
| `attribution` | `str` | `required` | — |
| `maintenance_note` | `str` | `required` | — |
| `caveat` | `str` | `required` | — |

#### `PositionItem`

One political position held, from the EveryPolitician record.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `label` | `str` | `required` | — |
| `country` | `str \| None` | `None` | — |
| `start_date` | `str \| None` | `None` | — |
| `end_date` | `str \| None` | `None` | — |
| `current` | `bool` | `False` | True when the record carries a start but no end date — held now, as far as the dataset knows. |

#### `LegalPersonRelationsResponse`

Every company a legal person has held a role in on the Estonian e-Business Register — current, former and deleted (the legal-person-relations ticket). Listed, never drawn.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `code` | `str` | `required` | — |
| `country` | `str` | `required` | — |
| `status` | `str` | `required` | `ok`; `not_configured` (no REST key on this server); `unavailable` (RIK refused or did not answer — `detail` says why, `reason` in the degradation vocabulary). Never "no companies" when it was not read. |
| `reason` | `str \| None` | `None` | — |
| `detail` | `str \| None` | `None` | — |
| `total` | `int` | `0` | — |
| `truncated` | `bool` | `False` | — |
| `companies` | `list[LegalPersonCompany]` | `[]` | — |
| `retrieved_at` | `str \| None` | `None` | — |
| `attribution` | `str` | `required` | — |
| `caveat` | `str` | `required` | — |

#### `LegalPersonCompany`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | `required` | — |
| `code` | `str \| None` | `None` | — |
| `status_code` | `str \| None` | `None` | — |
| `status` | `str \| None` | `None` | — |
| `legal_form` | `str \| None` | `None` | — |
| `current` | `bool` | `False` | A live company in which the legal person holds a role today. |
| `relations` | `list[LegalPersonRelation]` | `required` | — |

#### `LegalPersonRelation`

One role a legal person held in one company.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `role` | `str` | `required` | — |
| `role_code` | `str \| None` | `None` | — |
| `since` | `str \| None` | `None` | — |
| `until` | `str \| None` | `None` | — |
| `ended` | `bool` | `False` | — |
| `founder` | `bool` | `False` | — |
| `share_percent` | `str \| None` | `None` | — |
| `share_size` | `str \| None` | `None` | — |
| `share_currency` | `str \| None` | `None` | — |

### `GET /resolve-national-id`

A register number resolved to LEIs through GLEIF's reverse lookup.

#### `ResolveNationalIdResponse`

LEIs that carry a given national company-registration number.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `number` | `str` | `required` | — |
| `country` | `str \| None` | `None` | — |
| `ra_code` | `str \| None` | `None` | — |
| `matches` | `list[NationalIdMatch]` | `required` | — |
| `checksum_warning` | `str \| None` | `None` | Advisory only (Phase A, rigour adoption): set when the number fails its national scheme's check digit — the query still runs, this just explains an otherwise-mystifying empty result. None when no validator applies. |

#### `NationalIdMatch`

One LEI record carrying the queried national registration number.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `name` | `str` | `required` | — |
| `jurisdiction` | `str \| None` | `None` | — |

### `GET /narrative`

The AI summary written from the run's evidence packet.

#### `NarrativeResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str \| None` | `None` | — |
| `subject_name` | `str` | `required` | — |
| `summary` | `str` | `required` | — |
| `claims` | `list[dict[str, Any]]` | `list()` | — |
| `limitations` | `list[str]` | `list()` | — |
| `overall_confidence` | `str` | `'low'` | — |
| `model` | `str` | `required` | — |
| `prompt_version` | `str` | `required` | — |
| `run_id` | `str` | `''` | Deterministic identity of this exact narrative (see dispositions.py) — analyst dispositions are keyed to it, so a regenerate starts a new sheet. |
| `generated_at` | `str` | `''` | — |
| `packet` | `dict[str, Any]` | `required` | The packet is returned so the UI can resolve each claim's cited ids (f/r/g) back to the underlying fact, risk or gap for citation chips. |
| `validation_ok` | `bool` | `True` | Surfaced for transparency / debugging — what (if anything) was rejected. |
| `dropped_claims` | `list[dict[str, Any]]` | `list()` | — |
| `validation_issues` | `list[str]` | `list()` | — |
| `uncited_gaps` | `list[str]` | `list()` | Packet gaps no surviving claim cites ("clear fallbacks, not silent gaps"). |

### `GET /nz-associations/person`

A New Zealand role holder's other companies, graded by address match.

#### `PersonAssociationsResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `company_number` | `str` | `required` | — |
| `name` | `str` | `required` | — |
| `available` | `bool` | `required` | — |
| `reason` | `str \| None` | `None` | — |
| `subject_name` | `str \| None` | `None` | — |
| `person` | `PersonAssociations \| None` | `None` | — |

#### `PersonAssociations`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | `required` | — |
| `role_here` | `list[str]` | `[]` | — |
| `other_company_count` | `int` | `required` | — |
| `high_confidence_count` | `int` | `required` | — |
| `address_match_count` | `int` | `0` | — |
| `name_only_count` | `int` | `0` | — |
| `as_director` | `int` | `required` | — |
| `as_shareholder` | `int` | `required` | — |
| `total_records_under_name` | `int` | `0` | — |
| `truncated` | `bool` | `False` | — |
| `companies` | `list[AssociatedCompany]` | `[]` | — |

#### `AssociatedCompany`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `number` | `str` | `required` | — |
| `name` | `str \| None` | `None` | — |
| `nzbn` | `str \| None` | `None` | — |
| `roles` | `list[str]` | `[]` | — |
| `share_percentage` | `float \| int \| None` | `None` | — |
| `confidence` | `str` | `required` | — |
| `basis` | `str` | `required` | — |
| `link` | `str \| None` | `None` | — |

### `GET /leads`

Candidate parents and owners a lookup did not confirm.

#### `LeadsResponse`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `searched_name` | `str \| None` | `None` | — |
| `search_terms` | `str \| None` | `None` | What was sent to GLEIF's fulltext search: the name with its legal-form words removed (Phase 310), so a reader can see why a candidate came up. |
| `parent` | `LeadParent \| None` | `None` | The direct parent the Golden Copy mirror last held for the record, or null — no mirror, or none filed. |
| `candidates` | `list[LeadCandidate]` | `required` | Active LEI records whose name matches, best tier first, at most five. |
| `gleif_unavailable_reason` | `str \| None` | `None` | Why GLEIF could not be searched (`held_for_lookups` / `rate_limited` / `unreachable`), or null when it answered — an empty list with null here means GLEIF found nothing similar. |
| `note` | `str` | `required` | The sentence every consumer must keep beside the list. |

#### `LeadCandidate`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `name` | `str` | `required` | — |
| `jurisdiction` | `str \| None` | `None` | — |
| `registration_status` | `str \| None` | `None` | — |
| `match` | `str` | `required` | `search_rank.MATCH_TIERS` word for the name: exact / same_name / all_tokens / distinctive_tokens. Never fuzzy. |
| `matched_name` | `str \| None` | `None` | The other name GLEIF files that the match rests on — a former legal name, most often — and GLEIF's type for it; both null when the legal name itself matched. |
| `matched_name_type` | `str \| None` | `None` | — |
| `name_only` | `bool` | `True` | Always True: only the name agrees. Carried so no consumer can forget. |

#### `LeadParent`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `lei` | `str` | `required` | — |
| `name` | `str \| None` | `None` | — |
| `entity_status` | `str \| None` | `None` | — |
| `registration_status` | `str \| None` | `None` | — |
| `relationship_status` | `str \| None` | `None` | — |
| `standing` | `bool` | `required` | Whether GLEIF still serves the relationship. A retired record's relationships are usually retired with it, so this is often False. |

### Objects inside lists

The dict shapes the `list[dict[str, Any]]` fields above carry, where they are not a BODS statement or a signal.

#### `CrossSourceLink`

A confirmed bridge: ``hits`` all describe the same subject.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `key` | `str` | `required` | Which identifier the hits share — `lei`, `gb_coh`, `wikidata_qid`, `opensanctions_id` … (a `SourceHit.identifiers` key), or `name_dob` for a person match. |
| `key_value` | `str` | `required` | The shared value. |
| `confidence` | `str` | `required` | `strong` — a shared structured identifier; `possible` — a name + birth-year match between persons. |
| `hits` | `list[SourceHit]` | `list()` | The hits bridged, each reduced to `source_id`, `hit_id` and `name`. The serialised link also carries `independent_source_count`: how many independent origins the hits represent once source lineage is applied — the number to count as corroboration, never `len(hits)`. |

#### `PossiblySame`

A name-only 'likely same' candidate between two entity statements.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `a` | `str` | `required` | The `statementId` of the first entity statement. |
| `b` | `str` | `required` | The `statementId` of the second. |
| `reason` | `str` | `required` | Why they are offered as the same — a name match within one jurisdiction with no shared identifier. |
| `a_name` | `str` | `''` | The first statement's name, so the report can render the pair without the bundle. |
| `b_name` | `str` | `''` | The second statement's name. |
| `jurisdiction` | `str` | `''` | The jurisdiction code both carry. |
| `a_source` | `str` | `''` | Which source asserted each record — the key context for a human reviewing a name-only match ("GLEIF vs OpenCorporates" reads very differently from "OpenAleph vs OpenAleph"). |
| `b_source` | `str` | `''` | The source that asserted the second — "GLEIF vs OpenCorporates" reads very differently from "OpenAleph vs OpenAleph". |

## BODS statements

`bods[]`, `/export` and the MCP export carry [BODS v0.4](https://standard.openownership.org/en/0.4.0/) statements — the standard's own schema documents their fields. What this page adds is what OpenCheck puts in them that the standard does not define, and how it uses the constructs the standard leaves open.

### Identifiers and dates

`statementId` is content-addressed and `recordId` is the stable identity a relationship references; `declarationSubject` names each source's own subject. [Identifiers](identifiers.md) explains the ids; [Dates](dates.md) explains `statementDate`, `publicationDate`, `source.retrievedAt` and the interest dates.

### Extension keys

Keys BODS v0.4 does not define. Its Source, Interest and Annotation objects are open, and libcovebods reports an unknown key as an additional field, not an error. Every key OpenCheck writes is registered in `opencheck.data_dictionary.EXTENSIONS`; a mapper that emits an unregistered `opencheck*` key fails the test suite.

| Key | Where | Type | Meaning | Example | Since |
|---|---|---|---|---|---|
| `opencheckSourceId` | /source (every statement OpenCheck publishes) | string | The adapter id of the source the statement was mapped from — `companies_house`, `gleif`, `meip` … — the key of the REGISTRY and of `/sources`. Beside `source.description` (a display name) so licence decisions never have to recover the adapter from a label: RDF `bods:license`, Senzing `DATA_LICENSE`, the FullCheck network's `contributing_source_ids` and `LICENSES.md` all read it. On the OECD's MEIP statements it is the one key OpenCheck adds. | `"companies_house"` | Phase 267 |
| `opencheckOfficerPosition` | /recordDetails/interests[] (relationship statements) | string | The officer position behind an interest typed `otherInfluenceOrControl` or `seniorManagingOfficial` that is an appointment, not ownership or control — "Secretary", "Supervisor", "Authorised person". BODS has no role code for a secretary or an agent, so the mappers type the interest and mark it; the graph, the exported diagram and the ownership map read the mark and draw a *role* edge labelled with the position. A reader that does not know the key still reads the interest type. | `"Secretary"` | Phase 340 |
| `gleifLegalEntityEvent` | /annotations[] (a `commenting` annotation on a GLEIF entity statement) | object | GLEIF's Legal Entity Event as published — `type`, `status`, `effectiveDate`, `recordedDate`, plus `effectiveDay`, the calendar day OpenCheck read from the filer's local-midnight timestamp. The annotation's sentence states the same event in words; the object lets a consumer key on GLEIF's codes. | `{"type": "MERGERS_AND_ACQUISITIONS", "status": "COMPLETED", "effectiveDate": "2024-10-24T22:00:00Z", "effectiveDay": "2024-10-25"}` | Phase 305 |

### Annotations

OpenCheck transforms a lot on the way to BODS: register role labels become ``interestType`` codes, nature-of-control codes become English prose, dates change format. The output carried none of that. A reader could see ``seniorManagingOfficial`` but not the Estonian label it came from, or a ``birthDate`` of ``1975-08`` without knowing whether the register published only a month or OpenCheck truncated a full date.

One rule applies to every annotation: **the statement carries the usable value; the annotation carries the register's words.** An annotation object has `statementPointerTarget` (an RFC 6901 pointer into its own statement — never a prefix, never `/`), `motivation`, `description`, optionally `transformedContent`, `creationDate` and `createdBy` (`{"name": "OpenCheck", "uri": "https://opencheck.world"}`).

#### Motivations

| Motivation | When OpenCheck writes it |
|---|---|
| `transformation` | OpenCheck changed the source's value on the way to BODS — a role label became an interest type, a date changed format, a partial date was dropped, a Nasdaq CSD account prefix was stripped from a name. `description` states what the source said; `transformedContent` mirrors the emitted value. |
| `commenting` | The value is exactly what the source published, but a reader could misread it — an imprecise `birthDate`, a reporting exception, a succession partner, a GLEIF event, a founder's historic role, a discrepancy report the register flags. |
| `identifying` | Which real-world thing the statement is about, where BODS gives the key no home: a register's own person key (`EE-ARIREGISTER-HASH`), a Wikidata Q-id, an OpenSanctions record id, an SEC CIK — written by `person_identifier_note` in one fixed sentence shape so exports can read the identifier back. |
| `correcting` | Defined by BODS; OpenCheck does not write it. |
| `linking` | Defined by BODS; OpenCheck does not write it. |

#### Annotation helpers

The functions in `opencheck/bods/annotations.py` that build the annotations OpenCheck writes, with what each records.

| Helper | Records |
|---|---|
| `transformation()` | An annotation recording that OpenCheck transformed a source value. |
| `identifying()` | An annotation recording *which* real-world thing a statement is about. |
| `commenting()` | An annotation that adds context without claiming a transformation. |
| `entry_date_as_start()` | The `startDate` of an interest is the register's entry date (Phase 317). |
| `date_rounding_annotation()` | Record that a date was rounded because the source lacked precision. |
| `person_identifier_note()` | An `identifying` annotation carrying a person identifier that is not an identity document — a Wikidata Q-id, an aggregator's record id, a register's own person key. |

## Risk signals and degraded sources

`risk_signals[]` on a lookup, a report, a deepen and the `risk_signals` stream event. The codes and their rules are in [Risk signals](risk-signals.md); this is the object each one is.

### `RiskSignal`

One risk assertion about a hit, with explanation.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `code` | `str` | `required` | The signal code (`SANCTIONED`, `RELATED_PEP`, `COMPLEX_OWNERSHIP_LAYERS` …) — see [Risk signals](risk-signals.md). |
| `confidence` | `str` | `required` | `high`, `medium` or `low`. For a name match it is the lower of the rule's confidence and the match's; an uncorroborated name match never reads `high`. |
| `summary` | `str` | `required` | One sentence stating the finding, built from the evidence. The verdict and the AI summary quote it. |
| `source_id` | `str` | `required` | The source whose data the rule keyed off (`opensanctions`, `icij`, `gleif` …), or `opencheck` for a rule computed over the merged graph. |
| `hit_id` | `str` | `required` | The hit the signal is attached to — a source's record id, or the subject's LEI for a graph-level rule. |
| `evidence` | `dict[str, Any]` | `dict()` | The raw bits the rule keyed off, so a reader can see why without re-running it. Keys vary by code; the ones that name BODS statements (`statement_id`, `subject_statement_id(s)`, `matches[].statement_id`, `jurisdictions[].statement_id`, `longest_path[]`, `ended_relationship_statement_ids`) are resolved to `statementId`s and drive the graph badges. |
| `kind` | `str` | `'risk'` | `"risk"` (default) or `"context"`. A *context* signal is a structural observation that is NOT an adverse finding — it is worth showing, and it may feed a composite rule, but it must not be presented as a risk or counted in "N risk signals".  This lives on the signal rather than in a per-surface exclusion list because the count is rendered independently by the results page, the OG share card and the share-page meta description; three hand-kept lists would drift the way the curated homepage claims have before. Classify once here, and every surface agrees by construction. |

Closed vocabularies on a signal:

| Field | Values |
|---|---|
| `kind` | `risk` — an adverse finding, counted in "N risk signals"; `context` — a structural observation worth showing, never counted as a finding. |
| `confidence` | `high`, `medium`, `low`. |

### `DegradedSource`

One upstream screen that did not fully run for this lookup.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | `required` | Adapter id of the upstream that failed ("opensanctions", "icij", …); "opencheck" when the failure happened before reaching any upstream. |
| `check` | `str` | `required` | Which derived check degraded ("cross_source_names", "icij_offshore_leaks"). |
| `affected_signals` | `list[str]` | `required` | Risk codes whose absence is now unreliable (e.g. RELATED_SANCTIONED). |
| `detail` | `str` | `required` | Human-readable summary — counts only, never names. |
| `reason` | `str` | `required` | One of the `reason` values below. |

An empty `risk_signals` beside a non-empty `degraded_sources` is **not** a clean screen: a degradation says a screen or source did not fully run, and every reader — the verdict, the MCP summary, the batch row — treats it as a check that did not happen.

#### `reason`

| Value | Meaning |
|---|---|
| `upstream_error` | the upstream service errored |
| `timeout` | the upstream service timed out |
| `not_configured` | the required API credential is not configured |
| `rate_limited` | the upstream service rate-limited the request |
| `truncated` | OpenCheck's per-lookup limit on related parties was reached |

#### `check`

| Value | Meaning |
|---|---|
| `source_fetch` | The source itself did not answer, so none of its records were consulted. |
| `source_read` | The source returned a record and then failed while it was being read in full — a partial answer. |
| `cross_source_names` | Related-party sanctions and PEP screening did not fully run. |
| `icij_offshore_leaks` | Offshore Leaks screening did not fully run. |
| `openaleph_percolation` | OpenAleph archive screening did not fully run. |

## Exports

`GET /export?format=` and the MCP `opencheck_export_bods` serve the same bundle in every format. `json`, `jsonl`, `xml` and `zip` are the BODS statements themselves (the zip adds a manifest and `LICENSES.md`); `rdf` is BODS as TriG under the [BODS RDF vocabulary](https://vocab.openownership.org/); `cypher` is a Neo4j script with one node per party and one relationship per interest. The tabular families below are documented column by column, and each zip carries this section as `DATA-DICTIONARY.md`.

### CSV and Excel — `format=csv` (zip of three tables), `format=xlsx` (one workbook, a sheet per table) and `format=gql` (the same tables plus BigQuery DDL and GQL queries)

Three tables projected by [bods-gql](https://github.com/StephenAbbott/bods-gql): one row per entity statement, per person statement and per relationship statement. Every cell is text; empty tables are omitted (a company with no natural persons ships no `person_nodes.csv`). Every column name below is a dataclass field of the library, read at generation time.

#### `entity_nodes.csv`

| Column | Meaning |
|---|---|
| `record_id` | The BODS `recordId` — the party's or relationship's stable identity across versions. Relationships reference parties by it. |
| `statement_id` | The BODS `statementId` — this statement's own id, a digest of its content under the record id, so an edited record is a new statement. |
| `statement_date` | `statementDate`: the day the source's record was true (the register's own date where it publishes one, else the snapshot cut or retrieval). |
| `record_status` | `recordStatus`: `new`, `updated` or `closed`. A closed relationship has ended. |
| `declaration_subject` | `declarationSubject`: the `recordId` of the subject of the network this statement belongs to — per source, each bundle's own subject. |
| `name` | `recordDetails.name` — the legal name as the source publishes it. |
| `entity_type` | `entityType.type`: `registeredEntity`, `legalEntity`, `arrangement`, `anonymousEntity`, `unknownEntity`, `state`, `stateBody`. |
| `entity_subtype` | `entityType.subtype` from the closed v0.4 codelist (`trust`, `nomination`, `stateBody` …), or empty. |
| `entity_type_details` | `entityType.details` — the register's own wording for the entity's form ("Private limited company", "Local Company"). |
| `is_component` | `isComponent`: true for a hop inside an indirect ownership chain whose primary relationship lists it in `component_records_json`. |
| `jurisdiction_name` | `jurisdiction.name` — the country or subdivision in words. |
| `jurisdiction_code` | `jurisdiction.code` — ISO 3166-1 alpha-2 or 3166-2 (`US-DE`), or empty when the source gives only a name. |
| `founding_date` | `foundingDate` as a full ISO day; a partial date the source gave is dropped and said in an annotation. |
| `dissolution_date` | `dissolutionDate` as a full ISO day, where the register or GLEIF's completed terminal event dates it. |
| `uri` | `uri` — the record's own page on the source, where the source has one. |
| `primary_identifier_id` | The first `identifiers[].id` — usually the LEI (`XI-LEI`) or the register number. |
| `primary_identifier_scheme` | The scheme of the first identifier (`XI-LEI`, `GB-COH`, an RA code for a register OpenCheck has no adapter for). |
| `registered_address` | The `registered` address, as one line. |
| `registered_country` | The registered address's country code. |
| `publisher_name` | `publicationDetails.publisher.name` — "OpenCheck" for statements OpenCheck mapped, the OECD / Open Ownership for statements passed through verbatim. |
| `publication_date` | `publicationDetails.publicationDate` — when the statement was published. |
| `bods_version` | `publicationDetails.bodsVersion` — always `0.4`. |
| `identifiers_json` | Every `identifiers[]` entry as JSON (`scheme`, `id`, `schemeName`, `uri`). |
| `addresses_json` | Every `addresses[]` entry as JSON (`type`, `address`, `postCode`, `country`). |
| `alternate_names_json` | `alternateNames[]` as JSON — former names, trading names, GLEIF's transliterated names. |
| `source_json` | The statement's `source` object as JSON: `type`, `description`, `url`, `retrievedAt` and `opencheckSourceId`. |
| `annotations_json` | The statement's `annotations` array as JSON — see *Annotations*. Empty when none. |
| `node_label` | The property-graph label: `Entity`. |

#### `person_nodes.csv`

| Column | Meaning |
|---|---|
| `record_id` | The BODS `recordId` — the party's or relationship's stable identity across versions. Relationships reference parties by it. |
| `statement_id` | The BODS `statementId` — this statement's own id, a digest of its content under the record id, so an edited record is a new statement. |
| `statement_date` | `statementDate`: the day the source's record was true (the register's own date where it publishes one, else the snapshot cut or retrieval). |
| `record_status` | `recordStatus`: `new`, `updated` or `closed`. A closed relationship has ended. |
| `declaration_subject` | `declarationSubject`: the `recordId` of the subject of the network this statement belongs to — per source, each bundle's own subject. |
| `name` | The person's full name (`names[].fullName` of the first name). |
| `family_name` | `names[].familyName` of the first name, where the source splits names. |
| `given_name` | `names[].givenName` of the first name, where the source splits names. |
| `person_type` | `personType`: `knownPerson`, `anonymousPerson` or `unknownPerson`. |
| `is_component` | `isComponent`: true for a hop inside an indirect ownership chain whose primary relationship lists it in `component_records_json`. |
| `birth_date` | `birthDate` at the precision the register publishes — `YYYY`, `YYYY-MM` or a full day. |
| `death_date` | `deathDate`, rarely published. |
| `nationality_code` | The first `nationalities[].code` (ISO 3166-1 alpha-2). |
| `pep_status` | `politicalExposure.status`: `isPep`, `isNotPep` or `unknown` — as the source states it, never OpenCheck's screening result. |
| `publisher_name` | `publicationDetails.publisher.name` — "OpenCheck" for statements OpenCheck mapped, the OECD / Open Ownership for statements passed through verbatim. |
| `publication_date` | `publicationDetails.publicationDate` — when the statement was published. |
| `bods_version` | `publicationDetails.bodsVersion` — always `0.4`. |
| `names_json` | Every `names[]` entry as JSON (`type`, `fullName`, `familyName`, `givenName`). |
| `identifiers_json` | Every `identifiers[]` entry as JSON (`scheme`, `id`, `schemeName`, `uri`). |
| `addresses_json` | Every `addresses[]` entry as JSON (`type`, `address`, `postCode`, `country`). |
| `nationalities_json` | Every `nationalities[]` entry as JSON (`name`, `code`). |
| `political_exposure_json` | The `politicalExposure` object as JSON — status and the source's `details[]`. |
| `source_json` | The statement's `source` object as JSON: `type`, `description`, `url`, `retrievedAt` and `opencheckSourceId`. |
| `annotations_json` | The statement's `annotations` array as JSON — see *Annotations*. Empty when none. |
| `node_label` | The property-graph label: `Person`. |

#### `ownership_edges.csv`

| Column | Meaning |
|---|---|
| `record_id` | The BODS `recordId` — the party's or relationship's stable identity across versions. Relationships reference parties by it. |
| `statement_id` | The BODS `statementId` — this statement's own id, a digest of its content under the record id, so an edited record is a new statement. |
| `interested_party` | The `recordId` of the party holding the interest (`interestedParty`) — the edge's source node. |
| `subject` | The `recordId` of the party the interest is in (`subject`) — the edge's target node. |
| `statement_date` | `statementDate`: the day the source's record was true (the register's own date where it publishes one, else the snapshot cut or retrieval). |
| `record_status` | `recordStatus`: `new`, `updated` or `closed`. A closed relationship has ended. |
| `declaration_subject` | `declarationSubject`: the `recordId` of the subject of the network this statement belongs to — per source, each bundle's own subject. |
| `is_component` | `isComponent`: true for a hop inside an indirect ownership chain whose primary relationship lists it in `component_records_json`. |
| `interest_types` | Every `interests[].type`, joined — `shareholding`, `votingRights`, `appointmentOfBoard`, `seniorManagingOfficial`, `otherInfluenceOrControl` …. |
| `is_beneficial_ownership` | True when any interest carries `beneficialOwnershipOrControl: true` — a declared beneficial owner under the register's regime; empty where the regime makes no such claim. |
| `direct_or_indirect` | `interests[].directOrIndirect` of the first interest: `direct`, `indirect` or `unknown`. |
| `share_exact` | `share.exact` of the first interest with a share, as a percentage. |
| `share_minimum` | `share.minimum` of a banded share (inclusive). |
| `share_maximum` | `share.maximum` of a banded share (inclusive). |
| `share_exclusive_minimum` | `share.exclusiveMinimum` — the band's lower end, exclusive ("more than 25%"). |
| `share_exclusive_maximum` | `share.exclusiveMaximum` — the band's upper end, exclusive. |
| `interest_start_date` | `interests[].startDate` of the first interest. |
| `interest_end_date` | `interests[].endDate` of the first interest — set on an ended interest; the record is then `closed`. |
| `publisher_name` | `publicationDetails.publisher.name` — "OpenCheck" for statements OpenCheck mapped, the OECD / Open Ownership for statements passed through verbatim. |
| `publication_date` | `publicationDetails.publicationDate` — when the statement was published. |
| `bods_version` | `publicationDetails.bodsVersion` — always `0.4`. |
| `interests_json` | Every `interests[]` object as JSON, including `details` (the register's words), `share` and `opencheckOfficerPosition`. |
| `component_records_json` | `componentRecords[]` as JSON — the hops a primary indirect relationship is assembled from. |
| `source_json` | The statement's `source` object as JSON: `type`, `description`, `url`, `retrievedAt` and `opencheckSourceId`. |
| `annotations_json` | The statement's `annotations` array as JSON — see *Annotations*. Empty when none. |
| `subject_unspecified_json` | `subject` when it names no record: an `unspecified` object with `reason` and `description`. |
| `interested_party_unspecified_json` | `interestedParty` when it names no record — a withheld or unknown owner, `reason` from the v0.4 codelist and the register's `description`. |
| `edge_label` | The property-graph label: `HAS_INTEREST`. |

### Batch screening — `rows.csv` in the batch zip and the `rows[]` of `opencheck_batch_lookup`

One row per LEI screened, in paste order. The same columns, in the same order, as the page's client-side download (`frontend/src/lib/batch.ts`).

#### `rows.csv`

| Column | Meaning |
|---|---|
| `lei` | The LEI screened. |
| `legal_name` | GLEIF's legal name. |
| `jurisdiction` | GLEIF's jurisdiction code (ISO 3166-1 alpha-2, or 3166-2 for US states). |
| `register_status` | The register status from the subject profile (`live`, `dissolved`, `liquidation`, `unknown`). |
| `lei_registration` | The LEI record's own registration status (`ISSUED`, `LAPSED`, `RETIRED` …) — the LEI's status, never the company's. |
| `lei_registration_since` | The day a non-ISSUED status dates from (a lapse is dated by the missed renewal). |
| `verdict` | The one-sentence template verdict. |
| `risk_count` | Number of signals with `kind: risk` after merging. |
| `risk_codes` | Those signals' codes, `;`-joined. |
| `context_count` | Number of signals with `kind: context` — structural context, never a finding. |
| `context_codes` | Those signals' codes, `;`-joined. |
| `sources_applicable` | How many sources applied to this company (the GLEIF anchor included). |
| `sources_answered` | How many applicable sources replied — with a record or with none. |
| `sources_with_data` | How many returned a record. |
| `degraded` | `true` when any screen or source did not fully run — never read the row as clean while set. |
| `degraded_sources` | The degraded `source_id`s, `;`-joined. |
| `state` | `done` for a screened row, `failed` for one that was not. |
| `reason` | Why a row failed (an unknown LEI, a rate limit), empty when done. |
| `report_url` | The report page for the LEI on opencheck.world. |

### Senzing — `format=senzing`

One JSON record per party in [Senzing's entity specification](https://www.senzing.com/docs/entity_specification/), features as a list of single-key objects. Relationships are `REL_ANCHOR` / `REL_POINTER` features; an unspecified party becomes a placeholder record so the pointer resolves. Keys are observed by running the writer over the sample bundle.

#### `senzing`

| Column | Meaning |
|---|---|
| `DATA_SOURCE` | Always `OPENCHECK` — the Senzing data source every record is loaded under. |
| `RECORD_ID` | The BODS `statementId` of the party; for a placeholder record, a synthetic id for the unspecified side of a relationship. |
| `DATA_LICENSE` | The licence of the statement's source, from `opencheckSourceId`. |
| `ATTRIBUTION` | The source's attribution line. |
| `FEATURES` | The list of feature objects; every key below `RECORD_TYPE` is a feature. |
| `RECORD_TYPE` | `ORGANIZATION` for an entity statement, `PERSON` for a person statement. |
| `NAME_ORG` | An organisation name; `NAME_TYPE` says whether it is the `PRIMARY` name or an `ALTERNATE` one. |
| `NAME_TYPE` | `PRIMARY` or `ALTERNATE`, beside each name feature. |
| `LEI_NUMBER` | The LEI, from an `XI-LEI` identifier. |
| `NATIONAL_ID_NUMBER` | A register number from any other identifier scheme, with `NATIONAL_ID_TYPE` naming the scheme. |
| `NATIONAL_ID_TYPE` | The identifier's scheme (`GB-COH`, `NO-BRC` …) or scheme name. |
| `NATIONAL_ID_COUNTRY` | The country the scheme belongs to, read from the scheme's prefix. |
| `REGISTRATION_DATE` | `foundingDate`. |
| `REGISTRATION_COUNTRY` | `jurisdiction.code`. |
| `ADDR_TYPE` | Senzing's `BUSINESS` / `MAILING` / `HOME` from the BODS address type. |
| `ADDR_FULL` | An address as one line, with `ADDR_TYPE` and `ADDR_COUNTRY` beside it. |
| `ADDR_COUNTRY` | The address's country code. |
| `REL_ANCHOR_DOMAIN` | `OPENCHECK` — every party is a relationship anchor in that domain. |
| `REL_ANCHOR_KEY` | The party's `statementId`, which pointers refer to. |
| `REL_POINTER_DOMAIN` | `OPENCHECK` — a relationship pointer from a party to another party. |
| `REL_POINTER_KEY` | The `statementId` of the party pointed at. |
| `REL_POINTER_ROLE` | The relationship's role, one pointer per interest: the interest type upper-cased (`SHAREHOLDING`, `SENIOR_MANAGING_OFFICIAL` …) with a share suffix where one is stated, or `INTERESTED_PARTY_OF` when no interest is listed. |
| `REL_POINTER_FROM_DATE` | The interest's `startDate`. |
| `REL_POINTER_THRU_DATE` | The interest's `endDate` — set on an ended interest. |
| `NAME_FULL` | A person's full name; `NAME_TYPE` as above. |
| `DATE_OF_BIRTH` | `birthDate` at the register's precision. |
| `NATIONALITY` | A `nationalities[].code`, one feature per nationality. |
| `UNSPECIFIED_PARTY` | On a placeholder record: which side (`subject` / `interestedParty`) of the relationship names no record. |
| `BODS_RELATIONSHIP_ID` | On a placeholder record: the relationship statement it stands in for. |
| `UNSPECIFIED_REASON` | On a placeholder record: the BODS `unspecified.reason` code (`informationUnknownToPublisher`, `interestedPartyExemptFromDisclosure` …). |
| `UNSPECIFIED_DESCRIPTION` | On a placeholder record: the register's own words for why the party is not named. |

### FollowTheMoney — `format=ftm`

One [FollowTheMoney](https://followthemoney.tech/) entity per party and per interest, as JSON lines with `id`, `schema` and `properties`. Property names below are FtM's; the list is what the writer produced for the sample bundle.

#### `ftm`

| Column | Meaning |
|---|---|
| `id` | The FollowTheMoney entity id — the BODS `statementId` for a party, a derived id for an interest. |
| `schema` | The FtM schema: `Company` or `Organization` (entity), `Person`, `LegalEntity` (an unspecified party), `Ownership` or `Directorship` (an interest). |
| `properties` | The FtM property map; every key below is a property. |
| `name` | The party's names. |
| `alias` | Alternate names. |
| `leiCode` | The LEI. |
| `registrationNumber` | Register numbers from non-LEI identifier schemes. |
| `jurisdiction` | `jurisdiction.code`. |
| `incorporationDate` | `foundingDate`. |
| `address` | Addresses, one line each. |
| `dissolutionDate` | `dissolutionDate`. |
| `birthDate` | `birthDate`. |
| `nationality` | `nationalities[].code`. |
| `notes` | On a `LegalEntity` placeholder: the same note — which side, the reason code and the register's words. |
| `role` | The interest type in words ("shareholding", "senior managing official", "other influence or control"). |
| `startDate` | The interest's `startDate`. |
| `owner` | On an `Ownership`: the interested party's entity id. |
| `asset` | On an `Ownership`: the subject's entity id. |
| `percentage` | On an `Ownership`: the share, exact or as a band ("25–50%"). |
| `ownershipType` | On an `Ownership`: the interest types in words. |
| `endDate` | The interest's `endDate`. |
| `director` | On a `Directorship`: the interested party's entity id. |
| `organization` | On a `Directorship`: the subject's entity id. |
| `description` | On an interest with an unspecified party: the note saying which side is unspecified, the reason code and the register's words. |

### Google AML AI — `format=amlai`

The three input tables of the [AML AI data model](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model), one NDJSON file each, produced by [bods-aml-ai](https://github.com/StephenAbbott/bods-aml-ai). Keys are observed by running the transform over the sample bundle; `nationalities`, `addresses` and `supplementary_data_payload` are nested records.

#### `party.ndjson`

| Column | Meaning |
|---|---|
| `party_id` | The AML AI party id — the BODS `recordId`. |
| `validity_start_time` | The row's validity start: the statement's `publicationDate` as a timestamp. |
| `type` | `COMPANY` for an entity statement, `CONSUMER` for a person. |
| `source_system` | Always `BODS`. |
| `name` | The party's name. |
| `birth_date` | A person's `birthDate` as a BigQuery date (a partial date is rounded). |
| `addresses` | Repeated address records (`type`, `address_line`, `city`, `postal_code`, `region_code`). |
| `nationalities` | Repeated nationality records (`region_code`). |
| `establishment_date` | An entity's `foundingDate` as a BigQuery date. |
| `residencies` | Repeated residency records (`region_code`) from an entity's jurisdiction. |

#### `party_supplementary_data.ndjson`

| Column | Meaning |
|---|---|
| `party_supplementary_data_id` | Which supplementary field the row carries (`ownership_share_pct`, `pep_flag` …). |
| `party_id` | The party the row describes. |
| `validity_start_time` | The row's validity start. |
| `supplementary_data_payload` | `{"value": <number>}` — the field's value. |
| `source_system` | Always `BODS`. |

#### `account_party_link.ndjson`

| Column | Meaning |
|---|---|
| `account_id` | A synthetic account standing for the subject company (`bods-acct-<recordId>`): AML AI models relationships through accounts, and OpenCheck holds none. |
| `party_id` | The party linked to the account. |
| `validity_start_time` | The earliest relationship date, else the subject's publication date. |
| `role` | `PRIMARY_HOLDER` for the subject, `SUPPLEMENTARY_HOLDER` for each interested party. |
| `source_system` | Always `BODS`. |

## MCP tools

Each tool, what it does and the top-level keys of the object it returns, read from `opencheck/mcp/server.py` and the shaping functions it delegates to. Keys shared with the API carry the meaning given above; `hint` is a sentence for the calling agent. [MCP server](mcp-server.md) covers transport and tiers.

### `opencheck_search`

Find a company's Legal Entity Identifier (LEI) from a name or free text.

Arguments: `query`, `kind`, `limit`.

Keys: `query`, `kind`, `count`, `total`, `truncated`, `candidates`, `hint`.

On a refused call: `error`.

### `opencheck_resolve_national_id`

Resolve a national company-registration number to its LEI(s).

Arguments: `number`, `country`, `ra_code`.

The `ResolveNationalIdResponse` object (API responses above).

### `opencheck_lookup`

Run customer due diligence on a legal entity by its LEI.

Arguments: `lei`, `deepen_top`.

Keys: `lei`, `legal_name`, `jurisdiction`, `summary`, `verdict`, `profile`, `lei_successor`, `knowability`, `integrated_ownership`, `listing`, `institutional_shareholders`, `identifiers`, `derived_identifiers`, `risk_signals`, `degraded_sources`, `sources`, `coverage`, `counts`, `licensing`, `license_notices`, `hint`.

On a refused call: `error`, `status`.

### `opencheck_register_lookup`

Run due diligence on a company that has no LEI, by its register number.

Arguments: `scheme`, `id`, `deepen_top`, `name`.

Keys: `lei`, `legal_name`, `jurisdiction`, `summary`, `verdict`, `profile`, `lei_successor`, `knowability`, `integrated_ownership`, `listing`, `institutional_shareholders`, `identifiers`, `derived_identifiers`, `risk_signals`, `degraded_sources`, `sources`, `coverage`, `counts`, `licensing`, `license_notices`, `hint`, `scheme`, `id`.

On a refused call: `error`, `status`.

### `opencheck_batch_lookup`

Screen a list of companies by LEI — one compact row per company.

Arguments: `leis`, `deepen_top`.

Keys: `cap`, `accepted`, `rejected`, `overflow`, `rows`, `failed`, `counts`, `hint`.

### `opencheck_export_bods`

Export an entity's beneficial-ownership graph as BODS v0.4 statements.

Arguments: `lei`, `format`, `deepen_top`, `include_subsidiaries`.

Keys: `lei`, `format`, `record_count`, `records`, `bods_issues`, `license_notices`, `statement_count`, `subsidiary_statement_count`, `statements`, `integrated_ownership`.

On a refused call: `error`, `status`.

### `opencheck_owners`

Who owns this company above a threshold — calculated and declared.

Arguments: `lei`, `threshold`, `rule`, `depth`, `deepen_top`.

Keys: `lei`, `legal_name`, `jurisdiction`, `rule`, `calculated_owners`, `declared_beneficial_owners`, `in_both`, `listed_without_a_figure`, `chain_stops`, `depth`, `as_of`, `method`, `note`, `fullcheck_url`, `license_notices`, `hint`, `summary`, `sanctions_ownership_test`.

On a refused call: `error`, `status`.

### `opencheck_ownership_map`

Map who owns a company: one SVG image plus a text table of every edge.

Arguments: `lei`, `source`, `depth`, `deepen_top`.

Returns content blocks — a JSON text block and an SVG image — not a structured object.

### `opencheck_person_check`

Screen one person for PEP / sanctions / offshore-leaks signals.

Arguments: `name`, `birth_year`.

Keys: `query`, `birth_year`, `risk_signals`, `strong_matches`, `weak_match_count`, `cross_source_links`, `sources_checked`, `caveats`, `hint`.

On a refused call: `error`.

### `opencheck_person_positions`

List one person's positions across companies, keyed by a register's own person key — never by name.

Arguments: `scheme`, `id`, `country`.

For a Companies House officer id, the `PersonAppointmentsResponse` object without `bods`, cut to the first 100 appointments with `truncated: true` when cut; for an Estonian person key, the `PersonPositionsResponse` object; for an Estonian legal person, the `LegalPersonRelationsResponse` object (API responses above). `error` on a refused call.

### `opencheck_save_report`

Keep a check of an LEI as a saved report: a link to exactly what OpenCheck found, on this date, that does not change afterwards.

Arguments: `lei`, `deepen_top`.

Keys: `report_id`, `url`, `json_url`, `content_hash`, `lei`, `legal_name`, `run_completed_at`, `saved_at`, `expires_at`, `manage_token`, `verdict`, `licensing`, `note`.

On a refused call: `error`, `status`, `code`.

### `opencheck_subsidiaries`

What a legal entity consolidates: its GLEIF subsidiary network.

Arguments: `lei`, `format`, `include_declared`.

The `SubsidiariesResponse` object (API responses above) plus `statement_count` (for `format=bods`), `measures` (a sentence saying what GLEIF Level 2 measures), `declared` (the `DeclaredSubsidiariesResponse` object when `include_declared`) and `license_notices`. `error` on a refused call.

### `opencheck_list_sources`

List the data sources OpenCheck consults, with licence and live status.

Arguments: none.

Keys: `count`, `sources`.
