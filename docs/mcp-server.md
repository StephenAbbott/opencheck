# OpenCheck MCP server

OpenCheck exposes its LEI-driven due-diligence pipeline as **Model Context
Protocol (MCP)** tools, so AI agents can invoke it directly — the agent-native
counterpart to the OpenAPI surface. It's advertised to ARD discovery services
via an `application/mcp-server+json` entry in
[`ai-catalog.json`](./ard.md).

## Where it lives

| Item | Value |
|---|---|
| Package | `backend/opencheck/mcp/` |
| Endpoint (streamable HTTP) | `https://api.opencheck.world/mcp` |
| Descriptor | `https://api.opencheck.world/.well-known/mcp.json` |
| Transport | streamable HTTP, stateless |

The MCP app is **mounted in-process** on the existing FastAPI service
(`app.mount("/mcp", …)`). It calls the same pipeline functions the REST routes
call (`routers.lookup.lookup`, `routers.search.search`, …), so it shares the
15-minute replay cache and the startup cache warm-up and can never diverge from
the REST path. The build is defensive: if the MCP package fails to import, the
REST API still starts (the mount is skipped with a logged warning).

The streamable-HTTP session manager is entered in the FastAPI lifespan
(`mcp.session_manager.run()`) — a mounted sub-app does not get its lifespan run
by the parent, so without this `/mcp` requests would fail. DNS-rebinding
protection is disabled (`TransportSecuritySettings`): it guards localhost-bound
dev servers, not a public API behind a reverse proxy, and a fixed Host allowlist
would `421` in production.

## Tools (v1)

| Tool | Purpose |
|---|---|
| `opencheck_search(query, kind="entity")` | Name → candidate entities with LEIs |
| `opencheck_resolve_national_id(number, country="", ra_code="")` | National registration number → LEI(s) |
| `opencheck_lookup(lei, deepen_top=5)` | Identity, identifiers, risk signals, source coverage |
| `opencheck_batch_lookup(leis, deepen_top=5)` | Up to 20 LEIs → one compact row each (name, jurisdiction, register status, verdict sentence, risk/context counts, coverage, `degraded`), plus `failed` rows and `rejected` tokens — see below |
| `opencheck_export_bods(lei, format="json", deepen_top=3)` | Full ownership graph — BODS v0.4 (`json`/`jsonl`), Senzing JSON entity records (`senzing`), or FollowTheMoney entities (`ftm`) |
| `opencheck_person_check(name, birth_year=None)` | Screen one person (PEP / sanctions / offshore-leaks) — evidence-shaped: signals from strong matches only, per-source outcomes, caveats |
| `opencheck_list_sources()` | Adapter inventory with licence + live status |

`narrative` is deliberately **not** exposed (it spends model tokens per call).
Responses are flattened by `mcp/shaping.py` into compact, agent-readable
structures.

### What `opencheck_batch_lookup` returns (Phase 164)

A thin loop over `opencheck_lookup`, never a second pipeline: each row is the
single lookup's `LookupResponse` reduced by `shaping.shape_batch_row` to what a
list reader scans, so a batch row for LEI X and `opencheck_lookup(X)` cannot
disagree. Rows run **two at a time** (`OPENCHECK_BATCH_CONCURRENCY`) against
the shared GLEIF budget — twenty companies not seen recently take about two
minutes; a re-run inside the 15-minute replay window is free.

- **`accepted` / `rejected` / `overflow`** — the list is parsed tolerantly
  (whitespace, commas, semicolons), each token checksum-validated and
  de-duplicated; every rejection carries a reason; valid LEIs beyond the cap of
  20 are counted in `overflow`, never silently dropped.
- **`rows[]`** in paste order: `lei`, `legal_name`, `jurisdiction`,
  `register_status` (Phase 151 liveness, from the subject profile),
  `verdict`, `risk_count` / `risk_codes` and `context_count` / `context_codes`
  (the Phase 153 kind split), `coverage {applicable, answered}` with the GLEIF
  anchor counted (Phase 156), `degraded` + `degraded_sources`, `licensing`,
  `report_url`.
- **`failed[]`** — a row that could not be screened (unknown LEI → 404; GLEIF
  throttle refusal → 503 with `retryable: true`) is a row with
  `degraded: true`, not an exception: one bad LEI never aborts the other
  nineteen, and the batch is not clean while `failed` is non-empty.
- Rows are never ranked by severity — OpenCheck does not grade companies.

The same loop backs `GET /batch-stream?leis=…` for the web app (SSE:
`batch_start`, then `row_done` / `row_failed` in completion order, then
`batch_done`; heavy rate tier; Phase 144 bot gate) and, since Phase 167,
`GET /batch-export?leis=…` — one zip with `bundle.json` (every row's BODS
statements, de-duplicated by `statementId`), `rows.csv`, `manifest.json` and a
`LICENSES.md` computed over the union of contributing sources, so one CC-BY-NC
source in one row makes the whole bundle non-commercial and the notes say so.
An agent wanting the merged graph can call `opencheck_export_bods` per LEI, or
fetch `/batch-export` directly.

### What `opencheck_lookup` returns (Phase 153)

- **`risk_signals[]` rows carry `kind`** — `risk` (a finding) or `context`
  (how the company is put together, not a finding against it) — mirroring the
  split every other surface has rendered since Phases 111/116. A row with no
  `kind` on the wire is `risk`, the same default as `lib/signalKind.ts`. The
  `summary` line names the two sets separately (`Risk signals: …` and
  `Structural context (not risk findings): …`) and `counts` carries
  `risk_signals` / `context_signals`. Before this the row dropped `kind`, so an
  agent reading Shell plc reported four risk signals including "non-EU
  jurisdiction" — the Phase 111 failure on the surface most likely to be quoted.
- **Identical rows merge.** Per-bundle context signals fire once per deepened
  source; rows with the same `(code, kind, summary)` collapse into one, with the
  contributing adapters listed in `sources`.
- **`verdict`** — the deterministic one-line sentence the results page opens
  with (`opencheck.verdict`).
- **`profile`** — what the registers say the company *is* (Phase 154):
  `legal_form`, `register_status` (`liveness` live / pending / terminal, the
  register that said it, its date and raw label), `founding_date` and
  `registered_address`, each listing the sources that state the value and
  how many of them are independent. Facts, never findings — a dissolved
  company is reported as dissolved; whether that matters is the reader's call.
- **`licensing`** — the composite licence verdict over the sources that
  returned data (`commercial_use`, `attribution_required`, `share_alike`,
  `headline`, `warnings`), computed by the same `licensing.assess` the web
  Download panel calls. `license_notices` remains the per-bundle notices
  adapters attach themselves — usually empty, which is why `licensing` exists:
  a bundle holding CC-BY-NC OpenSanctions statements shipped
  `license_notices: []` to agents while the panel said "NOT for commercial
  use". The `summary` line ends with the licensing headline.

## resolve_national_id

The MCP `resolve_national_id` tool wraps the new **`GET /resolve-national-id`**
endpoint (`number`, optional `country` ISO-alpha-2 or explicit `ra_code`). It
reuses the GLEIF adapter's `search_by_local_id` (queries GLEIF's three local-id
filter fields, de-duplicated by LEI) — the inverse of the normal LEI-first flow.
The `country → RA code` map (`_RA_BY_COUNTRY` in `routers/lookup.py`) mirrors
`frontend/src/lib/raCodes.ts` and the RA table in `CLAUDE.md`; keep them in sync
when adding a register.

## Local testing

```bash
# unit/integration tests (offline, deterministic)
cd backend && python -m pytest tests/test_mcp.py -q

# drive the live protocol with the MCP Inspector against a running server
npx @modelcontextprotocol/inspector
#   transport: streamable-http   url: http://localhost:8000/mcp
```

## Dependency

`mcp>=1.2` (added to `backend/pyproject.toml`). The MCP package is imported at
app startup; the dependency must be installed for `/mcp` to mount.
