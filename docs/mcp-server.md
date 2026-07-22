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
| `opencheck_export_bods(lei, format="json", deepen_top=3)` | Full ownership graph — BODS v0.4 (`json`/`jsonl`), Senzing JSON entity records (`senzing`), or FollowTheMoney entities (`ftm`) |
| `opencheck_person_check(name, birth_year=None)` | Screen one person (PEP / sanctions / offshore-leaks) — evidence-shaped: signals from strong matches only, per-source outcomes, caveats |
| `opencheck_list_sources()` | Adapter inventory with licence + live status |

`narrative` is deliberately **not** exposed (it spends model tokens per call).
Responses are flattened by `mcp/shaping.py` into compact, agent-readable
structures; `license_notices` are preserved so agents don't redistribute
CC-BY-NC data unknowingly.

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
