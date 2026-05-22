# OpenCheck — development notes for Claude

## Architecture overview

- **Backend**: FastAPI, split into `backend/opencheck/routers/` (health, search, lookup, export).
- **Frontend**: React + Tailwind, split into `frontend/src/components/` (icons, risk, export, cdd).
- **Sources**: each adapter lives in `backend/opencheck/sources/<name>.py`, registered in `sources/__init__.py`.
- **BODS mapping**: each adapter has a corresponding `map_<name>()` function in `bods/mapper.py`, exported from `bods/__init__.py`.

---

## Critical rule: new source adapters require changes in TWO places in `routers/lookup.py`

`routers/lookup.py` contains **two independent derived-identifier blocks** — one for the synchronous `/lookup` endpoint and one for the SSE `/lookup-stream` endpoint. They must be kept in sync.

When wiring a new adapter:

1. **Sync `/lookup` derived block** (search for `# Build derived identifiers` in the first half of the file) — add the RA code check here.
2. **SSE `/lookup-stream` derived block** (search for `# Build derived identifiers (same logic as /lookup)`) — add the **identical** RA code check here.

Forgetting step 2 means the frontend (which uses `/lookup-stream`) will never dispatch to the adapter, even though the `/lookup` API endpoint works correctly. This is exactly what happened with Corporations Canada (fixed in commit `603c086`).

Similarly, the result-handling `elif source_id == "<name>":` block in the SSE event loop, and the `applicable_ids.append` / `_add_task` calls, must all be present in the stream path.

### Checklist for a new adapter

- [ ] `sources/<name>.py` — adapter class
- [ ] `sources/schemas/<name>.py` — Pydantic bundle schema
- [ ] `sources/__init__.py` — import + REGISTRY entry
- [ ] `bods/mapper.py` — `map_<name>()` function
- [ ] `bods/__init__.py` — import + `__all__` entry
- [ ] `routers/lookup.py` sync path — RA code → derived dict, dispatch (`_w1.append`), result handler
- [ ] `routers/lookup.py` SSE path — **same three things** as sync path
- [ ] `tests/test_<name>.py` — adapter + mapper tests
- [ ] `tests/test_sources.py` — add to expected registry set
- [ ] `tests/test_app.py` — add to expected sources endpoint set
- [ ] `.env` — API key if required (never committed)
- [ ] `README.md` + `ATTRIBUTIONS.md` — document the source

---

## Available skills

Two Cowork skills are available and should be used proactively:

- **`/beneficial-ownership-data`** — use for any questions about beneficial ownership data, policy, registers, the BODS standard, FATF, EU AML, GLEIF→BODS mapping, OpenOwnership, or BO data in procurement/extractives.
- **`/gleif-data`** — use for any questions about LEIs, the GLEIF registry, LEI issuers (LOUs), registration authorities, ownership relationships in GLEIF, or LEI statistics. Has live access to the GLEIF API and Statistics MCP servers.

---

## Other conventions

- API keys go in `.env` only — never committed to the repo.
- Schema files use `extra="allow"` via `_Base` so unknown API fields don't break validation.
- `validate_raw()` is called at the end of `fetch()` on the fully-assembled bundle, before returning.
- BODS interest type for **directors/managing officials** is `seniorManagingOfficial`, not `appointmentOfBoard`.
- `appointmentOfBoard` is for right-to-appoint-and-remove style ownership interests.
