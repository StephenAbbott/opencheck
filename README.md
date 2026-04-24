# OpenCheck

A chatbot-style corporate intelligence tool built on open data. OpenCheck looks up companies and people in UK Companies House, GLEIF, OpenSanctions, OpenAleph, EveryPolitician, and Wikidata; produces a useful intelligence report with clearly attributed sources; maps any ownership it finds to the Beneficial Ownership Data Standard (BODS) v0.4; and lets you download your results in the format of your choice via a family of BODS adapters.

> **Status:** early scaffolding. See [`docs/plan.md`](docs/plan.md) for the project plan.

## Requirements

- **Docker** (with Compose v2) — for the one-command dev experience
- **Python 3.11+** — if running the backend outside Docker
- **Node 20+** — if running the frontend outside Docker
- **uv** — Python package manager (<https://docs.astral.sh/uv/>). `pip install uv` if you need it
- API keys (see `.env.example`) — not required for Phase 0 stubs, will be for Phase 1

## Running the dev stack

```bash
cp .env.example .env
docker compose up --build
```

Once the services are up:

- Backend: <http://localhost:8000>
  - Health: <http://localhost:8000/health>
  - Source inventory: <http://localhost:8000/sources>
  - Stub search: `curl 'http://localhost:8000/search?q=rosneft&kind=entity'`
  - OpenAPI docs: <http://localhost:8000/docs>
- Frontend: <http://localhost:5173>

## Running each side locally

Backend:

```bash
cd backend
uv sync
uv run uvicorn opencheck.app:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

## Running tests

```bash
cd backend
uv run pytest
```

## Project structure

```
opencheck/
  backend/              FastAPI app, source adapters, BODS mapper, exports
    opencheck/
      app.py            FastAPI entry
      sources/          One module per data source
      bods/             (phase 1+) BODS mapper and validator
      reports/          (phase 2+) report composer and risk rules
      exports/          (phase 4+) thin wrappers over Stephen's BODS adapters
  frontend/             React + TypeScript + Vite + Tailwind
  docs/                 Project plan and design notes
  data/
    cache/
      demos/            Curated demo corpus (checked in)
      live/             Runtime cache (gitignored)
  ATTRIBUTIONS.md       Source licensing and attribution
```

## Licensing and attribution

OpenCheck's own code is MIT-licensed (see [`LICENSE`](LICENSE)). The data OpenCheck retrieves from third-party sources is licensed under each source's own terms — see [`ATTRIBUTIONS.md`](ATTRIBUTIONS.md). Downloaded exports include a per-file `LICENSE_NOTICE.md` listing every source that contributed data.

## Related projects

- [Beneficial Ownership Data Standard (BODS) v0.4](https://standard.openownership.org/en/0.4.0/)
- [GODIN — Global Open Data Integration Network](https://godin.gleif.org/)
- Stephen Abbott Pugh's BODS adapter repos at <https://github.com/StephenAbbott>
