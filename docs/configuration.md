# OpenCheck — Configuration

Copy `.env.example` to `.env` and fill in the keys you have. None are required to run the project — every adapter falls back to stubs without one.

| Variable | Purpose |
|----------|---------|
| `OPENCHECK_ALLOW_LIVE` | Master switch. `true` enables live HTTP calls for adapters whose key is set. |
| `OPENCHECK_CORS_ORIGIN` | CORS origin for the frontend dev server. |
| `COMPANIES_HOUSE_API_KEY` | UK Companies House API key (free; <https://developer.company-information.service.gov.uk/>). |
| `INPI_USERNAME` | INPI (France) API username for the Registre National des Entreprises. |
| `INPI_PASSWORD` | INPI (France) API password. |
| `KVK_API_KEY` | KvK (Netherlands) Handelsregister API key. |
| `BOLAGSVERKET_API_KEY` | Bolagsverket (Sweden) API key for the company information portal. |
| `ZEFIX_USERNAME` | Zefix (Switzerland) API username. |
| `ZEFIX_PASSWORD` | Zefix (Switzerland) API password. |
| `OPENCORPORATES_API_KEY` | OpenCorporates API key — unlocks live company + officer data via the OC REST API. |
| `OPENCORPORATES_RELATIONSHIPS_FILE` | Path to the OC Relationships bulk CSV file. When set, network relationship data is read from this file instead of the live `/network` API endpoint (which requires a premium tier). |
| `BCE_BELGIUM_DB_FILE` | Path to the SQLite database built by `scripts/extract_bce.py`. When set, the BCE Belgium adapter provides enterprise-number-keyed lookup (via GLEIF bridge) and FTS5 name search for Belgian entities from the monthly KBO open data ZIP. |
| `ARIREGISTER_USERNAME` | Username for the Estonian e-Business Register SOAP/XML API (`ariregxmlv6.rik.ee`). Free RIK contract credentials. |
| `ARIREGISTER_PASSWORD` | Password for the Estonian e-Business Register SOAP/XML API. |
| `BRIGHTQUERY_DB_FILE` | Path to the SQLite database built by `scripts/extract_brightquery.py`. When set, the BrightQuery adapter provides LEI-keyed lookup of US entities and their executives from OpenData.org bulk data. |
| `CORPORATIONS_CANADA_API_KEY` | API key for the ISED Corporations Canada API Gateway. |
| `FIRMENBUCH_API_KEY` | Free API key for the Austrian Firmenbuch (Justiz Online) SOAP service. |
| `OPENSANCTIONS_API_KEY` | OpenSanctions API key (also unlocks the EveryPolitician PEPs dataset). |
| `OPENALEPH_API_KEY` | OpenAleph API key (optional — unlocks restricted collections). |
| `WIKIDATA_SPARQL_ENDPOINT` | Override the default Wikidata Query Service endpoint. |
| `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` | Comma-separated ISO codes added to the EU+EEA set used by `NON_EU_JURISDICTION` (e.g. `GB,CH`). |
| `OPENCHECK_AMLA_EU_EEA_OVERRIDE` | When set, replaces the EU+EEA default entirely. |
| `OPENCHECK_DATA_ROOT` | Override the cache root (used by tests; defaults to `./data`). |
| `ANTHROPIC_API_KEY` | Optional — reserved for future intent extraction / phrasing. |

## Deployment on Render

A `render.yaml` blueprint is included for one-click deployment to [Render](https://render.com):

1. Push the repo to GitHub.
2. In the Render dashboard → **New → Blueprint**, point at the repo. Render creates both services automatically.
3. Set the secret env vars in the Render dashboard (under each service's **Environment** tab):
   - `COMPANIES_HOUSE_API_KEY`
   - `OPENCORPORATES_API_KEY`
   - `OPENSANCTIONS_API_KEY`
   - `OPENALEPH_API_KEY` (optional)
4. Once the backend service is live, copy its URL (e.g. `https://opencheck-api.onrender.com`) and set it as `VITE_API_BASE_URL` on the frontend static site, then trigger a redeploy of the frontend.

The backend runs as a Docker Web Service (uvicorn + Python 3.11); the frontend builds as a Render Static Site (Vite). Both use the free tier. The backend image bundles the pre-extracted BODS demo fixtures from `data/cache/` so the demo subjects work without a mounted volume.

## Tests

```bash
cd backend
uv run pytest             # 913 tests, ~6s
```

Frontend type check:

```bash
cd frontend
npm run build             # tsc + vite build
```

The backend tests use [`pytest-httpx`](https://github.com/Colin-b/pytest_httpx) to mock every live HTTP call, so the suite runs offline. Test files mirror the adapter / endpoint structure: `test_companies_house_live.py`, `test_gleif_live.py`, `test_lookup_endpoint.py`, `test_export_endpoint.py`, etc.
