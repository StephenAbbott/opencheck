# OpenCheck — Configuration

Copy `.env.example` to `.env` and fill in the keys you have. None are required to run the project — every adapter falls back to stubs without one.

| Variable | Purpose |
|----------|---------|
| `OPENCHECK_ALLOW_LIVE` | Master switch. `true` enables live HTTP calls for adapters whose key is set. |
| `OPENCHECK_CORS_ORIGIN` | CORS origin for the frontend dev server. |
| `OPENCHECK_PUBLIC_API_BASE` | Public origin of the API itself, used to build absolute `og:image` URLs on the `/share/{lei}` page (default `https://api.opencheck.world` — only override for non-production deployments). |
| `OPENCHECK_FRONTEND_ORIGIN` | Public origin of the frontend, used as the `/share/{lei}` redirect target and `og:url` (default `https://opencheck.world`). Separate from `OPENCHECK_CORS_ORIGIN`, which is a CORS policy value and may legitimately be `*`. |
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
| `OPENALEPH_API_KEY` | OpenAleph API key (optional — unlocks restricted collections **and enables the two POST steps** in the lookup cascade: the FtM `POST /api/2/match` step and the text-based percolation name step (`POST /api/2/beta/percolate`, OpenAleph 5.3.1). The flagship instance rejects anonymous POSTs to both paths, so without the key both steps are skipped and only the free-text name fallback runs). Set on Render as well as in `.env`. |
| `WIKIRATE_API_KEY` | Wikirate REST API key (effectively required for the `wikirate` ESG source — anonymous server-side requests are blocked by Wikirate's Cloudflare bot protection; the adapter skips silently without it). Sent as the `X-API-Key` header; rate limit 60 req/min. Set on Render as well as in `.env`. |
| `WIKIDATA_SPARQL_ENDPOINT` | Override the default Wikidata Query Service endpoint. |
| `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` | Comma-separated ISO codes added to the EU+EEA set used by `NON_EU_JURISDICTION` (e.g. `GB,CH`). |
| `OPENCHECK_AMLA_EU_EEA_OVERRIDE` | When set, replaces the EU+EEA default entirely. |
| `OPENCHECK_DATA_ROOT` | Override the cache root (used by tests; defaults to `./data`). |
| `OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE` | Process-wide budget for requests to `api.gleif.org` (default `50`). GLEIF rate-limits by IP at 60 req/min across *everything* this deployment sends it — anchor lookups, `/securities` ISINs, the Time Machine, subsidiary reveals — so OpenCheck keeps its own ceiling under GLEIF's and queues bursts instead of burning GLEIF's sliding window with 429s (which still count against it). `0` disables the throttle. |
| `OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S` | How long one GLEIF request may wait for a budget slot (default `15`) before giving up and taking the degradation path: stale cache, then the entity-pages Golden Copy snapshot, then a `503` with retry advice. |
| `OPENCHECK_GLEIF_SNAPSHOT_AFTER_S` | When the anchor LEI exists in the entity-pages Golden Copy, serve the snapshot if the live GLEIF fetch hasn't completed within this many seconds (default `5`) instead of sitting out the full throttle wait. Only applies when a snapshot row exists; `0` disables the early fallback. |
| `OPENCHECK_ENTITY_PAGES_DB_URL` | Where the backend downloads `entity_pages.sqlite(.gz)` — the `entity-pages-latest` GitHub release asset the monthly `refresh-entity-pages-db` workflow publishes. Since Phase 178 that file is the **GLEIF mirror**: the page columns plus every Level 1 field the BODS mapper reads (`detail_json`, deflated row-by-row against a shared dictionary held in `meta`), the RR relationship records and the REPEX reporting exceptions — ~1.8 GB on disk from a ~0.9 GB asset, inflated as it streams so the archive is never written to disk. Alone (no `_FILE`), the Phase 88 arrangement: downloaded into `/tmp` on every boot. With `_FILE` set as well (Phase 180, the persistent disk): downloaded to that path when the file is absent, and **replaced when the asset is not the one the file came from** — the asset's `Last-Modified` is stamped into the file as `meta.asset_last_modified` by the boot that downloaded it and compared on the next (Phase 181; a file without the stamp falls back to `meta.built_at` plus an hour's slack, because the workflow uploads a couple of minutes *after* the build finishes and Phase 180's plain "newer than the build" re-downloaded the same asset on every deploy) — so the monthly rebuild lands on a disk that survives deploys; when the asset cannot be checked the file is kept. |
| `OPENCHECK_ENTITY_PAGES_DB_FILE` | A local `entity_pages.sqlite`. On Render (Phase 180) this is `/var/data/entity_pages.sqlite` on the 10 GB persistent disk `render.yaml` declares — paid instance types only; a service with a disk runs as a single instance and a deploy costs a few seconds of downtime ([Render disks](https://render.com/docs/disks)). Locally, build one with `scripts/build_entity_pages_db.py --out …` (full: ~12 min on a 2-vCPU machine, and 4.8 GB of scratch before the compression pass and `VACUUM` bring it to 1.8 GB) or `--sample 50000` for a small one. A Phase 88 (v1) file still opens; the mirror pieces read as "not held". The file is read through once at boot so its pages are in the OS cache before the first lookup. |
| `OPENCHECK_MIRROR_REFRESH_INTERVAL_S` | Phase 180. How often the in-process refresh (`opencheck/mirror_refresh.py`) polls GLEIF's Golden Copy publish API and applies the smallest delta covering the gap between the mirror's watermark (`meta.source_publish_datetime`) and the latest publish: IntraDay (≤ 8 h), LastDay (≤ 1 d), LastWeek (≤ 7 d), LastMonth (≤ 31 d); a wider gap re-downloads the release asset. Default `3600`; `0` disables. Runs only on a Phase 178 mirror (a v1 file waits for a full asset), applies the three delta files with the same code as the full build (`mirror_build`), writes the watermark last, and never advances it on failure — the monthly full rebuild remains the self-heal. State on `GET /mirror` (`refresh`). |
| `OPENCHECK_PSC_GRAPH_DB_FILE` | Phase 186. A local `psc_graph.sqlite` — every active Companies House PSC record, keyed for a local corporate-PSC chain walk (`opencheck/psc_graph.py`). On Render it is `/var/data/psc_graph.sqlite` on the persistent disk (2.2 GB beside the 1.8 GB mirror). Built daily from the register's own snapshot by `refresh-psc-graph.yml` and published as the `psc-graph-latest` release asset; locally, `scripts/build_psc_graph.py --out …` (about seven minutes; the 2.2 GB zip is streamed, never written). Unset = no graph: the Companies House adapter walks the register live. State on `GET /pscgraph`. |
| `OPENCHECK_PSC_GRAPH_DB_URL` | Where the file is downloaded from when absent and replaced from when the asset is not the one on disk (the Phase 181 rule, on this file's own `meta`). Default: the `psc-graph-latest` release asset. Read only when a file path is configured — a 2.2 GB file belongs on the disk, not in `/tmp` on every boot. Empty disables the download. |
| `OPENCHECK_PSC_GRAPH_REFRESH_INTERVAL_S` | How often the asset check re-runs in-process so the daily seed lands without a deploy (default `21600`, six hours; `0` disables the loop — boot still checks once). Outcomes tallied on `GET /pscgraph` (`refresh`). |
| `COMPANIES_HOUSE_STREAM_KEY` | Phase 187. The Companies House *streaming* API key — a different credential from `COMPANIES_HOUSE_API_KEY`, one connection per key (bods-stream holds its own). With `OPENCHECK_PSC_GRAPH_DB_FILE` set, the app keeps one connection to `stream.companieshouse.gov.uk/persons-with-significant-control` and applies every change to the local graph as the register publishes it (`opencheck/psc_stream.py`): a changed record is upserted, a ceased one dated, a deleted one removed; the cursor (`meta.stream_timepoint`) and the register's clock (`stream_published_at`) are written after every batch, so a restart resumes where it left off. `416` (cursor too old) resumes live and records a gap the next daily seed closes; `429` waits a minute; `401` stops. Counters on `GET /pscgraph` (`stream`). Unset = no stream; the graph stays as seeded. |
| `OPENCHECK_PSC_STREAM_ENABLED` | `false` keeps the consumer off even with a key and a file (default `true`). |
| `OPENCHECK_CH_GRAPH_FIRST` | Phase 188. `true` finds the corporate-PSC chain above a UK subject on the local graph first: one index walk instead of a hop-by-hop crawl, then Companies House is asked for the subject and every company the graph named **at once** (six in flight), so the register still describes every company exactly as before and only the serial dependency between hops is gone — ASDA's nine-company chain is one round-trip rather than six. The graph is a proposal, never the answer: the walk that follows reads the register's own PSC lists and follows those, so a company the graph did not know is still fetched live (counted `missed` on `/signalstats`) and one the graph named that the register's chain does not reach is a wasted prefetch (`extra`). Both numbers, and the graph's dates, ride on the bundle's `chain_source` — **but only when the graph was actually asked**; a chain walked live carries `source` and `related` and nothing else, because there is no proposal to score. No graph, an unreadable one, or a failed prefetch falls back to the walk of Phase 177 — the flag can never change the answer, only how fast it arrives. Needs `OPENCHECK_PSC_GRAPH_DB_FILE`. Ships `false`; **the comparison it was waiting on was run on 2026-09-09 and passed** — on the curated UK examples (ASDA 9 companies, Vosper 6, JCB 1, Babcock 0) the graph proposed exactly the chain the register returned, `missed 0` and `extra 0` on every one, and the same subject's BODS output was statement-for-statement identical with the flag off and on. Read the counters (`/signalstats.companies_house_walks.chain` and `.graph`) before turning it on anywhere new: `missed` is how far behind the register the local copy is running, `extra` the cost of a stale seed. Source cards say when a chain came this way, with the graph's snapshot date and stream watermark. |
| `OPENCHECK_GLEIF_MIRROR_FIRST` | Phase 179. `true` serves the GLEIF anchor — Level 1 record, direct and ultimate parents or the reporting exceptions filed in their place, the first page of children — and the subsidiary network from the entity-pages **mirror** first, and goes live only for an LEI the mirror lacks (default `false`: the Phase 143 order, live → stale cache → snapshot). Needs a Phase 178 file; a v1 file is never served first. A mirror hit still spends **one** live call, the Level 1 record (cached seven days), for the mapping-file ids the Golden Copy does not carry (`ocid`, `spglobal`, `bic`, `mic`, `qcc`) — skipped when the throttle has no headroom or GLEIF does not answer within `OPENCHECK_GLEIF_SNAPSHOT_AFTER_S`, in which case the anchor is served without them. Search, national-ID resolution and `/field-modifications` stay live. Read `/mirror` for the hit rate and the calls saved before flipping this on in production. |
| `OPENCHECK_GLEIF_LIVE_CONFIRM` | Phase 179, measurement only. `true` compares the live Level 1 record the top-up fetched with the mirror's on every field the BODS mapper reads and counts agreement on `/mirror` (`confirm.same` / `confirm.differs`, plus the differing field names). Never changes what is served; costs nothing beyond the top-up. Default `false`. |
| `OPENCHECK_SOURCE_HEALTH_URL` | Where `GET /source-health` reads the weekly sweep's `source-health.json` (default: the `source-health-latest` GitHub release asset the sweep uploads; `source-health-history.json` is read from beside it). Refreshed at most hourly; served stale, and marked so, when the asset cannot be re-read. Set empty to switch the sources page's health strip off. |
| `OPENCHECK_SOURCE_HEALTH_FILE` | A local `source-health.json` instead of the URL (a developer reading their own sweep; tests). Wins over the URL when both are set. |
| `OPENCHECK_CH_PSC_MAX_DEPTH` | How many hops of UK corporate-PSC chain the Companies House adapter follows above the subject (default `6`; each hop is four register calls, within the 600 requests / 5 min per key). A corporate PSC filed with a UK registration number is followed after the number is normalised to the canonical eight-character form — PSC filings routinely drop leading zeros (`2999029` for `02999029`). Corporate PSCs that are not followed (non-UK register, an unparseable number, the depth cap, a fetch failure) are listed on the bundle as `unfollowed_pscs` with a reason, and the UK-side reasons are reported as degradation. |
| `OPENCHECK_BOT_GATE_LOOKUP_STREAM` | Refuse declared automated clients (bot User-Agents) on `/lookup-stream` with a `403` pointing at `/lookup` and the `/entity` pages (default `true`). The plain `/lookup` JSON API is never gated — `python`/`curl` UAs are its legitimate callers. |
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
4. Once the backend service is live, copy its URL (e.g. `https://api.opencheck.world`) and set it as `VITE_API_BASE_URL` on the frontend static site, then trigger a redeploy of the frontend.

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
