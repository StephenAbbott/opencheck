# Securities (ISINs) panel

The entity-level **Securities** section shows the ISINs linked to a company's
LEI and surfaces any that are sanctioned. It combines three open datasets, each
in the role it's good at:

| Source | Role | Licence |
|---|---|---|
| GLEIF `/lei-records/{lei}/isins` | Authoritative LEI→ISIN list + **total count**. Fetch count + one page only (Deutsche Bank ≈ 22,500 ISINs). | CC0 |
| OpenFIGI `/v3/mapping` | Type the handful of ISINs we actually display (security type, name, ticker, exchange). | Open (FIGI standard) |
| OpenSanctions `securities.csv` | The **sanctioned subset** (LEI → sanctioned ISINs + regime), incl. EO 14071 investment bans. | CC-BY-NC 4.0 |

Since Phase 236 the panel also opens with the company's **primary listing**
from LSEG PermID (venue, ticker and a verified venue link), read from the
lookup's frozen `listing` event rather than fetched here — see
[listing.md](listing.md).

`GET /securities?lei=&page=` assembles these lazily (the frontend fetches it only
when the section renders) and never enumerates every ISIN.

## When GLEIF cannot be asked (Phases 145 and 252)

The GLEIF call is *discretionary* (Phase 234): when fewer than
`OPENCHECK_GLEIF_LOOKUP_RESERVE` slots of the per-minute budget are left,
OpenCheck refuses it without sending anything so lookups keep their anchor
calls. GLEIF itself can also refuse (a 429) or not answer. None of these takes
the panel down — the sanctioned overlay is a local index and always runs.

Since Phase 253 each ISIN page is **cached**, keyed on LEI + page + page size:

| Age of the cached page | What `/securities` does |
|---|---|
| ≤ 1 day (`ISINS_FRESH_DAYS`) | Serves it; GLEIF is not asked. |
| older | Asks GLEIF; on success, replaces the entry. |
| ≤ 30 days (`ISINS_STALE_MAX_DAYS`) and GLEIF cannot be asked | Serves it with `isin_list_stale: true`, dated by `isin_list_as_of`. |
| no usable entry and GLEIF cannot be asked | `isin_list_available: false`, count and page empty. |

A zero-ISIN answer is cached like any other — most LEIs (about 97% in GLEIF's
own ISIN-to-LEI file, 28 Sept 2026) have none, and those are the answers least
worth re-asking for. A failed answer is never cached.

`isin_list_unavailable_reason` says why GLEIF could not be asked, and the panel
says it in words (`frontend/src/lib/securities.ts`):

| Reason | Meaning |
|---|---|
| `held_for_lookups` | OpenCheck kept its last GLEIF slots for lookups. **Nothing was sent to GLEIF.** |
| `rate_limited` | GLEIF answered 429 (or did recently — the penalty box), or the process budget ran out. |
| `unreachable` | GLEIF did not answer (timeout, connection error, 5xx). |

Before Phase 253 the panel said "GLEIF is rate-limiting or unreachable" in all
three cases, including the first, where GLEIF was never asked.

## Sanctioned overlay — bulk index

OpenSanctions has **no live "sanctioned securities by LEI" API** — that
`securities` collection is a packaging of a bulk CSV export. So the overlay reads
a local index built from that CSV.

Build it (a few hundred KB; most sanctioned companies are private with no
LEI/ISINs, so the index is a small fraction of the 8.8 MB source):

```bash
cd backend
python scripts/extract_securities.py --output ../data/securities/sanctioned_isins.json
# or from a local copy:
python scripts/extract_securities.py --input securities.csv --output ../data/securities/sanctioned_isins.json
```

Then point the service at it:

```
OPENCHECK_SECURITIES_INDEX_FILE=/abs/path/to/data/securities/sanctioned_isins.json
OPENFIGI_API_KEY=...        # optional — raises the OpenFIGI rate limit
```

When neither variable is set, the panel runs on GLEIF + OpenFIGI alone (no
sanctioned banner). The index is **not committed** (CC-BY-NC; see `.gitignore`);
rebuild it periodically to stay current with sanctions updates.

## File vs URL

The index can be loaded from a local file **or** a URL — set whichever suits the
host (the file wins if both are set):

```
OPENCHECK_SECURITIES_INDEX_FILE=/abs/path/sanctioned_isins.json   # local / bundled
OPENCHECK_SECURITIES_INDEX_URL=https://…/sanctioned_isins.json    # GitHub raw / release / S3
```

On **Render** (ephemeral filesystem) the URL form is preferred: host the JSON
(it's small — ~187 LEIs / ~13k ISINs, a few hundred KB) and the backend
downloads it once at startup (off the event loop), so you can refresh the index
by re-uploading the file with no image rebuild. The local-file form requires a
`COPY` into the Docker image.

Licensing reminder: the index is derived from OpenSanctions (CC-BY-NC) — hosting
it publicly is redistribution, fine for OpenCheck's non-commercial use **with
attribution**. A private release asset or your own S3 bucket avoids the question.
