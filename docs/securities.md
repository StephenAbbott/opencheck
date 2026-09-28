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

## GLEIF's ISIN-to-LEI file answers first (Phase 258)

GLEIF publishes its complete ISIN-to-LEI mapping every day as a keyless zip
(`https://mapping.gleif.org/api/v2/isin-lei/latest` names the newest file).
OpenCheck holds it as a local SQLite table (`opencheck/isin_index.py`) and
`/securities` reads that before calling GLEIF:

- **An LEI the file lists** is paged locally. The count is one indexed read
  (`counts`), the page a slice of at most two zlib chunks of 500 ISINs
  (`chunks`), so an issuer with 636,388 ISINs costs the same as one with 3.
- **An LEI the file does not list** is answered `total: 0` with no call. On
  28 Sept 2026 the file held 9,150,433 rows across 98,677 LEIs — 2.9% of the
  ~3.4 million GLEIF has issued — so this is the commonest answer.
- **ISIN order** (Stephen, 28 Sept 2026). GLEIF's API pages its ISINs in no
  stable order (Shell's page 1 opens `US82266MXH68…`); the table's are
  sorted, so page 2 today is page 2 tomorrow and a saved report's page reads
  the same. The first 20 shown therefore differ from GLEIF's own page 1.
- **Dated by the file**: `isin_list_source: "gleif_file"` and
  `isin_list_as_of` = the file's `uploadedAt`; the panel says "From GLEIF's
  ISIN-to-LEI file of 28 Sept 2026, listed in ISIN order."
- **Built on the server**, never committed and never a release asset: at boot
  when the file is absent, and whenever GLEIF names a newer file (checked
  every `OPENCHECK_ISIN_INDEX_REFRESH_INTERVAL_S`, six hours; fifteen minutes
  after a failed check). A build takes ~40 s and ~50 MB of memory and writes
  ~90 MB of SQLite; it is written beside the old table and swapped in, so a
  failed build keeps the old one. On Render the file is
  `/var/data/isin_lei.sqlite` (`OPENCHECK_ISIN_INDEX_DB_FILE`).
- **Never an old count**: a table whose file is older than
  `OPENCHECK_ISIN_INDEX_MAX_AGE_DAYS` (3) is not used, and `/securities`
  falls back to the cached live call below. Parity with live GLEIF was exact
  for every LEI checked (Shell 1,813; `529900W18LQJJN6SJ336` 636,388;
  `549300TS3U4JKMR1B479` 531,250; every Shell page against the sorted list).

`/signalstats` → `gleif.isin_index` reports the table (file, age, rows, last
sync outcome); `gleif.securities_served` counts how each `/securities`
answer was served (file / cache / live / stale / unavailable by reason).

## When GLEIF cannot be asked (Phases 145 and 253)

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
