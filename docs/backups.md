# Backups of OpenCheck's own SQLite files (Phase 260)

OpenCheck writes two files it promises to keep:

| File | Setting | Why it matters |
|---|---|---|
| `watchlist.sqlite` | `OPENCHECK_WATCHLIST_DB_FILE` | Watched companies, their baselines and the change log a feed reader follows |
| `saved_reports.sqlite` | `OPENCHECK_SAVED_REPORTS_DB_FILE` | Saved reports — records a reader can re-verify by SHA-256 — and the live disposition sheets |

Both sit on Render's one persistent disk. Everything else on that disk (the
GLEIF mirror, the PSC graph, MEIP, the ISIN table) can be rebuilt from its
publisher; these two cannot. Since Phase 260 each is backed up once a day,
off the host.

## Where the backups go, and why there

To an asset of one release (`sqlite-backups`) on a **private** GitHub
repository. Not the public `StephenAbbott/opencheck` releases the rebuildable
artefacts use: a saved report is an unlisted capability, and a watchlist says
what someone is watching. So, three layers:

1. **The repository must be private.** The task asks GitHub before every
   upload and refuses — recording the error, uploading nothing — when the
   answer is not `private: true`.
2. **The file is encrypted** — AES-256-GCM in 1 MiB chunks, key derived from
   `OPENCHECK_BACKUP_PASSPHRASE` by scrypt — so a leaked token, a repository
   later made public or a mis-shared asset does not leak the contents.
3. **The token can do nothing else**: a fine-grained token with Contents
   read/write on that one repository.

## Setting it up

1. Create a **private** repository, e.g. `StephenAbbott/opencheck-backups`,
   with a README (a release needs a commit to tag).
2. Create a fine-grained personal access token: *Repository access → Only
   select repositories →* that repository; *Permissions → Contents: Read and
   write*. Nothing else.
3. Choose a long passphrase and **store it somewhere other than Render** (a
   password manager). Without it no backup restores.
4. Set on Render: `OPENCHECK_BACKUP_GITHUB_REPO`, `OPENCHECK_BACKUP_GITHUB_TOKEN`,
   `OPENCHECK_BACKUP_PASSPHRASE`. Redeploy.
5. Ten minutes after boot the task makes its first check. Read
   `GET /watchstats` → `backups.files.*.last_ok_at` (or `last_error`), or take
   one at once from the Render shell:
   `cd backend && uv run python scripts/restore_backup.py backup-now`.

## How it works

`opencheck/backups.py`, started from the app's lifespan when all three
settings are set:

- **Hourly, it asks the release whether a backup is due** — the newest asset
  for a file older than `OPENCHECK_BACKUP_INTERVAL_S` (a day). Reading "due"
  off the release, not a timer in memory, means a deploy (and Render deploys
  often) neither resets the schedule nor skips it.
- **The copy is SQLite's online backup API**, consistent while the watcher,
  the mirror-refresh hook and request handlers write, WAL included; switched
  to a standalone journal and `PRAGMA integrity_check`-ed.
- **It is encrypted, then decrypted again and re-checked before upload** — a
  backup that does not restore is not one — and uploaded as
  `watchlist-20260928T031500Z.sqlite.gz.enc`, labelled with its schema
  version. The newest `OPENCHECK_BACKUP_KEEP` (14) per file are kept.
- All work is on a thread and streamed through temporary files, so neither
  the event loop nor memory carries the file.
- A failure is logged and recorded per file on `/watchstats` and never
  raises; the other file still gets its turn, and the next hourly check tries
  again.

`/watchstats` → `backups` carries file names, dates, sizes, schema versions
and errors — never the repository, the token or an asset URL.

## Restoring

`backend/scripts/restore_backup.py`, with the same three settings in the
environment or `backend/.env`:

```
cd ~/code/opencheck/backend
uv run python scripts/restore_backup.py list
uv run python scripts/restore_backup.py restore watchlist --out /tmp/watchlist.sqlite
uv run python scripts/restore_backup.py restore saved_reports --asset <name> --out /tmp/saved_reports.sqlite
uv run python scripts/restore_backup.py restore-file ./x.sqlite.gz.enc --out ./x.sqlite
```

A restore decrypts to a temporary file beside `--out`, integrity-checks it
and only then renames it into place; it never replaces an existing file
without `--force`. A wrong passphrase, a flipped bit or a truncated file
fails to decrypt rather than restoring short — every chunk is sealed with its
position and the last one says it is last.

To put a restored file into service: stop the service, copy it over the path
in the setting (delete any `-wal` / `-shm` beside the old one), start it.

## Schema versions

Both files carry `PRAGMA user_version` (`opencheck/sqlite_schema.py`).
Version 1 is the schema as it first shipped (Phases 215 and 216), so a file
from before Phase 260 is stamped 1 on open and nothing else changes. A
later change appends a `Migration` to the store's `MIGRATIONS`; the missing
steps run in one `BEGIN IMMEDIATE` transaction. A file at a version *newer*
than the running build — a rollback — is refused, and `/watch` and
`/saved-reports` answer 503 saying so, rather than an older build writing
rows a newer schema reads differently. A restored backup carries its version,
so the same rule protects a restore from the wrong build.
