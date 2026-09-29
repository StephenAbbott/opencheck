"""Restore, list or take an OpenCheck SQLite backup (Phase 260).

The API backs up ``watchlist.sqlite`` and ``saved_reports.sqlite`` once a day
to a release on a private GitHub repository (``opencheck/backups.py``). This
script is the other half.

List what the release holds::

    uv run python scripts/restore_backup.py list

Restore the newest watchlist backup to a new file (never overwrites without
``--force``; the file is integrity-checked before it is put in place)::

    uv run python scripts/restore_backup.py restore watchlist --out /tmp/watchlist.sqlite

Restore a specific asset, or a file you already downloaded::

    uv run python scripts/restore_backup.py restore saved_reports \\
        --asset saved_reports-20260928T031500Z.sqlite.gz.enc --out ./saved_reports.sqlite
    uv run python scripts/restore_backup.py restore-file ./x.sqlite.gz.enc --out ./x.sqlite

Take a backup now, whether or not one is due (on the Render shell)::

    uv run python scripts/restore_backup.py backup-now

It reads ``OPENCHECK_BACKUP_GITHUB_REPO``, ``OPENCHECK_BACKUP_GITHUB_TOKEN``,
``OPENCHECK_BACKUP_PASSPHRASE`` and ``OPENCHECK_BACKUP_RELEASE_TAG`` from the
environment or ``backend/.env``, as the app does. To put a restored file into
service, stop the service, copy it over the path in
``OPENCHECK_WATCHLIST_DB_FILE`` / ``OPENCHECK_SAVED_REPORTS_DB_FILE`` (removing
any ``-wal`` / ``-shm`` beside it) and start it again.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from opencheck import backups  # noqa: E402
from opencheck.config import get_settings  # noqa: E402


def _store() -> backups.GitHubReleaseStore:
    s = get_settings()
    if not (s.backup_github_repo and s.backup_github_token):
        sys.exit("Set OPENCHECK_BACKUP_GITHUB_REPO and OPENCHECK_BACKUP_GITHUB_TOKEN.")
    return backups.GitHubReleaseStore(s.backup_github_repo, s.backup_github_token, s.backup_release_tag)


def _passphrase() -> str:
    p = get_settings().backup_passphrase_secret
    if not p:
        sys.exit("Set OPENCHECK_BACKUP_PASSPHRASE.")
    return p


def cmd_list(_: argparse.Namespace) -> int:
    store = _store()
    try:
        for a in store.assets():
            parsed = backups.parse_asset_name(a.get("name", ""))
            if parsed:
                print(f"{a['name']}\t{a.get('size')} bytes\t{a.get('label') or ''}")
    finally:
        store.close()
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    store = _store()
    try:
        found = store.backups(args.file)
        if args.asset:
            found = [a for a in found if a["name"] == args.asset]
        if not found:
            sys.exit(f"No backup of {args.file!r}" + (f" named {args.asset}" if args.asset else "") + ".")
        asset = found[-1]
        tmp = Path(args.out).with_name(Path(args.out).name + ".download")
        with tmp.open("wb") as fh:
            store.download(asset["id"], fh)
    finally:
        store.close()
    try:
        with tmp.open("rb") as fh:
            version = backups.restore(fh, Path(args.out), _passphrase(), force=args.force)
    finally:
        tmp.unlink(missing_ok=True)
    print(f"Restored {asset['name']} to {args.out} (schema v{version}, integrity ok).")
    return 0


def cmd_restore_file(args: argparse.Namespace) -> int:
    with Path(args.path).open("rb") as fh:
        version = backups.restore(fh, Path(args.out), _passphrase(), force=args.force)
    print(f"Restored {args.path} to {args.out} (schema v{version}, integrity ok).")
    return 0


def cmd_backup_now(_: argparse.Namespace) -> int:
    results = backups.run_once(force=True)
    print(json.dumps(results, indent=2))
    if not results:
        print("Nothing ran: backups are not configured, or neither file exists here.", file=sys.stderr)
        return 1
    return 1 if any(r.get("action") == "failed" for r in results) else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="list the backups on the release").set_defaults(func=cmd_list)
    r = sub.add_parser("restore", help="download and restore a backup")
    r.add_argument("file", choices=["watchlist", "saved_reports"])
    r.add_argument("--asset", help="a specific asset name (default: the newest)")
    r.add_argument("--out", required=True)
    r.add_argument("--force", action="store_true", help="replace --out if it exists")
    r.set_defaults(func=cmd_restore)
    rf = sub.add_parser("restore-file", help="restore a backup file already on disk")
    rf.add_argument("path")
    rf.add_argument("--out", required=True)
    rf.add_argument("--force", action="store_true")
    rf.set_defaults(func=cmd_restore_file)
    sub.add_parser("backup-now", help="back both files up now").set_defaults(func=cmd_backup_now)
    args = ap.parse_args(argv)
    try:
        return args.func(args)
    except backups.BackupError as exc:
        sys.exit(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
