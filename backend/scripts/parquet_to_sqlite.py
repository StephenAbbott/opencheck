"""Build the SQLite dump that ``extract_bods_subgraphs.py`` expects, directly
from the parquet files produced by ``setup_bods_data.py``.

This avoids downloading Open Ownership's separate SQLite dumps
(``gleif_version_0_4.db`` / ``uk_version_0_4.db``) — the parquet you already
built contains the full BODS statement data; this just transcodes it into the
table shape the subgraph walker queries.

Every ``*.parquet`` file in the source directory becomes a SQLite table named
after the file (e.g. ``entity_statement.parquet`` -> table ``entity_statement``).
SQLite identifiers are case-insensitive, so the walker's camelCase table names
(``entity_recordDetails_identifiers``) resolve to the lowercase tables created
here.

Usage::

    python scripts/parquet_to_sqlite.py \\
      --parquet-dir data/bods/gleif/parquet \\
      --output data/bods/gleif/gleif_version_0_4.db

    python scripts/parquet_to_sqlite.py \\
      --parquet-dir data/bods/uk_psc/parquet \\
      --output data/bods/uk_psc/uk_version_0_4.db

Then point the walker at the result::

    python scripts/extract_bods_subgraphs.py \\
      --gleif data/bods/gleif/gleif_version_0_4.db \\
      --uk data/bods/uk_psc/uk_version_0_4.db \\
      --leis 213800E11LI1SCETU492 --max-hops 3
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover
    print("ERROR: duckdb is required.  Run: pip install duckdb", file=sys.stderr)
    raise SystemExit(1)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parquet-dir", required=True, type=Path, help="Directory of *.parquet files")
    ap.add_argument("--output", required=True, type=Path, help="SQLite db to (re)create")
    args = ap.parse_args(argv)

    parquet_dir: Path = args.parquet_dir.expanduser()
    if not parquet_dir.is_dir():
        print(f"Parquet directory not found: {parquet_dir}", file=sys.stderr)
        return 1
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        print(f"No .parquet files in {parquet_dir}", file=sys.stderr)
        return 1

    out: Path = args.output.expanduser()
    if out.exists():
        out.unlink()
    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{out}' AS db (TYPE SQLITE);")

    t0 = time.time()
    for f in files:
        table = f.stem  # filename without .parquet
        print(f"  {table} … ", end="", flush=True)
        ti = time.time()

        # The OO parquet stores BODS field names in camelCase (statementId,
        # recordId, directOrIndirect, …). extract_bods_subgraphs.py uses plain
        # sqlite3 + SELECT * and looks up lowercase keys (row["statementid"]),
        # so we lower-case every column name on the way into SQLite.
        con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(f)])
        cols = [d[0] for d in con.description]
        select_list = ", ".join(f'"{c}" AS "{c.lower()}"' for c in cols)
        con.execute(
            f'CREATE TABLE db."{table}" AS SELECT {select_list} FROM read_parquet(?)',
            [str(f)],
        )
        n = con.execute(f'SELECT count(*) FROM db."{table}"').fetchone()[0]
        print(f"{n:,} rows ({time.time() - ti:.1f}s)")

    con.close()
    print(f"Done -> {out}  ({time.time() - t0:.1f}s total)")
    print("Note: the walker will build its own indexes on first run (a few minutes).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
