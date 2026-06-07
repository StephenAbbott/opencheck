"""Phase 7 — Neo4j CSV export for the OpenCheck demo graph.

Usage (from repo root):

    make export-neo4j

Or manually:

    cd backend && python scripts/export_neo4j.py [--demo-dir ../data/demo] [--out ../data/demo/neo4j]

What it does
------------
1. Reads all *.jsonl files in data/demo/ (the 9 Phase-0 anchor entities built
   by ``make build-demo``).
2. Deduplicates statements by statementId (4 cross-entity duplicates exist in
   the current demo set).
3. Writes a single combined JSONL file (data/demo/all_demo.jsonl) for
   inspection / archiving.
4. Calls ``bods-neo4j to-csv`` on the combined file to produce:
     data/demo/neo4j/
       entity.csv, person.csv, identifier.csv, address.csv, country.csv,
       unspecified_party.csv
       owns.csv, controls.csv, manages.csv, is_party_to.csv,
       has_other_interest.csv, has_identifier.csv, has_address.csv,
       located_in.csv, registered_in.csv, born_in.csv
       import.cypher   (LOAD CSV script for a running Neo4j instance)
       import.sh       (cypher-shell wrapper)

The import.cypher file can be run against a Neo4j 5.x instance started with:

    docker compose up -d   # from the bods-neo4j repo, or any Neo4j 5.x instance

Exit codes: 0 = success, 1 = error.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DEMO_DIR = _REPO_ROOT / "data" / "demo"
_DEFAULT_OUT_DIR = _REPO_ROOT / "data" / "demo" / "neo4j"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _load_demo_statements(demo_dir: Path) -> list[dict]:
    """Read all *.jsonl files in demo_dir, dedup by statementId."""
    jsonl_files = sorted(demo_dir.glob("*.jsonl"))
    if not jsonl_files:
        log.error("No *.jsonl files found in %s — run 'make build-demo' first", demo_dir)
        sys.exit(1)

    seen: set[str] = set()
    statements: list[dict] = []
    duplicates = 0
    for path in jsonl_files:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            stmt = json.loads(line)
            sid = stmt.get("statementId", "")
            if sid in seen:
                duplicates += 1
                continue
            seen.add(sid)
            statements.append(stmt)

    log.info(
        "Loaded %d unique statements from %d files (%d duplicates skipped)",
        len(statements),
        len(jsonl_files),
        duplicates,
    )
    return statements


def _write_combined(statements: list[dict], out_path: Path) -> None:
    out_path.write_text("\n".join(json.dumps(s, ensure_ascii=False) for s in statements) + "\n")
    log.info("Wrote combined JSONL → %s (%d statements)", out_path, len(statements))


def _run_bods_neo4j_to_csv(combined_jsonl: Path, out_dir: Path) -> None:
    """Run ``bods-neo4j to-csv`` via subprocess.

    bods-neo4j is installed in the venv (or globally). We look for it on PATH
    and also check the backend venv.
    """
    # Try venv first, then PATH
    venv_bin = Path(__file__).resolve().parents[1] / ".venv" / "bin" / "bods-neo4j"
    if venv_bin.exists():
        cmd = str(venv_bin)
    else:
        cmd = shutil.which("bods-neo4j")
        if cmd is None:
            log.error(
                "bods-neo4j not found. Install it with:\n"
                "  pip install git+https://github.com/StephenAbbott/bods-neo4j.git"
            )
            sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Running: %s to-csv %s -o %s", cmd, combined_jsonl, out_dir)
    result = subprocess.run(
        [cmd, "to-csv", str(combined_jsonl), "-o", str(out_dir)],
        capture_output=False,  # let stdout/stderr stream to terminal
    )
    if result.returncode != 0:
        log.error("bods-neo4j to-csv exited with code %d", result.returncode)
        sys.exit(result.returncode)


def _summarise(out_dir: Path) -> None:
    csvs = sorted(out_dir.glob("*.csv"))
    log.info("CSV files written to %s:", out_dir)
    for csv in csvs:
        lines = len(csv.read_text().splitlines()) - 1  # subtract header
        log.info("  %-32s  %d rows", csv.name, lines)
    cypher = out_dir / "import.cypher"
    if cypher.exists():
        log.info("  import.cypher                     (LOAD CSV script)")
    shell = out_dir / "import.sh"
    if shell.exists():
        log.info("  import.sh                         (cypher-shell wrapper)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--demo-dir",
        type=Path,
        default=_DEFAULT_DEMO_DIR,
        help="Directory containing per-entity *.jsonl files (default: data/demo/)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=_DEFAULT_OUT_DIR,
        help="Output directory for Neo4j CSVs (default: data/demo/neo4j/)",
    )
    args = parser.parse_args()

    log.info("=== Phase 7: Neo4j CSV export ===")
    log.info("Demo dir : %s", args.demo_dir)
    log.info("Output   : %s", args.out)

    # Step 1: load + dedup
    statements = _load_demo_statements(args.demo_dir)

    # Step 2: write combined JSONL
    combined_path = args.demo_dir / "all_demo.jsonl"
    _write_combined(statements, combined_path)

    # Step 3: bods-neo4j to-csv
    _run_bods_neo4j_to_csv(combined_path, args.out)

    # Step 4: summarise
    _summarise(args.out)
    log.info("=== Export complete ===")
    log.info("")
    log.info("To import into a running Neo4j 5.x instance:")
    log.info("  cd <neo4j-home> && cypher-shell -u neo4j -p <pass> < %s/import.cypher", args.out)
    log.info("Or use the bods-neo4j Docker setup:")
    log.info("  git clone https://github.com/StephenAbbott/bods-neo4j && cd bods-neo4j")
    log.info("  docker compose up -d")
    log.info("  bash %s/import.sh", args.out)


if __name__ == "__main__":
    main()
