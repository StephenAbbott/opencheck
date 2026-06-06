"""One-command build step for the OpenCheck demo graph.

Encodes the 9 Phase-0 anchor entities, re-extracts their BODS subgraphs
from the local SQLite databases, merges GLEIF + UK PSC bundles per entity
into a combined ``data/demo/{lei}.jsonl``, validates everything with
lib-cove-bods, and writes a manifest.

Usage
-----

From the ``backend/`` directory::

    python scripts/build_demo.py

Defaults assume the standard local data layout::

    backend/data/bods/gleif/gleif_version_0_4.db
    backend/data/bods/uk_psc/uk_version_0_4.db

Override with --gleif / --uk::

    python scripts/build_demo.py \\
        --gleif /path/to/gleif_version_0_4.db \\
        --uk /path/to/uk_version_0_4.db

Add --skip-extract to skip re-running the extraction (e.g. if the SQLite
DBs are absent but JSON-Lines already exist) and jump straight to
merge + validate::

    python scripts/build_demo.py --skip-extract

Output
------

* ``data/cache/bods_data/gleif/<LEI>.jsonl``   — GLEIF subgraph (via extract)
* ``data/cache/bods_data/uk/<GB-COH>.jsonl``   — UK PSC subgraph (via extract)
* ``data/demo/<LEI>.jsonl``                    — combined GLEIF+UK bundle
* ``data/demo/manifest.json``                  — build metadata
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Demo set — Phase 0 anchor entities
# Keep in sync with the Phase 0 Notion sub-page and EXAMPLE_LEIS in App.tsx.
# ---------------------------------------------------------------------------

DEMO_LEIS: list[dict] = [
    {
        "lei": "4OFD47D73QFJ1T1MOF29",
        "name": "Daily Mail and General Trust P L C",
        "ch": "00184594",
        "features": ["complex_corporate_structure", "trust_or_arrangement"],
        "note": "Complex corporate structure spanning jurisdictions",
    },
    {
        "lei": "213800LH1BZH3DI6G760",
        "name": "BP P.L.C.",
        "ch": "00102498",
        "features": [
            "trust_or_arrangement",
            "complex_ownership_layers",
            "complex_corporate_structure",
            "non_eu_jurisdiction",
        ],
        "note": "Large corporate group with a complex structure spread across jurisdictions",
    },
    {
        "lei": "253400JT3MQWNDKMJE44",
        "name": "Rosneft Deutschland GmbH",
        "ch": None,
        "features": ["sanctioned", "related_sanctioned"],
        "note": "International network with connections from a sanctioned Russian entity",
    },
    {
        "lei": "2138008KTNTDICZU8L25",
        "name": "Bank Saderat PLC",
        "ch": "01126618",
        "features": ["sanctioned", "related_sanctioned", "non_eu_jurisdiction"],
        "note": "Sanctioned Iranian bank with connected sanctioned individuals",
    },
    {
        "lei": "2138008RB4WDK7HYYS91",
        "name": "Biffa PLC",
        "ch": "10336040",
        "features": ["complex_ownership_layers", "non_eu_jurisdiction"],
        "note": "UK waste management firm which wins lots of government contracts",
    },
    {
        "lei": "2138002S3XGZ38WN5Q72",
        "name": "Hornsea 1 Limited",
        "ch": "07640868",
        "features": ["complex_ownership_layers", "non_eu_jurisdiction"],
        "note": "UK offshore wind company with connections to the UAE",
    },
    {
        "lei": "213800DBE5Y9ZM58PN63",
        "name": "Care UK Social Care Limited",
        "ch": "07068789",
        "features": ["complex_ownership_layers"],
        "note": "UK care home chain with complex ownership structure",
    },
    {
        "lei": "213800E11LI1SCETU492",
        "name": "Taqa Bratani Limited",
        "ch": "05975475",
        "features": ["complex_ownership_layers", "non_eu_jurisdiction", "related_sanctioned"],
        "note": "UAE-owned oil and gas company with operations in the UK",
    },
    {
        "lei": "213800AG2V6YE68H5N63",
        "name": "Newcastle United Football Company Limited",
        "ch": "00031014",
        "features": ["complex_ownership_layers", "non_eu_jurisdiction"],
        "note": "UK football club with complex but declared ownership ties to Saudi Arabia",
    },
]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_GLEIF_DB = _BACKEND_ROOT / "data" / "bods" / "gleif" / "gleif_version_0_4.db"
_DEFAULT_UK_DB = _BACKEND_ROOT / "data" / "bods" / "uk_psc" / "uk_version_0_4.db"
_CACHE_ROOT = _REPO_ROOT / "data" / "cache" / "bods_data"
_DEMO_DIR = _REPO_ROOT / "data" / "demo"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    stmts: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                stmts.append(json.loads(line))
    return stmts


def _write_jsonl(path: Path, statements: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for stmt in statements:
            fh.write(json.dumps(stmt, ensure_ascii=False) + "\n")


def _dedup_by_statement_id(statements: list[dict]) -> list[dict]:
    """Remove duplicate statements (same statementId), keeping first occurrence."""
    seen: set[str] = set()
    out: list[dict] = []
    for s in statements:
        sid = s.get("statementId")
        if sid and sid in seen:
            continue
        if sid:
            seen.add(sid)
        out.append(s)
    return out


def _normalize_bundle(statements: list[dict]) -> list[dict]:
    """Heal known OO pipeline serialization artifacts in a full BODS bundle.

    The bods-data.openownership.org SQLite dumps (pre-Phase-2-fix) contain
    several BODS 0.3-era structures that libcovebods 0.16 rejects.
    extract_bods_subgraphs.py fixes these at extraction time; this function
    applies the same fixes at merge time so --skip-extract on stale cached
    files still produces valid output.

    Fixed:
    * ``source.type`` — comma-joined string → array
    * ``addresses[].country`` — bare ISO-2 string → ``{"code":…, "name":…}``
    * ``isComponent`` — missing required field; default False
    * ``declarationSubject`` — missing required field; fall back to recordId / statementId
    * ``subject`` / ``interestedParty`` in relationship statements — 0.3-era
      ``{"describedByEntityStatement": "<statementId>"}`` objects → plain
      recordId strings (resolved via a statementId→recordId lookup table built
      from the entity/person statements in the same bundle).
    """
    import copy

    # Build statementId → recordId map from entity / person statements.
    sid_to_rid: dict[str, str] = {}
    for s in statements:
        rt = s.get("recordType")
        if rt in ("entity", "person"):
            sid = s.get("statementId")
            rid = s.get("recordId")
            if sid and rid:
                sid_to_rid[sid] = rid

    result = []
    for s in statements:
        s = copy.deepcopy(s)

        # --- source.type ---
        src = s.get("source")
        if isinstance(src, dict):
            t = src.get("type")
            if isinstance(t, str):
                src["type"] = [x for x in t.split(",") if x]

        rd = s.get("recordDetails") or {}
        rt = s.get("recordType")

        # --- addresses[].country ---
        for addr in rd.get("addresses") or []:
            if not isinstance(addr, dict):
                continue
            c = addr.get("country")
            if isinstance(c, str) and c:
                addr["country"] = {"code": c, "name": c}

        # --- isComponent (required inside recordDetails for all types) ---
        if "isComponent" not in rd:
            rd["isComponent"] = False

        # --- declarationSubject (top-level, required for entity / person) ---
        # Relationship statements also require this in BODS 0.4, but it must
        # reference the statementId of the subject entity — a value only
        # available from a fresh extraction.  We only fix the entity/person
        # case here; stale relationship rows require re-extraction (make
        # build-demo) to resolve cleanly.
        if rt in ("entity", "person") and "declarationSubject" not in s:
            s["declarationSubject"] = (
                s.get("recordId") or s.get("statementId") or ""
            )

        # --- subject / interestedParty (relationship) ---
        if rt == "relationship":
            for field in ("subject", "interestedParty"):
                val = rd.get(field)
                if isinstance(val, dict):
                    # 0.3-era: {"describedByEntityStatement": "<statementId>"}
                    inner_sid = val.get("describedByEntityStatement")
                    if inner_sid:
                        # Prefer recordId; fall back to the statementId itself
                        rd[field] = sid_to_rid.get(inner_sid, inner_sid)

        if rd:
            s["recordDetails"] = rd
        result.append(s)

    return result


def _gb_coh_from_statements(statements: list[dict]) -> str | None:
    for s in statements:
        if s.get("recordType") == "entity":
            for ident in (s.get("recordDetails") or {}).get("identifiers") or []:
                if ident.get("scheme") == "GB-COH" and ident.get("id"):
                    return ident["id"]
    return None


# ---------------------------------------------------------------------------
# Extraction step
# ---------------------------------------------------------------------------


def run_extraction(
    gleif_db: Path,
    uk_db: Path | None,
    leis: list[str],
    max_hops: int = 3,
) -> None:
    """Invoke extract_bods_subgraphs.main() programmatically."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from extract_bods_subgraphs import main as extract_main  # noqa: PLC0415

    argv = [
        "--gleif", str(gleif_db),
        "--leis", *leis,
        "--output", str(_CACHE_ROOT),
        "--max-hops", str(max_hops),
    ]
    if uk_db and uk_db.is_file():
        argv += ["--uk", str(uk_db)]
    else:
        print("  (--uk DB not found — skipping UK PSC extraction)")

    rc = extract_main(argv)
    if rc != 0:
        print(f"ERROR: extract_bods_subgraphs exited {rc}", file=sys.stderr)
        sys.exit(rc)


# ---------------------------------------------------------------------------
# Merge step
# ---------------------------------------------------------------------------


def merge_bundles(entry: dict) -> list[dict]:
    """Merge GLEIF + UK PSC JSON-Lines for one anchor entity.

    GLEIF statements come first; UK PSC statements are appended, with
    duplicates (same statementId) dropped.
    """
    lei = entry["lei"]
    gleif_path = _CACHE_ROOT / "gleif" / f"{lei}.jsonl"
    gleif_stmts = _load_jsonl(gleif_path)

    # Prefer the CH number recorded in the demo set; fall back to whatever
    # the GLEIF subgraph itself asserts.
    ch = entry.get("ch") or _gb_coh_from_statements(gleif_stmts)
    uk_stmts: list[dict] = []
    if ch:
        uk_path = _CACHE_ROOT / "uk" / f"{ch}.jsonl"
        uk_stmts = _load_jsonl(uk_path)

    combined = _dedup_by_statement_id(gleif_stmts + uk_stmts)
    return _normalize_bundle(combined)


# ---------------------------------------------------------------------------
# Validation step
# ---------------------------------------------------------------------------


def validate_bundle(statements: list[dict]) -> tuple[list, list]:
    """Run lib-cove-bods on a list of statements.

    Returns (json_schema_errors, additional_check_errors).
    Raises ImportError if libcovebods is not installed.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from validate_demo_subgraph import validate  # noqa: PLC0415

    return validate(statements)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def write_manifest(results: list[dict], demo_dir: Path) -> Path:
    manifest = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "gleif_db": str(_DEFAULT_GLEIF_DB),
        "uk_db": str(_DEFAULT_UK_DB),
        "anchor_count": len(DEMO_LEIS),
        "entities": results,
    }
    path = demo_dir / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the OpenCheck demo graph from source SQLite DBs."
    )
    parser.add_argument(
        "--gleif",
        type=Path,
        default=_DEFAULT_GLEIF_DB,
        help=f"Path to gleif_version_0_4.db (default: {_DEFAULT_GLEIF_DB})",
    )
    parser.add_argument(
        "--uk",
        type=Path,
        default=_DEFAULT_UK_DB,
        help=f"Path to uk_version_0_4.db (default: {_DEFAULT_UK_DB})",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=3,
        help="BFS depth for subgraph extraction (default: 3)",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction — use whatever JSON-Lines files are already on disk.",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip lib-cove-bods validation (faster, for iteration).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEMO_DIR,
        help=f"Output directory for combined bundles + manifest (default: {_DEMO_DIR})",
    )
    args = parser.parse_args(argv)

    leis = [e["lei"] for e in DEMO_LEIS]
    print(f"OpenCheck demo graph build — {len(DEMO_LEIS)} anchor entities")
    print(f"  GLEIF db : {args.gleif}")
    print(f"  UK PSC db: {args.uk}")
    print(f"  Output   : {args.output}")
    print()

    # ------------------------------------------------------------------
    # Step 1: Extract
    # ------------------------------------------------------------------
    if args.skip_extract:
        print("Step 1/3  Extraction — SKIPPED (--skip-extract)")
    else:
        if not args.gleif.is_file():
            print(
                f"ERROR: GLEIF db not found at {args.gleif}\n"
                "       Run scripts/setup_bods_data.py first, or pass --skip-extract\n"
                "       if JSON-Lines files are already present.",
                file=sys.stderr,
            )
            return 1
        print(f"Step 1/3  Extracting {len(leis)} subgraphs (max-hops={args.max_hops})…")
        run_extraction(
            gleif_db=args.gleif,
            uk_db=args.uk if args.uk.is_file() else None,
            leis=leis,
            max_hops=args.max_hops,
        )
        print()

    # ------------------------------------------------------------------
    # Step 2: Merge GLEIF + UK PSC per entity → data/demo/{lei}.jsonl
    # ------------------------------------------------------------------
    print("Step 2/3  Merging GLEIF + UK PSC bundles…")
    results: list[dict] = []
    for entry in DEMO_LEIS:
        lei = entry["lei"]
        combined = merge_bundles(entry)
        if not combined:
            print(f"  WARNING: {lei} ({entry['name']}) — no statements found; skipping")
            results.append({"lei": lei, "name": entry["name"], "error": "no statements"})
            continue

        out_path = args.output / f"{lei}.jsonl"
        _write_jsonl(out_path, combined)
        n_entity = sum(1 for s in combined if s.get("recordType") == "entity")
        n_person = sum(1 for s in combined if s.get("recordType") == "person")
        n_rel = sum(1 for s in combined if s.get("recordType") == "relationship")
        print(
            f"  {lei}  {entry['name'][:40]:<40}  "
            f"{len(combined):>5} stmts  "
            f"({n_entity}E/{n_person}P/{n_rel}R)"
        )
        results.append(
            {
                "lei": lei,
                "name": entry["name"],
                "ch": entry.get("ch"),
                "features": entry["features"],
                "note": entry["note"],
                "combined_path": str(out_path.relative_to(_REPO_ROOT)),
                "statement_counts": {
                    "total": len(combined),
                    "entity": n_entity,
                    "person": n_person,
                    "relationship": n_rel,
                },
            }
        )
    print()

    # ------------------------------------------------------------------
    # Step 3: Validate
    # ------------------------------------------------------------------
    if args.skip_validate:
        print("Step 3/3  Validation — SKIPPED (--skip-validate)")
        n_pass = sum(1 for r in results if "error" not in r)
    else:
        if args.skip_extract:
            print(
                "Step 3/3  Validating combined bundles with lib-cove-bods…\n"
                "          NOTE: --skip-extract uses cached JSON-Lines on disk.\n"
                "          Pre-Phase-2 files (BODS 0.3-era) may show schema errors\n"
                "          for unspecified-party format and relationship declarationSubject\n"
                "          that only re-extraction (make build-demo) can resolve."
            )
        else:
            print("Step 3/3  Validating combined bundles with lib-cove-bods…")
        try:
            n_pass = 0
            for entry, result in zip(DEMO_LEIS, results):
                if "error" in result:
                    continue
                combined_path = _REPO_ROOT / result["combined_path"]
                stmts = _load_jsonl(combined_path)
                js_errors, additional = validate_bundle(stmts)
                ok = not js_errors and not additional
                verdict = "PASS" if ok else f"FAIL ({len(js_errors)} schema, {len(additional)} additional)"
                print(f"  {entry['lei']}  {verdict}")
                result["validation"] = {"pass": ok, "schema_errors": len(js_errors), "additional_errors": len(additional)}
                if ok:
                    n_pass += 1
        except ImportError:
            print(
                "  WARNING: lib-cove-bods not installed — skipping validation.\n"
                "           pip install libcovebods",
                file=sys.stderr,
            )
            args.skip_validate = True
            n_pass = sum(1 for r in results if "error" not in r)
        print()

    # ------------------------------------------------------------------
    # Manifest
    # ------------------------------------------------------------------
    manifest_path = write_manifest(results, args.output)
    print(f"Manifest written → {manifest_path.relative_to(_REPO_ROOT)}")
    print()

    n_total = len([r for r in results if "error" not in r])
    if args.skip_validate:
        print(f"Done — {n_total}/{len(DEMO_LEIS)} entities built (validation skipped).")
        return 0
    else:
        print(f"Done — {n_pass}/{len(DEMO_LEIS)} entities build + validate clean.")
        return 0 if n_pass == len(DEMO_LEIS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
