"""BODS quality check script.

Generates BODS bundles for five representative LEIs using the live adapters
that don't require API keys, then runs lib-cove-bods against each bundle and
writes a combined findings report.

Usage:
    cd backend
    OPENCHECK_ALLOW_LIVE=true python scripts/bods_quality_check.py

Outputs:
    scripts/bods_review/           directory of per-LEI JSON bundles
    scripts/bods_review/report.json   combined lib-cove-bods findings
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

# Ensure the opencheck package is importable.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Must be set before importing opencheck modules so get_settings() picks it up.
os.environ.setdefault("OPENCHECK_ALLOW_LIVE", "true")

from opencheck.config import get_settings  # noqa: E402
get_settings.cache_clear()

from opencheck.sources.gleif import GleifAdapter          # noqa: E402
from opencheck.sources.brreg import BrregAdapter           # noqa: E402
from opencheck.sources.ur_latvia import UrLatviaAdapter    # noqa: E402
from opencheck.bods import (                               # noqa: E402
    map_gleif,
    map_brreg,
    map_ur_latvia,
)

# ---------------------------------------------------------------------------
# Test subjects
# ---------------------------------------------------------------------------

SUBJECTS = [
    {
        "label": "Biffa PLC (UK)",
        "lei": "2138008RB4WDK7HYYS91",
        "adapters": ["gleif"],
        # Companies House key not available; OO bundle may exist locally.
    },
    {
        "label": "Avinor AS (Norway)",
        "lei": "5967007LIEEXZX8ZW078",
        "adapters": ["gleif", "brreg"],
        # Brreg: keyless, full officers including CEO + board.
    },
    {
        "label": "Latvenergo AS (Latvia)",
        "lei": "213800DJRB539Q1EMW75",
        "adapters": ["gleif", "ur_latvia"],
        # UR Latvia: keyless, entity + BOs + officers + SIA members.
    },
    {
        "label": "Eesti Energia AS (Estonia)",
        "lei": "5493005044RTLQ5RZU70",
        "adapters": ["gleif"],
        # Ariregister requires local DB file; not available here.
    },
    {
        "label": "Costco Wholesale Corporation (US)",
        "lei": "29DX7H14B9S6O3FD6V18",
        "adapters": ["gleif"],
        # BrightQuery requires local DB; SEC EDGAR needs live API + name search.
    },
]

# ---------------------------------------------------------------------------
# Adapter instances
# ---------------------------------------------------------------------------

_gleif = GleifAdapter()
_brreg = BrregAdapter()
_ur_latvia = UrLatviaAdapter()


# ---------------------------------------------------------------------------
# Per-LEI bundle generation
# ---------------------------------------------------------------------------

async def build_bundle(subject: dict) -> list[dict]:
    """Fetch and map BODS statements for a single subject across all adapters."""
    lei = subject["lei"]
    adapters = subject["adapters"]
    statements: list[dict] = []

    # --- GLEIF ---
    if "gleif" in adapters:
        print(f"  [gleif] fetching {lei}...")
        try:
            gleif_bundle = await _gleif.fetch(lei)
            if not gleif_bundle.get("is_stub"):
                stmts = list(map_gleif(gleif_bundle))
                statements.extend(stmts)
                print(f"  [gleif] → {len(stmts)} statements")
            else:
                print(f"  [gleif] returned stub")
        except Exception as exc:
            print(f"  [gleif] ERROR: {exc}")

    # --- Brreg ---
    if "brreg" in adapters:
        # Derive no_orgnr from GLEIF registered-as field.
        try:
            gleif_raw = await _gleif.fetch(lei)
            record = gleif_raw.get("record") or {}
            attrs = record.get("attributes") or {}
            entity = attrs.get("entity") or {}
            registered_as = entity.get("registeredAs") or ""
            registered_at_id = (entity.get("registeredAt") or {}).get("id") or ""
            print(f"  [brreg] registeredAt={registered_at_id} registeredAs={registered_as}")
            if registered_as:
                from opencheck.sources.brreg import normalise_orgnr
                no_orgnr = normalise_orgnr(registered_as)
                print(f"  [brreg] fetching no_orgnr={no_orgnr}...")
                brreg_bundle = await _brreg.fetch(no_orgnr, legal_name=subject["label"])
                if not brreg_bundle.get("is_stub"):
                    stmts = list(map_brreg(brreg_bundle))
                    statements.extend(stmts)
                    print(f"  [brreg] → {len(stmts)} statements")
                else:
                    print(f"  [brreg] returned stub")
        except Exception as exc:
            print(f"  [brreg] ERROR: {exc}")

    # --- UR Latvia ---
    if "ur_latvia" in adapters:
        try:
            gleif_raw = await _gleif.fetch(lei)
            record = gleif_raw.get("record") or {}
            attrs = record.get("attributes") or {}
            entity = attrs.get("entity") or {}
            registered_as = entity.get("registeredAs") or ""
            registered_at_id = (entity.get("registeredAt") or {}).get("id") or ""
            print(f"  [ur_latvia] registeredAt={registered_at_id} registeredAs={registered_as}")
            if registered_as:
                from opencheck.sources.ur_latvia import normalise_regcode
                lv_regcode = normalise_regcode(registered_as)
                print(f"  [ur_latvia] fetching lv_regcode={lv_regcode}...")
                lv_bundle = await _ur_latvia.fetch(lv_regcode, legal_name=subject["label"])
                if not lv_bundle.get("is_stub"):
                    stmts = list(map_ur_latvia(lv_bundle))
                    statements.extend(stmts)
                    print(f"  [ur_latvia] → {len(stmts)} statements")
                else:
                    print(f"  [ur_latvia] returned stub")
        except Exception as exc:
            print(f"  [ur_latvia] ERROR: {exc}")

    return statements


# ---------------------------------------------------------------------------
# lib-cove-bods analysis
# ---------------------------------------------------------------------------

def run_libcovebods(json_path: Path) -> dict:
    """Run lib-cove-bods against a JSON bundle file and return the full result."""
    import libcovebods.data_reader as dr
    import libcovebods.run_tasks as rt
    import libcovebods.jsonschemavalidate as jsv
    from libcovebods.schema import SchemaBODS
    from libcovebods.config import LibCoveBODSConfig

    config = LibCoveBODSConfig()
    data_reader = dr.DataReader(str(json_path))
    schema_obj = SchemaBODS(data_reader=data_reader, lib_cove_bods_config=config)

    # 1. JSON Schema validation (required fields, codelist values, types).
    schema_validator = jsv.JSONSchemaValidator(schema_obj)
    schema_errors = schema_validator.validate(data_reader)
    schema_error_dicts = []
    for e in schema_errors:
        try:
            schema_error_dicts.append(e.json())
        except Exception:
            schema_error_dicts.append({"message": str(e)})

    # 2. Additional checks (statement-reference integrity, date sanity,
    #    duplicate IDs, interest completeness, series checks, etc.)
    additional = rt.process_additional_checks(
        data_reader, config, schema_obj
    )

    return {
        "schema_errors": schema_error_dicts,
        "additional_checks": additional.get("additional_checks", []),
        "statistics": additional.get("statistics", {}),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    out_dir = Path(__file__).parent / "bods_review"
    out_dir.mkdir(exist_ok=True)

    all_findings: dict[str, dict] = {}

    for subject in SUBJECTS:
        lei = subject["lei"]
        label = subject["label"]
        slug = lei.lower()
        print(f"\n{'='*60}")
        print(f"Generating bundle: {label} ({lei})")
        print(f"{'='*60}")

        statements = await build_bundle(subject)

        # Write JSON array (what lib-cove-bods expects).
        json_path = out_dir / f"{slug}.json"
        with open(json_path, "w") as fh:
            json.dump(statements, fh, indent=2, ensure_ascii=False)
        print(f"  Written {len(statements)} statements → {json_path}")

        if not statements:
            print(f"  Skipping lib-cove-bods (no statements)")
            all_findings[lei] = {"label": label, "statement_count": 0, "skipped": True}
            continue

        # Run lib-cove-bods.
        print(f"  Running lib-cove-bods...")
        try:
            result = run_libcovebods(json_path)
            checks = result.get("additional_checks", [])
            stats = result.get("statistics", {})
            schema_errors = result.get("schema_errors", [])
            checks = result.get("additional_checks", [])
            stats = result.get("statistics", {})
            all_findings[lei] = {
                "label": label,
                "statement_count": len(statements),
                "statistics": stats,
                "schema_errors": schema_errors,
                "additional_checks": checks,
                "schema_error_count": len(schema_errors),
                "additional_error_count": sum(
                    1 for c in checks if c.get("type") == "error"
                ),
                "additional_warning_count": sum(
                    1 for c in checks if c.get("type") == "warning"
                ),
            }
            print(f"  Checks complete: {len(schema_errors)} schema errors, "
                  f"{len(checks)} additional checks "
                  f"({all_findings[lei]['additional_error_count']} errors, "
                  f"{all_findings[lei]['additional_warning_count']} warnings)")
        except Exception as exc:
            print(f"  lib-cove-bods ERROR: {exc}")
            import traceback; traceback.print_exc()
            all_findings[lei] = {"label": label, "statement_count": len(statements), "error": str(exc)}

    # Write combined report.
    report_path = out_dir / "report.json"
    with open(report_path, "w") as fh:
        json.dump(all_findings, fh, indent=2, ensure_ascii=False)
    print(f"\n{'='*60}")
    print(f"Report written → {report_path}")

    # Print human-readable summary.
    print("\n=== SUMMARY ===\n")
    for lei, findings in all_findings.items():
        label = findings.get("label", lei)
        count = findings.get("statement_count", 0)
        if findings.get("skipped"):
            print(f"{label}: no statements generated")
            continue
        schema_errs = findings.get("schema_error_count", 0)
        add_errs = findings.get("additional_error_count", 0)
        add_warns = findings.get("additional_warning_count", 0)
        print(f"{label} ({count} statements): "
              f"{schema_errs} schema errors, "
              f"{add_errs} additional errors, "
              f"{add_warns} warnings")

        # Schema errors.
        for se in findings.get("schema_errors", []):
            msg = se.get("message", str(se))
            if len(msg) > 140:
                msg = msg[:140] + "..."
            path = se.get("path", "")
            print(f"  [SCHEMA ERROR] {msg}  (path: {path})")

        # Additional checks.
        for check in findings.get("additional_checks", []):
            c_type = check.get("type", "?")
            c_name = check.get("name", "?")
            c_msg = check.get("message") or check.get("description", "")
            if isinstance(c_msg, str) and len(c_msg) > 140:
                c_msg = c_msg[:140] + "..."
            extra = check.get("statement_id") or check.get("count") or ""
            print(f"  [{c_type.upper()}] {c_name}: {c_msg} {extra or ''}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
