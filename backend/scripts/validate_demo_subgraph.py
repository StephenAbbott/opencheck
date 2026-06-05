"""Validate + profile an extracted BODS subgraph (Phase 2 of the graph-native
de-risking checklist).

Loads one or more ``.jsonl`` files produced by ``extract_bods_subgraphs.py``,
runs lib-cove-bods (JSON-schema + additional checks, same setup as
``tests/test_bods_libcovebods.py``), and prints a richness profile mapped to
the Phase 0 demo-set feature catalogue.

Usage::

    pip install libcovebods           # if not already in the venv
    python scripts/validate_demo_subgraph.py \\
      data/cache/bods_data/gleif/213800E11LI1SCETU492.jsonl \\
      data/cache/bods_data/uk/05975475.jsonl

Pass any number of .jsonl files; they are merged into one subgraph for both
validation and profiling.
"""

from __future__ import annotations

import json
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any


def load_statements(paths: list[str]) -> list[dict[str, Any]]:
    stmts: list[dict[str, Any]] = []
    for p in paths:
        path = Path(p)
        if not path.is_file():
            print(f"  ! not found: {p}", file=sys.stderr)
            continue
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    stmts.append(json.loads(line))
    return stmts


def validate(statements: list[dict[str, Any]]) -> tuple[list, list]:
    """Run lib-cove-bods. Returns (json_schema_errors, additional_checks)."""
    from libcovebods.config import LibCoveBODSConfig
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS
    import libcovebods.run_tasks

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(statements, fh)
        tmp = fh.name

    config = LibCoveBODSConfig()
    schema = SchemaBODS(data_reader=DataReader(tmp), lib_cove_bods_config=config)
    js_errors = JSONSchemaValidator(schema).validate(DataReader(tmp))
    additional = libcovebods.run_tasks.process_additional_checks(
        DataReader(tmp), config, schema
    )
    # JSON-schema errors are BODSValidationError objects with a .json() method.
    js = [e.json() if hasattr(e, "json") else e for e in js_errors]
    # process_additional_checks returns a dict; the checks live under a key.
    if isinstance(additional, dict):
        checks = additional.get("additional_checks", [])
    else:
        checks = list(additional)
    return js, checks


def profile(statements: list[dict[str, Any]]) -> None:
    by_type: Counter = Counter()
    jurisdictions: Counter = Counter()
    id_schemes: Counter = Counter()
    interest_types: Counter = Counter()
    direct_indirect: Counter = Counter()
    booc: Counter = Counter()
    person_types: Counter = Counter()
    share_bands: Counter = Counter()
    n_alt_names = n_dissolved = n_nationality = n_birthdate = n_address = n_enddate = n_share = 0

    for s in statements:
        rt = s.get("recordType") or s.get("statementType") or "?"
        by_type[rt] += 1
        rd = s.get("recordDetails") or {}

        if rt == "entity":
            juris = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
            code = juris.get("code") if isinstance(juris, dict) else juris
            if code:
                jurisdictions[code] += 1
            for i in rd.get("identifiers") or []:
                if i.get("scheme"):
                    id_schemes[i["scheme"]] += 1
            if rd.get("alternateNames"):
                n_alt_names += 1
            if rd.get("dissolutionDate"):
                n_dissolved += 1
        elif rt == "person":
            person_types[rd.get("personType") or "?"] += 1
            if rd.get("nationalities"):
                n_nationality += 1
            if rd.get("birthDate"):
                n_birthdate += 1
            if rd.get("addresses"):
                n_address += 1
        elif rt == "relationship":
            for it in rd.get("interests") or []:
                if it.get("type"):
                    interest_types[it["type"]] += 1
                if it.get("directOrIndirect"):
                    direct_indirect[it["directOrIndirect"]] += 1
                booc[str(it.get("beneficialOwnershipOrControl"))] += 1
                if it.get("endDate"):
                    n_enddate += 1
                sh = it.get("share")
                if isinstance(sh, dict):
                    n_share += 1
                    lo = sh.get("minimum", sh.get("exclusiveMinimum", "?"))
                    hi = sh.get("maximum", sh.get("exclusiveMaximum", "?"))
                    share_bands[f"{lo}-{hi}"] += 1
                elif sh is not None:
                    n_share += 1
                    share_bands[str(sh)] += 1

    def line(label: str, counter_or_val: Any) -> None:
        print(f"  {label:<34} {counter_or_val}")

    print("\n--- Richness profile ---")
    line("Statement types", dict(by_type))
    line("Entity jurisdictions", dict(jurisdictions))
    cross = "YES" if len(jurisdictions) > 1 else "no"
    line("Cross-border (>1 jurisdiction)", cross)
    line("Identifier schemes", dict(id_schemes))
    line("Interest types", dict(interest_types))
    line("directOrIndirect", dict(direct_indirect))
    line("beneficialOwnershipOrControl", dict(booc))
    line("Person types", dict(person_types))
    line("Persons w/ nationality", n_nationality)
    line("Persons w/ birthDate", n_birthdate)
    line("Persons w/ address", n_address)
    line("Entities w/ alternateNames", n_alt_names)
    line("Entities w/ dissolutionDate", n_dissolved)
    line("Interests w/ endDate (ceased)", n_enddate)
    line("Interests w/ share %", n_share)
    line("Share bands (min-max)", dict(share_bands))


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print(__doc__)
        return 2
    statements = load_statements(argv)
    print(f"Loaded {len(statements):,} statements from {len(argv)} file(s).")

    try:
        js_errors, additional = validate(statements)
    except ImportError:
        print("\n! lib-cove-bods not installed — run: pip install libcovebods", file=sys.stderr)
        profile(statements)
        return 1

    print("\n--- lib-cove-bods validation ---")
    print(f"  JSON-schema errors (hard): {len(js_errors)}  — grouped by message:")
    msg_counts: Counter = Counter()
    sample_path: dict[str, str] = {}
    for e in js_errors:
        m = e.get("message") if isinstance(e, dict) else str(e)
        msg_counts[m] += 1
        if m not in sample_path and isinstance(e, dict):
            sample_path[m] = str(e.get("path") or e.get("json_path") or e.get("path_ending") or "")
    for m, c in msg_counts.most_common(15):
        where = f"   e.g. @ {sample_path.get(m)}" if sample_path.get(m) else ""
        print(f"    {c:>4}x  {m}{where}")

    adv = [a for a in additional if (a.get("type") if isinstance(a, dict) else "") == "entity_identifiers_not_known_scheme"]
    real = [a for a in additional if a not in adv]
    print(f"  Additional checks (non-advisory): {len(real)}")
    real_types: Counter = Counter(a.get("type") if isinstance(a, dict) else str(a) for a in real)
    for t, c in real_types.most_common(10):
        print(f"    {c:>4}x  {t}")
    if adv:
        print(f"  (advisory) unknown identifier schemes: {len(adv)} - informational only")

    profile(statements)

    print("\n--- Verdict ---")
    print("  SCHEMA:", "PASS (0 hard errors)" if not js_errors else f"FAIL ({len(js_errors)} errors)")
    return 0 if not js_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
