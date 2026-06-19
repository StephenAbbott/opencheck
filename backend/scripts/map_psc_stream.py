"""Map captured PSC stream events to BODS v0.4 and validate the output.

Steps 4 + 5 of the "Create a UK PSC > BODS livestream demo" capture-and-validate
test. Reads the ``.jsonl`` produced by ``capture_psc_stream.py``, feeds each
event's PSC ``data`` block through OpenCheck's existing
``map_companies_house`` mapper (the stream ``data`` block is the same resource
shape the REST PSC endpoint returns, so the mapper is reused unchanged), writes
the resulting BODS statements to a ``.jsonl``, and runs lib-cove-bods plus
OpenCheck's in-process shape validator.

The point of the test: prove the *live* event shape maps cleanly to
schema-valid BODS v0.4, and surface any stream-vs-REST gaps (notably cessation
/ ``deleted`` lifecycle, which the current mapper skips) before the MVP build.

Usage (from ``backend/``)::

    python scripts/map_psc_stream.py data/cache/psc_stream/psc_stream_sample.jsonl \\
        --out data/cache/psc_stream/psc_stream_bods.jsonl

    # richer subject entities (company name/address/founding date) by looking up
    # each company profile via the REST API — needs the REST key:
    export COMPANIES_HOUSE_API_KEY=your_rest_key
    python scripts/map_psc_stream.py ... --fetch-profiles

Without ``--fetch-profiles`` the mapper falls back to a minimal subject entity
(``Company <number>``), which is still valid BODS — fine for the mapping test.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

# Make the ``opencheck`` package importable when run from backend/ or scripts/.
_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))


def company_number_from_uri(resource_uri: str) -> str | None:
    """Pull the company number out of a PSC resource URI.

    e.g. ``/company/00102498/persons-with-significant-control/individual/abc``
    -> ``00102498``.
    """
    parts = [p for p in (resource_uri or "").split("/") if p]
    if "company" in parts:
        i = parts.index("company")
        if i + 1 < len(parts):
            return parts[i + 1]
    return None


def load_events(path: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def _fetch_profile(client, number: str, key: str, cache: dict[str, dict]) -> dict[str, Any]:
    """Fetch + cache a company profile via the REST API (best-effort)."""
    if number in cache:
        return cache[number]
    import httpx

    try:
        resp = client.get(
            f"https://api.company-information.service.gov.uk/company/{number}",
            auth=httpx.BasicAuth(key, ""),
        )
        profile = resp.json() if resp.status_code == 200 else {}
    except Exception:  # noqa: BLE001 — profile is optional enrichment
        profile = {}
    cache[number] = profile
    return profile


def map_events(
    events: list[dict[str, Any]], fetch_profiles: bool
) -> tuple[list[dict[str, Any]], Counter]:
    """Map each event to BODS statements. Returns (statements, outcome tally)."""
    from opencheck.bods.mapper import map_companies_house

    outcome: Counter = Counter()
    statements: list[dict[str, Any]] = []

    client = None
    rest_key = os.environ.get("COMPANIES_HOUSE_API_KEY")
    profile_cache: dict[str, dict] = {}
    if fetch_profiles and rest_key:
        import httpx

        client = httpx.Client(timeout=30.0)
    elif fetch_profiles and not rest_key:
        print("  ! --fetch-profiles set but COMPANIES_HOUSE_API_KEY missing — using minimal profiles", file=sys.stderr)

    for ev in events:
        ev_type = (ev.get("event") or {}).get("type") or "?"
        data = ev.get("data")
        number = company_number_from_uri(ev.get("resource_uri", ""))

        if not data:
            # 'deleted' events carry no data block; they need the prior state
            # (an in-memory last-seen map keyed by resource_id) to close the
            # relationship. Out of scope for this offline harness.
            outcome["skipped: deleted / no data block"] += 1
            continue
        if not number:
            outcome["skipped: no company number in resource_uri"] += 1
            continue

        profile: dict[str, Any] = {}
        if client is not None and rest_key:
            profile = _fetch_profile(client, number, rest_key, profile_cache)

        bundle = {
            "source_id": "companies_house",
            "company_number": number,
            "profile": profile,
            "officers": {},
            "pscs": {"items": [data]},
            "related_companies": {},
        }
        try:
            result = map_companies_house(bundle)
        except Exception as exc:  # noqa: BLE001
            outcome[f"ERROR mapping ({type(exc).__name__})"] += 1
            continue

        statements.extend(result.statements)
        label = f"mapped: {ev_type}"
        if data.get("ceased_on"):
            label += " (ceased -> closed record)"
        outcome[label] += 1

    if client is not None:
        client.close()
    return statements, outcome


def validate_libcove(statements: list[dict[str, Any]]) -> tuple[int, int] | None:
    """Run lib-cove-bods. Returns (schema_errors, non_advisory_checks) or None."""
    try:
        from libcovebods.config import LibCoveBODSConfig
        from libcovebods.data_reader import DataReader
        from libcovebods.jsonschemavalidate import JSONSchemaValidator
        from libcovebods.schema import SchemaBODS
        import libcovebods.run_tasks
    except ImportError:
        return None

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(statements, fh)
        tmp = fh.name

    config = LibCoveBODSConfig()
    schema = SchemaBODS(data_reader=DataReader(tmp), lib_cove_bods_config=config)
    js_errors = JSONSchemaValidator(schema).validate(DataReader(tmp))
    additional = libcovebods.run_tasks.process_additional_checks(
        DataReader(tmp), config, schema
    )
    checks = additional.get("additional_checks", []) if isinstance(additional, dict) else list(additional)
    advisory = {"entity_identifiers_not_known_scheme"}
    non_advisory = [a for a in checks if (a.get("type") if isinstance(a, dict) else "") not in advisory]

    print("\n--- lib-cove-bods ---")
    print(f"  JSON-schema errors (hard) : {len(js_errors)}")
    msgs: Counter = Counter(
        (e.json().get("message") if hasattr(e, "json") else str(e)) for e in js_errors
    )
    for m, c in msgs.most_common(15):
        print(f"    {c:>4}x  {m}")
    print(f"  additional checks (non-advisory): {len(non_advisory)}")
    types: Counter = Counter(a.get("type") if isinstance(a, dict) else str(a) for a in non_advisory)
    for t, c in types.most_common(10):
        print(f"    {c:>4}x  {t}")
    return len(js_errors), len(non_advisory)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("input", help="captured PSC stream .jsonl (from capture_psc_stream.py)")
    ap.add_argument(
        "--out",
        default="data/cache/psc_stream/psc_stream_bods.jsonl",
        help="BODS output JSON-Lines path",
    )
    ap.add_argument(
        "--fetch-profiles",
        action="store_true",
        help="enrich subject entities via REST company-profile lookups (needs COMPANIES_HOUSE_API_KEY)",
    )
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    if not in_path.is_file():
        print(f"ERROR: input not found: {in_path}", file=sys.stderr)
        return 2

    events = load_events(str(in_path))
    print(f"Loaded {len(events):,} captured events from {in_path}")

    statements, outcome = map_events(events, args.fetch_profiles)

    # Each event is mapped as its own BODS package, so shared elements (the same
    # company entity, the same person) are re-emitted across events. Concatenating
    # them for bulk validation would create duplicate statementIds; dedupe by
    # statementId (keeps new + closed since those have distinct statementIds).
    # NOTE for the live service: re-asserting an already-seen element should use
    # recordStatus 'updated', not a second 'new' — that needs the same in-memory
    # state map as 'deleted' handling.
    seen_sids: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for s in statements:
        sid = s.get("statementId")
        if sid in seen_sids:
            continue
        seen_sids.add(sid)
        deduped.append(s)
    dropped = len(statements) - len(deduped)
    statements = deduped

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for s in statements:
            fh.write(json.dumps(s, ensure_ascii=False) + "\n")

    print("\n--- mapping outcome ---")
    for label, n in sorted(outcome.items()):
        print(f"  {label:<48} {n:>7,}")
    by_type: Counter = Counter(
        (s.get("recordType") or s.get("statementType") or "?") for s in statements
    )
    print(f"\n  BODS statements written : {len(statements):,}  -> {out_path}")
    print(f"  by type                 : {dict(by_type)}")
    if dropped:
        print(f"  ({dropped} duplicate statement(s) across events collapsed for bulk validation)")

    # In-process shape check (fast, always available).
    try:
        from opencheck.bods.validator import validate_shape

        issues = validate_shape(statements)
        print(f"\n  OpenCheck shape validator: {len(issues)} issue(s)")
        for i in issues[:10]:
            print(f"    - {i}")
    except Exception as exc:  # noqa: BLE001
        print(f"  ! shape validator unavailable: {exc}", file=sys.stderr)

    # Authoritative lib-cove-bods.
    result = validate_libcove(statements)

    print("\n--- verdict ---")
    if result is None:
        print("  lib-cove-bods not installed — run: pip install libcovebods")
        return 1
    schema_errors, _ = result
    print("  SCHEMA:", "PASS (0 hard errors)" if schema_errors == 0 else f"FAIL ({schema_errors})")
    closed = sum(
        n for k, n in outcome.items() if "ceased -> closed" in k
    )
    if closed:
        print(f"  NOTE : {closed} cessation event(s) emitted as BODS 'closed' records")
        print("         (recordStatus=closed, interest.endDate, shared recordId).")
    deleted = outcome.get("skipped: deleted / no data block", 0)
    if deleted:
        print(f"  NOTE : {deleted} 'deleted' event(s) carry no data — the live service")
        print("         needs an in-memory last-state map to close these by recordId.")
    return 0 if schema_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
