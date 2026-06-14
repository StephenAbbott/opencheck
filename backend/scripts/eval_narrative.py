#!/usr/bin/env python3
"""Offline eval harness for the narrative summary feature (Phase 0).

This is where we iterate on prompt wording *before any UI exists*. It loads the
golden evidence packets in ``tests/golden_narrative/``, generates a narrative for
each (when ``ANTHROPIC_API_KEY`` is set), runs the citation validator, and prints
a scorecard against a mechanical rubric.

Usage:
    # offline — validates fixtures + prints the rubric (no model call):
    python scripts/eval_narrative.py --dry-run

    # live — needs ANTHROPIC_API_KEY in the environment:
    ANTHROPIC_API_KEY=sk-ant-... python scripts/eval_narrative.py
    python scripts/eval_narrative.py --model claude-opus-4-6   # A/B a model

The rubric is deliberately checkable without a human:
  * grounded         — every returned claim cited a real packet fact (validator ok)
  * names_subject    — the subject name appears in the summary
  * flags_namematch  — name-matched packets are caveated in the text
  * surfaces_risks   — every packet risk code is mentioned
  * surfaces_gaps    — at least one packet gap is reflected when gaps exist
  * within_length    — summary is a single tight paragraph (<= 7 sentences)
Human review still matters for tone; the rubric just catches regressions fast.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Make ``opencheck`` importable when run from backend/ or repo root.
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from opencheck.narrative import EvidencePacket  # noqa: E402
from opencheck.narrative.summarise import (  # noqa: E402
    DEFAULT_MODEL,
    NarrativeUnavailable,
    summarise,
)

GOLDEN_DIR = BACKEND / "tests" / "golden_narrative"


def load_packets() -> list[tuple[str, EvidencePacket]]:
    packets = []
    for path in sorted(GOLDEN_DIR.glob("*.json")):
        data = json.loads(path.read_text())
        packets.append((path.name, EvidencePacket.model_validate(data)))
    return packets


def _sentences(text: str) -> list[str]:
    return [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s]


def score(packet: EvidencePacket, result_summary: str, validation) -> dict[str, bool]:
    """Mechanical rubric. Coverage is measured by *citation*, not by keyword
    guessing — every risk/gap id should be cited by at least one valid claim — so
    the rubric doesn't penalise good prose for not using a magic word."""
    text = result_summary.lower()
    cited: set[str] = set()
    for claim in validation.valid_claims:
        cited.update(claim.get("fact_ids") or [])
    return {
        "grounded": validation.ok,
        "names_subject": packet.subject_name.lower() in text,
        "flags_namematch": (
            packet.subject_confidence != "name-matched"
            or any(w in text for w in ("name", "name-matched", "not confirmed", "may not"))
        ),
        "surfaces_risks": packet.risk_ids().issubset(cited),
        "surfaces_gaps": packet.gap_ids().issubset(cited),
        "no_raw_codes": not any(r.code in result_summary for r in packet.risks),
        "within_length": 1 <= len(_sentences(result_summary)) <= 7,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=os.environ.get("OPENCHECK_NARRATIVE_MODEL", DEFAULT_MODEL))
    ap.add_argument("--dry-run", action="store_true", help="validate fixtures only; no model call")
    ap.add_argument("--show", action="store_true", help="print each generated paragraph")
    args = ap.parse_args()

    packets = load_packets()
    print(f"Loaded {len(packets)} golden packet(s) from {GOLDEN_DIR.relative_to(BACKEND)}\n")

    # Prefer the env var; fall back to backend/.env (same source every other
    # OpenCheck credential comes from) so the key need not be on the command line.
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        try:
            from opencheck.config import get_settings

            api_key = get_settings().anthropic_api_key
        except Exception:
            api_key = None
    if api_key:
        api_key = api_key.strip()  # guard against a copied trailing newline/space

    if args.dry_run or not api_key:
        if not args.dry_run:
            print("No ANTHROPIC_API_KEY found (checked env and backend/.env) — "
                  "running fixture validation only.")
            print("Add the key to backend/.env or export it, then re-run.\n")
        for name, packet in packets:
            print(f"✓ {name}: {len(packet.facts)} facts, {len(packet.risks)} risks, "
                  f"{len(packet.gaps)} gaps, confidence={packet.subject_confidence}")
        return 0

    rubric_keys = ["grounded", "names_subject", "flags_namematch", "surfaces_risks",
                   "surfaces_gaps", "no_raw_codes", "within_length"]
    totals = {k: 0 for k in rubric_keys}

    for name, packet in packets:
        try:
            result = summarise(packet, api_key=api_key, model=args.model)
        except NarrativeUnavailable as e:
            print(f"✗ {name}: {e}")
            continue
        card = score(packet, result.summary, result.validation)
        for k, v in card.items():
            totals[k] += int(v)
        marks = " ".join(f"{k}={'Y' if card[k] else 'N'}" for k in rubric_keys)
        print(f"• {name}\n    {marks}")
        if result.validation.dropped_claims:
            print(f"    DROPPED {len(result.validation.dropped_claims)} claim(s): "
                  f"{result.validation.issues}")
        if args.show:
            print(f"    “{result.summary}”")
        print()

    n = len(packets)
    print(f"Model: {args.model}   ({n} packets)")
    print("Rubric pass rates:")
    for k in rubric_keys:
        print(f"  {k:<16} {totals[k]}/{n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
