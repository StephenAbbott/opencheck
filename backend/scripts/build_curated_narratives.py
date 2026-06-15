#!/usr/bin/env python3
"""Pre-bake AI summaries for the curated homepage examples.

Curated examples use deterministic, pre-extracted bulk BODS data, so their
narratives are stable. Generating them once and serving the result as a static
file (``frontend/public/curated-narratives/<lei>.json``) means a first-time
visitor sees an instant, fully-cited summary with **no model call** — while live
lookups keep the on-demand "Generate summary" button.

Run once (and whenever the prompt or curated set changes), then commit the JSON:

    cd backend
    ANTHROPIC_API_KEY=sk-ant-... uv run python scripts/build_curated_narratives.py

Options via env:
    OPENCHECK_API_BASE        default https://api.opencheck.world
    OPENCHECK_NARRATIVE_MODEL default claude-sonnet-4-6
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx

# Repo paths.
BACKEND = Path(__file__).resolve().parents[1]
REPO = BACKEND.parent
OUT_DIR = REPO / "frontend" / "public" / "curated-narratives"

sys.path.insert(0, str(BACKEND))

from opencheck.narrative import build_evidence_packet  # noqa: E402
from opencheck.narrative.summarise import DEFAULT_MODEL, summarise  # noqa: E402

# Keep in sync with EXAMPLE_LEIS in frontend/src/App.tsx.
CURATED_LEIS = [
    "4OFD47D73QFJ1T1MOF29",  # Daily Mail and General Trust
    "213800LH1BZH3DI6G760",  # BP P.L.C.
    "253400JT3MQWNDKMJE44",  # Rosneft
    "2138008KTNTDICZU8L25",  # Bank Saderat PLC
    "2138008RB4WDK7HYYS91",  # Biffa PLC
    "2138002S3XGZ38WN5Q72",  # Hornsea 1 Limited
    "213800DBE5Y9ZM58PN63",  # Care UK Social Care
    "213800E11LI1SCETU492",  # Taqa Bratani Limited
    "213800AG2V6YE68H5N63",  # Newcastle United FC
]


def _narrative_response(report: dict, api_key: str, model: str) -> dict:
    """Mirror the shape returned by GET /narrative so the static file renders
    identically in the panel."""
    packet = build_evidence_packet(report)
    result = summarise(packet, api_key=api_key, model=model)
    return {
        "lei": packet.lei,
        "subject_name": packet.subject_name,
        "summary": result.summary,
        "claims": result.claims,
        "limitations": result.limitations,
        "overall_confidence": result.overall_confidence,
        "model": result.model,
        "prompt_version": result.prompt_version,
        "packet": packet.model_dump(),
        "validation_ok": result.validation.ok,
        "dropped_claims": result.validation.dropped_claims,
        "validation_issues": result.validation.issues,
    }


def main() -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY is not set — cannot generate summaries.", file=sys.stderr)
        return 1
    model = os.environ.get("OPENCHECK_NARRATIVE_MODEL", DEFAULT_MODEL)
    base = os.environ.get("OPENCHECK_API_BASE", "https://api.opencheck.world").rstrip("/")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ok = 0
    with httpx.Client(timeout=120.0) as client:
        for lei in CURATED_LEIS:
            try:
                r = client.get(f"{base}/lookup", params={"lei": lei, "deepen_top": 5})
                r.raise_for_status()
                report = r.json()
                payload = _narrative_response(report, api_key, model)
            except Exception as exc:  # noqa: BLE001 — report and continue
                print(f"✗ {lei}: {exc}", file=sys.stderr)
                continue
            out = OUT_DIR / f"{lei}.json"
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
            conf = payload["overall_confidence"]
            print(f"✓ {lei} → {out.relative_to(REPO)}  ({conf} confidence)")
            ok += 1

    print(f"\nGenerated {ok}/{len(CURATED_LEIS)} curated summaries with {model}.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
