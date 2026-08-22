"""Every signal code the backend can emit must have a label everywhere.

Four places name signal codes, and until now nothing checked any of them
against the backend's actual code list:

* ``og_image.SIGNAL_STYLE``                       — the OG share card
* ``RiskChip.RISK_PRESENTATION``                  — the results-page chip
* ``lib/graphStyle.SIGNAL_STYLE``                 — the graph node badge
  and, since Phase 124, the generated graph legend
* ``narrative.packet._RISK_LABELS``               — the LLM-facing label

``RiskChip.test.ts`` asserts every *graph badge* has a chip, but that is
one-directional and starts from the frontend, so a code the backend emits
and the frontend has never heard of passes silently. That is how
``SANCTIONED_SECURITY`` reached production with no graph badge at all,
falling back to a grey "!" at severity 0 — a sanctions-family finding
rendered as the lowest-priority marker on the graph.

The enumeration relies on signal codes being declared as module constants
with ``NAME == "NAME"``. That is not incidental: a code that exists only
as a string literal inside a function body cannot be enumerated, and so
cannot be checked. ``SANCTIONED_SECURITY`` was exactly that case and has
been promoted to a constant. Keep new codes declared the same way.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: Modules that declare emittable signal codes.
_CODE_MODULES = ("opencheck.risk", "opencheck.cross_check", "opencheck.securities")


def _backend_codes() -> set[str]:
    """Every signal code the backend can emit.

    Heuristic: module-level ``UPPER_NAME = "UPPER_NAME"``. Constants whose
    value differs from their name (``DEGRADED_TIMEOUT = "timeout"``,
    ``EU_HRTC_INSTRUMENT = "Delegated Regulation …"``) are configuration or
    prose, not codes, and are correctly excluded.
    """
    codes: set[str] = set()
    for name in _CODE_MODULES:
        mod = importlib.import_module(name)
        codes |= {
            n
            for n, v in vars(mod).items()
            if n.isupper() and isinstance(v, str) and v == n
        }
    return codes


def _keys_from_ts(path: Path, const: str) -> set[str]:
    """Top-level UPPER_SNAKE keys of a `const X: Record<...> = { ... }`."""
    text = path.read_text(encoding="utf-8")
    # Anchor on the ASSIGNMENT brace, not the first brace after the name —
    # the type annotation (`Record<string, { label: string }> = {`) contains
    # braces of its own, and latching onto those parses the wrong object.
    start = text.index(f"const {const}")
    assign = text.index("= {", start)
    depth, i, body_start = 0, assign, None
    for i in range(assign, len(text)):
        if text[i] == "{":
            depth += 1
            if depth == 1:
                body_start = i
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                break
    body = text[body_start : i + 1]
    # Keys at nesting depth 1 only — skip the nested style objects.
    return set(re.findall(r"^\s{2}([A-Z][A-Z0-9_]+)\s*:", body, re.M))


def test_backend_codes_are_enumerable() -> None:
    """Guards the heuristic itself — if this drops, coverage silently shrinks."""
    codes = _backend_codes()
    assert len(codes) >= 24, f"only found {len(codes)} codes: {sorted(codes)}"
    # Spot-check one code from each module so a module going missing fails here.
    assert {"SANCTIONED", "NON_EU_JURISDICTION"} <= codes  # risk
    assert "RELATED_PEP" in codes  # cross_check
    assert "SANCTIONED_SECURITY" in codes  # securities


def test_og_image_style_covers_every_code() -> None:
    from opencheck.og_image import SIGNAL_STYLE

    missing = sorted(_backend_codes() - set(SIGNAL_STYLE))
    assert not missing, f"og_image.SIGNAL_STYLE is missing: {missing}"


def test_narrative_labels_cover_every_code() -> None:
    from opencheck.narrative.packet import _RISK_LABELS

    missing = sorted(_backend_codes() - set(_RISK_LABELS))
    assert not missing, f"narrative packet _RISK_LABELS is missing: {missing}"


@pytest.mark.parametrize(
    ("rel_path", "const"),
    [
        ("frontend/src/components/risk/RiskChip.tsx", "RISK_PRESENTATION"),
        # Phase 124 moved SIGNAL_STYLE out of the component into
        # lib/graphStyle.ts, so the legend can be generated from it without
        # importing Cytoscape. BODSGraph re-exports it for existing callers.
        ("frontend/src/lib/graphStyle.ts", "SIGNAL_STYLE"),
    ],
)
def test_frontend_maps_cover_every_code(rel_path: str, const: str) -> None:
    """Read the TS maps as text.

    Crude, deliberately: the alternative is a hand-maintained list on the
    frontend side, which is another copy of the thing that drifts. Parsing
    the real source means the check cannot pass while the shipped map is
    wrong.
    """
    path = REPO / rel_path
    assert path.exists(), f"{rel_path} moved — update this test"
    missing = sorted(_backend_codes() - _keys_from_ts(path, const))
    assert not missing, f"{rel_path}:{const} is missing: {missing}"


def test_maps_do_not_carry_codes_the_backend_cannot_emit() -> None:
    """The other direction — a label for a code that no longer exists is
    dead weight and, worse, implies the engine still produces it."""
    from opencheck.og_image import SIGNAL_STYLE

    codes = _backend_codes()
    stale = sorted(set(SIGNAL_STYLE) - codes)
    assert not stale, f"og_image.SIGNAL_STYLE has codes the backend cannot emit: {stale}"
