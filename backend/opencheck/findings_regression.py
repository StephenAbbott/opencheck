"""Weekly findings regression: what the engine *finds* for a golden set (Phase 277).

The source-health sweep (Phase 121) asks whether every adapter is *alive*.
Nothing asked what the risk engine *concludes*, and the gap showed: Phases
126, 127, 132 and 139 all begin "Stephen found it in production", and the
three defects of 2 Sept (verdict wording, MCP shaping, per-card chip
duplication) sat on the Shell report for weeks. This is the sweep's sibling:
the same weekly workflow shape, a different assertion.

What it reads
-------------

Production, not the pipeline in-process: ``GET /lookup?refresh=true`` on the
deployed API, then the MCP ``opencheck_lookup`` tool, which replays the same
run from the server's cache at no cost. The point is to test what a reader
sees, deploy state included.

The golden set is ``backend/findings_golden/*.json``, one file per LEI: the
six curated homepage examples, Shell, three anchors that Phases 272/273
verified in production (Maersk, Bank Saderat PLC, ASDA Stores), Phase 282's
Regulatory DataCorp (a person false positive that must stay gone) and two
clean controls. Each file states **lower bounds and shapes**, never a snapshot —
upstream data legitimately changes, and a genuine change updates the file in
a reviewed PR, which is itself the audit trail.

What fails a run, by failure class
----------------------------------

Every finding names its class, because the class says where to look:

``kind_mismatch``
    A code expected as a risk finding arrives as context, or the reverse;
    a signal with no ``kind`` at all; one code carrying both kinds in one
    lookup. The Phase 111 class.
``missing_signal`` / ``unexpected_signal``
    An expected code is absent or below its confidence floor; a code the
    file rules out is present; a control (``no_risk``) carries a risk
    finding; a retired code (``risk.RETIRED_SIGNAL_CODES``) reappears.
``structural_repeated``
    A code the lookup collapses to one per run (``_STRUCTURAL_SIGNAL_CODES``)
    appears twice. The API half of the 2 Sept per-card duplication — the UI
    half went with Phase 245, which took structural chips off source cards.
``degraded_reads_clean``
    ``degraded_sources`` is non-empty, no risk finding was made, and the
    verdict says nothing about a source or screen that did not answer — an
    absence stated without its caveat. The Phase 146 class. (A verdict that
    states a finding carries no caveat by design, so it is not checked.)
``placeholder_badge``
    A source shows records while its liveness resolves ``stub`` — real data
    badged "Placeholder data", the Ariregister/ONRC class, seen live.
``card_drift``
    ``EXAMPLE_LEIS`` (the homepage cards) differs from the live risk codes or
    their highest confidence. The cards claim "every risk code production
    returns", so the comparison is the whole set.
``verdict``, ``source_not_found``, ``liveness``, ``lei_confirmation``,
``layers``, ``lei_registration``, ``mcp_mismatch``, ``lookup_failed``
    The remaining per-file shapes, and the MCP tool disagreeing with
    ``/lookup`` about the same run.

What it does not assert: the label maps (og_image, RiskChip, graphStyle,
narrative packet) against the backend's code list. ``tests/test_signal_label_coverage.py``
does that on every PR, offline, which is the better place for it.

Week-over-week
--------------

Pass/fail is the alarm; the diff is the interesting output. Each report
carries every LEI's actual signals, and the next run lists what appeared and
disappeared ("Rosneft: +RELATED_EXPORT_CONTROLLED (openaleph)"). The report
also records the rule stamps the watchlist uses — ``VERDICT_TEMPLATE`` and
``SIGNAL_RULES`` — so a change caused by a rule rather than by the company is
labelled as such, the way the watchlist suppresses it. The comparison is
against the last *published* report, not the last successful run: the
source-health sweep's "last success" baseline keeps a legitimate change red
every week.

The stamps are read from the checked-out code, which matches production only
once it is deployed; the report says which commit ran.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BACKEND = Path(__file__).resolve().parent.parent
GOLDEN_DIR = BACKEND / "findings_golden"
EXAMPLE_CARDS_TSX = BACKEND.parent / "frontend" / "src" / "components" / "HomePanels.tsx"

ROLES = ("curated", "anchor", "control")
CONFIDENCE_RANK: dict[str, int] = {"low": 1, "medium": 2, "high": 3}
KINDS = ("risk", "context")

#: Consumers (MCP shaping, ``signalKind.ts``) read a missing ``kind`` as risk.
DEFAULT_KIND = "risk"

#: Verdict stems ``verdict._incomplete_phrase`` builds. Matched loosely, so a
#: rewording that keeps the meaning does not red the run on the day it ships.
_INCOMPLETE_VERDICT = re.compile(r"did not run|did not answer|answered only in part", re.I)

#: Pause between LEIs. One fresh lookup a minute stays inside the per-IP lookup
#: budget (``OPENCHECK_RATE_LIMIT_LOOKUP``, 10/min) and leaves the process-wide
#: GLEIF throttle (50/min) to readers.
DEFAULT_PACE_S = 60.0
MAX_RETRIES = 2
_MAX_RETRY_WAIT_S = 120.0

FAIL = "fail"
WARN = "warn"


# --- expectations -------------------------------------------------------------


_REQUIRED: dict[str, type | tuple[type, ...]] = {
    "lei": str,
    "name": str,
    "role": str,
    "why": str,
    "example_card": bool,
    "no_risk": bool,
    "risk": dict,
    "context": list,
    "absent": list,
    "verdict": dict,
    "sources_found": list,
    "liveness": dict,
    "lei_confirmed_min": int,
    "intermediate_layers_min": (int, type(None)),
}
_LEI = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")


def validate_expectation(data: Mapping[str, Any], name: str = "<expectation>") -> None:
    """Raise ``ValueError`` naming the file and field when ``data`` is malformed."""
    for key, typ in _REQUIRED.items():
        if key not in data:
            raise ValueError(f"{name}: missing {key!r}")
        # bool is an int; a count written as true must not pass.
        if typ is int and isinstance(data[key], bool):
            raise ValueError(f"{name}: {key!r} must be an integer")
        if not isinstance(data[key], typ):
            raise ValueError(f"{name}: {key!r} has the wrong type")
    extra = set(data) - set(_REQUIRED)
    if extra:
        raise ValueError(f"{name}: unknown field(s) {sorted(extra)}")
    if not _LEI.match(data["lei"]):
        raise ValueError(f"{name}: {data['lei']!r} is not an LEI")
    if data["role"] not in ROLES:
        raise ValueError(f"{name}: role must be one of {ROLES}")
    for code, conf in data["risk"].items():
        if conf not in CONFIDENCE_RANK:
            raise ValueError(f"{name}: risk[{code!r}] confidence {conf!r} is not low/medium/high")
    if data["no_risk"] and data["risk"]:
        raise ValueError(f"{name}: no_risk files cannot also expect risk codes")
    clash = (set(data["risk"]) | set(data["context"])) & set(data["absent"])
    if clash:
        raise ValueError(f"{name}: {sorted(clash)} both expected and ruled out")
    both = set(data["risk"]) & set(data["context"])
    if both:
        raise ValueError(f"{name}: {sorted(both)} expected as both risk and context")
    verdict = data["verdict"]
    if set(verdict) != {"matches", "not_matches"}:
        raise ValueError(f"{name}: verdict needs exactly 'matches' and 'not_matches'")
    for pattern in [*verdict["matches"], *verdict["not_matches"]]:
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"{name}: bad verdict pattern {pattern!r}: {exc}") from exc
    for sid, allowed in data["liveness"].items():
        if not isinstance(allowed, list) or not allowed:
            raise ValueError(f"{name}: liveness[{sid!r}] must be a non-empty list")
    if data["lei_confirmed_min"] < 0:
        raise ValueError(f"{name}: lei_confirmed_min cannot be negative")


def load_expectations(directory: Path = GOLDEN_DIR) -> list[dict[str, Any]]:
    """Every golden file, validated, in file-name order."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        validate_expectation(data, path.name)
        if data["lei"] in seen:
            raise ValueError(f"{path.name}: {data['lei']} appears in two files")
        seen.add(data["lei"])
        data["_file"] = path.name
        out.append(data)
    return out


# --- the homepage cards --------------------------------------------------------


_CARD_BLOCK = re.compile(r"export const EXAMPLE_LEIS\b[^=]*=\s*\[(?P<body>.*?)\n\];", re.S)
_CARD_LEI = re.compile(r'lei:\s*"(?P<lei>[0-9A-Z]{20})"')
_CARD_SIGNAL = re.compile(r'\{\s*code:\s*"(?P<code>[A-Z_]+)",\s*confidence:\s*"(?P<conf>\w+)"\s*\}')


def parse_example_cards(tsx: str) -> dict[str, dict[str, str]]:
    """``EXAMPLE_LEIS`` from ``HomePanels.tsx`` as ``{lei: {code: confidence}}``.

    A parse, not an import: the regression runs in the backend environment.
    Raises ``ValueError`` when the block cannot be found, so a refactor of the
    file fails loudly instead of turning every card check into a silent pass.
    """
    block = _CARD_BLOCK.search(tsx)
    if not block:
        raise ValueError("EXAMPLE_LEIS block not found in HomePanels.tsx")
    body = block.group("body")
    starts = [(m.start(), m.group("lei")) for m in _CARD_LEI.finditer(body)]
    if not starts:
        raise ValueError("EXAMPLE_LEIS has no entries")
    cards: dict[str, dict[str, str]] = {}
    for i, (pos, lei) in enumerate(starts):
        end = starts[i + 1][0] if i + 1 < len(starts) else len(body)
        cards[lei] = {
            m.group("code"): m.group("conf") for m in _CARD_SIGNAL.finditer(body[pos:end])
        }
    return cards


def load_example_cards(path: Path = EXAMPLE_CARDS_TSX) -> dict[str, dict[str, str]]:
    return parse_example_cards(path.read_text(encoding="utf-8"))


# --- reading a lookup ------------------------------------------------------------


def _kind(sig: Mapping[str, Any]) -> str:
    return str(sig.get("kind") or DEFAULT_KIND)


def _higher(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if CONFIDENCE_RANK.get(a, 0) >= CONFIDENCE_RANK.get(b, 0) else b


def lei_confirming_sources(payload: Mapping[str, Any], lei: str) -> list[str]:
    """Independent sources publishing the subject's LEI — the SubjectCard badge.

    Mirrors ``countLeiConfirmingSources`` in ``frontend/src/lib/identifierBadge.ts``,
    collapsed by the same lineage rules (``sources.lineage``).
    """
    from .sources.lineage import independent_sources

    target = lei.strip().upper()
    sources: set[str] = set()
    for link in payload.get("cross_source_links") or []:
        if link.get("key") != "lei":
            continue
        if str(link.get("key_value", "")).strip().upper() != target:
            continue
        sources |= {str(h.get("source_id")) for h in link.get("hits") or [] if h.get("source_id")}
    return list(independent_sources(sorted(sources)))


def found_sources(payload: Mapping[str, Any]) -> list[str]:
    """Sources that returned a record (a non-stub hit)."""
    return sorted(
        {
            str(h["source_id"])
            for h in payload.get("hits") or []
            if h.get("source_id") and not h.get("is_stub")
        }
    )


def summarise(payload: Mapping[str, Any], lei: str) -> dict[str, Any]:
    """What a lookup found, in the shape the report stores and next week diffs.

    Codes only, with their kind, highest confidence and the sources that said
    them — never summaries or party names.
    """
    signals: dict[str, dict[str, Any]] = {}
    for sig in payload.get("risk_signals") or []:
        code = sig.get("code")
        if not code:
            continue
        row = signals.setdefault(
            str(code), {"kinds": [], "confidence": None, "sources": [], "count": 0}
        )
        if _kind(sig) not in row["kinds"]:
            row["kinds"].append(_kind(sig))
        row["confidence"] = _higher(row["confidence"], sig.get("confidence"))
        sid = sig.get("source_id")
        if sid and sid not in row["sources"]:
            row["sources"].append(sid)
        row["count"] += 1
    for row in signals.values():
        row["kinds"].sort()
        row["sources"].sort()
    shape = payload.get("graph_shape") or {}
    reg = (payload.get("subject_profile") or {}).get("lei_registration") or {}
    return {
        "legal_name": payload.get("legal_name"),
        "verdict": payload.get("verdict"),
        "signals": dict(sorted(signals.items())),
        "degraded": sorted(
            f"{d.get('source_id')}:{d.get('check')}" for d in payload.get("degraded_sources") or []
        ),
        "found": found_sources(payload),
        "lei_confirmed_by": lei_confirming_sources(payload, lei),
        "intermediate_layers": shape.get("intermediate_layers"),
        "lei_registration": reg.get("status"),
        "replayed": bool(payload.get("replayed")),
        "run_completed_at": payload.get("run_completed_at"),
    }


# --- evaluation -------------------------------------------------------------------


@dataclass
class Finding:
    check: str
    message: str
    severity: str = FAIL

    def to_dict(self) -> dict[str, str]:
        return {"check": self.check, "severity": self.severity, "message": self.message}


@dataclass
class Rules:
    """The engine constants the assertions read, injectable for tests."""

    structural_codes: frozenset[str]
    retired_codes: frozenset[str]

    @classmethod
    def from_code(cls) -> Rules:
        from . import risk
        from .routers.lookup import _STRUCTURAL_SIGNAL_CODES

        return cls(
            structural_codes=frozenset(_STRUCTURAL_SIGNAL_CODES),
            retired_codes=frozenset(getattr(risk, "RETIRED_SIGNAL_CODES", frozenset())),
        )


def _risk_codes(summary: Mapping[str, Any]) -> dict[str, str]:
    """Risk-kind codes with their highest confidence — what a card claims."""
    out: dict[str, str] = {}
    for code, row in summary["signals"].items():
        if "risk" in row["kinds"] and row["confidence"]:
            out[code] = row["confidence"]
    return out


def evaluate(
    exp: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    rules: Rules,
    cards: Mapping[str, Mapping[str, str]] | None = None,
    mcp: Mapping[str, Any] | None = None,
) -> tuple[list[Finding], dict[str, Any]]:
    """Check one lookup against its expectation file."""
    lei = exp["lei"]
    summary = summarise(payload, lei)
    sig = summary["signals"]
    findings: list[Finding] = []

    def fail(check: str, message: str) -> None:
        findings.append(Finding(check, message))

    # kind_mismatch — the contract on every signal, then the file's claims.
    for raw in payload.get("risk_signals") or []:
        if raw.get("code") and raw.get("kind") not in KINDS:
            fail(
                "kind_mismatch",
                f"{raw['code']} ({raw.get('source_id')}) carries no valid kind "
                f"({raw.get('kind')!r}); consumers will read it as {DEFAULT_KIND}",
            )
    for code, row in sig.items():
        if len(row["kinds"]) > 1:
            fail("kind_mismatch", f"{code} arrives as both risk and context in one lookup")
    for code in exp["risk"]:
        if code in sig and "risk" not in sig[code]["kinds"]:
            fail(
                "kind_mismatch",
                f"{code} is expected as a risk finding but arrives as {sig[code]['kinds']}",
            )
    for code in exp["context"]:
        if code in sig and "context" not in sig[code]["kinds"]:
            fail(
                "kind_mismatch",
                f"{code} is expected as context but arrives as {sig[code]['kinds']}",
            )

    # missing_signal / unexpected_signal
    for code, floor in exp["risk"].items():
        if code not in sig:
            fail("missing_signal", f"expected risk code {code} (≥ {floor}) is absent")
            continue
        conf = sig[code]["confidence"]
        if CONFIDENCE_RANK.get(conf or "", 0) < CONFIDENCE_RANK[floor]:
            fail("missing_signal", f"{code} is at {conf}, below the expected floor {floor}")
    for code in exp["context"]:
        if code not in sig:
            fail("missing_signal", f"expected context code {code} is absent")
    for code in exp["absent"]:
        if code in sig:
            fail(
                "unexpected_signal",
                f"{code} is present; this subject rules it out ({', '.join(sig[code]['sources'])})",
            )
    for code in sorted(rules.retired_codes & set(sig)):
        fail("unexpected_signal", f"retired code {code} is emitted again")
    if exp["no_risk"]:
        risky = sorted(_risk_codes(summary))
        if risky:
            fail("unexpected_signal", f"clean subject carries risk finding(s): {', '.join(risky)}")

    # structural_repeated — the lookup collapses these to one per run.
    for code in sorted(rules.structural_codes & set(sig)):
        if sig[code]["count"] > 1:
            fail(
                "structural_repeated",
                f"{code} appears {sig[code]['count']} times ({', '.join(sig[code]['sources'])}); "
                "it should be one signal per lookup",
            )

    # degraded_reads_clean — only where the verdict claims an absence. A
    # sentence stating a finding carries no completeness caveat by design
    # ("we found X" stays true whatever else failed; ``verdict.build_verdict``),
    # so the check applies when no non-structural risk finding was made.
    verdict = summary["verdict"] or ""
    findings_made = set(_risk_codes(summary)) - rules.structural_codes
    if summary["degraded"] and not findings_made and not _INCOMPLETE_VERDICT.search(verdict):
        fail(
            "degraded_reads_clean",
            f"{len(summary['degraded'])} source(s)/screen(s) degraded "
            f"({', '.join(summary['degraded'])}) but the verdict does not say so: {verdict!r}",
        )

    # verdict shape
    if not verdict:
        fail("verdict", "no verdict sentence")
    for pattern in exp["verdict"]["matches"]:
        if not re.search(pattern, verdict):
            fail("verdict", f"verdict does not match /{pattern}/: {verdict!r}")
    for pattern in exp["verdict"]["not_matches"]:
        if re.search(pattern, verdict):
            fail("verdict", f"verdict matches the ruled-out /{pattern}/: {verdict!r}")

    # sources, liveness, placeholder badges
    liveness = payload.get("source_liveness") or {}
    found = set(summary["found"])
    for sid in exp["sources_found"]:
        if sid not in found:
            fail("source_not_found", f"{sid} returned no record")
    for sid, allowed in exp["liveness"].items():
        got = (liveness.get(sid) or {}).get("liveness")
        if got not in allowed:
            fail("liveness", f"{sid} liveness is {got!r}, expected one of {allowed}")
    for sid in sorted(found):
        if (liveness.get(sid) or {}).get("liveness") == "stub":
            fail(
                "placeholder_badge",
                f"{sid} returned records but is badged as placeholder data (liveness 'stub')",
            )

    # LEI confirmation, layers, LEI registration
    confirmed = len(summary["lei_confirmed_by"])
    if confirmed < exp["lei_confirmed_min"]:
        fail(
            "lei_confirmation",
            f"LEI confirmed by {confirmed} independent source(s) "
            f"({', '.join(summary['lei_confirmed_by']) or 'none'}), expected ≥ {exp['lei_confirmed_min']}",
        )
    floor_layers = exp["intermediate_layers_min"]
    if floor_layers is not None:
        layers = summary["intermediate_layers"]
        if not isinstance(layers, int) or layers < floor_layers:
            fail(
                "layers",
                f"graph_shape.intermediate_layers is {layers!r}, expected ≥ {floor_layers}",
            )
    if not summary["lei_registration"]:
        fail("lei_registration", "subject_profile carries no LEI registration status (Phase 242)")

    # card_drift
    if exp["example_card"]:
        card = (cards or {}).get(lei)
        if card is None:
            fail("card_drift", "no EXAMPLE_LEIS card for this curated subject")
        else:
            live = _risk_codes(summary)
            for code in sorted(set(live) - set(card)):
                fail(
                    "card_drift",
                    f"production returns {code} ({live[code]}); the card does not list it",
                )
            for code in sorted(set(card) - set(live)):
                fail(
                    "card_drift",
                    f"the card lists {code} ({card[code]}); production no longer returns it",
                )
            for code in sorted(set(card) & set(live)):
                if card[code] != live[code]:
                    fail(
                        "card_drift",
                        f"{code}: the card says {card[code]}, production's highest is {live[code]}",
                    )

    # mcp_mismatch — the same run, through the MCP shaping.
    if mcp is not None:
        if mcp.get("error"):
            findings.append(
                Finding("mcp_mismatch", f"MCP opencheck_lookup failed: {mcp['error']}", WARN)
            )
        else:
            findings.extend(_compare_mcp(payload, mcp))

    return findings, summary


def _codes_by_kind(signals: Iterable[Mapping[str, Any]]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {k: set() for k in KINDS}
    for s in signals:
        if s.get("code"):
            out.setdefault(_kind(s), set()).add(str(s["code"]))
    return out


def _compare_mcp(payload: Mapping[str, Any], mcp: Mapping[str, Any]) -> list[Finding]:
    out: list[Finding] = []
    rest = _codes_by_kind(payload.get("risk_signals") or [])
    tool = _codes_by_kind(mcp.get("risk_signals") or [])
    for kind in KINDS:
        only_rest = sorted(rest.get(kind, set()) - tool.get(kind, set()))
        only_tool = sorted(tool.get(kind, set()) - rest.get(kind, set()))
        if only_rest or only_tool:
            out.append(
                Finding(
                    "mcp_mismatch",
                    f"{kind} codes differ — only in /lookup: {only_rest or 'none'}; "
                    f"only in MCP: {only_tool or 'none'}",
                )
            )
    if (mcp.get("verdict") or "") != (payload.get("verdict") or ""):
        out.append(
            Finding("mcp_mismatch", f"MCP verdict {mcp.get('verdict')!r} differs from /lookup's")
        )
    rest_deg = len(payload.get("degraded_sources") or [])
    tool_deg = len(mcp.get("degraded_sources") or [])
    if rest_deg != tool_deg:
        out.append(Finding("mcp_mismatch", f"degraded_sources: /lookup {rest_deg}, MCP {tool_deg}"))
    counts = mcp.get("counts") or {}
    risk_rows = sum(1 for r in mcp.get("risk_signals") or [] if _kind(r) == "risk")
    if counts.get("risk_signals") not in (None, risk_rows):
        out.append(
            Finding(
                "mcp_mismatch",
                f"MCP counts.risk_signals {counts.get('risk_signals')} ≠ its {risk_rows} risk rows",
            )
        )
    applicable = len(set(payload.get("sources_applicable") or []) | {"gleif"})
    if counts.get("sources_applicable") not in (None, applicable):
        out.append(
            Finding(
                "mcp_mismatch",
                f"MCP counts {counts.get('sources_applicable')} applicable sources; /lookup announces {applicable}",
            )
        )
    return out


# --- week over week ------------------------------------------------------------------


def rule_stamps() -> dict[str, Any]:
    """The watchlist's "rule changed, not the company" stamps (Phases 245, 273)."""
    from . import watchlist
    from .verdict import VERDICT_TEMPLATE

    return {
        "verdict_template": VERDICT_TEMPLATE,
        "signal_rules": watchlist.SIGNAL_RULES,
        "signal_rules_changed": {
            str(k): sorted(v) for k, v in watchlist.SIGNAL_RULES_CHANGED.items()
        },
    }


def _rule_moved_codes(prev: Mapping[str, Any], now: Mapping[str, Any]) -> tuple[set[str], bool]:
    """Codes whose rules changed between two stamp sets, and whether the verdict template did."""
    before = int(prev.get("signal_rules") or 0)
    after = int(now.get("signal_rules") or 0)
    changed = now.get("signal_rules_changed") or {}
    moved: set[str] = set()
    for version in range(before + 1, after + 1):
        moved |= set(changed.get(str(version), []))
    template_moved = prev.get("verdict_template") is not None and prev.get(
        "verdict_template"
    ) != now.get("verdict_template")
    return moved, template_moved


def diff_against(
    previous: Mapping[str, Any] | None, report: Mapping[str, Any]
) -> dict[str, list[str]]:
    """Per-LEI change lines against the last published report.

    A change on a code whose rule moved between the two runs is labelled
    "(rule change)" rather than reported as the company changing.
    """
    if not previous:
        return {}
    prev_subjects = {s["lei"]: s for s in previous.get("subjects", []) if s.get("summary")}
    moved, template_moved = _rule_moved_codes(
        previous.get("rules") or {}, report.get("rules") or {}
    )
    out: dict[str, list[str]] = {}
    for subj in report.get("subjects", []):
        before = prev_subjects.get(subj["lei"])
        now = subj.get("summary")
        if not before or not now:
            continue
        lines: list[str] = []
        a, b = before["summary"]["signals"], now["signals"]
        for code in sorted(set(b) - set(a)):
            tag = " (rule change)" if code in moved else ""
            lines.append(f"+{code} ({', '.join(b[code]['sources'])}){tag}")
        for code in sorted(set(a) - set(b)):
            tag = " (rule change)" if code in moved else ""
            lines.append(f"−{code} ({', '.join(a[code]['sources'])}){tag}")
        for code in sorted(set(a) & set(b)):
            if a[code]["kinds"] != b[code]["kinds"]:
                lines.append(
                    f"{code}: kind {'/'.join(a[code]['kinds'])} → {'/'.join(b[code]['kinds'])}"
                )
            if a[code]["confidence"] != b[code]["confidence"]:
                lines.append(
                    f"{code}: confidence {a[code]['confidence']} → {b[code]['confidence']}"
                )
            new_src = sorted(set(b[code]["sources"]) - set(a[code]["sources"]))
            gone_src = sorted(set(a[code]["sources"]) - set(b[code]["sources"]))
            if new_src or gone_src:
                lines.append(
                    f"{code}: sources {'+' + ', +'.join(new_src) if new_src else ''}"
                    f"{' ' if new_src and gone_src else ''}{'−' + ', −'.join(gone_src) if gone_src else ''}".rstrip()
                )
        if (before["summary"].get("verdict") or "") != (now.get("verdict") or ""):
            tag = " (verdict template changed)" if template_moved else ""
            lines.append(f"verdict changed{tag}: {now.get('verdict')!r}")
        if lines:
            out[subj["lei"]] = lines
    return out


# --- running against production ---------------------------------------------------


Sleep = Callable[[float], None]
McpCall = Callable[[str], Awaitable[dict[str, Any]]]


@dataclass
class Fetched:
    payload: dict[str, Any] | None = None
    error: str | None = None
    seconds: float = 0.0
    attempts: int = 0
    waits: list[float] = field(default_factory=list)


def fetch_lookup(
    client: Any,
    base_url: str,
    lei: str,
    *,
    sleep: Sleep = time.sleep,
    retries: int = MAX_RETRIES,
) -> Fetched:
    """``GET /lookup?refresh=true``, honouring ``Retry-After`` on 429 and 503.

    ``client`` is an ``httpx.Client`` (or anything with the same ``get``).
    """
    result = Fetched()
    url = f"{base_url.rstrip('/')}/lookup"
    started = time.monotonic()
    for attempt in range(retries + 1):
        result.attempts = attempt + 1
        try:
            resp = client.get(url, params={"lei": lei, "refresh": "true"})
        except Exception as exc:  # transport errors: report, do not crash the run
            result.error = f"{type(exc).__name__}"
            if attempt < retries:
                result.waits.append(30.0)
                sleep(30.0)
                continue
            break
        if resp.status_code in (429, 503) and attempt < retries:
            try:
                wait = float(resp.headers.get("Retry-After", "30"))
            except ValueError:
                wait = 30.0
            wait = min(max(wait, 1.0), _MAX_RETRY_WAIT_S)
            result.waits.append(wait)
            sleep(wait)
            continue
        if resp.status_code != 200:
            result.error = f"HTTP {resp.status_code}"
            break
        try:
            result.payload = resp.json()
            result.error = None
        except ValueError:
            result.error = "response was not JSON"
        break
    result.seconds = round(time.monotonic() - started, 1)
    return result


async def run(
    expectations: list[dict[str, Any]],
    *,
    client: Any,
    base_url: str,
    rules: Rules,
    cards: Mapping[str, Mapping[str, str]],
    mcp_call: McpCall | None = None,
    pace_s: float = DEFAULT_PACE_S,
    sleep: Sleep = time.sleep,
    previous: Mapping[str, Any] | None = None,
    stamps: Mapping[str, Any] | None = None,
    commit: str | None = None,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Run every expectation against production and build the report."""
    stamp_now = now or (lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    report: dict[str, Any] = {
        "generated_at": stamp_now(),
        "base_url": base_url,
        "commit": commit,
        "rules": dict(stamps) if stamps is not None else rule_stamps(),
        "compared_against": (previous or {}).get("generated_at"),
        "subjects": [],
    }
    for i, exp in enumerate(expectations):
        if i:
            sleep(pace_s)
        fetched = fetch_lookup(client, base_url, exp["lei"], sleep=sleep)
        entry: dict[str, Any] = {
            "lei": exp["lei"],
            "name": exp["name"],
            "role": exp["role"],
            "file": exp.get("_file"),
            "seconds": fetched.seconds,
            "attempts": fetched.attempts,
        }
        if fetched.payload is None:
            entry["findings"] = [
                Finding("lookup_failed", f"/lookup failed: {fetched.error}").to_dict()
            ]
            entry["summary"] = None
            report["subjects"].append(entry)
            continue
        mcp: dict[str, Any] | None = None
        if mcp_call is not None:
            try:
                mcp = await mcp_call(exp["lei"])
            except Exception as exc:
                mcp = {"error": type(exc).__name__}
        findings, summary = evaluate(exp, fetched.payload, rules=rules, cards=cards, mcp=mcp)
        entry["findings"] = [f.to_dict() for f in findings]
        entry["summary"] = summary
        report["subjects"].append(entry)
    report["changes"] = diff_against(previous, report)
    report["totals"] = totals(report)
    return report


def totals(report: Mapping[str, Any]) -> dict[str, Any]:
    subjects = report.get("subjects", [])
    by_check: dict[str, int] = {}
    failed: list[str] = []
    for s in subjects:
        fails = [f for f in s.get("findings", []) if f["severity"] == FAIL]
        if fails:
            failed.append(s["lei"])
        for f in s.get("findings", []):
            by_check[f["check"]] = by_check.get(f["check"], 0) + 1
    return {
        "subjects": len(subjects),
        "passed": len(subjects) - len(failed),
        "failed": len(failed),
        "failed_leis": failed,
        "by_check": dict(sorted(by_check.items())),
    }


def failed(report: Mapping[str, Any]) -> bool:
    return bool((report.get("totals") or totals(report))["failed"])


# --- the report ------------------------------------------------------------------------


def render_markdown(report: Mapping[str, Any]) -> str:
    t = report.get("totals") or totals(report)
    lines = [
        "# Findings regression",
        "",
        f"Run {report['generated_at']} against `{report['base_url']}`"
        + (f" (code at `{report['commit']}`)" if report.get("commit") else "")
        + f". **{t['passed']} of {t['subjects']} subjects as expected**, {t['failed']} not.",
        "",
        "Expectations are lower bounds and shapes (`backend/findings_golden/`). A genuine upstream "
        "change is fixed by updating the subject's file in a reviewed PR.",
        "",
        "| Subject | Role | Result | Risk codes | Context codes |",
        "|---|---|---|---|---|",
    ]
    for s in report.get("subjects", []):
        fails = [f for f in s.get("findings", []) if f["severity"] == FAIL]
        warns = [f for f in s.get("findings", []) if f["severity"] == WARN]
        mark = "❌" if fails else ("⚠️" if warns else "✅")
        summ = s.get("summary") or {}
        sig = summ.get("signals") or {}
        risk = ", ".join(c for c, r in sig.items() if "risk" in r["kinds"]) or "—"
        ctx = ", ".join(c for c, r in sig.items() if "context" in r["kinds"]) or "—"
        result = (
            f"{mark} {len(fails)} failed"
            if fails
            else (f"{mark} {len(warns)} warning(s)" if warns else mark)
        )
        lines.append(f"| {s['name']} (`{s['lei']}`) | {s['role']} | {result} | {risk} | {ctx} |")
    lines.append("")
    if t["by_check"]:
        lines.append("## Findings by class")
        lines.append("")
        for check, n in t["by_check"].items():
            lines.append(f"- `{check}`: {n}")
        lines.append("")
    for s in report.get("subjects", []):
        if not s.get("findings"):
            continue
        lines.append(f"### {s['name']} (`{s['lei']}`)")
        lines.append("")
        for f in s["findings"]:
            icon = "❌" if f["severity"] == FAIL else "⚠️"
            lines.append(f"- {icon} `{f['check']}` — {f['message']}")
        lines.append("")
    lines.append("## Since the last report")
    lines.append("")
    if not report.get("compared_against"):
        lines.append("No previous report to compare against.")
    elif not report.get("changes"):
        lines.append(f"No signal or verdict changed since {report['compared_against']}.")
    else:
        lines.append(f"Compared with {report['compared_against']}:")
        lines.append("")
        names = {s["lei"]: s["name"] for s in report.get("subjects", [])}
        for lei, changes in report["changes"].items():
            lines.append(f"- **{names.get(lei, lei)}**: " + "; ".join(changes))
    lines.append("")
    rules = report.get("rules") or {}
    lines.append(
        f"Rule stamps: verdict template {rules.get('verdict_template')}, "
        f"signal rules {rules.get('signal_rules')}."
    )
    return "\n".join(lines) + "\n"
