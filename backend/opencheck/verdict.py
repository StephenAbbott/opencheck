"""The verdict sentence — one deterministic line stating what was found.

Phase 122. The results page opens with the subject and then a single
sentence saying what the check turned up, before any evidence, any source
card, and before the AI summary. This module writes that sentence.

**It is a template, not a model call.** Everything it needs already exists
as structured data by the time the pipeline yields ``risk_signals``: the
code, the kind and the evidence of every signal. Rendering it here rather
than in the frontend means the page, the PDF, the share card and the API
cannot disagree about what the check said — and it costs nothing, works
offline, and renders identically on every load. The AI summary
(``narrative/``) is unchanged and still sits further down the page; it
explains, this states.

Two rules constrain every clause below, and they are the whole point:

1. **It says what the records contain, never what to conclude.** "Sanctions
   findings on the company itself" is a fact about the data. "High risk" is
   a judgement, and the judgement is the analyst's.
2. **It never implies completeness.** The sentence describes what was
   found; whether the check actually ran is the Coverage column's job, fed
   by ``degraded_sources``. A verdict that quietly read as "all clear"
   because three screens timed out would be the exact failure the degraded
   notice exists to prevent, so ``build_verdict`` takes the degraded list
   and refuses to say "nothing found" when anything failed to run.
"""

from __future__ import annotations

from typing import Any

# Signal codes, grouped by the clause they contribute to. Kept as literal
# strings rather than imported from ``risk`` so that adding a code there
# without deciding how it should read here is a visible omission (the code
# simply does not appear in the sentence) rather than a crash or a
# half-sentence.

_SUBJECT_SANCTIONS = ("SANCTIONED", "SANCTIONS_CONTROLLED", "SANCTIONED_SECURITY")
_RELATED_SANCTIONS = (
    "RELATED_SANCTIONED",
    "RELATED_SANCTIONS_CONTROLLED",
    "RELATED_SANCTIONS_LINKED",
)
_SANCTIONS_LINKED = ("SANCTIONS_LINKED", "COUNTER_SANCTIONED", "RELATED_COUNTER_SANCTIONED")
_SUBJECT_EXPORT = ("EXPORT_CONTROLLED", "EXPORT_CONTROL_LINKED", "EXPORT_RISK")
_RELATED_EXPORT = (
    "RELATED_EXPORT_CONTROLLED",
    "RELATED_EXPORT_CONTROL_LINKED",
    "RELATED_EXPORT_RISK",
)
_DEBARMENT = ("DEBARMENT", "RELATED_DEBARMENT")
_PEP = ("PEP", "RELATED_PEP")
_JURISDICTION = ("FATF_BLACK_LIST", "FATF_GREY_LIST", "EU_HIGH_RISK_THIRD_COUNTRY")
_LEAKS = ("OFFSHORE_LEAKS",)
_OPACITY = ("OPAQUE_OWNERSHIP", "NOMINEE", "TRUST_OR_ARRANGEMENT", "POSSIBLE_OBFUSCATION")
_STATE = ("STATE_CONTROLLED",)

#: Risk-kind codes whose whole contribution is the structure clause. They are
#: real signals, so the sentence must never say "no risk signals surfaced"
#: when one has fired — but they describe the shape of the company rather
#: than an adverse finding, so they get no risk clause of their own.
_STRUCTURE_CODES = frozenset({"COMPLEX_OWNERSHIP_LAYERS", "GLEIF_REPORTING_EXCEPTION"})

#: Risk clauses, in the order they are read. A sentence takes at most the
#: first two: three findings in one line stops being a sentence and starts
#: being a list, and the chips beside it already carry the full set.
_RISK_CLAUSES: tuple[tuple[tuple[str, ...], str], ...] = (
    (_SUBJECT_SANCTIONS, "sanctions findings on the company itself"),
    (_SUBJECT_EXPORT, "export-control findings on the company itself"),
    (_DEBARMENT, "procurement debarment findings"),
    (_RELATED_SANCTIONS, "sanctions findings on parties connected to it"),
    (_RELATED_EXPORT, "export-control findings on parties connected to it"),
    (_SANCTIONS_LINKED, "records linking it to sanctioned parties"),
    (_PEP, "a politically exposed person among the parties named"),
    (_JURISDICTION, "a jurisdiction on an international watch list"),
    (_LEAKS, "an appearance in offshore-leaks data"),
    (_OPACITY, "ownership recorded in a form that obscures who benefits"),
    (_STATE, "state ownership recorded on the company"),
)

_MAX_RISK_CLAUSES = 2


def _codes(signals: list[dict[str, Any]], kind: str | None = None) -> set[str]:
    return {
        str(s.get("code"))
        for s in signals
        if s.get("code") and (kind is None or (s.get("kind") or "risk") == kind)
    }


def _layer_depth(signals: list[dict[str, Any]]) -> int | None:
    """Longest corporate chain, from COMPLEX_OWNERSHIP_LAYERS' own evidence.

    Read rather than recomputed: the rule already walked the graph, and a
    second implementation here would be free to disagree with the chip.
    """
    best: int | None = None
    for s in signals:
        if s.get("code") != "COMPLEX_OWNERSHIP_LAYERS":
            continue
        layers = (s.get("evidence") or {}).get("layers")
        if isinstance(layers, int) and (best is None or layers > best):
            best = layers
    return best


def _structure_clause(signals: list[dict[str, Any]]) -> str | None:
    """The shape of the company, as a phrase. Never an adverse finding."""
    all_codes = _codes(signals)
    parts: list[str] = []

    depth = _layer_depth(signals)
    if depth:
        parts.append(f"an ownership chain {depth} layers deep")

    if "GLEIF_REPORTING_EXCEPTION" in all_codes and not depth:
        # A permitted reporting exception, not a failure to disclose.
        parts.append("no parent filed with GLEIF, under a permitted exception")

    if not parts:
        return None
    return " and ".join(parts)


def build_verdict(
    signals: list[dict[str, Any]],
    degraded: list[dict[str, Any]] | None = None,
    *,
    legal_name: str | None = None,
) -> str | None:
    """One sentence describing what the check found.

    ``signals`` is the merged signal list exactly as it crosses the wire
    (dicts from ``RiskSignal.to_dict``), ``degraded`` the ``DegradedSource``
    dicts from the same event. Returns ``None`` when there is nothing
    truthful to say — the caller renders no sentence rather than a hollow
    one.

    The subject is deliberately unnamed in most sentences ("the company
    itself"): the name is the ``h1`` directly above it, and repeating it
    reads as filler.
    """
    signals = signals or []
    degraded = degraded or []

    risk_codes = _codes(signals, kind="risk")

    clauses: list[str] = []
    for codes, phrase in _RISK_CLAUSES:
        if risk_codes.intersection(codes):
            clauses.append(phrase)
        if len(clauses) == _MAX_RISK_CLAUSES:
            break

    structure = _structure_clause(signals)

    if clauses:
        head = clauses[0] if len(clauses) == 1 else f"{clauses[0]}, and {clauses[1]}"
        sentence = head[0].upper() + head[1:]
        if structure:
            sentence = f"{sentence}, over {structure}"
        # No completeness caveat here on purpose. "We found X" stays true
        # whatever else failed to run; only an *absence* needs qualifying,
        # and the Coverage column carries the detail either way.
        return sentence + "."

    matched = {c for codes, _ in _RISK_CLAUSES for c in codes}
    unhandled = risk_codes - matched - _STRUCTURE_CODES

    if unhandled and not structure:
        # A code was added to risk.py without deciding how it should read
        # here. Saying "no risk signals surfaced" would be false, so the
        # sentence says nothing at all; the chips below still carry it.
        return None

    if structure and risk_codes:
        base = f"The records show {structure}"
    elif structure:
        base = f"No risk signals surfaced, over {structure}"
    elif degraded:
        base = "No risk signals surfaced"
    else:
        base = "No risk signals surfaced across the sources that answered"

    if degraded:
        base += f", but {_incomplete_phrase(degraded)}"
    return base + "."


#: Adapters record this when the SOURCE itself did not answer, as opposed to a
#: derived screen that could not run (see ``opencheck.degradation``).
_SOURCE_FETCH = "source_fetch"


def _incomplete_phrase(degraded: list[dict[str, Any]]) -> str:
    """Say which kind of gap this was — they are not the same claim.

    A screen that did not run undermines a clean result: an empty sanctions or
    PEP screen that never executed is indistinguishable from a clean one, which
    is what "not a clean screen" warns about. A *source* that did not answer is
    a coverage gap, not a screening gap — the Lithuanian register being
    unreachable says nothing about whether anyone was screened. Phrasing both
    the same way over-claims on one and under-explains the other.
    """
    screens = [d for d in degraded if d.get("check") != _SOURCE_FETCH]
    sources = [d for d in degraded if d.get("check") == _SOURCE_FETCH]

    parts: list[str] = []
    if screens:
        n = len({d.get("source_id") for d in screens if d.get("source_id")}) or len(screens)
        noun = "one check" if n == 1 else f"{n} checks"
        parts.append(f"{noun} did not run — an empty result there is not a clean screen")
    if sources:
        n = len({d.get("source_id") for d in sources if d.get("source_id")}) or len(sources)
        noun = "one source" if n == 1 else f"{n} sources"
        verb = "did not answer" if n == 1 else "did not answer"
        parts.append(f"{noun} {verb}, so its records were not consulted")
    return ", and ".join(parts)
