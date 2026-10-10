"""The verdict — at most two deterministic sentences stating what was found.

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
_SANCTIONS_LINKED = ("SANCTIONS_LINKED",)
#: Counter-sanctions are a designation by the *target* of mainstream sanctions
#: (Phase 105: ``sanction.counter``, slate chip, severity 2). They are kept
#: out of every sanctions clause on purpose: "records linking it to sanctioned
#: parties" was, until Phase 153, the headline Shell plc carried on the
#: strength of a name-only match against Russia's list of US citizens — the
#: exact misreading the counter-sanctions split exists to prevent. They get a
#: clause of their own, read last, that names what the record is.
_COUNTER_SANCTIONS = ("COUNTER_SANCTIONED", "RELATED_COUNTER_SANCTIONED")
_SUBJECT_EXPORT = ("EXPORT_CONTROLLED", "EXPORT_CONTROL_LINKED", "EXPORT_RISK")
_RELATED_EXPORT = (
    "RELATED_EXPORT_CONTROLLED",
    "RELATED_EXPORT_CONTROL_LINKED",
    "RELATED_EXPORT_RISK",
)
_DEBARMENT = ("DEBARMENT", "RELATED_DEBARMENT")
_PEP = ("PEP", "RELATED_PEP")
_JURISDICTION = ("FATF_BLACK_LIST", "FATF_GREY_LIST", "EU_HIGH_RISK_THIRD_COUNTRY")
#: Phase 319 — Annex I of the EU tax list. A clause of its own, never folded
#: into the watch-list clause above: that one stands for the AML lists, and a
#: tax-governance listing carries no AML obligation. Read after the AML
#: clause and after state ownership, so it only reaches the two-clause
#: sentence when little else did.
_TAX_LIST = ("EU_TAX_NON_COOPERATIVE",)
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
#: being a list, and the Risk signals section carries the full set.
#:
#: The third element is the clause's *possible* form (Phase 247), used when
#: every signal behind it is medium confidence or lower. Only the clauses
#: built on name matches have one (Stephen, 25 Sept 2026): a PEP, a related
#: party's listing, an offshore-leaks node, a debarment or counter-sanctions
#: record reached by name. There the confidence is about *identity* — is this
#: the same person — and "The records show a politically exposed person"
#: over nothing but "Possible name match only" chips asserted the identity
#: every chip declined to. State ownership, a watch-list jurisdiction and the
#: shape of the ownership are read off the company's own records; their
#: confidence is about something else, and they keep one wording.
_RISK_CLAUSES: tuple[tuple[tuple[str, ...], str, str | None], ...] = (
    (_SUBJECT_SANCTIONS, "sanctions findings on the company itself", None),
    (_SUBJECT_EXPORT, "export-control findings on the company itself", None),
    (
        _DEBARMENT,
        "procurement debarment findings",
        "possible procurement debarment findings",
    ),
    (
        _RELATED_SANCTIONS,
        "sanctions findings on parties connected to it",
        "possible sanctions findings on parties connected to it",
    ),
    (
        _RELATED_EXPORT,
        "export-control findings on parties connected to it",
        "possible export-control findings on parties connected to it",
    ),
    (_SANCTIONS_LINKED, "records linking it to sanctioned parties", None),
    (
        _PEP,
        "a politically exposed person among the parties named",
        "a possible politically exposed person among the parties named",
    ),
    (_JURISDICTION, "a jurisdiction on an international watch list", None),
    (
        _LEAKS,
        "an appearance in offshore-leaks data",
        "a possible appearance in offshore-leaks data",
    ),
    (_OPACITY, "ownership recorded in a form that obscures who benefits", None),
    (_STATE, "state ownership recorded on the company", None),
    (_TAX_LIST, "a jurisdiction on the EU's tax non-cooperation list", None),
    (
        _COUNTER_SANCTIONS,
        "a counter-sanctions designation by a non-mainstream authority",
        "a possible counter-sanctions designation by a non-mainstream authority",
    ),
)

#: Confidences that make a name-match clause "possible".
_TENTATIVE = frozenset({"medium", "low"})

_MAX_RISK_CLAUSES = 2


def _codes(signals: list[dict[str, Any]], kind: str | None = None) -> set[str]:
    return {
        str(s.get("code"))
        for s in signals
        if s.get("code") and (kind is None or (s.get("kind") or "risk") == kind)
    }


def _all_tentative(signals: list[dict[str, Any]], codes: tuple[str, ...]) -> bool:
    """True when every risk signal behind a clause is medium or lower."""
    behind = [
        s for s in signals
        if s.get("code") in codes and (s.get("kind") or "risk") == "risk"
    ]
    return bool(behind) and all(
        str(s.get("confidence") or "").lower() in _TENTATIVE for s in behind
    )


def _intermediate_layers(signals: list[dict[str, Any]]) -> int | None:
    """Intermediate corporate layers above the subject, from
    COMPLEX_OWNERSHIP_LAYERS' own evidence — the deepest if several.

    Read rather than recomputed: the rule already walked the graph, and a
    second implementation here would be free to disagree with the chip.

    Phase 272: the sentence now speaks of *intermediate* layers, as the chip
    does and as AMLA's final draft RTS does ("the number of intermediate
    layers between the customer and the beneficial owner"), so it no longer
    counts the subject. ``evidence.intermediate_layers`` is preferred;
    a signal from before Phase 272 (a saved report, a replayed run) carries
    only ``layers``, which includes the subject, so ``layers - 1`` is the
    same number.
    """
    best: int | None = None
    for s in signals:
        if s.get("code") != "COMPLEX_OWNERSHIP_LAYERS":
            continue
        ev = s.get("evidence") or {}
        n = ev.get("intermediate_layers")
        if not isinstance(n, int):
            layers = ev.get("layers")
            n = layers - 1 if isinstance(layers, int) else None
        if isinstance(n, int) and n >= 1 and (best is None or n > best):
            best = n
    return best


def _structure_sentence(signals: list[dict[str, Any]]) -> str | None:
    """The shape of the company, as a sentence of its own. Never a finding.

    Phase 245: this used to be a phrase hung off the risk clauses with
    "over" ("…, over no parent filed with GLEIF, under a permitted
    exception"), which made one sentence pivot twice. It is now a second
    sentence, so the finding is read first and the shape second.
    """
    all_codes = _codes(signals)

    intermediate = _intermediate_layers(signals)
    if intermediate:
        noun = "layer" if intermediate == 1 else "layers"
        return f"Its ownership chain has {intermediate} intermediate corporate {noun}."

    if "GLEIF_REPORTING_EXCEPTION" in all_codes:
        # A permitted reporting exception, not a failure to disclose.
        return "No parent is filed with GLEIF, which the LEI reporting rules permit."

    return None


def is_name_only_lead(signal: dict[str, Any]) -> bool:
    """A low-confidence, name-only offshore-leaks match on a related PERSON.

    Phase 293 (Stephen, 6 Oct 2026). Phase 282 caps a person match that
    nothing corroborates at ``low`` — ICIJ person nodes carry no birth date,
    so the jurisdiction is the only corroborator, and without it a match
    rests on spelling alone. Such a match stays on the card as a lead to
    review, but no longer makes the verdict say the records show an
    offshore-leaks appearance. A.P. Møller - Mærsk read "The records show a
    possible appearance in offshore-leaks data" on the strength of former
    chair Michael Pram Rasmussen against "MICHAEL RASMUSSEN" in two Paradise
    Papers registries; the same shape is DMGT's adjudicated true match
    NICHOLAS PAUL RATCLIFFE → "NICHOLAS RATCLIFFE", so the match is kept
    rather than dropped — only its weight in the sentence changes.

    A corroborated person match (``medium``), a match on the company's own
    name, and any entity match keep driving the verdict.
    """
    if signal.get("code") != "OFFSHORE_LEAKS":
        return False
    if (signal.get("kind") or "risk") != "risk":
        return False
    if str(signal.get("confidence") or "").lower() != "low":
        return False
    evidence = signal.get("evidence") or {}
    if evidence.get("kind") != "person" or evidence.get("subject"):
        return False
    gate = evidence.get("jurisdiction_gate") or {}
    return gate.get("status") != "corroborated"


def _lead_sentence(leads: list[dict[str, Any]]) -> str | None:
    """"A possible name-only offshore-leaks match on one related party is
    listed for review." — said only where the verdict would otherwise claim
    nothing surfaced. Counted by party: one person against two registries'
    records is one lead."""
    if not leads:
        return None
    parties = {
        str((s.get("evidence") or {}).get("subject_statement_id")
            or (s.get("evidence") or {}).get("search_name") or i)
        for i, s in enumerate(leads)
    }
    if len(parties) == 1:
        return (
            "A possible name-only offshore-leaks match on one related party "
            "is listed for review."
        )
    return (
        f"Possible name-only offshore-leaks matches on {len(parties)} related "
        "parties are listed for review."
    )


#: Bumped whenever the wording of ``build_verdict`` changes without the
#: facts behind it changing. The watchlist stores it beside each snapshot's
#: verdict and only reports a verdict change between snapshots written by the
#: same template — otherwise a deploy would make every watched company report
#: "verdict changed" on its next re-run. 1 = Phases 122–244 (one sentence),
#: 2 = Phase 245 (risk sentence, then structure sentence), 3 = Phase 247
#: ("possible" when every name-match signal behind a clause is medium or low),
#: 4 = Phase 272 ("has N intermediate corporate layers" instead of "is N+1
#: layers deep" — the same chain, counted without the subject), 5 = Phase
#: 293 (a low name-only offshore-leaks person match is a lead to review, not
#: a finding in the sentence).
VERDICT_TEMPLATE = 5


def build_verdict(
    signals: list[dict[str, Any]],
    degraded: list[dict[str, Any]] | None = None,
    *,
    legal_name: str | None = None,
) -> str | None:
    """At most two short sentences: what was found, then the company's shape.

    Phase 293: where the first sentence would say nothing surfaced but a low
    name-only offshore-leaks person match is on the card, one more short
    sentence names it as a lead to review (``is_name_only_lead``).

    ``signals`` is the merged signal list exactly as it crosses the wire
    (dicts from ``RiskSignal.to_dict``), ``degraded`` the ``DegradedSource``
    dicts from the same event. Returns ``None`` when there is nothing
    truthful to say — the caller renders no sentence rather than a hollow
    one.

    Risk first, structure second (Phase 245). The first sentence says what
    the records show — or that nothing surfaced, qualified by any check that
    did not run; the second, when there is one, says how the company is put
    together. Neither pivots on a preposition: "X, over Y, under Z" read as
    one clause qualifying the next, which is not what it meant.

    The subject is deliberately unnamed in most sentences ("the company
    itself"): the name is the ``h1`` directly above it, and repeating it
    reads as filler.
    """
    signals = signals or []
    degraded = degraded or []

    # Phase 293: low name-only offshore-leaks person matches are leads, read
    # out of the sentence and named after it only where it would otherwise
    # say nothing surfaced (``is_name_only_lead``).
    leads = [s for s in signals if is_name_only_lead(s)]
    if leads:
        signals = [s for s in signals if not is_name_only_lead(s)]
    lead = _lead_sentence(leads)

    risk_codes = _codes(signals, kind="risk")

    clauses: list[str] = []
    for codes, phrase, possible in _RISK_CLAUSES:
        if risk_codes.intersection(codes):
            if possible and _all_tentative(signals, codes):
                clauses.append(possible)
            else:
                clauses.append(phrase)
        if len(clauses) == _MAX_RISK_CLAUSES:
            break

    structure = _structure_sentence(signals)

    if clauses:
        head = clauses[0] if len(clauses) == 1 else f"{clauses[0]} and {clauses[1]}"
        # No completeness caveat here on purpose. "We found X" stays true
        # whatever else failed to run; only an *absence* needs qualifying,
        # and the Coverage column carries the detail either way.
        return _join(f"The records show {head}.", structure)

    matched = {c for codes, _, _ in _RISK_CLAUSES for c in codes}
    unhandled = risk_codes - matched - _STRUCTURE_CODES

    if unhandled and not structure:
        # A code was added to risk.py without deciding how it should read
        # here. Saying "no risk signals surfaced" would be false, so the
        # sentence says nothing at all; the chips below still carry it.
        return None

    if structure and risk_codes:
        # The only risk-kind signals are structural (a deep chain): they are
        # real signals, so "no risk signals surfaced" would be false. The
        # structure sentence is the whole finding.
        first = None
    elif degraded:
        first = f"No risk signals surfaced, but {_incomplete_phrase(degraded)}."
    else:
        first = "No risk signals surfaced across the sources that answered."

    if first is None:
        tail = None
        if degraded:
            gap = _incomplete_phrase(degraded)
            tail = f"{gap[0].upper()}{gap[1:]}."
        return _join(structure, lead, tail)
    return _join(first, lead, structure)


def _join(*sentences: str | None) -> str | None:
    kept = [s for s in sentences if s]
    return " ".join(kept) if kept else None


#: Adapters record this when the SOURCE itself did not answer, as opposed to a
#: derived screen that could not run (see ``opencheck.degradation``).
_SOURCE_FETCH = "source_fetch"
#: Phase 241: a source that returned a record and failed while it was read.
_SOURCE_READ = "source_read"
#: Phase 279: ``risk.DEGRADED_TRUNCATED`` — a screen's related-party limit.
_TRUNCATED = "truncated"


def _incomplete_phrase(degraded: list[dict[str, Any]]) -> str:
    """Say which kind of gap this was — they are not the same claim.

    A screen that did not run undermines a clean result: an empty sanctions or
    PEP screen that never executed is indistinguishable from a clean one, which
    is what "not a clean screen" warns about. A *source* that did not answer is
    a coverage gap, not a screening gap — the Lithuanian register being
    unreachable says nothing about whether anyone was screened. Phrasing both
    the same way over-claims on one and under-explains the other.
    """
    all_screens = [
        d for d in degraded if d.get("check") not in (_SOURCE_FETCH, _SOURCE_READ)
    ]
    # Phase 279: a screen that ran but read only the highest-ranked related
    # parties is a third claim again — it did run, so "did not run" would be
    # false, and "not a clean screen" holds only for the parties left out.
    capped = [d for d in all_screens if d.get("reason") == _TRUNCATED]
    screens = [d for d in all_screens if d.get("reason") != _TRUNCATED]
    sources = [d for d in degraded if d.get("check") == _SOURCE_FETCH]
    partial = [d for d in degraded if d.get("check") == _SOURCE_READ]

    parts: list[str] = []
    if screens:
        n = len({d.get("source_id") for d in screens if d.get("source_id")}) or len(screens)
        noun = "one check" if n == 1 else f"{n} checks"
        parts.append(f"{noun} did not run — an empty result there is not a clean screen")
    if capped:
        n = len({d.get("check") for d in capped if d.get("check")}) or len(capped)
        noun = "one check" if n == 1 else f"{n} checks"
        parts.append(f"{noun} answered only in part — not every related party was screened")
    if sources:
        n = len({d.get("source_id") for d in sources if d.get("source_id")}) or len(sources)
        noun = "one source" if n == 1 else f"{n} sources"
        verb = "did not answer" if n == 1 else "did not answer"
        parts.append(f"{noun} {verb}, so its records were not consulted")
    if partial:
        n = len({d.get("source_id") for d in partial if d.get("source_id")}) or len(partial)
        noun = "one source" if n == 1 else f"{n} sources"
        parts.append(f"{noun} answered only in part")
    return ", and ".join(parts)
