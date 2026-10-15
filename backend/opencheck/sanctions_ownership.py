"""The sanctions 50% rule, computed per regime (Phase 342).

Whether the parties a lookup found designated hold enough of the company,
between them or alone, to meet a sanctions regime's *ownership* test — OFAC's
50 Percent Rule, the EU's and the UK's "owned" limb — computed from the
shareholdings the sources state. The method is T-Rank's (the 50% rule
paper, docs.trank.no/white_papers, equations 1–5): sum what the designated
parties hold, mark every company that crosses the threshold as caught, let a
caught company's own holdings count, and repeat until nothing changes.

Stephen's decisions on the ticket (15 Oct 2026), each one a rule here:

* **A risk signal only when the low end meets the test.**
  ``SANCTIONS_OWNERSHIP_TEST`` fires when the *lowest* reading of every stake
  meets a regime's threshold, every party in the sum is designated under
  that regime, and each of them was matched on an identifier or with
  corroboration. "May meet" — the high ends meet it, or a name-only match
  would tip it — is ``SANCTIONS_OWNERSHIP_TEST_POSSIBLE``, context only.
* **Three regimes, computed separately, reported side by side.**

  - OFAC: 50% or more, blocked persons' holdings added together. A company
    caught by the rule is itself blocked, and its holdings count *in full*
    — never multiplied down the chain (OFAC's 2014 guidance).
  - EU: 50% or more of the proprietary rights, designated persons' holdings
    added together (Council Best Practices, 2024).
  - UK: more than 50%, held by *one* designated person, directly or through
    companies it owns more than 50% of. No aggregation across designated
    persons (OFSI's guidance; a joint arrangement is not read from records).
* **Identifier matches count; name-only matches do not.** A designated party
  is counted when its OpenSanctions match rests on a shared identifier or on
  corroboration (birth year, nationality, country). A name-only match is
  listed as "would meet the test if the match is confirmed", never summed.
* **The regime comes from the record's datasets.** A party on ``eu_fsf`` only
  is designated for the EU test only (``REGIME_DATASETS``).
* **Ownership only.** The EU and UK control tests — board appointments,
  influence, agreements — are not computed, and every surface says so.
* **Per source, never across sources.** Each source's shareholdings are
  computed on their own, as in Phase 339; two sources' stakes are never
  added. GLEIF and MEIP edges are not ownership and are left out.
* **One stake per holder per company per source.** When a source states the
  same holding twice, the larger reading is used; the two are never added.

``SANCTIONS_CONTROLLED`` (OpenSanctions' ``sanction.control`` topic) is
unchanged: that is the publisher's own judgement, this is OpenCheck's
arithmetic, and the two are shown side by side.

Pure: signals and statements in, at most one signal out. Deliberately
separate from ``integrated_ownership``, whose multiplied figure stays context
and is never read here (``tests/test_sanctions_ownership_phase342.py``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .bods.lifecycle import interest_ended, record_closed
from .bods.refs import resolver
from .integrated_ownership import EXCLUDED_DESCRIPTIONS, EXCLUDED_SOURCES, party_name, source_key
from .ownership_math import percent_text
from .risk import (
    SANCTIONS_OWNERSHIP_TEST,
    SANCTIONS_OWNERSHIP_TEST_POSSIBLE,
    RiskSignal,
)

METHOD = "sanctions_ownership_test"
METHOD_URL = "https://docs.trank.no/white_papers"
METHOD_CREDIT = (
    "Method: the 50% rule as published by T-Rank (https://docs.trank.no/white_papers)."
)

#: How the test is carried out, in plain words — printed under every table.
METHOD_DESCRIPTION = (
    "Computed by OpenCheck from the shareholdings the sources state, one source "
    "at a time: the holdings of parties designated under each regime are added "
    "(OFAC, EU) or taken one designated party at a time (UK); a company they "
    "hold enough of counts as caught, and its own holdings then count in full. "
    "The lowest reading of every stake decides whether the test is met; the "
    "highest decides whether it may be. Only parties matched on an identifier "
    "or with corroboration are counted. Ownership only: the EU and UK control "
    "tests are not computed. Arithmetic on the records, not a determination "
    "that the company is sanctioned, blocked or frozen."
)
NOT_COMPUTED = (
    "Control is not computed: the EU and UK also treat a company as caught when "
    "a designated person controls it (appoints its board, directs it, or by "
    "agreement), and OFAC licences and general licences are not read."
)

RESULT_MEETS = "meets"
RESULT_MAY_MEET = "may_meet"
RESULT_NOT_MET = "not_met"
RESULT_NONE = "no_designated_holder"

RESULT_WORDS = {
    RESULT_MEETS: "Meets the ownership test",
    RESULT_MAY_MEET: "May meet the ownership test",
    RESULT_NOT_MET: "Does not meet the ownership test",
    RESULT_NONE: "No party designated under this regime holds shares",
}

BASIS_IDENTIFIER = "identifier"
BASIS_CORROBORATED = "corroborated"
BASIS_NAME_ONLY = "name_only"
BASIS_WORDS = {
    BASIS_IDENTIFIER: "matched on an identifier",
    BASIS_CORROBORATED: "matched with corroboration",
    BASIS_NAME_ONLY: "name match only",
}
_BASIS_RANK = {BASIS_IDENTIFIER: 0, BASIS_CORROBORATED: 1, BASIS_NAME_ONLY: 2}

#: Signals whose evidence names a designated party: the related-party name
#: screen against OpenSanctions (``cross_check``). The listing's datasets ride
#: on the evidence since Phase 342.
_DESIGNATION_CODES = frozenset({"RELATED_SANCTIONED"})
_DESIGNATION_SOURCES = frozenset({"opensanctions"})

_EPS = 1e-9


@dataclass(frozen=True)
class Regime:
    key: str
    label: str
    rule: str
    threshold: float
    #: True: "or more" (≥); False: "more than" (>).
    inclusive: bool
    #: True: designated parties' holdings are added together.
    aggregate: bool
    datasets: frozenset[str]


#: OpenSanctions dataset names → regime. OFAC: the SDN list only — the 50
#: Percent Rule is about blocked persons; the non-SDN lists (``us_ofac_cons``)
#: carry their own, narrower directives and are not read as blocking. EU: the
#: Financial Sanctions Files and the Official Journal. UK: OFSI's
#: consolidated list and the FCDO's UK Sanctions List.
REGIMES: tuple[Regime, ...] = (
    Regime(
        "OFAC", "OFAC (US)",
        "50% or more, held by blocked persons together (OFAC's 50 Percent Rule)",
        0.5, True, True, frozenset({"us_ofac_sdn"}),
    ),
    Regime(
        "EU", "EU",
        "50% or more of the proprietary rights, held by designated persons together",
        0.5, True, True, frozenset({"eu_fsf", "eu_journal_sanctions", "eu_sanctions_map"}),
    ),
    Regime(
        "UK", "UK",
        "more than 50%, held by one designated person directly or through "
        "companies it owns (no aggregation across designated persons)",
        0.5, False, False, frozenset({"gb_hmt_sanctions", "gb_fcdo_sanctions"}),
    ),
)
REGIME_DATASETS: dict[str, frozenset[str]] = {r.key: r.datasets for r in REGIMES}
_ALL_REGIME_DATASETS: frozenset[str] = frozenset().union(*REGIME_DATASETS.values())


def regimes_for(datasets: Iterable[str]) -> list[str]:
    """The regimes a listing's OpenSanctions datasets designate it under."""
    ds = {str(d) for d in datasets or ()}
    return [r.key for r in REGIMES if r.datasets & ds]


# ----------------------------------------------------------------------
# Stakes
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Stake:
    """One holder's stake in one company, as an interval with exclusivity.

    ``lo_strict``: the stake is *more than* ``lo`` (an ``exclusiveMinimum``).
    ``hi`` None: no upper end is stated. ``hi_strict``: *less than* ``hi``.
    """

    lo: float
    lo_strict: bool
    hi: float | None
    hi_strict: bool

    def text(self) -> str:
        return stake_text(self.lo, self.lo_strict, self.hi, self.hi_strict)


def _num(share: Mapping[str, Any], key: str) -> float | None:
    v = share.get(key)
    if v is None or isinstance(v, bool):
        return None
    try:
        return max(0.0, min(float(v), 100.0)) / 100.0
    except (TypeError, ValueError):
        return None


def _share_stake(share: Any) -> Stake | None:
    """A BODS ``share`` object as a Stake; None when it states nothing."""
    if not isinstance(share, Mapping):
        return None
    exact = _num(share, "exact")
    if exact is not None:
        return Stake(exact, False, exact, False)
    lo, lo_strict = _num(share, "minimum"), False
    if lo is None:
        lo = _num(share, "exclusiveMinimum")
        lo_strict = lo is not None
    hi, hi_strict = _num(share, "maximum"), False
    if hi is None:
        hi = _num(share, "exclusiveMaximum")
        hi_strict = hi is not None
    if lo is None and hi is None:
        return None
    return Stake(lo or 0.0, lo_strict, hi, hi_strict)


_UNSTATED = Stake(0.0, False, None, False)


def _relationship_stake(stmt: Mapping[str, Any], as_of: str) -> Stake | None:
    """The current shareholding one relationship states, or None when it
    states no current shareholding (roles, control, voting rights, ended)."""
    closed = record_closed(dict(stmt))
    rd = stmt.get("recordDetails")
    rd = rd if isinstance(rd, Mapping) else {}
    current = [
        i for i in rd.get("interests") or []
        if isinstance(i, Mapping) and i.get("type") == "shareholding"
        and not interest_ended(dict(i), closed, as_of)
    ]
    if not current:
        return None
    best: Stake | None = None
    unstated = False
    for i in current:
        s = _share_stake(i.get("share"))
        if s is None:
            unstated = True
            continue
        best = s if best is None else _larger(best, s)
    if best is None:
        return _UNSTATED
    if unstated:
        return Stake(best.lo, best.lo_strict, None, False)
    return best


def _larger(a: Stake, b: Stake) -> Stake:
    lo, lo_s = max((a.lo, a.lo_strict), (b.lo, b.lo_strict))
    if a.hi is None or b.hi is None:
        return Stake(lo, lo_s, None, False)
    hi, hi_neg = max((a.hi, not a.hi_strict), (b.hi, not b.hi_strict))
    return Stake(lo, lo_s, hi, not hi_neg)


def stake_text(lo: float, lo_strict: bool, hi: float | None, hi_strict: bool) -> str:
    """A stake or a sum in words: "60%", "more than 25% up to 50%",
    "at least 10%", "share not stated"."""
    p_lo = percent_text(lo * 100)
    if hi is not None and abs(hi - lo) < _EPS and not lo_strict and not hi_strict:
        return p_lo
    low = ""
    if lo > _EPS or lo_strict:
        low = f"more than {p_lo}" if lo_strict else f"at least {p_lo}"
    if hi is None:
        return low or "share not stated"
    p_hi = percent_text(hi * 100)
    high = f"less than {p_hi}" if hi_strict else f"up to {p_hi}"
    return f"{low} {high}" if low else high


# ----------------------------------------------------------------------
# Designated parties
# ----------------------------------------------------------------------


@dataclass
class Designated:
    node: str
    name: str
    regimes: set[str] = field(default_factory=set)
    datasets: set[str] = field(default_factory=set)
    basis: str = BASIS_NAME_ONLY
    statement_ids: set[str] = field(default_factory=set)


def match_basis(evidence: Mapping[str, Any]) -> str:
    """How a related-party match was made: on an identifier, with
    corroboration, or on the name alone."""
    if evidence.get("identifier_match"):
        return BASIS_IDENTIFIER
    if evidence.get("corroboration"):
        return BASIS_CORROBORATED
    return BASIS_NAME_ONLY


def designated_parties(
    signals: Iterable[Mapping[str, Any]],
    canon: Mapping[str, str],
    names: Mapping[str, str],
) -> dict[str, Designated]:
    """Canonical node → the designation the lookup's signals carry for it."""
    out: dict[str, Designated] = {}
    for sig in signals:
        if (
            sig.get("code") not in _DESIGNATION_CODES
            or sig.get("source_id") not in _DESIGNATION_SOURCES
        ):
            continue
        ev = sig.get("evidence") or {}
        regimes = regimes_for(ev.get("datasets") or ())
        if not regimes:
            continue
        sids = [str(s) for s in ev.get("subject_statement_ids") or [] if s]
        if ev.get("subject_statement_id"):
            sids.append(str(ev["subject_statement_id"]))
        basis = match_basis(ev)
        for sid in dict.fromkeys(sids):
            node = canon.get(sid, sid)
            d = out.get(node)
            if d is None:
                name = names.get(node) or str(ev.get("search_name") or node)
                d = out[node] = Designated(node, name)
            d.regimes.update(regimes)
            # Only the lists that decide a regime — a record is on dozens.
            d.datasets.update(
                str(x) for x in ev.get("datasets") or () if str(x) in _ALL_REGIME_DATASETS
            )
            d.statement_ids.add(sid)
            if _BASIS_RANK[basis] < _BASIS_RANK[d.basis]:
                d.basis = basis
    return out


# ----------------------------------------------------------------------
# The propagation (T-Rank, equations 1–5)
# ----------------------------------------------------------------------


Edges = dict[tuple[str, str], Stake]  # (owner, owned) → stake, one source


def _passes(total: float, strict: bool, regime: Regime) -> bool:
    """Does a low-end sum (strictly above ``total`` when ``strict``) meet it?"""
    t = regime.threshold
    if total > t + _EPS:
        return True
    if abs(total - t) <= _EPS:
        return regime.inclusive or strict
    return False


def _may_pass(total: float | None, strict: bool, regime: Regime) -> bool:
    """Can a high-end sum (strictly below ``total`` when ``strict``; None for
    no stated upper end) meet it?"""
    if total is None:
        return True
    t = regime.threshold
    if total > t + _EPS:
        return True
    if abs(total - t) <= _EPS:
        return regime.inclusive and not strict
    return False


def _sum(stakes: list[Stake], high: bool) -> tuple[float | None, bool]:
    if high:
        if any(s.hi is None for s in stakes):
            return None, False
        return min(1.0, sum(s.hi or 0.0 for s in stakes)), any(s.hi_strict for s in stakes)
    return min(1.0, sum(s.lo for s in stakes)), any(s.lo_strict for s in stakes)


def _crosses(stakes: list[Stake], regime: Regime, high: bool) -> bool:
    if not stakes:
        return False
    total, strict = _sum(stakes, high)
    if high:
        return _may_pass(total, strict, regime)
    assert total is not None
    return _passes(total, strict, regime)


def _caught(edges: Edges, roots: set[str], regime: Regime, high: bool) -> set[str]:
    """Every node the roots hold enough of, repeated to a fixed point. The
    roots count as blocked; a caught company's holdings count in full."""
    caught = set(roots)
    owned_by: dict[str, list[tuple[str, Stake]]] = {}
    for (owner, owned), stake in edges.items():
        owned_by.setdefault(owned, []).append((owner, stake))
    for _ in range(len(owned_by) + 1):
        added = {
            x for x, holders in owned_by.items()
            if x not in caught and _crosses([s for o, s in holders if o in caught], regime, high)
        }
        if not added:
            break
        caught |= added
    return caught


def _caught_for(edges: Edges, parties: set[str], regime: Regime, high: bool) -> list[set[str]]:
    """The caught sets: one for all parties together (OFAC, EU), one per
    designated party (UK — no aggregation across designated persons)."""
    if regime.aggregate:
        return [_caught(edges, parties, regime, high)] if parties else []
    return [_caught(edges, {p}, regime, high) for p in sorted(parties)]


@dataclass
class _Outcome:
    met: bool
    group: set[str]


def _outcome(edges: Edges, parties: set[str], subject: str, regime: Regime, high: bool) -> _Outcome:
    groups = _caught_for(edges, parties, regime, high)
    hits = [g for g in groups if subject in g]
    if hits:
        return _Outcome(True, hits[0])
    # Not met: report the group holding most of the subject at the low end.
    def held(g: set[str]) -> float:
        return sum(s.lo for (o, x), s in edges.items() if x == subject and o in g)
    best = max(groups, key=held) if groups else set()
    return _Outcome(False, best)


def _upstream(edges: Edges, target: str) -> set[str]:
    owners: dict[str, set[str]] = {}
    for owner, owned in edges:
        owners.setdefault(owned, set()).add(owner)
    seen: set[str] = set()
    stack = [target]
    while stack:
        n = stack.pop()
        for o in owners.get(n, ()):
            if o not in seen:
                seen.add(o)
                stack.append(o)
    seen.discard(target)
    return seen


_SUBJECT = "\x00subject"


def _source_edges(
    statements: list[dict[str, Any]],
    subject_ids: Iterable[str],
    canon: Mapping[str, str],
    as_of: str,
) -> tuple[dict[str, Edges], dict[str, str]]:
    """source key → its current shareholding edges, and source key → label."""
    resolve = resolver(statements)
    subjects = {canon.get(s, s) for s in subject_ids if s}

    def node(raw: Any) -> str:
        sid = resolve(raw)
        if not sid:
            return ""
        c = canon.get(sid, sid)
        return _SUBJECT if c in subjects else c

    by_source: dict[str, Edges] = {}
    labels: dict[str, str] = {}
    for s in statements:
        if s.get("recordType") != "relationship":
            continue
        src_raw = s.get("source")
        src: Mapping[str, Any] = src_raw if isinstance(src_raw, Mapping) else {}
        if str(src.get("opencheckSourceId") or "") in EXCLUDED_SOURCES or (
            str(src.get("description") or "").strip() in EXCLUDED_DESCRIPTIONS
        ):
            continue
        rd_raw = s.get("recordDetails")
        rd: Mapping[str, Any] = rd_raw if isinstance(rd_raw, Mapping) else {}
        owned, owner = node(rd.get("subject")), node(rd.get("interestedParty"))
        if not owned or not owner or owned == owner:
            continue
        stake = _relationship_stake(s, as_of)
        if stake is None:
            continue
        key, label = source_key(s)
        labels.setdefault(key, label)
        edges = by_source.setdefault(key, {})
        prev = edges.get((owner, owned))
        edges[(owner, owned)] = stake if prev is None else _larger(prev, stake)
    return by_source, labels


# ----------------------------------------------------------------------
# The test, per regime and per source
# ----------------------------------------------------------------------


def _holders(
    edges: Edges, group: set[str], designated: Mapping[str, Designated],
    names: Mapping[str, str],
) -> list[dict[str, Any]]:
    """The subject's direct holders inside ``group``, with why each counts."""
    rows: list[dict[str, Any]] = []
    for (owner, owned), stake in sorted(edges.items()):
        if owned != _SUBJECT or owner not in group:
            continue
        d = designated.get(owner)
        row: dict[str, Any] = {
            "party": owner,
            "name": names.get(owner) or (d.name if d else owner),
            "stake": stake.text(),
            "minimum": round(stake.lo * 100, 4),
            "maximum": None if stake.hi is None else round(stake.hi * 100, 4),
        }
        if d is not None:
            row["designated"] = True
            row["basis"] = d.basis
        else:
            row["designated"] = False
            # A caught company: who holds enough of it, at this end.
            row["held_by"] = [
                {"name": names.get(o) or o, "stake": s.text()}
                for (o, x), s in sorted(edges.items())
                if x == owner and o in group
            ]
        rows.append(row)
    rows.sort(key=lambda r: (-r["minimum"], r["name"].casefold(), r["party"]))
    return rows


def _figure(edges: Edges, group: set[str]) -> dict[str, Any]:
    stakes = [s for (o, x), s in edges.items() if x == _SUBJECT and o in group]
    lo, lo_strict = _sum(stakes, high=False)
    hi, hi_strict = _sum(stakes, high=True)
    assert lo is not None
    return {
        "minimum": round(lo * 100, 4),
        "maximum": None if hi is None else round(hi * 100, 4),
        "text": stake_text(lo, lo_strict, hi, hi_strict) if stakes else "",
    }


def assess(
    statements: list[dict[str, Any]],
    subject_ids: Iterable[str],
    signals: Iterable[Mapping[str, Any]],
    as_of: str,
    canon: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    """Every regime's result on every source, or None when no designated
    party (under any of the three regimes) holds shares in the company."""
    subject_ids = [s for s in subject_ids if s]
    canon = dict(canon or {})
    names: dict[str, str] = {}
    for s in statements:
        sid = s.get("statementId")
        if s.get("recordType") in ("entity", "person") and isinstance(sid, str):
            c = canon.get(sid, sid)
            if not names.get(c):
                names[c] = party_name(s)
    subject_nodes = {canon.get(s, s) for s in subject_ids}
    designated = {
        n: d for n, d in designated_parties(signals, canon, names).items()
        if n not in subject_nodes
    }
    if not designated:
        return None
    by_source, labels = _source_edges(statements, subject_ids, canon, as_of)

    regimes_out: list[dict[str, Any]] = []
    any_row = False
    for regime in REGIMES:
        under = {n: d for n, d in designated.items() if regime.key in d.regimes}
        counted = {n for n, d in under.items() if d.basis != BASIS_NAME_ONLY}
        name_only = set(under) - counted
        rows: list[dict[str, Any]] = []
        for key in sorted(by_source):
            edges = by_source[key]
            up = _upstream(edges, _SUBJECT)
            if not (set(under) & up):
                continue
            low = _outcome(edges, counted & up, _SUBJECT, regime, high=False)
            high = _outcome(edges, counted & up, _SUBJECT, regime, high=True)
            with_names = _outcome(edges, (counted | name_only) & up, _SUBJECT, regime, high=False)
            if low.met:
                result, group = RESULT_MEETS, low.group
            elif high.met:
                result, group = RESULT_MAY_MEET, high.group
            else:
                result, group = RESULT_NOT_MET, low.group
            row: dict[str, Any] = {
                "source_id": key,
                "source": labels.get(key, key),
                "result": result,
                "figure": _figure(edges, group),
                "holders": _holders(edges, group, under, names),
                "if_confirmed": bool(not low.met and with_names.met),
            }
            if row["if_confirmed"]:
                row["name_only"] = sorted(
                    ({"party": n, "name": under[n].name} for n in with_names.group & name_only),
                    key=lambda p: (p["name"].casefold(), p["party"]),
                )
            rows.append(row)
        rank = {RESULT_MEETS: 0, RESULT_MAY_MEET: 1, RESULT_NOT_MET: 2}
        rows.sort(key=lambda r: (rank[r["result"]], -r["figure"]["minimum"], r["source"]))
        any_row = any_row or bool(rows)
        regimes_out.append({
            "regime": regime.key,
            "label": regime.label,
            "rule": regime.rule,
            "result": rows[0]["result"] if rows else RESULT_NONE,
            "if_confirmed": any(r["if_confirmed"] for r in rows),
            "sources": rows,
        })
    if not any_row:
        return None
    return {
        "method": METHOD,
        "as_of": as_of,
        "description": METHOD_DESCRIPTION,
        "not_computed": NOT_COMPUTED,
        "credit": METHOD_CREDIT,
        "regimes": regimes_out,
        "designated": [
            {
                "party": d.node,
                "name": d.name,
                "regimes": [r.key for r in REGIMES if r.key in d.regimes],
                "datasets": sorted(d.datasets),
                "basis": d.basis,
            }
            for d in sorted(designated.values(), key=lambda d: (d.name.casefold(), d.node))
        ],
    }


# ----------------------------------------------------------------------
# The signal
# ----------------------------------------------------------------------


def _join(items: list[str]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def regime_sentence(r: Mapping[str, Any]) -> str:
    """One regime's result in a sentence fragment: "OFAC (US): meets —
    designated parties hold 55% (OpenSanctions)"."""
    label, result = r["label"], r["result"]
    if result == RESULT_NONE:
        return f"{label}: no party designated under it holds shares"
    rows = r.get("sources") or []
    lead = rows[0] if rows else {}
    fig = (lead.get("figure") or {}).get("text") or ""
    where = f" ({lead['source']})" if lead.get("source") else ""
    through = any(not h.get("designated") for h in lead.get("holders") or [])
    held = f"{fig} held by designated parties" + (
        " directly or through companies they own" if through else ""
    ) + where
    if result == RESULT_MEETS:
        text = f"{label}: meets — {held}"
    elif result == RESULT_MAY_MEET and (lead.get("figure") or {}).get("maximum") is None and not (
        (lead.get("figure") or {}).get("minimum")
    ):
        text = f"{label}: may meet — designated parties hold shares, but no share is stated{where}"
    elif result == RESULT_MAY_MEET:
        text = f"{label}: may meet — {held}, a range that reaches the threshold"
    else:
        text = f"{label}: not met" + (f" — {held}" if fig else "")
    if r.get("if_confirmed") and result != RESULT_MEETS:
        text += "; it would be met if a name-only match were confirmed"
    return text


def regimes_sentence(regimes: list[Mapping[str, Any]]) -> str:
    """Every regime's result, side by side. Regimes with the same result on
    the same figure share one fragment ("OFAC (US), EU and UK: meets — …");
    those with no designated holder are named together at the end."""
    groups: dict[str, list[str]] = {}
    for r in regimes:
        if r["result"] == RESULT_NONE:
            continue
        tail = regime_sentence(r)[len(r["label"]):]
        groups.setdefault(tail, []).append(r["label"])
    parts = [f"{_join(labels)}{tail}" for tail, labels in groups.items()]
    none = [r["label"] for r in regimes if r["result"] == RESULT_NONE]
    if none:
        parts.append(
            f"{_join(none)}: no party designated under "
            + ("it" if len(none) == 1 else "them") + " holds shares"
        )
    return "; ".join(parts) + "."


def build_signal(
    signals: Iterable[Mapping[str, Any]],
    statements: list[dict[str, Any]],
    subject_ids: Iterable[str],
    *,
    subject: str,
    as_of: str,
    canon: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    """The lookup's ``SANCTIONS_OWNERSHIP_TEST`` (risk) or
    ``SANCTIONS_OWNERSHIP_TEST_POSSIBLE`` (context) signal, as a dict, or
    None when no regime meets or may meet the test."""
    subject_ids = list(subject_ids)
    payload = assess(statements, subject_ids, list(signals), as_of, canon)
    if payload is None:
        return None
    regimes = payload["regimes"]
    met = [r for r in regimes if r["result"] == RESULT_MEETS]
    possible = [r for r in regimes if r["result"] == RESULT_MAY_MEET or r["if_confirmed"]]
    if not met and not possible:
        return None
    lines = regimes_sentence(regimes)
    caveat = (
        " Computed by OpenCheck from stated shareholdings, one source at a time; "
        "ownership only — the EU and UK control tests are not computed — and not a "
        "determination that the company is sanctioned or blocked."
    )
    anchor = sorted(subject_ids)[0] if subject_ids else ""
    evidence = {**payload, "statement_id": anchor}
    if met:
        bases = {
            h.get("basis")
            for r in met for row in r["sources"] if row["result"] == RESULT_MEETS
            for h in row["holders"] if h.get("designated")
        }
        names = _join([r["label"] for r in met])
        return RiskSignal(
            code=SANCTIONS_OWNERSHIP_TEST,
            confidence="high" if bases <= {BASIS_IDENTIFIER} else "medium",
            summary=(
                f"Designated parties' stated shareholdings meet the sanctions "
                f"ownership test for {names}. {lines[0].upper()}{lines[1:]}{caveat}"
            ),
            source_id="opencheck",
            hit_id=subject,
            evidence=evidence,
            kind="risk",
        ).to_dict()
    names = _join([r["label"] for r in possible])
    return RiskSignal(
        code=SANCTIONS_OWNERSHIP_TEST_POSSIBLE,
        confidence="medium",
        summary=(
            f"Designated parties' stated shareholdings may meet the sanctions "
            f"ownership test for {names}; it is not met on the lowest reading of "
            f"the stakes, counting confirmed matches only. "
            f"{lines[0].upper()}{lines[1:]}{caveat}"
        ),
        source_id="opencheck",
        hit_id=subject,
        evidence=evidence,
        kind="context",
    ).to_dict()


def for_lookup(
    signals: Iterable[Mapping[str, Any]],
    statements: list[dict[str, Any]],
    subject_ids: Iterable[str],
    *,
    subject: str,
    as_of: str,
) -> dict[str, Any] | None:
    """``build_signal`` with the lookup's own canonicalisation (entity
    statements sharing a strong identifier are one node, as in Phase 339)."""
    from .integrated_ownership import lookup_canon

    return build_signal(
        signals, statements, subject_ids, subject=subject, as_of=as_of,
        canon=lookup_canon(statements),
    )


# ----------------------------------------------------------------------
# Report rows (PDF / Markdown / MCP)
# ----------------------------------------------------------------------


REPORT_HEADING = "Sanctions ownership test"
REPORT_COLUMNS = ("Regime", "Rule", "Result", "Holding", "Source")

SIGNAL_CODES = frozenset({SANCTIONS_OWNERSHIP_TEST, SANCTIONS_OWNERSHIP_TEST_POSSIBLE})


def signal_of(signals: Iterable[Mapping[str, Any]] | None) -> Mapping[str, Any] | None:
    """The run's sanctions-ownership signal, if it has one."""
    for s in signals or ():
        if isinstance(s, Mapping) and s.get("code") in SIGNAL_CODES:
            return s
    return None


def report_rows(signal: Mapping[str, Any] | None) -> list[tuple[str, str, str, str, str]]:
    """One row per regime and source: regime, rule, result, holding, source."""
    if not signal:
        return []
    out: list[tuple[str, str, str, str, str]] = []
    for r in (signal.get("evidence") or {}).get("regimes") or []:
        rows = r.get("sources") or []
        if not rows:
            out.append((r["label"], r["rule"], RESULT_WORDS[RESULT_NONE], "", ""))
            continue
        for row in rows:
            words = RESULT_WORDS.get(row["result"], row["result"])
            if row.get("if_confirmed"):
                words += " (would meet if a name-only match were confirmed)"
            holding = (row.get("figure") or {}).get("text") or ""
            held = "; ".join(
                f"{h['name']} {h['stake']}" + ("" if h.get("designated") else " (caught company)")
                for h in row.get("holders") or []
            )
            if held:
                holding = f"{holding} — {held}" if holding else held
            out.append((r["label"], r["rule"], words, holding, row.get("source") or ""))
    return out


def report_footer(signal: Mapping[str, Any] | None) -> list[str]:
    if not signal:
        return []
    ev = signal.get("evidence") or {}
    lines = [
        str(ev.get("description") or METHOD_DESCRIPTION),
        str(ev.get("not_computed") or NOT_COMPUTED),
    ]
    designated = ev.get("designated") or []
    if designated:
        # One line per distinct party as read: a bank's branches are separate
        # records with the same name and listing.
        said = dict.fromkeys(
            f"{d['name']} ({', '.join(d.get('regimes') or [])}; "
            f"{BASIS_WORDS.get(d.get('basis', ''), '')})"
            for d in designated
        )
        lines.append("Designated parties read: " + "; ".join(said) + ".")
    if ev.get("as_of"):
        lines.append(f"Computed from the sources as they stood on {ev['as_of']}.")
    lines.append(str(ev.get("credit") or METHOD_CREDIT))
    return lines
