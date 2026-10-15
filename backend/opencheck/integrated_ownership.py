"""Integrated ownership along the ownership path (Phase 339).

For every party above the subject — person or entity — how much of the
subject it holds, directly and through the companies it holds: T-Rank's
*integrated ownership* (docs.trank.no/white_papers, "Integrated ownership"),
multiply along a path, add across paths, give a company's own shares back to
its other holders. The arithmetic is ``ownership_math.integrated_ownership``,
the one Phase 338's state figure uses; this module decides which BODS
relationships feed it.

**A context figure, never a signal.** Nothing here is read by the risk engine,
the verdict or ``/signalstats`` (``tests/test_integrated_ownership_phase339.py``
pins that no such module imports it), and there is no threshold: a 26% holder
and a 2% holder are stated the same way. Stephen's decisions on the ticket
(15 Oct 2026):

* **Shareholding interests only.** A ``votingRights`` path is not a share of
  the company; a party reached only that way is listed as "voting rights, not
  shares, on this path" with no figure. Roles (directors, officers, senior
  managing officials) and ``otherInfluenceOrControl`` are not ownership and
  are not read at all.
* **Per source, never across sources.** Each source's relationships are
  computed on their own; when two sources reach one party, both figures are
  listed with their source. A figure is never the sum of two publishers.
* **GLEIF and MEIP are not ownership.** GLEIF Level 2 is accounting
  consolidation, MEIP group membership: neither publishes a share, and their
  edges are left out (``EXCLUDED_SOURCES``).
* **An ended holding has no figure.** A party whose every path has ended is
  listed as "formerly", with no figure.
* **No stand-ins.** An unstated share adds nothing to the lower bound and
  leaves the upper bound open ("at least X%"), as in Phase 338.

``frontend/src/lib/integratedOwnership.ts`` is the mirror. Both run the case
table ``frontend/src/lib/integratedOwnershipCases.json``, and
``tests/test_frontend_parity.py`` compares the excluded-source lists.

Pure: statements in, a JSON payload out. ``as_of`` decides which holdings have
ended; the lookup freezes it into the ``integrated_ownership`` event so a saved
report replays the figures that were true on the day it ran.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .bods.lifecycle import interest_ended, record_closed
from .bods.refs import resolver
from .ownership_math import REASON_WORDS, Edge, figure_text, share_bounds
from .ownership_math import integrated_ownership as _integrated

METHOD = "integrated_ownership"

#: What the reader is told the figure is, on the page, in the PDF / Markdown
#: report, in the MCP block and in the BODS annotation. Plain words; the
#: method is credited to T-Rank where it is described.
METHOD_DESCRIPTION = (
    "How much of the company each party holds, directly and through the "
    "companies it holds: shares are multiplied along each path and added "
    "across paths, and a company's own shares are given back to its other "
    "holders. Each source is computed on its own; figures from two sources "
    "are listed side by side, never added. Shareholdings only — voting "
    "rights, roles and control are not shares. For a person it is arithmetic "
    "on what was filed, not a determination of beneficial ownership. Context, "
    "not a finding."
)
METHOD_CREDIT = (
    "Method: integrated ownership as published by T-Rank "
    "(https://docs.trank.no/white_papers)."
)
METHOD_URL = "https://docs.trank.no/white_papers"

#: Sources whose relationships are not ownership. ``gleif`` / ``bods_gleif``:
#: GLEIF Level 2 is accounting consolidation; ``meip``: OECD-UNSD group
#: membership. Matched on ``source.opencheckSourceId`` and, for statements
#: stamped before Phase 267, on the description.
EXCLUDED_SOURCES: frozenset[str] = frozenset({"gleif", "bods_gleif", "meip"})
EXCLUDED_DESCRIPTIONS: frozenset[str] = frozenset({
    "GLEIF",
    "GLEIF — Global LEI Foundation (BODS bulk dataset)",
    "OECD-UNSD Multinational Enterprise Information Platform (MEIP)",
    "OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet.",
})

SHAREHOLDING = "shareholding"
VOTING_RIGHTS = "votingRights"

STATUS_CURRENT = "current"
STATUS_VOTING = "voting_rights_only"
STATUS_FORMERLY = "formerly"

STATUS_WORDS = {
    STATUS_VOTING: "Voting rights, not shares, on this path",
    STATUS_FORMERLY: "Formerly — every path to the company has ended",
}

_SUBJECT = "\x00subject"


def _rd(stmt: Mapping[str, Any]) -> Mapping[str, Any]:
    rd = stmt.get("recordDetails")
    return rd if isinstance(rd, Mapping) else {}


def source_key(stmt: Mapping[str, Any]) -> tuple[str, str]:
    """(key, label) of a statement's source: ``opencheckSourceId`` when
    stamped, else the description; the label is the description."""
    src = stmt.get("source")
    src = src if isinstance(src, Mapping) else {}
    desc = str(src.get("description") or "").strip()
    sid = str(src.get("opencheckSourceId") or "").strip()
    return (sid or desc), (desc or sid)


def _excluded(stmt: Mapping[str, Any]) -> bool:
    src = stmt.get("source")
    src = src if isinstance(src, Mapping) else {}
    sid = str(src.get("opencheckSourceId") or "").strip()
    desc = str(src.get("description") or "").strip()
    return sid in EXCLUDED_SOURCES or desc in EXCLUDED_DESCRIPTIONS


def party_name(stmt: Mapping[str, Any] | None) -> str:
    """The display name of an entity or person statement."""
    if not stmt:
        return ""
    rd = _rd(stmt)
    name = rd.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    for n in rd.get("names") or []:
        if isinstance(n, Mapping):
            full = str(n.get("fullName") or "").strip()
            if full:
                return full
    return ""


def _edge_kind(
    stmt: Mapping[str, Any], as_of: str
) -> tuple[str, tuple[float, float | None] | None] | None:
    """What one relationship contributes: ("share", bounds) for a current
    shareholding (bounds None when no share is stated), ("voting", None) for
    current voting rights with no current shareholding, ("ended", None) when
    it held shares or votes that have all ended, else None (roles, control)."""
    closed = record_closed(dict(stmt))
    kinds = []
    for i in _rd(stmt).get("interests") or []:
        if isinstance(i, Mapping) and i.get("type") in (SHAREHOLDING, VOTING_RIGHTS):
            kinds.append(i)
    if not kinds:
        return None
    current_share = [
        i for i in kinds
        if i.get("type") == SHAREHOLDING and not interest_ended(dict(i), closed, as_of)
    ]
    if current_share:
        lo: float | None = None
        hi: float | None = None
        stated = False
        unstated = False
        for i in current_share:
            b = share_bounds(i.get("share") if isinstance(i.get("share"), dict) else None)
            if b is None:
                unstated = True
                continue
            stated = True
            lo = b[0] if lo is None else max(lo, b[0])
            if b[1] is None:
                unstated = True
            else:
                hi = b[1] if hi is None else max(hi, b[1])
        if not stated:
            return "share", None
        return "share", (lo or 0.0, None if unstated else hi)
    if any(
        i.get("type") == VOTING_RIGHTS and not interest_ended(dict(i), closed, as_of)
        for i in kinds
    ):
        return "voting", None
    return "ended", None


def _upstream(edges: Iterable[tuple[str, str]], target: str) -> set[str]:
    owners: dict[str, set[str]] = {}
    for a, b in edges:
        owners.setdefault(b, set()).add(a)
    seen: set[str] = set()
    stack = [target]
    while stack:
        n = stack.pop()
        for a in owners.get(n, ()):
            if a not in seen:
                seen.add(a)
                stack.append(a)
    seen.discard(target)
    return seen


def figure_words(fig: Mapping[str, Any]) -> str:
    """A figure in words: "67%", "at least 24%", or why there is none."""
    text = figure_text(dict(fig))
    if text:
        return text
    reason = fig.get("reason")
    if reason and reason in REASON_WORDS:
        return f"No figure — {REASON_WORDS[reason]}"
    return "No figure"


def compute(
    statements: list[dict[str, Any]],
    subject_ids: Iterable[str],
    as_of: str,
    canon: Mapping[str, str] | Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """The ``integrated_ownership`` payload.

    ``subject_ids``: the statementIds of the subject's entity statements (one
    per source before reconciliation); they are treated as one company.
    ``canon``: statementId → canonical node id (FullCheck's reconcile remap),
    so one party named by two sources is one row; ids absent from it are
    their own node.
    """
    if callable(canon):
        canon_fn = canon
    else:
        table = dict(canon or {})

        def canon_fn(x: str) -> str:
            return table.get(x, x)

    resolve = resolver(statements)
    subjects = {canon_fn(s) for s in subject_ids if s}
    by_canon: dict[str, dict[str, Any]] = {}
    for s in statements:
        if s.get("recordType") in ("entity", "person") and isinstance(s.get("statementId"), str):
            cid = canon_fn(s["statementId"])
            prev = by_canon.get(cid)
            if prev is None or (not party_name(prev) and party_name(s)):
                by_canon[cid] = s

    def node(raw: Any) -> str:
        sid = resolve(raw)
        if not sid:
            return ""
        c = canon_fn(sid)
        return _SUBJECT if c in subjects else c

    # source key → its edges
    share: dict[str, list[Edge]] = {}
    voting: dict[str, list[tuple[str, str]]] = {}
    ended: dict[str, list[tuple[str, str]]] = {}
    labels: dict[str, str] = {}
    for s in statements:
        if s.get("recordType") != "relationship" or _excluded(s):
            continue
        rd = _rd(s)
        owned, owner = node(rd.get("subject")), node(rd.get("interestedParty"))
        if not owned or not owner:
            continue
        kind = _edge_kind(s, as_of)
        if kind is None:
            continue
        key, label = source_key(s)
        labels.setdefault(key, label)
        if kind[0] == "share":
            b = kind[1]
            share.setdefault(key, []).append(
                Edge(owner, owned, 0.0, None) if b is None else Edge(owner, owned, b[0], b[1])
            )
        elif kind[0] == "voting":
            voting.setdefault(key, []).append((owner, owned))
        else:
            ended.setdefault(key, []).append((owner, owned))

    holders: dict[str, dict[str, Any]] = {}

    def holder(pid: str) -> dict[str, Any]:
        h = holders.get(pid)
        if h is None:
            stmt = by_canon.get(pid)
            h = {
                "party": pid,
                "name": party_name(stmt) or pid,
                "kind": "person" if (stmt or {}).get("recordType") == "person" else "entity",
                "figures": [],
                "_voting": [],
                "_ended": [],
            }
            holders[pid] = h
        return h

    for key in sorted(set(share) | set(voting) | set(ended)):
        sedges = share.get(key, [])
        spairs = [(e.owner, e.owned) for e in sedges]
        with_shares = _upstream(spairs, _SUBJECT)
        with_votes = _upstream(spairs + voting.get(key, []), _SUBJECT)
        with_ended = _upstream(spairs + voting.get(key, []) + ended.get(key, []), _SUBJECT)
        for pid in sorted(with_shares):
            r = _integrated(sedges, {pid}, _SUBJECT)
            fig = {"source_id": key, "source": labels[key], **r.as_dict()}
            fig.pop("method", None)
            if "via" in fig:
                fig["via"] = [v for v in fig["via"] if v != _SUBJECT]
            fig["direct"] = any(e.owner == pid and e.owned == _SUBJECT for e in sedges)
            fig["via_names"] = [
                party_name(by_canon.get(v)) or v for v in fig.get("via", [])
            ]
            fig["text"] = figure_words(fig)
            holder(pid)["figures"].append(fig)
        for pid in sorted(with_votes - with_shares):
            holder(pid)["_voting"].append(key)
        for pid in sorted(with_ended - with_votes):
            holder(pid)["_ended"].append(key)

    out: list[dict[str, Any]] = []
    for h in holders.values():
        if h["figures"]:
            h["status"] = STATUS_CURRENT
            h["figures"].sort(
                key=lambda f: (-(f.get("minimum") or 0.0), f["source"], f["source_id"])
            )
            h["sources"] = [f["source"] for f in h["figures"]]
        elif h["_voting"]:
            h["status"] = STATUS_VOTING
            h["sources"] = sorted({labels[k] for k in h["_voting"]})
        else:
            h["status"] = STATUS_FORMERLY
            h["sources"] = sorted({labels[k] for k in h["_ended"]})
        if h["status"] != STATUS_CURRENT:
            h["note"] = STATUS_WORDS[h["status"]]
        del h["_voting"], h["_ended"]
        out.append(h)

    rank = {STATUS_CURRENT: 0, STATUS_VOTING: 1, STATUS_FORMERLY: 2}
    out.sort(
        key=lambda h: (
            rank[h["status"]],
            -max((f.get("minimum") or 0.0) for f in h["figures"]) if h["figures"] else 0.0,
            h["name"].casefold(),
            h["party"],
        )
    )
    return {
        "method": METHOD,
        "as_of": as_of,
        "description": METHOD_DESCRIPTION,
        "credit": METHOD_CREDIT,
        "holders": out,
    }


# ----------------------------------------------------------------------
# The lookup event and the frozen copy (exports, MCP)
# ----------------------------------------------------------------------
def lookup_canon(bods: list[dict[str, Any]]) -> dict[str, str]:
    """statementId → canonical id for a lookup's bundle: entity statements
    that share a strong identifier are one node (``consistency.referent_groups``
    — the rule FullCheck's reconcile merges on). People are not merged here;
    FullCheck merges them on name and date of birth together."""
    from .consistency import referent_groups

    out: dict[str, str] = {}
    for group in referent_groups(bods):
        ids = sorted(s["statementId"] for s in group if isinstance(s.get("statementId"), str))
        for i in ids:
            out[i] = ids[0]
    return out


def for_lookup(subject_ref: str, bods: list[dict[str, Any]], as_of: str) -> dict[str, Any]:
    """The ``integrated_ownership`` event payload for a lookup run."""
    from .subject_profile import subject_statements

    ids = [s["statementId"] for s in subject_statements(subject_ref, bods)
           if isinstance(s.get("statementId"), str)]
    payload = compute(bods, ids, as_of, lookup_canon(bods))
    payload["subject_ids"] = ids
    return payload


def with_figures(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """The holders that carry at least one figure or a status note."""
    if not payload:
        return []
    return [h for h in payload.get("holders") or [] if isinstance(h, dict)]


def route_words(fig: Mapping[str, Any]) -> str:
    """The route in words: directly, through A and B, or both."""
    names = [n for n in fig.get("via_names") or [] if n]
    if not names:
        return "directly" if fig.get("direct") else ""
    through = f"through {', '.join(names)}"
    return f"directly and {through}" if fig.get("direct") else through


def holder_line(h: Mapping[str, Any]) -> str:
    """One holder in words, for the annotation and the MCP summary:
    "Ministry of Finance — 67% (OpenSanctions); 50.1% (Wikidata)"."""
    if h.get("status") != STATUS_CURRENT:
        return f"{h.get('name')} — {h.get('note')} ({'; '.join(h.get('sources') or [])})"
    parts = []
    for f in h.get("figures") or []:
        route = route_words(f)
        route = f", {route}" if route else ""
        parts.append(f"{f.get('text')} ({f.get('source')}{route})")
    return f"{h.get('name')} — {'; '.join(parts)}"


def apply_to_bods(
    bods: list[dict[str, Any]], payload: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    """*bods* with one ``commenting`` annotation on the subject's first entity
    statement stating each holder's figure, the method and its credit. Copied,
    never mutated (the statements are also a saved report's event payloads).
    Nothing is added when no holder is listed."""
    import copy

    holders = with_figures(payload)
    ids = set((payload or {}).get("subject_ids") or [])
    if not holders or not ids:
        return bods
    idx = next(
        (
            i for i, s in enumerate(bods)
            if s.get("statementId") in ids and s.get("recordType") == "entity"
        ),
        None,
    )
    if idx is None:
        return bods
    stmt = copy.deepcopy(bods[idx])
    lines = "; ".join(holder_line(h) for h in holders)
    ann: dict[str, Any] = {
        "statementPointerTarget": "/",
        "motivation": "commenting",
        "description": (
            f"Integrated ownership, added by OpenCheck: {lines}. "
            f"{METHOD_DESCRIPTION} {METHOD_CREDIT}"
        ),
        "url": METHOD_URL,
        "createdBy": {"name": "OpenCheck", "uri": "https://opencheck.world"},
    }
    if (payload or {}).get("as_of"):
        ann["creationDate"] = payload["as_of"]  # type: ignore[index]
    stmt["annotations"] = [*(stmt.get("annotations") or []), ann]
    out = list(bods)
    out[idx] = stmt
    return out


# ----------------------------------------------------------------------
# The report table (PDF / HTML / Markdown) — read from the frozen payload
# ----------------------------------------------------------------------
REPORT_HEADING = "Integrated ownership"
REPORT_COLUMNS = ("Holder", "Integrated ownership", "Source", "Route")
#: Rows printed in a report; the rest are counted beneath the table.
REPORT_LIMIT = 40


def report_rows(payload: Mapping[str, Any] | None) -> list[tuple[str, str, str, str]]:
    """One row per figure (a holder reached by two sources has two rows), and
    one row for a holder listed with a note. Never re-computed: a saved
    report prints what was frozen with the run."""
    rows: list[tuple[str, str, str, str]] = []
    for h in with_figures(payload)[:REPORT_LIMIT]:
        name = str(h.get("name") or "")
        if h.get("kind") == "person":
            name = f"{name} (person)"
        if h.get("status") != STATUS_CURRENT:
            rows.append((name, str(h.get("note") or ""), "; ".join(h.get("sources") or []), ""))
            continue
        for f in h.get("figures") or []:
            rows.append(
                (name, str(f.get("text") or ""), str(f.get("source") or ""), route_words(f))
            )
    return rows


def report_footer(payload: Mapping[str, Any] | None) -> list[str]:
    """The notes under the table: what the figure is, the credit, the date,
    and how many holders were left out."""
    if not payload:
        return []
    out = [str(payload.get("description") or METHOD_DESCRIPTION),
           str(payload.get("credit") or METHOD_CREDIT)]
    if payload.get("as_of"):
        out.append(f"Computed from the sources as they stood on {payload['as_of']}.")
    extra = len(with_figures(payload)) - REPORT_LIMIT
    if extra > 0:
        plural = "s" if extra != 1 else ""
        out.append(
            f"{extra} further holder{plural} not shown; the full list is in the JSON export."
        )
    return out
