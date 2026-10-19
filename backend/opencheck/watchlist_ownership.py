"""watchlist_ownership — ``ownership_changed``: a statement-level ownership diff
on every watchlist re-run (pKYC stage E, Route 2).

Until this module ``CHANGE_KINDS`` had no ownership kind. An owner replaced by
another owner with the same risk profile moved no signal, no status and no
coverage, so the re-run wrote nothing — in any register. This module keeps a
**person-free ownership digest** on the watch's baseline and compares it with
the new run's: one row per ownership-or-control relationship with the subject,
per source, keyed on the party's record; added, ended and re-sized rows become
one change each, worded without names. It catches nothing on its own — it
needs a re-run some delta caused (GLEIF, OpenSanctions, the Estonian diff, a
PSC filing, a manual re-check) — and complements Route 1's trigger, which says
*that* the register moved; this says *what*.

Stephen's decisions (18–19 Oct 2026, the stage E scoping page and this
ticket):

- **A person's digest key is a register key, else no row** (Q3). A person is
  compared only when the mapper keyed it on the register's own person key —
  a Companies House officer id or PSC item id, an Estonian ``isikukood_hash``,
  a Wikidata Q-id, a Slovak ``kuv_id`` — read back from the ``identifying``
  annotation Phase 255 puts it in. A person with none (OpenCorporates
  officers, Norway's name-derived hash) is **not a row**: the digest counts
  them as *could not be compared* per source and the entry says so. Never a
  name, a date of birth, or a hash of either, salted or not.
- **Materiality** (Q4, confirmed 19 Oct 2026): ``ownership_changed`` is
  **recheck**; **escalate** when a shareholding or voting-rights band at or
  above 25 % is added or ended — a party becoming or ceasing to be a
  registrable controller (25 % is the PSC and AMLD threshold; registers
  publish bands, so "crossing 25 %" is a ≥25 % band appearing or
  disappearing, not a figure moving). A move *between* bands above 25 %
  (50→75) is a change of degree, not of who is registrable: recheck.
- **Ownership, per source, never across sources**: the Phase 339 rules.
  GLEIF's reporting-exception bridges (a filing, not a party), MEIP (group
  membership, not ownership) and officer-only roles are left out, so the
  digest is the 2–10 rows the scoping page measured, not the 6–60 with
  officers.
- **Absence is a finding only when the producer answered**: a row whose
  source is degraded on the new run is ``ownership_unchecked``, never ended
  (the stage A ``signal_unchecked`` rule).

A baseline stored before this module carries no digest; the first re-run
after deploy records one and compares nothing (the Phase 301 baseline-shape
rule: widening the snapshot must never write a change for every watched LEI).
The digest is frozen with its ``as_of`` day, so a saved baseline is re-read
with no clock. Words shared with the page are :data:`INTEREST_WORDS` /
:data:`PARTY_WORDS` / :data:`CHANGE_WORDS`, mirrored in
``frontend/src/lib/watchlist.ts`` and pinned by ``tests/test_watchlist_ownership.py``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from .bods import lifecycle as _lifecycle
from .bods import refs as _refs
from .bods.annotations import person_identifiers_from_annotations
from .bods.officer_roles import officer_position
from .integrated_ownership import EXCLUDED_SOURCES, source_key

#: The digest's own version, stored on it. Bump when the row shape changes
#: so a baseline digest of an older shape is replaced, not compared.
DIGEST_VERSION = 1

#: The two interest types whose share band decides "crossing 25 %".
BAND_INTERESTS: frozenset[str] = frozenset({"shareholding", "votingRights"})
#: The threshold a band must reach for the escalate rule (Q4).
THRESHOLD = 25.0
#: Officer roles: a role, not ownership — left out of the digest.
OFFICER_INTERESTS: frozenset[str] = frozenset({"seniorManagingOfficial", "boardMember", "boardChair"})

#: The feed's and the page's words. Closed vocabularies, mirrored in
#: ``lib/watchlist.ts`` and pinned by a test.
INTEREST_WORDS: dict[str, str] = {
    "shareholding": "shareholding",
    "votingRights": "voting rights",
    "appointmentOfBoard": "right to appoint the board",
    "otherInfluenceOrControl": "other influence or control",
    "seniorManagingOfficial": "senior managing official",
    "settlor": "settlor",
    "trustee": "trustee",
    "protector": "protector",
    "beneficiaryOfLegalArrangement": "beneficiary",
    "rightsToSurplusAssetsOnDissolution": "rights to surplus assets",
    "rightsToProfitOrIncome": "rights to profit or income",
    "rightsGrantedByContract": "rights granted by contract",
    "conditionalRightsGrantedByContract": "conditional rights granted by contract",
    "controlViaCompanyRulesOrAgreement": "control via company rules or agreement",
    "controlByLegalFramework": "control by legal framework",
    "boardMember": "board membership",
    "boardChair": "board chair",
    "enjoymentAndUseOfAssets": "enjoyment and use of assets",
    "nominee": "nominee holding",
    "nominator": "nominator",
    "unknownInterest": "interest of unstated kind",
    "unpublishedInterest": "interest whose details are withheld",
}
PARTY_WORDS: dict[str, str] = {
    "person": "A person",
    "entity": "A company",
    "bo": "A registered beneficial owner",
}
CHANGE_WORDS: dict[str, str] = {
    "added": "added",
    "ended": "ended",
    "changed": "changed",
}

#: The Phase 193 officer-id note is not in the Phase 255 identifier shape;
#: read its id the same way the page does.
_OFFICER_NOTE_RE = re.compile(r"Grouped on Companies House officer id (\S+):")


# ---------------------------------------------------------------------------
# Reading the bundle
# ---------------------------------------------------------------------------


def _rd(stmt: Mapping[str, Any]) -> Mapping[str, Any]:
    rd = stmt.get("recordDetails")
    return rd if isinstance(rd, Mapping) else {}


def _excluded_source(stmt: Mapping[str, Any]) -> bool:
    from .integrated_ownership import EXCLUDED_DESCRIPTIONS

    src = stmt.get("source")
    src = src if isinstance(src, Mapping) else {}
    return (
        str(src.get("opencheckSourceId") or "").strip() in EXCLUDED_SOURCES
        or str(src.get("description") or "").strip() in EXCLUDED_DESCRIPTIONS
    )


def person_register_key(stmt: Mapping[str, Any]) -> str | None:
    """The register's own key for a person, or ``None``: the first
    ``scheme:id`` the Phase 255 ``identifying`` annotation carries (a PSC item
    id, an Estonian hash, a Q-id…), else the Phase 193 officer id. A person
    with neither is invisible to the diff (Q3)."""
    idents = person_identifiers_from_annotations(dict(stmt))
    if idents:
        first = sorted(idents, key=lambda i: (i["scheme"], i["id"]))[0]
        return f"{first['scheme']}:{first['id']}"
    for a in stmt.get("annotations") or []:
        if isinstance(a, Mapping) and a.get("motivation") == "identifying":
            m = _OFFICER_NOTE_RE.search(str(a.get("description") or ""))
            if m:
                return f"GB-COH-OFFICER:{m.group(1)}"
    return None


def band_of(interest: Mapping[str, Any]) -> list[Any] | None:
    """``[low, high, exclusive_low, exclusive_high]`` of a band interest's
    share, ``None`` when it states none. ``exact`` is a band of one value."""
    share = interest.get("share")
    if not isinstance(share, Mapping):
        return None
    exact = share.get("exact")
    if isinstance(exact, (int, float)):
        return [float(exact), float(exact), False, False]
    # BODS v0.4: ``exclusiveMinimum`` / ``exclusiveMaximum`` are *numbers* —
    # the exclusive bound itself (a PSC "25-to-50" band is exclusiveMinimum
    # 25, maximum 50) — so each end is whichever of the pair is stated.
    lo, exlo = share.get("minimum"), share.get("exclusiveMinimum")
    hi, exhi = share.get("maximum"), share.get("exclusiveMaximum")
    low = float(lo) if isinstance(lo, (int, float)) else float(exlo) if isinstance(exlo, (int, float)) else None
    high = float(hi) if isinstance(hi, (int, float)) else float(exhi) if isinstance(exhi, (int, float)) else None
    if low is None and high is None:
        return None
    return [low, high, low is not None and not isinstance(lo, (int, float)), high is not None and not isinstance(hi, (int, float))]


def band_at_or_above_threshold(band: list[Any] | None) -> bool:
    """Whether a band's **low end** reaches the 25 % line — the Q4 test: a
    party that holds at least this much is a registrable controller."""
    if not band:
        return False
    lo = band[0]
    if lo is None:
        return False
    # An exclusive floor at 25 ("more than 25 %", the PSC register's own
    # 25-to-50 band) is a registrable holding too: the line is the bound,
    # whichever side of it the register puts the party.
    return lo >= THRESHOLD


def band_words(band: list[Any] | None) -> str:
    if not band:
        return "an unstated share"
    lo, hi, exlo, exhi = band

    def n(v: float) -> str:
        return f"{v:g}%"

    if lo is not None and hi is not None and lo == hi:
        return f"exactly {n(lo)}"
    if lo is not None and hi is not None:
        if exlo and exhi:
            return f"over {lo:g}% to under {hi:g}%"
        if exlo:
            return f"over {lo:g}% to {hi:g}%"
        if exhi:
            return f"{lo:g}% to under {hi:g}%"
        return f"{lo:g}–{hi:g}%"
    if lo is not None:
        return f"{'more than' if exlo else 'at least'} {n(lo)}"
    return f"{'under' if exhi else 'at most'} {n(hi)}"


def _direct(values: set[str]) -> str:
    values = {v for v in values if v}
    if not values:
        return "unknown"
    return values.pop() if len(values) == 1 else "mixed"


def digest(bods: list[dict[str, Any]], subject_ids: Iterable[str], as_of: str) -> dict[str, Any]:
    """The person-free ownership digest of one run.

    ``rows`` is keyed ``"{source_id}|{party key}"``; each row carries the
    source, the party kind, its ownership interests, the bands of its
    shareholding / voting-rights interests, whether any interest is flagged
    as beneficial ownership, direct / indirect, and whether every
    relationship for the pair has ended at ``as_of``. ``uncompared`` counts,
    per source, the parties left out for want of a register key. Nothing in
    it names a person.
    """
    from .bods.mapper import gleif_reporting_exception

    subject = set(subject_ids)
    index = _refs.statement_index(bods)
    resolve = _refs.resolver(bods)
    rows: dict[str, dict[str, Any]] = {}
    uncompared: dict[str, int] = {}
    uncompared_seen: set[str] = set()

    for stmt in bods:
        if stmt.get("recordType") != "relationship" or _excluded_source(stmt):
            continue
        rd = _rd(stmt)
        if resolve(rd.get("subject")) not in subject:
            continue
        if _refs.unspecified_party(rd.get("interestedParty")) is not None:
            continue  # a missing-information filing, not a party
        if gleif_reporting_exception(dict(stmt)) is not None:
            continue
        party_sid = resolve(rd.get("interestedParty"))
        party = index.get(party_sid)
        if not party:
            continue
        party_kind = "person" if party.get("recordType") == "person" else "entity"
        if party_kind == "person" and str(_rd(party).get("personType") or "") in ("unknownPerson", "anonymousPerson"):
            continue  # a bridge or a withheld party — no one to compare
        if party_kind == "entity" and str(_rd(party).get("entityType", {}).get("type") if isinstance(_rd(party).get("entityType"), Mapping) else "") in ("unknownEntity", "anonymousEntity"):
            continue
        interests = [i for i in rd.get("interests") or [] if isinstance(i, Mapping)]
        ownership = [
            i for i in interests
            if str(i.get("type") or "") not in OFFICER_INTERESTS and officer_position(i) is None
        ]
        if not ownership:
            continue  # an officer appointment, however typed
        source_id, _label = source_key(stmt)
        if party_kind == "person":
            key = person_register_key(party)
            if key is None:
                tag = f"{source_id}|{party.get('recordId') or party_sid}"
                if tag not in uncompared_seen:
                    uncompared_seen.add(tag)
                    uncompared[source_id] = uncompared.get(source_id, 0) + 1
                continue
            party_key = key
        else:
            party_key = str(party.get("recordId") or party_sid)
        closed = _lifecycle.record_closed(dict(stmt))
        ended = _lifecycle.relationship_lifecycle([dict(i) for i in ownership], closed, as_of).ended
        row_key = f"{source_id}|{party_key}"
        row = rows.get(row_key)
        if row is None:
            row = {
                "source_id": source_id,
                "party": party_kind,
                "interests": set(),
                "bands": [],
                "bo": False,
                "direct": set(),
                "ended": True,
            }
            rows[row_key] = row
        # A current relationship for the pair makes the row current; its
        # interests are what the row describes. An ended one beside it only
        # counts while nothing current has been seen.
        if ended and not row["ended"]:
            continue
        if not ended and row["ended"]:
            row.update(interests=set(), bands=[], bo=False, direct=set(), ended=False)
        for i in ownership:
            kind = str(i.get("type") or "")
            row["interests"].add(kind)
            if i.get("beneficialOwnershipOrControl") is True:
                row["bo"] = True
            row["direct"].add(str(i.get("directOrIndirect") or ""))
            if kind in BAND_INTERESTS:
                b = band_of(i)
                if b is not None and b not in row["bands"]:
                    row["bands"].append(b)

    out_rows = {
        k: {
            "source_id": r["source_id"],
            "party": r["party"],
            "interests": sorted(r["interests"]),
            "bands": sorted(r["bands"], key=lambda b: (b[0] if b[0] is not None else -1, b[1] if b[1] is not None else 101)),
            "bo": r["bo"],
            "direct": _direct(r["direct"]),
            "ended": r["ended"],
        }
        for k, r in sorted(rows.items())
    }
    return {"version": DIGEST_VERSION, "as_of": as_of, "rows": out_rows, "uncompared": dict(sorted(uncompared.items()))}


# ---------------------------------------------------------------------------
# The diff
# ---------------------------------------------------------------------------


def _crossed(before: Mapping[str, Any] | None, after: Mapping[str, Any] | None) -> bool:
    """The Q4 test: a ≥25 % band present on one side and not the other."""
    had = any(band_at_or_above_threshold(b) for b in (before or {}).get("bands") or []) if before and not before.get("ended") else False
    has = any(band_at_or_above_threshold(b) for b in (after or {}).get("bands") or []) if after and not after.get("ended") else False
    return had != has


def _comparable(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (tuple(row.get("interests") or []), tuple(tuple(b) for b in row.get("bands") or []), bool(row.get("bo")), row.get("direct"))


def diff(before: Mapping[str, Any] | None, after: Mapping[str, Any], degraded_sources: Iterable[str]) -> list[dict[str, Any]]:
    """Every ownership change between two digests, one per row. ``before``
    may be ``None`` or of another version: then nothing is compared. A row
    gone or ended while its source is degraded is ``ownership_unchecked``
    (one per source, counted), never ended."""
    if not before or int(before.get("version") or 0) != int(after.get("version") or 0):
        return []
    degraded = set(degraded_sources)
    rows_a: dict[str, dict[str, Any]] = dict(before.get("rows") or {})
    rows_b: dict[str, dict[str, Any]] = dict(after.get("rows") or {})
    uncompared_b: dict[str, int] = dict(after.get("uncompared") or {})
    changes: list[dict[str, Any]] = []
    unchecked: dict[str, int] = {}

    def change(kind: str, key: str, a: Mapping[str, Any] | None, b: Mapping[str, Any] | None) -> None:
        row = b or a or {}
        changes.append(
            {
                "kind": "ownership_changed",
                "change": kind,
                "source_id": row.get("source_id"),
                "party": row.get("party"),
                "key": key.split("|", 1)[1] if "|" in key else key,
                "interests": sorted(set((a or {}).get("interests") or []) | set((b or {}).get("interests") or [])),
                "bands_before": list((a or {}).get("bands") or []) if a else None,
                "bands_after": list((b or {}).get("bands") or []) if b else None,
                "bo": bool((b or a or {}).get("bo")),
                "crossed_25": _crossed(a, b),
                "uncompared": uncompared_b.get(str(row.get("source_id")), 0),
            }
        )

    for key in sorted(set(rows_a) | set(rows_b)):
        a, b = rows_a.get(key), rows_b.get(key)
        source_id = str((b or a or {}).get("source_id") or "")
        if a and not b:
            if source_id in degraded:
                unchecked[source_id] = unchecked.get(source_id, 0) + 1
            elif not a.get("ended"):
                change("ended", key, a, None)
            continue
        if b and not a:
            if not b.get("ended"):
                change("added", key, None, b)
            continue
        assert a is not None and b is not None
        if not a.get("ended") and b.get("ended"):
            if source_id in degraded:
                unchecked[source_id] = unchecked.get(source_id, 0) + 1
            else:
                change("ended", key, a, b)
            continue
        if a.get("ended") and not b.get("ended"):
            change("added", key, a, b)
            continue
        if not a.get("ended") and _comparable(a) != _comparable(b):
            change("changed", key, a, b)
    for source_id, n in sorted(unchecked.items()):
        changes.append({"kind": "ownership_unchecked", "source_id": source_id, "parties": n})
    return changes


# ---------------------------------------------------------------------------
# Words
# ---------------------------------------------------------------------------


def interest_words(types: Iterable[str]) -> str:
    words = [INTEREST_WORDS.get(t, t) for t in types]
    if not words:
        return "interest"
    if len(words) == 1:
        return words[0]
    return ", ".join(words[:-1]) + " and " + words[-1]


def bands_words(bands: list[list[Any]] | None) -> str:
    if not bands:
        return ""
    return " and ".join(band_words(b) for b in bands)


def change_sentence(change: Mapping[str, Any], source_label: str) -> str:
    """The feed's sentence for an ``ownership_changed`` /
    ``ownership_unchecked`` change — never a name."""
    if change.get("kind") == "ownership_unchecked":
        n = int(change.get("parties") or 0)
        return (
            f"{n} part{'y' if n == 1 else 'ies'}' holdings could not be re-checked — "
            f"{source_label} did not answer."
        )
    who = PARTY_WORDS["bo"] if change.get("bo") else PARTY_WORDS.get(str(change.get("party") or ""), "A party")
    what = interest_words(change.get("interests") or [])
    kind = str(change.get("change") or "")
    before, after = bands_words(change.get("bands_before")), bands_words(change.get("bands_after"))
    if kind == "changed":
        if before != after:
            body = f"{who}'s {what} changed from {before or 'an unstated share'} to {after or 'an unstated share'}"
        else:
            body = f"{who}'s {what} changed"
    else:
        band = after if kind == "added" else before
        body = f"{who}'s {what}{f' of {band}' if band else ''} {CHANGE_WORDS.get(kind, kind)}"
    return f"{body} ({source_label})."


def uncompared_sentence(changes: Iterable[Mapping[str, Any]], label: Any) -> list[str]:
    """One line per source whose parties could not be compared, from the
    ``uncompared`` count each ownership change carries."""
    seen: dict[str, int] = {}
    for c in changes:
        if c.get("kind") == "ownership_changed" and int(c.get("uncompared") or 0):
            seen[str(c.get("source_id"))] = int(c["uncompared"])
    return [
        f"{n} part{'y' if n == 1 else 'ies'} could not be compared ({label(sid)} publishes no person key)."
        for sid, n in sorted(seen.items())
    ]
