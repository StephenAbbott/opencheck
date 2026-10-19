"""watchlist_materiality — does a watchlist change matter? Triage as data.

Part A of the "Watchlist as pKYC" ticket. Every entry the watchlist writes
used to carry equal weight: a ``coverage_changed`` from a briefly degraded
register sat beside ``signal_new: SANCTIONED``. This module maps each change
the diff can name (:data:`opencheck.watchlist.CHANGE_KINDS`, the material
GLEIF fields, GLEIF's Legal Entity Events, the risk-signal codes and the
register tier's own trigger) to one of three **materiality tiers**, and the
entry carries the worst of its changes' tiers as its headline class.

The three tiers (Stephen, 17 Oct 2026)
-------------------------------------

* ``note`` — **Note only.** Recorded for the history; nothing to act on by
  itself.
* ``recheck`` — **Recheck the source.** Something about the company's record
  or exposure moved; open the report and look. "Source", not "register": a
  recheck can be prompted by OpenSanctions, ICIJ or OpenAleph as much as by a
  national register.
* ``escalate`` — **Escalate.** A finding against the subject, or the subject
  ceased or changed hands; route to whoever decides.

What to *do* stays the reader's. The tier says what is proportionate; it
starts no re-run and opens no case.

The tables are a closed vocabulary, mirrored in words only by the frontend
(``lib/watchlist.ts`` renders what the entry carries and never re-grades) and
stated publicly in ``docs/watchlist.md``; ``tests/test_watchlist_materiality.py``
pins the three against each other. :data:`SIGNAL_TIER` is **its own table,
not derived from the graph's badge severity** (``lib/graphStyle.ts``): that
ranking was tuned for badge stacking — ``COUNTER_SANCTIONED`` is deliberately
inverted below ``SANCTIONS_LINKED`` so a counter-listing never outranks a real
finding on a node — and must not silently decide triage.

Decisions recorded on the Notion ticket "pKYC stage A: materiality tier per
change kind" (17 Oct 2026):

* ``signal_unchecked`` / ``context_unchecked`` are **note**. Absence of
  evidence is not a finding; part C's periodic-review line is where "N
  sources not re-fetched since" is said.
* Legal Entity Events: a ``COMPLETED`` event carries its type's tier;
  ``IN_PROGRESS`` one step lower; ``WITHDRAWN_CANCELLED`` is note (the event
  did not happen). An unknown status is read as in progress, never as
  completed; an unknown type is recheck, never silent.
* ``jurisdiction`` (snapshot and GLEIF field) is **escalate**;
  ``direct_children`` is note; ``OFFSHORE_LEAKS`` and ``STATE_CONTROLLED``
  are note; the two context codes ``SANCTIONS_OWNERSHIP_TEST_POSSIBLE`` and
  ``RECORD_CONSISTENCY`` stay note with every other context code.
* Graded **once, at write time, and kept** (:data:`MATERIALITY_RULES` is
  stored beside the tier). The entry is the record; part D hangs
  dispositions off it, and "why was A reviewed and B not" needs the tier A
  carried on the day. An entry written before this module is graded on read
  under the current table and labelled so, never back-filled.
"""

from __future__ import annotations

from typing import Any

from .bods import gleif_events as _gleif_events

#: The table's version, mirroring ``watchlist.SIGNAL_RULES`` and
#: ``verdict.VERDICT_TEMPLATE``. Bump it whenever a row moves tier, and say
#: what moved in :data:`MATERIALITY_RULES_CHANGED`; an entry stores the
#: version it was graded under so a later re-drawing of a line is visible as
#: the rule's doing, not the company's.
MATERIALITY_RULES = 1

#: version -> what that version changed. 1 is the first table.
MATERIALITY_RULES_CHANGED: dict[int, str] = {1: "the first table (pKYC stage A)"}

NOTE = "note"
RECHECK = "recheck"
ESCALATE = "escalate"
#: Ordered, lowest first. The entry's tier is the ``max`` over its changes.
TIERS: tuple[str, ...] = (NOTE, RECHECK, ESCALATE)
_RANK = {t: i for i, t in enumerate(TIERS)}

#: The label a reader sees. The frontend carries the same three strings
#: (``MATERIALITY_LABELS`` in ``lib/watchlist.ts``), pinned by a test.
TIER_LABELS: dict[str, str] = {
    NOTE: "Note only",
    RECHECK: "Recheck the source",
    ESCALATE: "Escalate",
}

#: One sentence per tier, for the feed and the page's legend.
TIER_SENTENCES: dict[str, str] = {
    NOTE: "Recorded for the history; nothing to act on by itself.",
    RECHECK: "Something about the company's record or exposure moved; open the report and look.",
    ESCALATE: "A finding against the subject, or the subject ceased or changed hands; route to whoever decides.",
}

#: The Atom ``<category scheme>`` the materiality category is published under,
#: beside the existing tier category (``term="gleif"`` …), which is unchanged.
ATOM_SCHEME = "https://opencheck.world/watchlist/materiality"


# ---------------------------------------------------------------------------
# Table 1 — change kinds (the snapshot diff)
# ---------------------------------------------------------------------------

#: ``CHANGE_KIND`` -> tier, for every kind whose tier does not depend on its
#: content. The kinds missing here are graded by a rule below:
#: ``gleif_field`` (Table 2), ``register_status`` (terminal or not),
#: ``signal_new`` (Table 3), ``signal_unchecked`` (note — decided).
KIND_TIER: dict[str, str] = {
    "dissolution_date": ESCALATE,  # the register says the company has ceased
    "legal_name": RECHECK,  # a rename, or a different entity under the same number
    "jurisdiction": ESCALATE,  # redomiciliation — rare and real
    "legal_form": RECHECK,  # conversion (Ltd → plc) often accompanies a restructuring
    "founding_date": NOTE,  # a source corrected the incorporation date
    "identifier": RECHECK,  # a re-registration, or a wrong join
    "signal_retired": NOTE,  # good news is still news; never outranks note
    "signal_unchecked": NOTE,  # the producer did not answer — absence of evidence
    "context_new": NOTE,  # kind="context" never counts as a finding
    "context_retired": NOTE,
    "context_unchecked": NOTE,
    "coverage_changed": NOTE,  # about OpenCheck, not the company
    "coverage_unchecked": NOTE,
    "verdict": NOTE,  # the sentence moved with no graded kind behind it
}

#: ``register_status``: a move into a terminal class (dissolved, struck off,
#: liquidated) is escalate; any other move is recheck.
REGISTER_STATUS_TERMINAL_TIER = ESCALATE
REGISTER_STATUS_OTHER_TIER = RECHECK


# ---------------------------------------------------------------------------
# Table 2 — the material GLEIF fields (``gleif_field``, Tier 1)
# ---------------------------------------------------------------------------

#: One row per key of ``routers/watch.py::FIELD_WORDS``; a test fails if the
#: two key sets differ. ``entity_status`` and ``corporate_events`` are listed
#: for completeness and graded by the rules beneath.
GLEIF_FIELD_TIER: dict[str, str] = {
    "direct_parent_lei": ESCALATE,  # ownership/control change — the first-listed pKYC trigger
    "ultimate_parent_lei": ESCALATE,
    "successors": ESCALATE,  # the entity was absorbed or merged away
    "expiration_date": ESCALATE,  # entity ceased
    "expiration_reason": ESCALATE,
    "jurisdiction": ESCALATE,
    "entity_status": RECHECK,  # escalate when it moves to INACTIVE — see entity_status_tier()
    "corporate_events": RECHECK,  # by event type and status — see event_tier()
    "legal_name": RECHECK,
    "legal_form": RECHECK,
    "legal_address_country": RECHECK,  # a move across a border (Phase 300 added it for this)
    "hq_address_country": RECHECK,
    "registered_at": RECHECK,  # re-registration, or the issuer corrected a wrong number
    "registered_as": RECHECK,
    "direct_exception": RECHECK,  # a parent appeared or was withdrawn behind a reporting exception
    "ultimate_exception": RECHECK,
    "direct_children": NOTE,  # below the subject: its own corporate activity, not its exposure
    "registration_status": NOTE,  # the LEI record's status, not the company's (Phase 242)
    "validated_at": NOTE,
    "creation_date": NOTE,
    "category": NOTE,
    "sub_category": NOTE,
    "conformity_flag": NOTE,
}

#: GLEIF's ``entity.status`` value that means the entity has ceased.
_INACTIVE = "INACTIVE"


def entity_status_tier(new: Any) -> str:
    """``entity_status`` moving to INACTIVE is escalate; any other move recheck."""
    return ESCALATE if str(new or "").upper() == _INACTIVE else GLEIF_FIELD_TIER["entity_status"]


#: Legal Entity Event types (LEI-CDF 3.1). A terminal type — the Phase 305
#: set, which dates a dissolution — is escalate; every other type the CDF
#: defines is recheck; an unknown future type is recheck, never silent.
#: ``EXCLUDED_TYPES`` (address and other-name changes) never reach the feed.
EVENT_TERMINAL_TIER = ESCALATE
EVENT_OTHER_TIER = RECHECK
#: The non-terminal LEI-CDF 3.1 types, named so the doc can list them.
EVENT_RECHECK_TYPES: frozenset[str] = frozenset(
    {
        "CHANGE_LEGAL_NAME",
        "CHANGE_LEGAL_FORM",
        "VOLUNTARY_ARRANGEMENT",
        "TRANSFORMATION_BRANCH_TO_SUBSIDIARY",
        "TRANSFORMATION_SUBSIDIARY_TO_BRANCH",
        "TRANSFORMATION_UMBRELLA_TO_STANDALONE",
        "SPINOFF",
        "DEMERGER",
        "REVERSE_TAKEOVER",
        "ACQUISITION_BRANCH",
        "TRANSFER_BRANCH",
        "CLOSURE_BRANCH",
    }
)

#: Event status -> how many steps below the type's tier the event sits.
#: COMPLETED carries the type's tier; IN_PROGRESS one step lower (a terminal
#: event in progress is recheck); WITHDRAWN_CANCELLED is note — the event did
#: not happen, and the record of its withdrawal is history.
_STATUS_STEPS: dict[str, int] = {"COMPLETED": 0, "IN_PROGRESS": 1}


def _lower(tier: str, steps: int) -> str:
    return TIERS[max(0, _RANK[tier] - steps)]


def event_tier(event: dict[str, Any]) -> str:
    """The tier of one Legal Entity Event, by type and status."""
    etype = str(event.get("type") or "").upper()
    status = str(event.get("status") or "").upper()
    if status == "WITHDRAWN_CANCELLED":
        return NOTE
    base = EVENT_TERMINAL_TIER if etype in _gleif_events.TERMINAL_TYPES else EVENT_OTHER_TIER
    # An unknown status is read as in progress — never as completed.
    return _lower(base, _STATUS_STEPS.get(status, 1))


def _events_of(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [e for e in value if isinstance(e, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def gleif_field_tier(change: dict[str, Any]) -> str:
    """The tier of one ``gleif_field`` change."""
    field = str(change.get("field") or "")
    if field == "corporate_events":
        events = _events_of(change.get("new"))
        return max((event_tier(e) for e in events), key=_RANK.__getitem__, default=NOTE)
    if field == "entity_status":
        return entity_status_tier(change.get("new"))
    # An unknown field is a material field the watchlist started reading
    # after this table was written: recheck, never silent.
    return GLEIF_FIELD_TIER.get(field, RECHECK)


# ---------------------------------------------------------------------------
# Table 3 — risk-signal codes (``signal_new``)
# ---------------------------------------------------------------------------

#: Drawn at two lines: **escalate** = a listing or control finding against
#: the subject or a party in its chain, or a black-list jurisdiction;
#: **recheck** = a linkage, PEP, debarment or EU high-risk jurisdiction;
#: **note** = structure, state ownership, grey and tax lists, leaks,
#: counter-sanctions, and every ``kind="context"`` code. A code the backend
#: later adds and this table does not know is recheck, never silent.
SIGNAL_TIER: dict[str, str] = {
    # escalate
    "SANCTIONED": ESCALATE,
    "RELATED_SANCTIONED": ESCALATE,
    "SANCTIONS_CONTROLLED": ESCALATE,
    "RELATED_SANCTIONS_CONTROLLED": ESCALATE,
    "SANCTIONS_OWNERSHIP_TEST": ESCALATE,
    "SANCTIONED_SECURITY": ESCALATE,
    "EXPORT_CONTROLLED": ESCALATE,
    "RELATED_EXPORT_CONTROLLED": ESCALATE,
    "FATF_BLACK_LIST": ESCALATE,
    # recheck
    "SANCTIONS_LINKED": RECHECK,
    "RELATED_SANCTIONS_LINKED": RECHECK,
    "EXPORT_CONTROL_LINKED": RECHECK,
    "RELATED_EXPORT_CONTROL_LINKED": RECHECK,
    "DEBARMENT": RECHECK,
    "RELATED_DEBARMENT": RECHECK,
    "PEP": RECHECK,
    "RELATED_PEP": RECHECK,
    "EU_HIGH_RISK_THIRD_COUNTRY": RECHECK,
    # note
    "COUNTER_SANCTIONED": NOTE,
    "RELATED_COUNTER_SANCTIONED": NOTE,
    "EXPORT_RISK": NOTE,
    "RELATED_EXPORT_RISK": NOTE,
    "FATF_GREY_LIST": NOTE,
    "EU_TAX_NON_COOPERATIVE": NOTE,
    "TRUST_OR_ARRANGEMENT": NOTE,
    "COMPLEX_OWNERSHIP_LAYERS": NOTE,
    "POSSIBLE_OBFUSCATION": NOTE,
    "NOMINEE": NOTE,
    "OPAQUE_OWNERSHIP": NOTE,
    "STATE_CONTROLLED": NOTE,  # notable, not a risk
    "OFFSHORE_LEAKS": NOTE,  # the leaks data is often years out of date
    # context codes (kind="context"): note, by the Table 1 rule
    "NON_EU_JURISDICTION": NOTE,
    "SUBSIDIARY_LISTED_JURISDICTION": NOTE,
    "GLEIF_REPORTING_EXCEPTION": NOTE,
    "RELATED_PEP_SUBJECT_ROLE": NOTE,
    "RECORD_CONSISTENCY": NOTE,
    "SANCTIONS_OWNERSHIP_TEST_POSSIBLE": NOTE,
    # retired (Phase 272) — never diffed, kept so the table names every code
    "COMPLEX_CORPORATE_STRUCTURE": NOTE,
}


def signal_tier(code: Any) -> str:
    return SIGNAL_TIER.get(str(code or ""), RECHECK)


# ---------------------------------------------------------------------------
# The register tier's own trigger (Tier 3, Phase 364)
# ---------------------------------------------------------------------------

#: A register-tier entry is written even when the re-run's snapshot shows
#: nothing new, because the register's change is itself the news. Its
#: trigger ``changes`` are graded here (defaults, 17 Oct 2026 — see the
#: ticket's Q9): the company leaving the register's open data is escalate
#: (deleted companies are not published); a shareholder or beneficial-owner
#: list moving, or the name, status or legal form changing, is recheck; the
#: registry-card persons or supervisory board moving, or the company
#: appearing in the open data, is note. A terminal status reached through
#: the re-run is escalated by the snapshot's ``register_status`` rule.
REGISTER_TRIGGER_TIER: dict[str, str] = {
    "removed": ESCALATE,
    "shareholders": RECHECK,
    "beneficial_owners": RECHECK,
    "name": RECHECK,
    "status": RECHECK,
    "legal_form": RECHECK,
    "officers": NOTE,
    "supervisory_board": NOTE,
    "added": NOTE,
}


def register_trigger_tier(trigger: dict[str, Any] | None) -> str:
    """The tier the register tier's own trigger carries."""
    changes = (trigger or {}).get("changes") or {}
    if not isinstance(changes, dict):
        return NOTE
    tiers = [REGISTER_TRIGGER_TIER.get(k, RECHECK) for k, v in changes.items() if v]
    return max(tiers, key=_RANK.__getitem__, default=NOTE)


#: The PSC stream tier's own trigger (pKYC stage E, Route 1; Stephen's Q4,
#: 18 Oct 2026): a Companies House PSC filing on the company is **recheck**
#: whatever it counted — a person or company with significant control was
#: added, changed, ceased or removed, and the counts-only trigger does not
#: say which band moved. The escalate rule for a ≥25 % band added or ended
#: belongs to Route 2, where the share band is known.
PSC_TRIGGER_TIER = RECHECK


def psc_trigger_tier(trigger: dict[str, Any] | None) -> str:
    """The tier a ``psc`` entry's trigger carries: recheck when it counted
    anything, note for an empty one (never written in practice)."""
    kinds = (trigger or {}).get("kinds") or {}
    counted = any(isinstance(v, dict) and any(v.values()) for v in kinds.values()) if isinstance(kinds, dict) else False
    return PSC_TRIGGER_TIER if counted else NOTE


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------


def tier_for_change(change: dict[str, Any]) -> str:
    """The tier of one change the diff named."""
    kind = str(change.get("kind") or "")
    if kind == "gleif_field":
        return gleif_field_tier(change)
    if kind == "register_status":
        new = change.get("new") or {}
        liveness = new.get("liveness") if isinstance(new, dict) else None
        return REGISTER_STATUS_TERMINAL_TIER if liveness == "terminal" else REGISTER_STATUS_OTHER_TIER
    if kind == "signal_new":
        return signal_tier(change.get("code"))
    # An unknown kind is a kind the diff started naming after this table
    # was written: recheck, never silent.
    return KIND_TIER.get(kind, RECHECK)


def tier_for_entry(changes: list[dict[str, Any]], *, tier: str | None = None, trigger: dict[str, Any] | None = None) -> str:
    """The entry's tier: the worst of its changes' tiers and, for a register
    or PSC tier entry, of its trigger's. An entry with no changes ("the re-run found
    no difference") is note."""
    tiers = [tier_for_change(c) for c in changes if isinstance(c, dict)]
    if tier == "register":
        tiers.append(register_trigger_tier(trigger))
    if tier == "psc":
        tiers.append(psc_trigger_tier(trigger))
    if tier == "review":
        # pKYC stage C: a note about the list (the expiry notice), never about
        # a company — note whatever else it carries.
        return NOTE
    return max(tiers, key=_RANK.__getitem__, default=NOTE)


def grade(changes: list[dict[str, Any]], *, tier: str | None = None, trigger: dict[str, Any] | None = None) -> tuple[str, list[dict[str, Any]]]:
    """``(entry tier, changes each carrying its own ``materiality``)``. The
    change dicts are copied; the caller's list is not mutated."""
    graded = [{**c, "materiality": tier_for_change(c)} for c in changes if isinstance(c, dict)]
    return tier_for_entry(changes, tier=tier, trigger=trigger), graded


def is_material(tier: str) -> bool:
    """The page's "Material only" filter: recheck and escalate."""
    return _RANK.get(tier, 0) >= _RANK[RECHECK]
