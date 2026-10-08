"""The successor GLEIF names on an LEI record, followed forward (Phase 307).

GLEIF's Level 1 record carries ``entity.successorEntities`` — the entity or
entities an LEI holder's issuer records as having taken its place. Until this
phase OpenCheck stored the field (the Golden Copy mirror, ``mirror_build``;
the watchlist's ``successors`` material field, Phase 300) and read it on no
lookup surface, so a retired LEI's page said "retired" and stopped — the
reader had to find the surviving company by hand.

What the field is, measured on the 7 Oct 2026 Golden Copy (3,454,955
records; the ticket *Retired LEIs, successors and former names - October
2026* holds the full table):

* 52,404 records name a successor — 45,739 of the 254,762 INACTIVE records
  (18 %), and 6,502 DUPLICATE registrations, every one of them.
* It is a property of the merger family: 99.6 % of INACTIVE records whose
  latest completed terminal event is ``MERGERS_AND_ACQUISITIONS`` name one,
  100 % of ``ABSORPTION`` and ``BREAKUP`` — and **0.3 %** of ``DISSOLUTION``,
  0.1 % of ``LIQUIDATION``, none of ``BANKRUPTCY`` / ``INSOLVENCY``. A
  dissolved company with no successor (Barrick Gold Inc.,
  ``5493002CWGHR03YL8X75``) is the ordinary case, and this module says so
  by saying nothing: no successor named, no payload.
* A third are name-only: 17,407 records name a successor with no LEI.
* Successors chain: 2,734 INACTIVE records name a successor that is itself
  INACTIVE/RETIRED; 1,361 need two hops, 78 three to five, none more.

Rules, each one a decision (Stephen, 8 Oct 2026):

* **GLEIF's assertion, stated as GLEIF's.** The sentence says GLEIF *names*
  a successor. It never says "owns", "absorbed" or "became" on its own
  account: the event GLEIF filed (a merger, an absorption, a demerger)
  supplies the verb, and a successor named with no event is named and no
  more. Nothing here reaches ``risk.py`` or the verdict.
* **A DUPLICATE registration is not a successor.** GLEIF uses the same field
  for "another LEI was issued for this same entity" (6,502 records, 3,138
  with the identical legal name). The payload's ``relation`` is
  ``duplicate`` and the sentence says the record is a duplicate of the
  other — never that the entity merged into itself.
* **The chain is walked to its end, through the mirror only.** One LEI
  named → the Golden Copy mirror (``entity_pages.get_store()``, a local
  read, never the live API) is followed until a record names no further
  successor, at most :data:`MAX_HOPS` hops; the sentence names the final
  record and the hop count. With no mirror the first hop is still what
  GLEIF named, so it is shown, with ``chain_source: null`` and
  ``chain_complete: false``: the reader is told the trail was not followed,
  never that it ends there. Several LEIs named (a demerger, a breakup) are
  listed and not followed — there is no single forward.
* **A name-only successor is a name.** No match against the mirror is made
  for it here: the mirror has no name index, and a name match would be
  OpenCheck's assertion, not GLEIF's. Shown as a name, no link.
* **Frozen with the run.** The payload rides on the ``subject_profile``
  event beside ``lei_registration`` (Phase 242); a saved report replays what
  GLEIF named on the day of the run.
"""

from __future__ import annotations

import re
from typing import Any

from .bods import gleif_events as _events
from .lei_registration import LABELS as _REGISTRATION_LABELS

#: How far a successor chain is followed through the mirror. The longest
#: chain in the 7 Oct 2026 Golden Copy is five hops; the cap is a cycle and
#: cost guard, not a limit anything measured reaches.
MAX_HOPS = 6

#: GLEIF event types that explain a succession — the ones on which the field
#: is filled. Any other completed terminal event (a dissolution with a
#: successor named, 598 records) is still reported, as what it is.
SUCCESSION_TYPES: frozenset[str] = frozenset(
    {"MERGERS_AND_ACQUISITIONS", "ABSORPTION", "DEMERGER", "BREAKUP", "SPINOFF"}
)

#: Words the sentence must never contain — a succession is a record-keeping
#: fact; each of these would turn it into a judgement or a claim of
#: ownership GLEIF does not make.
BANNED_WORDS: tuple[str, ...] = (
    "owns", "owned", "acquired by", "controls", "risk", "suspicious",
    "shell", "hidden", "disappeared",
)

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


def _clean_lei(value: Any) -> str | None:
    lei = str(value or "").strip().upper()
    return lei if _LEI_SHAPE.match(lei) else None


def _clean_name(value: Any) -> str | None:
    name = " ".join(str(value or "").split())
    return name or None


def _named(entity_block: dict[str, Any]) -> list[dict[str, Any]]:
    """The successors as GLEIF files them, ``{"lei", "name"}`` each, in file
    order, with the singular ``successorEntity`` folded in when the list is
    empty (the live API fills both; an older cached record may hold one)."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()
    candidates = list(entity_block.get("successorEntities") or [])
    single = entity_block.get("successorEntity")
    if isinstance(single, dict) and (single.get("lei") or single.get("name")):
        candidates.append(single)
    for item in candidates:
        if not isinstance(item, dict):
            continue
        lei = _clean_lei(item.get("lei"))
        name = _clean_name(item.get("name"))
        # GLEIF files an LEI in the name field on one record; read it as one.
        if not lei and name and _LEI_SHAPE.match(name.removeprefix("LEI")):
            lei, name = name.removeprefix("LEI"), None
        if not lei and not name:
            continue
        key = (lei, None if lei else name)
        if key in seen:
            continue
        seen.add(key)
        out.append({"lei": lei, "name": name})
    return out


def _explaining_event(entity_block: dict[str, Any]) -> dict[str, Any] | None:
    """The completed event that explains the succession: the latest completed
    event of a :data:`SUCCESSION_TYPES` type, else the latest completed
    terminal event (``gleif_events.dissolution_event``), else ``None``."""
    events = _events.events_of(entity_block)
    best: tuple[str, dict[str, Any]] | None = None
    for event in events:
        if str(event.get("status") or "").upper() != "COMPLETED":
            continue
        if str(event.get("type") or "").upper() not in SUCCESSION_TYPES:
            continue
        day = _events.event_day(event.get("effectiveDate")) or ""
        if best is None or day > best[0]:
            best = (day, event)
    if best is None:
        found = _events.dissolution_event(events)
        if found is None:
            return None
        best = found
    day, event = best
    return {
        "type": str(event.get("type") or "").upper(),
        "status": str(event.get("status") or "").upper(),
        "effective_day": day or None,
    }


def from_gleif_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    """The ``lei_successor`` payload from a GLEIF ``lei-records`` resource,
    before the chain is followed — ``None`` when GLEIF names no successor.

    Shape::

        {"relation": "successor" | "duplicate",
         "named": [{"lei": "...", "name": "..."}],      # as GLEIF files them
         "event": {"type": "MERGERS_AND_ACQUISITIONS", "status": "COMPLETED",
                   "effective_day": "2020-04-17"} | None,
         "chain": [{"lei", "name", "entity_status", "registration_status"}],
         "chain_source": "mirror" | None,
         "chain_complete": False,
         "hops": 0,
         "source_id": "gleif",
         "sentence": "..."}
    """
    attrs = (record or {}).get("attributes") or {}
    entity = attrs.get("entity") or {}
    if not isinstance(entity, dict):
        return None
    named = _named(entity)
    if not named:
        return None
    registration = str(((attrs.get("registration") or {}).get("status")) or "").upper()
    out: dict[str, Any] = {
        "relation": "duplicate" if registration == "DUPLICATE" else "successor",
        "named": named,
        "event": _explaining_event(entity),
        "chain": [],
        "chain_source": None,
        "chain_complete": False,
        "hops": 0,
        "source_id": "gleif",
    }
    out["sentence"] = sentence(out)
    return out


def follow(payload: dict[str, Any] | None, store: Any = None) -> dict[str, Any] | None:
    """Walk the successor chain through the Golden Copy mirror.

    ``store`` is an ``entity_pages.EntityStore`` (``get_store()`` when not
    given); with none the payload is returned as it came, trail unfollowed.
    Followed only when exactly one LEI is named: a demerger's several
    successors are listed, not walked. Each hop is the mirror's row — name,
    entity status, registration status — and the walk stops at a record that
    names no further successor (``chain_complete``), at :data:`MAX_HOPS`, at a
    cycle, or at an LEI the mirror lacks (all ``chain_complete: false``).
    """
    if not payload:
        return payload
    out = {**payload, "chain": [], "chain_source": None, "chain_complete": False, "hops": 0}
    named = out.get("named") or []
    leis = [n["lei"] for n in named if n.get("lei")]
    if len(named) != 1 or len(leis) != 1:
        out["sentence"] = sentence(out)
        return out
    if store is None:
        from . import entity_pages

        store = entity_pages.get_store()
    if store is None:
        out["sentence"] = sentence(out)
        return out
    out["chain_source"] = "mirror"
    seen: set[str] = set()
    current = leis[0]
    chain: list[dict[str, Any]] = []
    while current and current not in seen and len(chain) < MAX_HOPS:
        seen.add(current)
        row = store.get(current)
        if row is None:
            break
        chain.append(
            {
                "lei": row.lei,
                "name": row.name,
                "entity_status": row.entity_status,
                "registration_status": row.registration_status,
            }
        )
        next_named = _named({"successorEntities": (row.detail or {}).get("successorEntities") or []})
        next_leis = [n["lei"] for n in next_named if n.get("lei")]
        if not next_leis:
            out["chain_complete"] = True
            break
        if len(next_leis) != 1:
            # The trail forks (a demerger downstream); it ends here, honestly.
            break
        current = next_leis[0]
    out["chain"] = chain
    out["hops"] = len(chain)
    out["sentence"] = sentence(out)
    return out


# ---------------------------------------------------------------------------
# Wording
# ---------------------------------------------------------------------------


def _label(entry: dict[str, Any]) -> str:
    """"NAME (LEI)", "NAME" or "LEI" — whatever GLEIF filed."""
    name, lei = entry.get("name"), entry.get("lei")
    if name and lei:
        return f"{name} ({lei})"
    return str(name or lei or "an unnamed entity")


def _join(labels: list[str]) -> str:
    if len(labels) <= 1:
        return labels[0] if labels else ""
    return ", ".join(labels[:-1]) + " and " + labels[-1]


def _event_clause(event: dict[str, Any] | None) -> str:
    if not event or not event.get("type"):
        return ""
    words = _events.type_words(event["type"])
    day = event.get("effective_day")
    if event.get("status") == "COMPLETED" and day:
        return f", on a {words} completed on {_events.human_day(day)}"
    if event.get("status") == "COMPLETED":
        return f", on a completed {words}"
    return f", on a {words} ({_events.status_words(event.get('status'))})"


def _registration_words(status: str | None) -> str:
    s = str(status or "").upper()
    return (_REGISTRATION_LABELS.get(s) or s.replace("_", " ").capitalize() or "unknown").lower()


def sentence(payload: dict[str, Any] | None) -> str | None:
    """The sentence the MCP profile, the report and the page carry.

    * duplicate: "GLEIF records this LEI as a duplicate: the same entity is
      registered as NAME (LEI)."
    * one LEI, chain followed to an end: "GLEIF names NAME (LEI) as this
      entity's successor, on a merger or acquisition completed on 17 April
      2020. Its LEI is issued." — or, after hops: "… That record is itself
      retired; GLEIF's successor records lead on to NAME2 (LEI2), whose LEI
      is issued (2 hops)."
    * one LEI, not followed: "… The trail was not followed further."
    * name only: "GLEIF names NAME as this entity's successor, with no LEI."
    * several: "GLEIF names X (LEI) and Y as this entity's successors, on a
      demerger completed on …."
    """
    if not payload or not payload.get("named"):
        return None
    named: list[dict[str, Any]] = payload["named"]
    labels = [_label(n) for n in named]
    if payload.get("relation") == "duplicate":
        return (
            f"GLEIF records this LEI as a duplicate: the same entity is registered as "
            f"{_join(labels)}. This is the status of the LEI record, not of the company."
        )
    plural = "successors" if len(named) > 1 else "successor"
    first = f"GLEIF names {_join(labels)} as this entity's {plural}{_event_clause(payload.get('event'))}."
    if len(named) > 1:
        return first
    only = named[0]
    if not only.get("lei"):
        return first[:-1] + ", with no LEI."
    chain: list[dict[str, Any]] = payload.get("chain") or []
    if not chain:
        return f"{first} The trail was not followed further."
    head, last = chain[0], chain[-1]
    if len(chain) == 1:
        tail = f"Its LEI is {_registration_words(head.get('registration_status'))}."
        if not payload.get("chain_complete"):
            tail += " That record names a successor of its own; the trail was not followed further."
        return f"{first} {tail}"
    hops = len(chain) - 1
    lead = (
        f"That record is itself {_registration_words(head.get('registration_status'))}; "
        f"GLEIF's successor records lead on to {_label(last)}, whose LEI is "
        f"{_registration_words(last.get('registration_status'))} "
        f"({hops} {'hop' if hops == 1 else 'hops'} on)."
    )
    if not payload.get("chain_complete"):
        lead += " That record names a successor of its own; the trail was not followed further."
    return f"{first} {lead}"


def final(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Where a follow-forward link should point: the last record of a
    followed chain, else the one LEI GLEIF named, else ``None`` (several
    named, or a name only). ``{"lei", "name", "registration_status"}``."""
    if not payload:
        return None
    chain = payload.get("chain") or []
    if chain:
        last = chain[-1]
        return {
            "lei": last.get("lei"),
            "name": last.get("name"),
            "registration_status": last.get("registration_status"),
        }
    named = [n for n in payload.get("named") or [] if n.get("lei")]
    if len(named) == 1 and len(payload.get("named") or []) == 1:
        return {"lei": named[0]["lei"], "name": named[0].get("name"), "registration_status": None}
    return None


def report_value(report: dict[str, Any] | None) -> str | None:
    """The identifiers-table value for the PDF/HTML and Markdown reports —
    the frozen profile's sentence, never re-fetched."""
    profile = (report or {}).get("subject_profile") or {}
    succ = profile.get("lei_successor") if isinstance(profile, dict) else None
    if not isinstance(succ, dict) or not succ.get("named"):
        return None
    return succ.get("sentence") or sentence(succ)
