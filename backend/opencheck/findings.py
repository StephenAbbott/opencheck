"""Plain-English findings for source hits.

A ``finding`` is one sentence saying what a source actually said about the
subject — "2 people with significant control on file, including Jane SMITH
with 50% to 75% of shares." It is deliberately distinct from
``SourceHit.summary``, which is an *identifier fragment* ("GB-COH 00102498",
"OC gb/00102498 · Active") consumed by search-result rows, the share card and
``og_image.py``. Never repurpose ``summary`` — a dozen call sites depend on
its current shape.

Every template in this module is built from :func:`clauses_to_sentence`, so a
field the source did not publish simply shortens the sentence instead of
emitting ``None`` or a dangling "at %".

The ten rules
-------------

1. **One sentence.** Target 90 characters, hard cap 140
   (:data:`MAX_FINDING_CHARS`). :func:`clauses_to_sentence` enforces the cap by
   dropping trailing clauses, so put the most important clause first.
2. **Never name the source.** The row already says GLEIF, and the card already
   says OpenSanctions. Naming a *collection*, *dataset* or *register office*
   the source itself names is fine; naming the adapter is not.
3. **Lead with what changes a decision** — who controls it, what it is listed
   on, whether it is still trading — not with identifiers.
4. **Every value comes from a field the adapter already parsed.** No
   inference, no arithmetic the source did not do. Counting the rows a source
   returned is reporting; ranking them, converting codes to country names, or
   summing shares is not.
5. **Numbers exactly as filed.** 40.4% stays 40.4%. A percentage below 50 must
   NEVER be described with a word implying control ("majority", "controls") —
   see :func:`holding_clause`, which is the only place a stake is worded.
6. **State absence in the same voice as presence.** "No person with
   significant control named; the filing states that no registrable person
   exists" — not silence, and not a warning.
7. **Assert nothing about risk or corroboration.** That is the signals layer's
   job. Say "appears on 3 published listings", never "sanctioned" as a
   judgement; say "mentions, not identity matches", never "confirmed".
8. **Degrade, never emit a slot marker.** A missing field drops its clause.
   No "None", no empty brackets, no trailing "at %".
9. **House style.** "beneficial ownership" is two words, never hyphenated.
   Dates read as "3 June 2026" (:func:`human_date`). Plurals are handled
   ("1 subsidiary" / "2 subsidiaries") — see :func:`plural`.
10. **One fixture test per template** asserting the exact string, plus a
    null-fixture test asserting the degraded form. See
    ``tests/test_findings.py``.
"""

from __future__ import annotations

import re
from typing import Any

#: Hard cap from rule 1. ``clauses_to_sentence`` drops trailing clauses until
#: the sentence fits; a lead clause that is on its own longer than this is
#: returned anyway, because truncating mid-word would break rule 8.
MAX_FINDING_CHARS = 140

#: Advisory target from rule 1 — templates aim here, tests pin the hard cap.
TARGET_FINDING_CHARS = 90

_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


# ---------------------------------------------------------------------------
# Shared sentence machinery
# ---------------------------------------------------------------------------


def clauses_to_sentence(parts: list[str | None], *, sep: str = ", ") -> str | None:
    """Join non-empty *parts* into one capitalised, full-stopped sentence.

    Falsy clauses (``None``, ``""``, whitespace) are dropped, which is how a
    template degrades: a source that published no incorporation date simply
    gets a shorter sentence rather than "incorporated on None".

    Returns ``None`` when nothing survives, so the caller can leave
    ``SourceHit.finding`` unset and the row renders its ``summary`` as before.

    The hard cap of rule 1 is enforced here rather than in each template:
    trailing clauses are dropped until the sentence fits :data:`MAX_FINDING_CHARS`.
    That is why every template orders its clauses most-important-first.
    """
    kept = [str(p).strip() for p in parts if p and str(p).strip()]
    while kept:
        text = sep.join(kept)
        sentence = text[0].upper() + text[1:]
        if not sentence.endswith((".", "!", "?")):
            sentence += "."
        if len(sentence) <= MAX_FINDING_CHARS or len(kept) == 1:
            return sentence
        kept.pop()
    return None


def human_date(value: str | None) -> str | None:
    """Render an ISO date as house style: ``2026-06-03`` → ``3 June 2026``.

    Accepts a bare date or a full timestamp (Wikidata's SPARQL returns
    ``1909-04-14T00:00:00Z``). Anything unparseable returns ``None`` so the
    clause drops out rather than surfacing a raw timestamp.
    """
    if not value:
        return None
    match = _ISO_DATE.match(str(value).strip())
    if not match:
        return None
    year, month, day = (int(g) for g in match.groups())
    if not 1 <= month <= 12:
        return None
    return f"{day} {_MONTHS[month - 1]} {year}"


def plural(count: int, singular: str, plural_form: str | None = None) -> str:
    """``plural(1, "subsidiary", "subsidiaries")`` → ``"1 subsidiary"`` (rule 9)."""
    if count == 1:
        return f"{count} {singular}"
    return f"{count} {plural_form or singular + 's'}"


def percent(value: float | int | str | None) -> str | None:
    """Format a share exactly as filed — ``40.4`` → ``"40.4%"``, ``50.0`` → ``"50%"``.

    Rule 5: no rounding to a "nicer" number, no promotion of 49.9 to "about
    half". ``None`` for anything non-numeric so the clause drops.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return f"{number:g}%"


def holding_clause(name: str | None, share_percent: float | int | str | None) -> str | None:
    """Word one party's stake — the ONLY place a percentage is described.

    Rule 5 lives here: a stake below 50 is never given a word implying control.
    "Rosneftegaz JSC holds 40.4%" is a minority holding stated as filed;
    only at 50 or above does the sentence say "a majority stake". A holder
    with no published percentage is reported as an owner, not as a controller.
    """
    if not name:
        return None
    holder = str(name).strip()
    if not holder:
        return None
    share = percent(share_percent)
    if share is None:
        return f"{holder} is recorded as an owner"
    try:
        value = float(share_percent)  # type: ignore[arg-type]
    except (TypeError, ValueError):  # pragma: no cover — percent() already gated
        return f"{holder} is recorded as an owner"
    if value >= 50:
        return f"{holder} holds a majority stake of {share}"
    return f"{holder} holds {share}"


def _article(word: str) -> str:
    """"a" / "an" for a following *word*. Vowel-letter rule; good enough for
    the entity-type vocabularies these templates draw on."""
    return "an" if word[:1].lower() in "aeiou" else "a"


def _join_names(names: list[str], limit: int = 2) -> str:
    """``["A", "B"]`` → ``"A and B"``; more than *limit* names are counted, not
    listed, so one long list cannot blow the character cap."""
    kept = [n for n in names if n]
    if not kept:
        return ""
    if len(kept) == 1:
        return kept[0]
    if len(kept) <= limit:
        return " and ".join(kept)
    return f"{kept[0]} and {len(kept) - 1} others"


# ---------------------------------------------------------------------------
# bods_gleif — Open Ownership's BODS v0.4 extract of the GLEIF bulk data
# ---------------------------------------------------------------------------


def finding_bods_gleif(payload: dict[str, Any], statement_id: str) -> str | None:
    """What the GLEIF relationship file says about who this entity reports to.

    Deviation from the suggested shape: this extract carries **no ownership
    percentages and no parent names** — GLEIF Level 2 publishes accounting
    consolidation links, and Open Ownership's Parquet relationship table holds
    only statement ids plus ``directOrIndirect``. So the sentence counts and
    qualifies the parent links rather than naming a holder and a stake.
    """
    statements = payload.get("bods_statements") or []
    entity: dict[str, Any] = {}
    parents: list[dict[str, Any]] = []
    children = 0
    for stmt in statements:
        kind = stmt.get("statementType")
        details = stmt.get("recordDetails") or {}
        if kind == "entityStatement" and not entity:
            entity = details
        elif kind == "relationshipStatement":
            subject = (details.get("subject") or {}).get("describedByEntityStatement")
            party = (details.get("interestedParty") or {}).get("describedByEntityStatement")
            if subject == statement_id:
                parents.append(details)
            elif party == statement_id:
                children += 1

    directness = {
        str(interest.get("directOrIndirect") or "").lower()
        for rel in parents
        for interest in (rel.get("interests") or [])
    } - {""}

    if not parents:
        # GLEIF's own term for this is a reporting exception, but the
        # exception reason is not in the extract — so state the absence
        # plainly (rule 6) rather than naming a reason we do not hold.
        parent_clause: str | None = "no parent relationship is reported"
    elif directness == {"direct"}:
        parent_clause = f"reports {plural(len(parents), 'direct parent')}"
    elif directness == {"indirect"}:
        parent_clause = f"reports {plural(len(parents), 'indirect parent')}"
    elif directness:
        parent_clause = f"reports {plural(len(parents), 'parent')}, direct and indirect"
    else:
        parent_clause = f"reports {plural(len(parents), 'parent')}"

    child_clause = (
        f"{plural(children, 'subsidiary', 'subsidiaries')} report to it" if children else None
    )

    dissolved = human_date(entity.get("dissolutionDate"))
    if dissolved:
        status_clause: str | None = f"dissolved {dissolved}"
    else:
        jurisdiction = (entity.get("incorporatedInJurisdiction") or {}).get("name")
        status_clause = f"incorporated in {jurisdiction}" if jurisdiction else None

    return clauses_to_sentence([parent_clause, child_clause, status_clause], sep="; ")


# ---------------------------------------------------------------------------
# opensanctions
# ---------------------------------------------------------------------------


def finding_opensanctions(item: dict[str, Any]) -> str | None:
    """How many published lists carry this record, and under which topics.

    Rule 7 is why this counts listings and quotes the source's own topic
    vocabulary instead of calling anyone sanctioned: the topic is what
    OpenSanctions recorded, the judgement is the signals layer's.

    Deviation from the suggested shape: dataset names are opaque slugs
    (``eu_fsf``, ``us_ofac_sdn``), and turning them into "the EU, UK and US
    regimes" would be inference the adapter never did (rule 4). The count is
    exact; the regimes are not claimed.
    """
    datasets = [d for d in (item.get("datasets") or []) if d]
    props = item.get("properties") or {}
    topics = [str(t) for t in (item.get("topics") or props.get("topics") or []) if t]
    schema = str(item.get("schema") or "").strip()

    if datasets:
        lead: str | None = f"appears on {plural(len(datasets), 'published listing')}"
    elif schema:
        lead = f"held as {_article(schema)} {schema} record"
    else:
        lead = None

    # The topic strings are OpenSanctions' own vocabulary, quoted rather than
    # translated: rendering ``sanction`` as "sanctioned" would turn a source's
    # classification into OpenCheck's judgement (rule 7).
    topic_clause = (
        f"recorded under the {'topic' if len(topics) == 1 else 'topics'} "
        f"{_join_names(topics)}"
        if topics
        else None
    )

    return clauses_to_sentence([lead, topic_clause])


# ---------------------------------------------------------------------------
# companies_house (covers PSC — persons with significant control)
# ---------------------------------------------------------------------------

# Companies House PSC statement codes → plain English. The register's own
# spelling of the first code carries an upstream typo ("signficant"); both
# spellings are mapped so a silent upstream fix cannot blank the clause.
_PSC_STATEMENT_PHRASES: dict[str, str] = {
    "no-individual-or-entity-with-signficant-control":
        "the filing states that no registrable person exists",
    "no-individual-or-entity-with-significant-control":
        "the filing states that no registrable person exists",
    "psc-exists-but-not-identified":
        "the filing states that a controller exists but has not been identified",
    "psc-details-not-confirmed":
        "the filing records a controller whose details are unconfirmed",
    "steps-to-find-psc-not-yet-completed":
        "the filing records that the search for a controller is not complete",
    "psc-contacted-but-no-response":
        "the filing records a controller contacted without a response",
    "restrictions-notice-issued-to-psc":
        "a restrictions notice is recorded against the controller",
    "psc-has-failed-to-confirm-changed-details":
        "the filing records changed details the controller has not confirmed",
}

_PSC_BAND = re.compile(
    r"^(ownership-of-shares|voting-rights|right-to-share-surplus-assets)"
    r"-(\d+)-to-(\d+)-percent"
)
_PSC_BAND_NOUNS = {
    "ownership-of-shares": "of shares",
    "voting-rights": "of voting rights",
    "right-to-share-surplus-assets": "of surplus assets",
}
_PSC_NATURE_PHRASES = {
    "right-to-appoint-and-remove-directors": "the right to appoint and remove directors",
    "right-to-appoint-and-remove-members": "the right to appoint and remove members",
    "significant-influence-or-control": "significant influence or control",
}


def psc_nature_phrase(codes: list[str] | None) -> str | None:
    """Render the first Companies House nature-of-control code as prose.

    Rule 5 in band form: a 25-to-50 band reads "25% to 50% of shares" and
    never acquires a control word on the way out. Bands are stated exactly as
    the register banded them — they are not converted to a midpoint.
    """
    for raw in codes or []:
        code = str(raw or "").strip().lower()
        if not code:
            continue
        match = _PSC_BAND.match(code)
        if match:
            noun = _PSC_BAND_NOUNS[match.group(1)]
            return f"{match.group(2)}% to {match.group(3)}% {noun}"
        for prefix, phrase in _PSC_NATURE_PHRASES.items():
            if code.startswith(prefix):
                return phrase
    return None


def finding_companies_house(bundle: dict[str, Any]) -> str | None:
    """Who is on the beneficial ownership filing, or what the filing says instead."""
    pscs = [p for p in ((bundle.get("pscs") or {}).get("items") or []) if p]
    active = [p for p in pscs if not p.get("ceased_on")]
    statements = (bundle.get("psc_statements") or {}).get("items") or []
    officers = (bundle.get("officers") or {}).get("items") or []

    if active:
        lead: str | None = plural(
            len(active),
            "person with significant control",
            "people with significant control",
        ) + " on file"
        first = active[0]
        name = str(first.get("name") or "").strip()
        band = psc_nature_phrase(first.get("natures_of_control"))
        if name and band:
            detail: str | None = f"including {name} with {band}"
        elif name:
            detail = f"including {name}"
        else:
            detail = None
        # "…on file, including X" — an apposition, not a second statement.
        sep = ", "
    else:
        lead = "no person with significant control named"
        detail = next(
            (
                _PSC_STATEMENT_PHRASES[str(s.get("statement") or "")]
                for s in statements
                if str(s.get("statement") or "") in _PSC_STATEMENT_PHRASES
            ),
            None,
        )
        # "…named; the filing states…" — the absence and what stands in its
        # place are two statements, so they take a semicolon (rule 6).
        sep = "; "

    officer_clause = f"{plural(len(officers), 'officer')} listed" if officers else None

    return clauses_to_sentence([lead, detail, officer_clause], sep=sep)


# ---------------------------------------------------------------------------
# opencorporates
# ---------------------------------------------------------------------------


def finding_opencorporates(bundle: dict[str, Any]) -> str | None:
    """Whether the company is still trading, since when, and who is on the board.

    Deviation from the suggested shape: the payload carries only a
    ``jurisdiction_code`` (``gb``, ``us_de``), not a jurisdiction *name*.
    Expanding "gb" to "England and Wales" would be both inference and wrong
    (Scotland and Northern Ireland share the code), so the sentence leaves
    jurisdiction to ``summary`` and leads with trading status instead.
    """
    company = bundle.get("company") or {}
    officers = bundle.get("officers") or []

    dissolved = human_date(company.get("dissolution_date"))
    incorporated = human_date(company.get("incorporation_date"))
    status = str(company.get("current_status") or company.get("company_status") or "").strip()

    if dissolved:
        lead: str | None = f"dissolved {dissolved}"
    elif status and incorporated:
        lead = f"{status.lower()} since {incorporated}"
    elif status:
        lead = status.lower()
    elif incorporated:
        lead = f"incorporated {incorporated}"
    else:
        lead = None

    company_type = str(company.get("company_type") or "").strip()
    type_clause = f"registered as {_article(company_type)} {company_type}" if company_type else None

    officer_clause = f"{plural(len(officers), 'officer')} on file" if officers else None

    return clauses_to_sentence([lead, officer_clause, type_clause])


# ---------------------------------------------------------------------------
# openaleph
# ---------------------------------------------------------------------------


def finding_openaleph(
    item: dict[str, Any], mentions: dict[str, Any] | None = None
) -> str | None:
    """What the archive holds, and how loudly the name echoes through it.

    ``mentions`` is the ``/entities/{id}/mentions`` payload the lookup
    pipeline attaches after the hit is built (``raw.openaleph_mentions``); it
    is name-derived, which is why the sentence says so out loud rather than
    letting a reader take a document count for an identity match (rule 7).
    """
    mention_clause: str | None = None
    total = int((mentions or {}).get("total") or 0)
    if total:
        collections = [c for c in ((mentions or {}).get("collections") or []) if c]
        across = f" across {plural(len(collections), 'collection')}" if collections else ""
        mention_clause = (
            f"{plural(total, 'document')} mention this name{across} "
            "— mentions, not identity matches"
        )

    # The collection label and the FtM schema are both already in ``summary``,
    # so the finding names the archive once and spends its characters on the
    # mention count instead.
    collection = item.get("collection") or {}
    label = str(collection.get("label") or collection.get("foreign_id") or "").strip()
    held_clause = f"indexed in {label}" if label else None

    return clauses_to_sentence([mention_clause, held_clause], sep="; ")


# ---------------------------------------------------------------------------
# ted_eu (Tenders Electronic Daily — the EU procurement notice feed)
# ---------------------------------------------------------------------------


def finding_ted_eu(bundle: dict[str, Any]) -> str | None:
    """How often this party turns up in EU procurement, and how recently.

    Deviation: the suggested "since 2019" clause is not derivable. The
    ``notices`` list is one page of ``total_notice_count``, so the earliest
    date in hand is not the earliest date filed — asserting it would be
    arithmetic the source did not do (rule 4). Only the most recent notice
    on the page is dated, and it is labelled as such.
    """
    total = int(bundle.get("total_notice_count") or 0)
    wins = int(bundle.get("confirmed_wins") or 0)
    notices = bundle.get("notices") or []

    if total:
        lead: str | None = f"named in {plural(total, 'EU procurement notice')}"
    else:
        lead = "no EU procurement notice matched this party"

    win_clause = f"{wins} confirmed as contracts won" if wins else None

    latest = human_date(
        next((n.get("publication_date") for n in notices if n.get("publication_date")), None)
    )
    date_clause = f"most recently {latest}" if latest else None

    return clauses_to_sentence([lead, win_clause, date_clause])


# ---------------------------------------------------------------------------
# wikidata
# ---------------------------------------------------------------------------


def finding_wikidata(summary: dict[str, Any]) -> str | None:
    """What Wikidata says this subject is, and who is above it.

    Deviation: only three identifier schemes are parsed out of the SPARQL
    result (LEI, OpenCorporates, ISIN), so "7 identifiers published" cannot
    occur — the count is of the cross-identifiers the adapter actually
    captured, and it is last because identifiers do not change a decision
    (rule 3).

    Wikidata's own note on ``share_percent`` is that it is indicative — it
    conflates capital, voting and time — which is why the stake is worded by
    :func:`holding_clause` and never by a control word below 50%.
    """
    if not summary:
        return None

    if summary.get("is_person"):
        positions = summary.get("positions") or []
        position = str((positions[0] if positions else {}).get("label") or "").strip()
        started = human_date((positions[0] if positions else {}).get("start"))
        if position and started:
            lead: str | None = f"recorded as {position} since {started}"
        elif position:
            lead = f"recorded as {position}"
        else:
            lead = None
        citizenships = [str(c.get("label") or "") for c in (summary.get("citizenships") or [])]
        joined = _join_names(citizenships)
        return clauses_to_sentence([lead, f"citizen of {joined}" if joined else None], sep="; ")

    owners = summary.get("controlling_owners") or []
    parents = [str(p.get("label") or "") for p in (summary.get("parent_orgs") or [])]
    if owners:
        lead = holding_clause(owners[0].get("name"), owners[0].get("share_percent"))
    elif parents:
        lead = f"part of {_join_names(parents)}"
    else:
        lead = None

    instances = summary.get("instance_of") or []
    instance = str((instances[0] if instances else {}).get("label") or "").strip()
    type_clause = f"described as {_article(instance)} {instance}" if instance else None

    identifiers = summary.get("identifiers") or {}
    id_clause = (
        f"{plural(len(identifiers), 'cross-identifier')} published" if identifiers else None
    )

    return clauses_to_sentence([lead, type_clause, id_clause], sep="; ")
