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

from .topics import topic_list

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
# GLEIF — the live Level 1 + Level 2 bundle (this is the source every lookup
# shows; ``bods_gleif`` below is the curated Parquet extract)
# ---------------------------------------------------------------------------

# GLEIF Level 2 is an ACCOUNTING CONSOLIDATION relationship, not a
# shareholding. Its endpoints are the direct and ultimate *accounting
# consolidating parent* — IS_DIRECTLY_CONSOLIDATED_BY / IS_ULTIMATELY_
# CONSOLIDATED_BY — meaning "this entity's figures are consolidated into that
# entity's group accounts". GLEIF publishes no percentage anywhere in Level 2,
# and a consolidating parent is not necessarily a shareholder at all. So every
# GLEIF sentence says "consolidated by" and never "owned by", "holds", or a
# figure with a % on it. ``test_findings.py`` fails the build if one ever does.
#
# The reasons an entity may file no parent are equally specific: a *reporting
# exception* is a permitted filing defined by the LEI ROC policy, not a refusal
# to disclose, and the phrasing below says so.
_GLEIF_EXCEPTION_PHRASES: dict[str, str] = {
    "NATURAL_PERSONS": "control rests with natural persons",
    "NO_KNOWN_PERSON": "no controlling person is known",
    "NO_LEI": "the parent has no LEI",
    "NON_CONSOLIDATING": "the parent prepares no consolidated accounts",
    # NON_PUBLIC absorbed the five reasons below in Reporting Exceptions
    # Format 2.1 (2022-03-01); records still carrying the old codes read the
    # same way, because they mean the same thing.
    "NON_PUBLIC": "the relationship is not public",
    "BINDING_LEGAL_COMMITMENTS": "the relationship is not public",
    "LEGAL_OBSTACLES": "the relationship is not public",
    "DISCLOSURE_DETRIMENTAL": "the relationship is not public",
    "DETRIMENT_NOT_EXCLUDED": "the relationship is not public",
    "CONSENT_NOT_OBTAINED": "the relationship is not public",
}


def _gleif_parent_name(parent: dict[str, Any] | None) -> str | None:
    """``direct_parent.attributes.entity.legalName.name`` — the parent's own
    Level 1 record, which is what the parent endpoints return."""
    if not parent:
        return None
    attrs = parent.get("attributes") or parent
    entity = attrs.get("entity") or {}
    name = (entity.get("legalName") or {}).get("name")
    return str(name).strip() or None if name else None


def _gleif_parent_lei(parent: dict[str, Any] | None) -> str:
    """``attributes.lei``, falling back to the record ``id`` as the mapper does."""
    if not parent:
        return ""
    attrs = parent.get("attributes") or parent
    return str(attrs.get("lei") or parent.get("id") or "").strip().upper()


def _gleif_exception_reason(exception: dict[str, Any] | None) -> str:
    """The exception's reason code, under either spelling.

    **The live API uses ``reason``**; Open Ownership's SQLite dump uses
    ``exceptionReason`` (the same split `mapper.py` documents at its own
    reader). Verified live 2026-08-22 against BP, Rosneft and SEB, whose
    payloads read
    ``{"validFrom": null, "validTo": null, "lei": …, "category":
    "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "reason": "NO_KNOWN_PERSON",
    "reference": null}`` under ``attributes``. ``category`` is GLEIF's own
    field and says ``ACCOUNTING_CONSOLIDATION`` in as many words — which is
    the whole argument for the vocabulary above.
    """
    if not exception:
        return ""
    attrs = exception.get("attributes") or exception
    return str(attrs.get("exceptionReason") or attrs.get("reason") or "").strip().upper()


def _gleif_child_clause(children: int, *, direct: bool = True) -> str | None:
    """Subsidiaries as GLEIF holds them: entities that *report* this one as
    their consolidating parent — a reporting relationship, not an owned asset.

    ``direct`` is only true where the data says so. The live bundle's count
    comes from GLEIF's ``/direct-children`` endpoint, so it is direct by
    construction; the Parquet extract's children are whatever the relationship
    table holds, which includes ultimate links, so that path drops the word
    unless every child interest is filed as direct.
    """
    if not children:
        return None
    if direct:
        noun = plural(children, "direct subsidiary", "direct subsidiaries")
    else:
        noun = plural(children, "subsidiary", "subsidiaries")
    return f"{noun} {'reports' if children == 1 else 'report'} to it"


def _gleif_exception_clause(reason: str, *, ultimate: bool = False, both: bool = False) -> str:
    """Word a reporting exception as the permitted filing it is."""
    level = (
        "no consolidating parent is reported at either level"
        if both
        else "no ultimate consolidating parent is reported"
        if ultimate
        else "no consolidating parent is reported"
    )
    phrase = _GLEIF_EXCEPTION_PHRASES.get(reason)
    if not phrase:
        # An exception code we do not have wording for is still a permitted
        # filing — report that much rather than guessing at its meaning.
        return f"{level}; a permitted exception is filed instead"
    return f"{level}: {phrase}, a permitted exception"


def finding_gleif(bundle: dict[str, Any]) -> str | None:
    """Who consolidates this entity's accounts, per GLEIF Level 2.

    Reads ``GleifAdapter.fetch`` output: ``direct_parent`` / ``ultimate_parent``
    (each the parent's full Level 1 record, so the parent's legal name is
    available), ``direct_parent_exception`` / ``ultimate_parent_exception``
    (reporting-exception reason codes), and ``direct_children_total``.

    See the vocabulary note above ``_GLEIF_EXCEPTION_PHRASES``: consolidation
    is not ownership, so nothing here says owned, holds, or a percentage.
    """
    if not bundle or bundle.get("is_stub") or not bundle.get("record"):
        return None

    direct = bundle.get("direct_parent")
    ultimate = bundle.get("ultimate_parent")
    direct_name = _gleif_parent_name(direct)
    ultimate_name = _gleif_parent_name(ultimate)
    direct_reason = _gleif_exception_reason(bundle.get("direct_parent_exception"))
    ultimate_reason = _gleif_exception_reason(bundle.get("ultimate_parent_exception"))

    lead: str | None
    second: str | None = None

    if direct and ultimate and _gleif_parent_lei(direct) == _gleif_parent_lei(ultimate):
        # One entity filed at both levels — say it once, and say it is both.
        lead = (
            f"consolidated by {direct_name}, its direct and ultimate parent"
            if direct_name
            else "consolidated by one entity reported as both direct and ultimate parent"
        )
    elif direct and ultimate:
        lead = f"consolidated by {direct_name}" if direct_name else "a direct parent is reported"
        second = (
            f"ultimately by {ultimate_name}"
            if ultimate_name
            else "a separate ultimate parent is reported"
        )
    elif direct:
        lead = f"consolidated by {direct_name}" if direct_name else "a direct parent is reported"
        if ultimate_reason:
            second = _gleif_exception_clause(ultimate_reason, ultimate=True)
    elif ultimate:
        lead = (
            f"ultimately consolidated by {ultimate_name}"
            if ultimate_name
            else "an ultimate parent is reported"
        )
        if direct_reason:
            second = _gleif_exception_clause(direct_reason)
    elif direct_reason and ultimate_reason == direct_reason:
        lead = _gleif_exception_clause(direct_reason, both=True)
    elif direct_reason and ultimate_reason:
        lead = _gleif_exception_clause(direct_reason)
        # The lead has already said "a permitted exception"; repeating the
        # whole frame for the ultimate level pushes the sentence past the cap
        # and the tail clause is then dropped entirely — losing the fact.
        ultimate_phrase = _GLEIF_EXCEPTION_PHRASES.get(ultimate_reason)
        second = f"ultimately, {ultimate_phrase}" if ultimate_phrase else None
    elif direct_reason:
        lead = _gleif_exception_clause(direct_reason)
    elif ultimate_reason:
        lead = _gleif_exception_clause(ultimate_reason, ultimate=True)
    else:
        # GLEIF expects either a parent or an exception; neither on file is a
        # fact about the filing, stated as such rather than left blank.
        lead = "no consolidating parent and no reporting exception are on file"

    children = int(bundle.get("direct_children_total") or 0)
    child_clause = _gleif_child_clause(children)

    return clauses_to_sentence([lead, second, child_clause], sep="; ")


# ---------------------------------------------------------------------------
# bods_gleif — Open Ownership's BODS v0.4 extract of the GLEIF bulk data
# ---------------------------------------------------------------------------


def finding_bods_gleif(payload: dict[str, Any], statement_id: str) -> str | None:
    """The same Level 2 facts as :func:`finding_gleif`, from the Parquet extract.

    This path serves only the curated bulk-BODS examples; every live lookup
    goes through :func:`finding_gleif`. The vocabulary is deliberately
    identical — "consolidating parent", never ownership — so the two paths
    cannot describe the same relationship with two different verbs.

    Deviation from the suggested shape: this extract carries **no percentages
    and no parent names**. Open Ownership's Parquet relationship table holds
    statement ids plus ``directOrIndirect`` and nothing else, so the sentence
    counts and qualifies the parent links rather than naming a parent. Where
    the live bundle can say "consolidated by X", this can only say how many
    consolidating parents were reported.
    """
    statements = payload.get("bods_statements") or []
    entity: dict[str, Any] = {}
    parents: list[dict[str, Any]] = []
    children: list[dict[str, Any]] = []
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
                children.append(details)

    def _directness(rels: list[dict[str, Any]]) -> set[str]:
        return {
            str(interest.get("directOrIndirect") or "").lower()
            for rel in rels
            for interest in (rel.get("interests") or [])
        } - {""}

    directness = _directness(parents)

    if not parents:
        # GLEIF's own term for this is a reporting exception, but the
        # exception reason is not in the extract — so state the absence
        # plainly (rule 6) rather than naming a reason we do not hold.
        parent_clause: str | None = "no consolidating parent is reported"
    elif directness == {"direct"}:
        parent_clause = f"reports {plural(len(parents), 'direct consolidating parent')}"
    elif directness == {"indirect"}:
        parent_clause = f"reports {plural(len(parents), 'indirect consolidating parent')}"
    elif directness:
        parent_clause = (
            f"reports {plural(len(parents), 'consolidating parent')}, direct and indirect"
        )
    else:
        parent_clause = f"reports {plural(len(parents), 'consolidating parent')}"

    child_clause = _gleif_child_clause(
        len(children), direct=_directness(children) == {"direct"}
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

    Rule 7 is why this counts listings and names the source's own topics
    instead of calling anyone sanctioned: the topic is what OpenSanctions
    recorded, the judgement is the signals layer's. The topics are rendered
    through ``opencheck.topics`` — English, and still nouns.

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

    # OpenSanctions' own vocabulary, in English. The clause keeps the
    # source-attribution framing rule 7 asks for — it says what OpenSanctions
    # *recorded the record under*, not what the company is — while the labels
    # themselves are noun phrases naming the topic ("sanctions listing"),
    # never adjectives applied to the subject ("sanctioned"). Printing the raw
    # slug quoted upstream less faithfully, not more: `corp.disqual` is a key
    # in a taxonomy, and it reached readers on a live Rosneft lookup.
    # Quoted, because that is what makes the frame unambiguous: the words
    # inside the marks are OpenSanctions' name for a category, not OpenCheck
    # describing the company. It also fixes the reading — "recorded under the
    # topic sanctions listing" and "recorded under the topic "sanctions
    # listing"" are the same claim, and only one of them parses.
    labels = [f'"{label}"' for label in topic_list(topics)]
    topic_clause = (
        f"recorded under the {'topic' if len(labels) == 1 else 'topics'} "
        f"{_join_names(labels)}"
        if labels
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


# ---------------------------------------------------------------------------
# everypolitician
# ---------------------------------------------------------------------------


def finding_everypolitician(hit_summary: str, related_party: str) -> str | None:
    """What an EveryPolitician record on the report is, said plainly.

    Unlike every other template here, this describes a **name match on a
    related party**, not a fact about the subject. EveryPolitician holds
    politicians, and the subject of an OpenCheck lookup is a company; the row
    exists because a person named in the company's records shares a name with
    a record in the PEP dataset.

    The sentence therefore says what it is and what it is not, in the wording
    the risk layer already uses for the same finding: a name match is not an
    identity match, and being a PEP is not a finding of wrongdoing. Rule 2
    (assert nothing about risk) is not breached — a PEP listing is what the
    source published, and the sentence draws no conclusion from it.
    """
    party = (related_party or "").strip()
    detail = (hit_summary or "").strip()
    # The caveat rides in the lead clause, not as a trailing one: clauses are
    # dropped from the end to meet the length cap, and "not confirmed to be
    # the same person" is the one clause that must never be the one that goes.
    lead = (
        f"Possible name match only for {party}, a party named in this "
        "company's records — not confirmed to be the same person"
        if party
        else "Possible name match only for a party named in this company's "
        "records — not confirmed to be the same person"
    )
    return clauses_to_sentence(
        [lead, f"listed as {detail}" if detail else None],
        sep="; ",
    )


# ---------------------------------------------------------------------------
# gemi_greece
# ---------------------------------------------------------------------------


def finding_gemi_greece(bundle: dict[str, Any]) -> str | None:
    """What ΓΕΜΗ holds: status, since when, and who is on file.

    The owner/officer split follows ``persons[].category`` — ``Εταίροι`` are
    partners with real holdings, ``Διοικητικό συμβούλιο`` are board members.

    An **ΑΕ publishes no partners at all**: its share register is not part of
    ΓΕΜΗ publicity. This sentence therefore never says owners are missing,
    absent or withheld for a Greek company — it reports what is on file and
    stops. Saying "no owners found" would read as a transparency failure where
    the register is simply doing what Greek law provides.
    """
    from .sources.gemi_greece import (  # local import avoids circular
        CATEGORY_BOARD,
        CATEGORY_PARTNERS,
        english_label,
        parse_percentage,
        status_is_active,
    )

    company = bundle.get("company")
    if not isinstance(company, dict):
        return None

    people = [p for p in (company.get("persons") or []) if isinstance(p, dict)]
    partners = [p for p in people if str(p.get("category") or "").strip() == CATEGORY_PARTNERS]
    board = [p for p in people if str(p.get("category") or "").strip() == CATEGORY_BOARD]

    status_label = english_label("companyStatuses", company.get("status"))
    active = status_is_active(company.get("status"))
    since = human_date(company.get("incorporationDate"))
    if active and since:
        lead: str | None = f"Active since {since}"
    elif status_label and since:
        lead = f"{status_label}, registered {since}"
    elif status_label:
        lead = status_label
    elif since:
        lead = f"Registered {since}"
    else:
        lead = None

    # The largest declared holding is the most informative single fact about
    # a partnership, so it leads the ownership clause where one exists.
    partner_clause: str | None = None
    if partners:
        best = max(
            partners,
            key=lambda p: parse_percentage(p.get("percentage")) or 0.0,
        )
        partner_clause = holding_clause(
            str(best.get("personName") or best.get("businessName") or "").strip() or None,
            parse_percentage(best.get("percentage")),
        )
        if partner_clause and len(partners) > 1:
            partner_clause = (
                f"{plural(len(partners), 'partner')} on file, "
                f"the largest {partner_clause}"
            )
        elif not partner_clause:
            partner_clause = f"{plural(len(partners), 'partner')} on file"

    board_clause = f"{plural(len(board), 'board member')} listed" if board else None

    return clauses_to_sentence([lead, partner_clause, board_clause])


# ---------------------------------------------------------------------------
# climatetrace — GEM ownership tracker + Climate TRACE emissions
# ---------------------------------------------------------------------------


def finding_climatetrace(bundle: dict[str, Any]) -> str | None:
    """Energy-asset footprint and corporate status per the ownership tracker.

    Reads the fetch bundle: ``entity_status`` (dissolved/amalgamated/joint
    venture — the August 2026 lifecycle fields), ``projects`` (the GEOT
    ownership-closure totals) and ``emissions`` (the satellite-derived
    aggregate). A dissolved or amalgamated status leads — it changes a
    decision more than any asset count (rule 3). The status is worded
    "recorded as": it is the tracker's record, not a registry filing.
    """
    if not bundle or bundle.get("is_stub"):
        return None

    entity_status = bundle.get("entity_status") or {}
    status = entity_status.get("status")
    status_clause: str | None = None
    if status == "amalgamated":
        successor = entity_status.get("merged_into_name")
        status_clause = (
            f"recorded as amalgamated into {successor}"
            if successor
            else "recorded as amalgamated into another entity"
        )
    elif status == "dissolved":
        status_clause = "recorded as dissolved"

    jv_clause = "identified as a joint venture" if entity_status.get("jv") else None

    projects = bundle.get("projects")
    projects_clause: str | None = None
    if isinstance(projects, dict):
        live, operating, _controlled = (projects.get("total") or [0, 0, 0])[:3]
        if live:
            projects_clause = f"{plural(int(live), 'live energy project')} on file"
            if operating:
                projects_clause += f" ({int(operating)} operating)"
        else:
            projects_clause = "no live energy projects on file"

    emissions = bundle.get("emissions") or {}
    emissions_clause: str | None = None
    try:
        total_co2e = float(emissions.get("total_co2e_tonnes") or 0)
    except (TypeError, ValueError):
        total_co2e = 0.0
    if total_co2e > 0:
        year = emissions.get("year") or 2024
        if total_co2e >= 1_000_000:
            quantity = f"{total_co2e / 1_000_000:.1f} Mt"
        else:
            quantity = f"{total_co2e:,.0f} t"
        emissions_clause = f"{year} emissions estimated at {quantity} CO₂e"

    return clauses_to_sentence(
        [status_clause, projects_clause, emissions_clause, jv_clause], sep="; "
    )
