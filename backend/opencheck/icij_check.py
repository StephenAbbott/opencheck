"""Cross-check entity and officer names against the ICIJ Offshore Leaks
reconciliation API.

Why this exists
---------------

The ICIJ Offshore Leaks database covers the Panama Papers, Paradise Papers,
Pandora Papers, Bahamas Leaks, and the original Offshore Leaks dataset —
roughly 800,000 offshore entities and their associated individuals.  The
reconciliation API (OpenRefine-compatible) lets us check any name against
the full database in a single batched HTTP call.

This module complements ``cross_check.py`` (which checks against OpenSanctions
and EveryPolitician). The two are intentionally separate because:

* ICIJ requires no API key — it works in live mode without credentials.
* The matching algorithm is ICIJ's own (score 0–100) rather than our
  local string similarity.
* The signal it fires (``OFFSHORE_LEAKS``) maps directly to an existing
  risk code already surfaced by the OpenAleph adapter; this adds the
  name-based pathway alongside the entity-id pathway.

Reconciliation API
------------------

Endpoint: ``POST https://offshoreleaks.icij.org/api/v1/reconcile``
Content-Type: ``application/x-www-form-urlencoded``
Body param: ``queries`` — JSON-encoded dict of query objects.

ICIJ moved the service to the ``/api/v1/`` prefix and upgraded it to
Reconciliation Service API **v0.2**; the bare ``/reconcile`` path now 404s.
Form-encoded ``queries`` is still the spec-mandated transport in v0.2
(the service MUST accept it), so only the URL changed for us — but the
result ``id`` is now a **bare node id** (e.g. ``"12345"``) rather than a
full URL, so ``_node_url()`` rebuilds the public link from it.

Query object::

    {
      "q0-entity":       {"query": "A NAME", "limit": 2, "type": ".../entity"},
      "q0-officer":      {"query": "A NAME", "limit": 2, "type": ".../officer"},
      "q0-intermediary": {"query": "A NAME", "limit": 2, "type": ".../intermediary"},
      ...
    }

Each name is asked for once per screened node type (see ``_SCREENED_TYPES``,
which excludes ``Address`` and explains why). A per-query ``type`` is honoured
independently inside a batch, so this costs queries rather than round trips —
but a *list* of types in one query object is silently ignored by the service,
which is why they cannot be combined.

Response::

    {
      "q0": {
        "result": [
          {
            "id": "12345",
            "name": "ENTITY NAME",
            "score": 90,
            "match": true,
            "types": [{"id": ".../schema/oldb/entity", "name": "Entity"}],
            "description": "Entity node extracted from the Panama Papers data."
          }
        ]
      }
    }

Scores are on a 0–100 scale. ``match: true`` is ICIJ's own "this is the
one" flag. Since Phase 237 it decides nothing here — not the score
threshold, not the confidence — because ICIJ's scorer is the input this
module does not trust (it rated ENERGEN BIOGAS ↔ BIOGAS ENERGY 90/100). It is
kept on ``evidence["icij_match"]`` as a recorded fact.

Node details: the ``extend`` service (Phase 237)
------------------------------------------------

A reconciliation result carries a name, a type and a sentence, and nothing
that can tell two same-named companies apart. The same endpoint's data
extension (``POST`` with an ``extend`` form field instead of ``queries``)
returns, per node id, ``country_codes`` (ISO 3166-1 alpha-2, from the node's
addresses and jurisdiction) and ``valid_until`` — ICIJ's own statement of how
far the leak's documents run ("The Panama Papers data is current through
2015"). ``jurisdiction`` and ``incorporation_date`` are declared properties
too, but came back empty on every node sampled on 2026-09-24, so nothing
relies on them. One extra request per batch, and only when a candidate has
already passed the name gates. Two gates follow (``_gate``):

* **Date.** A party founded (or born) after the year the leak's documents
  end cannot be in it. The cutoff is ICIJ's ``valid_until`` where the extend
  call answered, else ``_LEAK_CUTOFF_YEARS``. Such a match is dropped.
* **Jurisdiction.** The party's own country — the BODS ``jurisdiction`` code
  and its address countries — among the node's ``country_codes`` is the one
  corroboration an ICIJ match can carry. Only a corroborated *entity* match
  is ``high``; every other match, and every person match (Stephen, 24 Sept
  2026: a common name in a country is not corroboration), is ``medium``.

A failed extend call degrades nothing: the date gate still runs off the
table, and the matches stay at ``medium`` — the safe direction.

Two more v0.2 shape changes, confirmed live 2026-07-30: the node type moved
from ``type`` to ``types`` (both are read), and ``description`` is now a
free-text sentence — ``"<NodeType> node extracted from the <Dataset>
data."``, where the dataset may carry a sub-collection ("Paradise Papers -
Appleby") — rather than the bullet-separated ``"Panama Papers · British
Virgin Islands"``. ``_parse_dataset`` / ``_parse_collection`` /
``_parse_jurisdiction`` handle both.

Reference: https://offshoreleaks.icij.org/docs/reconciliation
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from . import names
from .config import get_settings
from .http import build_client, sanitize_name_query
from .risk import (
    OFFSHORE_LEAKS,
    DegradedSource,
    RiskSignal,
    classify_degradation_reason,
    former_party_ids,
    pick_degradation_reason,
)
from .subject_identity import SubjectIdentity, subject_identity

_LOG = logging.getLogger(__name__)

_RECONCILE_URL = "https://offshoreleaks.icij.org/api/v1/reconcile"

# Public (human-readable) node page. The reconciliation result's ``id`` is a
# bare node identifier under spec v0.2, so the link is rebuilt from it.
_NODE_URL_TEMPLATE = "https://offshoreleaks.icij.org/nodes/{id}"

# Maximum number of names to check in a single run (bounds total HTTP calls).
_MAX_TARGETS = 30

# Node types we screen against, mapped to their schema URI.
#
# ``Address`` is DELIBERATELY ABSENT. ICIJ also indexes postal addresses, and
# comparing a company name to an address string is a category error: measured
# live 2026-07-30 over 14 real subjects / 350 names, Address took **57.8% of
# every result set and produced 0 signals** — 0 of 1,959 Address results
# cleared the similarity gate, best seen 0.429. Worse, because ICIJ's result
# ordering is NOT score-descending, those addresses displaced real matches out
# of the result window: Glencore plc and BP p.l.c. each returned zero
# offshore-leaks signals in production while exact, score-100 matches for
# ``Glencore plc``, ``Glencore International AG``, ``Glencore Group Funding
# Ltd`` and BP's ``BRITANNIC TRADING LIMITED`` sat in the database unseen.
#
# Scoping is done server-side, one query per type. A LIST of types in a single
# query object is silently ignored by the service (no error, unfiltered
# results), so the types must be asked for separately — but each query object
# in a batch carries its own ``type`` and is honoured independently, so this
# costs queries, not round trips.
_SCHEMA_BASE = "https://offshoreleaks.icij.org/schema/oldb/"
_SCREENED_TYPES: dict[str, str] = {
    "Entity": _SCHEMA_BASE + "entity",
    "Officer": _SCHEMA_BASE + "officer",
    "Intermediary": _SCHEMA_BASE + "intermediary",
}

# Results requested per name per type. Two, because ICIJ genuinely holds the
# same organisation as separate nodes across leaks (``Glencore plc`` appears
# three times), and those are distinct evidence. Going deeper than two only
# reaches the marginal tail — measured: 18 signals at 2, 19 at 3.
_RESULTS_PER_TYPE = 2

# Names per API batch. Each name costs len(_SCREENED_TYPES) queries and the
# service manifest declares ``batchSize: 25``, so 8 names = 24 queries fits
# inside one request.
_BATCH_SIZE = 8

# ICIJ score threshold (0–100). A coarse first filter and nothing more.
# Until Phase 237 ICIJ's ``match: true`` overrode it and made the signal
# "high"; it now does neither (see the module docstring).
_MIN_SCORE = 70

# The year each leak's documents end, for the date gate when the extend call
# did not answer. ICIJ's own ``valid_until`` is preferred (it is per
# sub-collection: Pandora's providers run from 2016 to 2018). Keys are
# ``(dataset, collection)`` lower-cased; ``""`` is the whole dataset, and
# where the sub-collections disagree it takes the LATEST (or, where ICIJ
# publishes no dataset-wide line, the publication year), so the table can
# only ever keep a match the precise cutoff would drop — never the reverse.
# Figures are ICIJ's valid_until lines as read on 2026-09-24.
_LEAK_CUTOFF_YEARS: dict[tuple[str, str], int] = {
    ("offshore leaks", ""): 2010,  # "current through 2010"
    ("panama papers", ""): 2015,  # "current through 2015"
    ("bahamas leaks", ""): 2016,  # "current through early 2016"
    ("paradise papers", "appleby"): 2014,  # "Appleby data is current through 2014"
    ("paradise papers", ""): 2017,  # registries through 2016; published Nov 2017
    ("pandora papers", "trident trust"): 2016,
    ("pandora papers", "alemán, cordero, galindo & lee (alcogal)"): 2018,
    ("pandora papers", ""): 2021,  # providers vary; published Oct 2021
}

# "The Panama Papers data is current through 2015",
# "The Bahamas Leaks data is current through early 2016."
_VALID_UNTIL_RE = re.compile(r"current through\D*(\d{4})", re.IGNORECASE)

# Node properties asked of the extend service. ``valid_until`` and
# ``country_codes`` exist on every node type; the others are Entity-only and
# are asked for in case ICIJ starts populating them.
_EXTEND_PROPERTIES = ("country_codes", "valid_until", "jurisdiction")

# Node ids per extend request — the manifest's batchSize.
_EXTEND_BATCH = 25

# Secondary sanity check: even if ICIJ scores high, the returned name must be
# this similar to what we searched. Uses the shared Phase-D scorer (see
# ``_name_sim``).
#
# History: 0.93 (PR #86) was the highest cut that killed the legal-form
# collisions a character scorer cannot distinguish from true matches
# ("CASTROL HOLDINGS INTERNATIONAL" vs "COSCO INTERNATIONAL HOLDINGS" at
# 0.92) — bought at the cost of two named true matches just under it
# (NICHOLAS PAUL RATCLIFFE 0.878; MOET HENNESSY INTERNATIONAL 0.877, which
# left LVMH with no offshore-leaks signal at all). Phase 120 moves the
# boilerplate burden to the distinctive-token gate
# (``names.distinctive_token_agreement``, applied in
# ``_signal_from_match``), which kills those collisions BY SHAPE rather
# than by score — re-measured on the rebuilt 14-subject corpus it also
# caught two collisions 0.93 had been letting through (WIGMORE 1↔WIGMORE
# at 0.9375, PRACTICE PLUS↔PRACTICE PLAN at 0.9444). With the gate
# carrying that load, the threshold returns to 0.87, recovering both named
# true matches. Measured on the production pool (2 results/type, ≤30
# targets): precision 69%→≥80%, recall 75%→100% of adjudicated true
# matches. See scripts/eval_icij_distinctive.py and
# docs/icij-distinctive-token-evaluation.md; re-measure there whenever the
# candidate pool changes shape (the PR #86 lesson).
_MIN_NAME_SIM = 0.87

# A name has to be specific enough to screen. "S +" — the real GLEIF legal
# name of an LVMH subsidiary — sanitises to "S", which matches an ICIJ officer
# node literally named "s" at score 100 and similarity 1.00: a high-confidence
# offshore-leaks hit off a single character. Length of the comparable form is
# the right test here; ``matching.is_matchable_name`` is NOT, since its
# single-token rule is calibrated for person names and would discard perfectly
# specific one-word companies (KENZO, CELINE, BERLUTI).
_MIN_COMPARABLE_CHARS = 3

# Pulls the dataset out of a v0.2 description sentence — see
# ``_parse_collection`` for the shapes this was verified against.
_SENTENCE_RE = re.compile(r"extracted from the\s+(.+?)\s+data\b", re.IGNORECASE)

# Guard for the passthrough of an unrecognised dataset name: prose that got
# this far is a description shape we do not parse yet, not a leak label.
_PROSE_RE = re.compile(r"\b(node|extracted|from)\b", re.IGNORECASE)

# Human-friendly labels for ICIJ dataset descriptions. Keys are the dataset
# name as it appears in the description, lower-cased.
_DATASET_LABELS: dict[str, str] = {
    "panama papers": "Panama Papers",
    "paradise papers": "Paradise Papers",
    "pandora papers": "Pandora Papers",
    "bahamas leaks": "Bahamas Leaks",
    "offshore leaks": "Offshore Leaks",
    "fbme bank": "FBME Bank",
}


# ---------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------


#: Name of this derived check in ``DegradedSource.check`` records.
CHECK_NAME = "icij_offshore_leaks"


async def assess_icij_names(
    bods: list[dict[str, Any]],
    *,
    max_targets: int = _MAX_TARGETS,
    min_score: int = _MIN_SCORE,
    min_name_sim: float = _MIN_NAME_SIM,
    degraded: list[DegradedSource] | None = None,
    subject_lei: str | None = None,
) -> list[RiskSignal]:
    """Return ``OFFSHORE_LEAKS`` risk signals for entities and persons in
    the BODS bundle whose names match a record in the ICIJ Offshore Leaks
    database.

    ``subject_lei`` (Phase 235) names the looked-up company. Its statements
    (``subject_identity``) are not related parties, so they leave the
    related-party targets — but, unlike OpenSanctions and OpenAleph, ICIJ has
    no subject-level adapter, so the subject's own names are screened here
    once (``_subject_targets``) and a match is worded as the company's own
    ("The looked-up company's name …"), attributed to one statement, and
    deduplicated to one signal per ICIJ record (Stephen, 23 Sept 2026).
    Before, CLP HOLDINGS LIMITED read "Related entity 'CLP HOLDINGS LIMITED'"
    twice — once for GLEIF's statement of it and once for OpenAleph's.

    ``degraded`` is an optional out-collector (issue #50): when one or
    more reconciliation batches fail, a :class:`DegradedSource` record is
    appended so callers can surface that the offshore-leaks screen is
    incomplete — an empty result is then not a clean screen. Records
    carry counts only, never the names being screened.

    No-op (returns ``[]``) when:

    * Live mode is off (offline/demo mode — expected, not a degradation).
    * The bundle has no person/entity statements.
    * The ICIJ reconciliation API is unreachable (errors are swallowed so
      one network problem doesn't poison the rest of the risk pipeline).
    """
    if not bods:
        return []

    settings = get_settings()
    if not settings.allow_live:
        return []

    identity = subject_identity(subject_lei, bods)
    targets = _subject_targets(identity) + _collect_targets(
        bods, exclude=identity.statement_ids
    )[:max_targets]
    if not targets:
        return []

    # Batch targets into groups to avoid oversized requests.
    signals: list[RiskSignal] = []
    failed = 0
    batches = 0
    skipped_names = 0
    reason_counts: dict[str, int] = {}
    for batch_start in range(0, len(targets), _BATCH_SIZE):
        batch = targets[batch_start: batch_start + _BATCH_SIZE]
        batches += 1
        try:
            batch_signals = await _check_batch(
                batch, min_score=min_score, min_name_sim=min_name_sim
            )
            signals.extend(batch_signals)
            continue
        except httpx.HTTPStatusError as exc:
            # The endpoint answered but rejected us. A 404 here means the
            # reconciliation service moved again (it did once already — see
            # the module docstring); 429 means we're being throttled. Loud
            # enough to notice without reading raw access logs.
            _LOG.warning(
                "ICIJ Offshore Leaks reconciliation failed: HTTP %s from %s "
                "(batch of %d name(s)).",
                exc.response.status_code,
                _RECONCILE_URL,
                len(batch),
            )
            if not _retry_per_name(exc):
                failed += 1
                skipped_names += len(batch)
                reason = classify_degradation_reason(exc)
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                continue
        except Exception as exc:  # noqa: BLE001
            # Network error, timeout, or unexpected response shape. Still
            # swallowed so one upstream problem can't sink the rest of the
            # risk pipeline — but no longer silent. No per-name retry: these
            # failures are service-level, so ten more requests would only
            # add latency (and load) to an upstream that is already down.
            failed += 1
            skipped_names += len(batch)
            reason = classify_degradation_reason(exc)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            _LOG.warning(
                "ICIJ Offshore Leaks reconciliation failed: %s: %s "
                "(%d name(s) in this batch skipped).",
                type(exc).__name__,
                exc,
                len(batch),
            )
            continue

        # Deterministic upstream rejection (4xx/5xx, but not 404/429): one
        # poison query — e.g. a name whose unbalanced double quote breaks
        # ICIJ's Lucene parser with a bare 500 — sinks the whole batch. Retry
        # each name individually so a bad name only loses itself instead of
        # taking up to nine clean names down with it.
        batch_skipped = 0
        for target in batch:
            try:
                signals.extend(
                    await _check_batch(
                        [target], min_score=min_score, min_name_sim=min_name_sim
                    )
                )
            except Exception as exc:  # noqa: BLE001
                batch_skipped += 1
                reason = classify_degradation_reason(exc)
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                _LOG.warning(
                    "ICIJ Offshore Leaks per-name retry failed: %s: %s "
                    "(1 name skipped).",
                    type(exc).__name__,
                    exc,
                )
        if batch_skipped:
            failed += 1
            skipped_names += batch_skipped
        else:
            _LOG.info(
                "ICIJ Offshore Leaks per-name retry recovered all %d name(s) "
                "from a failed batch.",
                len(batch),
            )

    if failed:
        # One line the operator can alert on: screening ran but is degraded,
        # so an empty OFFSHORE_LEAKS result is not the same as "no matches".
        _LOG.warning(
            "ICIJ Offshore Leaks screening degraded: %d of %d batch(es) failed; "
            "offshore-leaks risk signals may be incomplete for this lookup.",
            failed,
            batches,
        )
        if degraded is not None:
            degraded.append(
                DegradedSource(
                    source_id="icij",
                    check=CHECK_NAME,
                    affected_signals=[OFFSHORE_LEAKS],
                    detail=(
                        f"{failed} of {batches} reconciliation batch(es) "
                        f"failed; {skipped_names} of {len(targets)} name(s) "
                        "were not screened against the Offshore Leaks "
                        "database."
                    ),
                    reason=pick_degradation_reason(reason_counts),
                )
            )

    return _dedupe(signals)


# ---------------------------------------------------------------------
# Target extraction
# ---------------------------------------------------------------------

_KIND_PERSON = "person"
_KIND_ENTITY = "entity"


#: At most this many distinct names of the subject are screened. Sources
#: spell the subject differently (GLEIF's Cyrillic legal name, OpenSanctions'
#: English one), and ICIJ matches on the spelling; three covers the legal
#: name and two others without letting a many-sourced subject crowd the batch.
_MAX_SUBJECT_NAMES = 3


def _subject_targets(identity: SubjectIdentity) -> list[dict[str, Any]]:
    """The looked-up company's own names, as ``subject`` targets.

    One per distinct normalised name among the subject's entity statements,
    the anchor statement's name first; every one is attributed to the anchor
    statement (``SubjectIdentity.anchor_statement_id``) so two spellings that
    hit the same ICIJ record collapse to one signal in ``_dedupe``.
    """
    if not identity:
        return []
    anchor = identity.anchor_statement_id()
    ordered = sorted(
        identity.statements, key=lambda s: 0 if s.get("statementId") == anchor else 1
    )
    # One company, however many sources describe it: its facts for the
    # Phase 237 gates are pooled across the identity set — the EARLIEST
    # founding date any source gives (so a source that dates a
    # re-registration cannot drop a true match) and every country.
    founded: str | None = None
    countries: set[str] = set()
    for stmt in identity.statements:
        f, c = _party_facts(stmt.get("recordDetails") or {}, _KIND_ENTITY)
        if f and (founded is None or f < founded):
            founded = f
        countries |= c
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for stmt in ordered:
        name = str((stmt.get("recordDetails") or {}).get("name") or "").strip()
        key = _normalise(name)
        if not name or not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {"kind": _KIND_ENTITY, "statement_id": anchor, "name": name,
             "subject": True, "founded": founded,
             "countries": sorted(countries)}
        )
        if len(out) >= _MAX_SUBJECT_NAMES:
            break
    return out


def _collect_targets(
    bods: list[dict[str, Any]], *, exclude: frozenset[str] | set[str] = frozenset()
) -> list[dict[str, Any]]:
    """Extract ``{kind, statement_id, name}`` records from a BODS bundle.

    ``exclude`` is the subject identity set (Phase 235): the looked-up
    company's own statements are screened by ``_subject_targets``, never as
    related parties.

    Mirrors ``cross_check._collect_targets`` but shared here to keep the
    ICIJ module self-contained.  Skips placeholder types
    (``unknownPerson`` / ``anonymousEntity``) and records with empty names.

    ``former`` (Phase 220) marks a party whose every link in the bundle has
    ended (``risk.former_party_ids``). It is still screened; the signal says
    "former".
    """
    former = former_party_ids(bods)
    out: list[dict[str, Any]] = []
    for stmt in bods:
        record_type = stmt.get("recordType") or ""
        rd = stmt.get("recordDetails") or {}
        sid = stmt.get("statementId") or ""
        if not sid or sid in exclude:
            continue
        if record_type == "person":
            person_type = rd.get("personType") or ""
            if person_type and person_type != "knownPerson":
                continue
            name = _person_name(rd)
            if not name:
                continue
            born, countries = _party_facts(rd, _KIND_PERSON)
            out.append(
                {"kind": _KIND_PERSON, "statement_id": sid, "name": name,
                 "former": sid in former, "founded": born,
                 "countries": sorted(countries)}
            )
        elif record_type == "entity":
            entity_type = (
                (rd.get("entityType") or {}).get("type")
                if isinstance(rd.get("entityType"), dict)
                else rd.get("entityType")
            )
            if entity_type in {"anonymousEntity", "unknownEntity"}:
                continue
            name = (rd.get("name") or "").strip()
            if not name:
                continue
            founded, countries = _party_facts(rd, _KIND_ENTITY)
            out.append(
                {"kind": _KIND_ENTITY, "statement_id": sid, "name": name,
                 "former": sid in former, "founded": founded,
                 "countries": sorted(countries)}
            )
    return out


def _party_facts(rd: dict[str, Any], kind: str) -> tuple[str | None, set[str]]:
    """What the party's own statement says about when and where it is.

    Returns ``(date, countries)``: the ``foundingDate`` of an entity or the
    ``birthDate`` of a person (as published — ``YYYY``, ``YYYY-MM`` or a full
    date; only the year is compared), and the ISO 3166-1 alpha-2 countries of
    its ``jurisdiction`` (an entity's; ``US-DE`` counts as ``US``) and of its
    addresses. Used by the Phase 237 gates in ``_gate``.
    """
    raw_date = rd.get("foundingDate") if kind == _KIND_ENTITY else rd.get("birthDate")
    date = str(raw_date).strip() if raw_date else None
    if date and not re.match(r"^\d{4}", date):
        date = None
    countries: set[str] = set()

    def _add(code: Any) -> None:
        text = str(code or "").strip().upper()
        if re.match(r"^[A-Z]{2}(-|$)", text):
            countries.add(text[:2])

    if kind == _KIND_ENTITY:
        juris = rd.get("jurisdiction")
        if isinstance(juris, dict):
            _add(juris.get("code"))
    for addr in rd.get("addresses") or []:
        if isinstance(addr, dict):
            country = addr.get("country")
            _add(country.get("code") if isinstance(country, dict) else country)
    return date, countries


def _person_name(rd: dict[str, Any]) -> str:
    """The person's own name, by BODS name type.

    Same rule and the same reasoning as ``cross_check._person_full_name``:
    ``legal`` first, ``individual`` tolerated for third-party data, position
    only as a last resort. A statement now carries a source's aliases as
    ``alternative`` entries, and screening the alias instead of the name
    would be a silent change of subject.
    """
    names = rd.get("names") or []
    if not isinstance(names, list):
        return ""

    def _typed(wanted: str) -> dict[str, Any] | None:
        return next(
            (
                n
                for n in names
                if isinstance(n, dict) and n.get("type") == wanted and n.get("fullName")
            ),
            None,
        )

    pick = _typed("legal") or _typed("individual") or next(
        (n for n in names if isinstance(n, dict) and n.get("fullName")),
        None,
    )
    if pick is None:
        return ""
    return (pick.get("fullName") or "").strip()


# ---------------------------------------------------------------------
# Batch reconciliation
# ---------------------------------------------------------------------


def _retry_per_name(exc: httpx.HTTPStatusError) -> bool:
    """Whether a failed batch is worth retrying one name at a time.

    True for deterministic rejections (400/422/500-style), where a single
    poison query is the likely culprit. False for 429 (throttled — more
    requests make it worse) and 404 (the service moved — every retry would
    404 too; see the module docstring, it has moved once already).
    """
    status = exc.response.status_code
    return status not in (404, 429)


async def _check_batch(
    targets: list[dict[str, Any]],
    *,
    min_score: int,
    min_name_sim: float = _MIN_NAME_SIM,
) -> list[RiskSignal]:
    """POST one batch of names to the ICIJ reconciliation API and parse
    the results into risk signals.

    Each name is asked for once per screened node type (see
    ``_SCREENED_TYPES``), so a batch of N names sends N × 3 query objects in
    a single request — the service honours a per-query ``type`` independently
    within a batch, so type scoping costs queries, not round trips.

    Names are sanitised before they reach the query dict — the reconcile
    endpoint 500s on any query containing an unbalanced double quote (the
    ASCII gershayim in Israeli company names, בע"מ) or a dangling Lucene
    operator ("S +"). Names that sanitise to nothing, or that are too short
    to be worth screening (``_MIN_COMPARABLE_CHARS``), are skipped.
    """
    queries: dict[str, Any] = {}
    keyed_targets: dict[str, dict[str, Any]] = {}
    for i, t in enumerate(targets):
        q = sanitize_name_query(t["name"])
        if not _screenable(q):
            continue
        for type_name, type_uri in _SCREENED_TYPES.items():
            key = f"q{i}-{type_name.lower()}"
            queries[key] = {
                "query": q,
                "limit": _RESULTS_PER_TYPE,
                "type": type_uri,
            }
            keyed_targets[key] = t
    if not queries:
        return []

    async with build_client() as client:
        response = await client.post(
            _RECONCILE_URL,
            data={"queries": json.dumps(queries)},
        )
        response.raise_for_status()
        raw = response.json()

        # Name gates first, so the extend call is only made — and only asks
        # about nodes — that could become a signal.
        candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for query_key, target in keyed_targets.items():
            query_result = raw.get(query_key) or {}
            results = query_result.get("result") or []
            for match in results:
                if _passes_name_gates(
                    match, target, min_score=min_score, min_name_sim=min_name_sim
                ):
                    candidates.append((match, target))
        if not candidates:
            return []
        details = await _fetch_node_details(
            client, [str(m.get("id") or "") for m, _ in candidates]
        )

    signals: list[RiskSignal] = []
    for match, target in candidates:
        node = details.get(_bare_id(match.get("id"))) if details is not None else None
        sig = _signal_from_match(
            match,
            target,
            min_score=min_score,
            min_name_sim=min_name_sim,
            node=node,
            details_answered=details is not None,
        )
        if sig is not None:
            signals.append(sig)
    return signals


def _bare_id(raw_id: Any) -> str:
    """The node id the extend service keys its rows on — the bare id, also
    when a (pre-v0.2) result carried the full node URL."""
    text = str(raw_id or "").strip().rstrip("/")
    return text.rsplit("/", 1)[-1] if text.startswith(("http://", "https://")) else text


async def _fetch_node_details(
    client: httpx.AsyncClient, raw_ids: list[str]
) -> dict[str, dict[str, Any]] | None:
    """``country_codes`` and ``valid_until`` for each node, from the
    reconciliation service's data extension (Phase 237).

    Returns ``{node_id: {"country_codes": [...], "valid_until": str,
    "jurisdiction": str}}``, or ``None`` when the service did not answer.
    ``None`` is never a degradation: the date gate falls back to
    ``_LEAK_CUTOFF_YEARS`` and every match stays at ``medium``, which is the
    direction that cannot overstate a finding.
    """
    ids = list(dict.fromkeys(i for i in (_bare_id(r) for r in raw_ids) if i))
    if not ids:
        return {}
    out: dict[str, dict[str, Any]] = {}
    try:
        for start in range(0, len(ids), _EXTEND_BATCH):
            extend = {
                "ids": ids[start: start + _EXTEND_BATCH],
                "properties": [{"id": p} for p in _EXTEND_PROPERTIES],
            }
            response = await client.post(
                _RECONCILE_URL, data={"extend": json.dumps(extend)}
            )
            response.raise_for_status()
            rows = (response.json() or {}).get("rows") or {}
            if not isinstance(rows, dict):
                return None
            for node_id, row in rows.items():
                if isinstance(row, dict):
                    out[str(node_id)] = _parse_node_row(row)
    except Exception as exc:  # noqa: BLE001
        _LOG.warning(
            "ICIJ Offshore Leaks node details unavailable: %s: %s — "
            "matches kept at medium confidence, date gate on the table.",
            type(exc).__name__,
            exc,
        )
        return None
    return out


def _parse_node_row(row: dict[str, Any]) -> dict[str, Any]:
    """One extend row → plain values. Each property is a list of
    ``{"str": ...}`` cells."""

    def _cells(prop: str) -> list[str]:
        cells = row.get(prop) or []
        if not isinstance(cells, list):
            return []
        return [
            str(c.get("str") or "").strip()
            for c in cells
            if isinstance(c, dict) and str(c.get("str") or "").strip()
        ]

    countries = sorted(
        {c.upper() for c in _cells("country_codes") + _cells("jurisdiction")
         if re.fullmatch(r"[A-Za-z]{2}", c)}
    )
    valid = _cells("valid_until")
    return {"country_codes": countries, "valid_until": valid[0] if valid else ""}


# ---------------------------------------------------------------------
# Signal construction
# ---------------------------------------------------------------------


def _passes_name_gates(
    match: dict[str, Any],
    target: dict[str, Any],
    *,
    min_score: int,
    min_name_sim: float = _MIN_NAME_SIM,
) -> bool:
    """Whether a reconciliation result names the target at all.

    False when:
    * The ICIJ score is below ``min_score``. ICIJ's ``match: true`` no longer
      overrides this (Phase 237).
    * The returned name is too dissimilar to the searched name
      (secondary sanity check, guards against ICIJ index collisions).
    * The target is an ENTITY and the two names disagree on their
      distinctive tokens (``names.distinctive_token_agreement``) — the
      Phase 120 gate. Character similarity cannot tell "BIFFA CORPORATE
      HOLDINGS LTD" (true) from "Barb Holdco Limited" (false): the shared
      filler dominates the comparison. Person names carry no legal forms
      and every token is distinctive, so persons rely on the similarity
      threshold alone.
    """
    score = int(match.get("score") or 0)
    if score < min_score:
        return False

    matched_name: str = (match.get("name") or "").strip()
    if not matched_name:
        return False

    # Secondary name-similarity sanity check.
    if _name_sim(target["name"], matched_name) < min_name_sim:
        return False

    # Distinctive-token gate. ICIJ's own scorer rated the ENERGEN/BIOGAS
    # collision 90/100, so neither its score nor its ``match`` flag earns a
    # bypass. Entity targets always; person targets only when either name
    # carries a legal form, because BODS person statements sometimes hold
    # corporate officers and those are organisations for matching purposes.
    # Real personal names ("NICHOLAS PAUL RATCLIFFE") are all distinctive
    # tokens and rely on the similarity threshold alone.
    entityish = (
        target["kind"] != _KIND_PERSON
        or names.has_org_form_tokens(target["name"])
        or names.has_org_form_tokens(matched_name)
    )
    if entityish and not names.distinctive_token_agreement(
        target["name"], matched_name
    ):
        return False
    return True


def _leak_cutoff(
    dataset: str, collection: str, node: dict[str, Any] | None
) -> tuple[int | None, str]:
    """The last year a leak's documents cover, and where that came from.

    ICIJ's own ``valid_until`` (via the extend call) first — it is per
    sub-collection — then ``_LEAK_CUTOFF_YEARS``. ``(None, "")`` for a leak
    neither knows (FBME Bank; a future dataset): the date gate is then not
    applied, and the evidence says so.
    """
    if node:
        m = _VALID_UNTIL_RE.search(node.get("valid_until") or "")
        if m:
            return int(m.group(1)), "icij"
    ds = dataset.lower()
    coll = collection.lower()
    year = _LEAK_CUTOFF_YEARS.get((ds, coll))
    if year is None:
        year = _LEAK_CUTOFF_YEARS.get((ds, ""))
    return (year, "table") if year is not None else (None, "")


def _gate(
    target: dict[str, Any],
    *,
    dataset: str,
    collection: str,
    node: dict[str, Any] | None,
    details_answered: bool,
) -> tuple[bool, str, dict[str, Any]]:
    """The Phase 237 gates: ``(keep, confidence, evidence)``.

    * **Date** — a party whose founding (entity) or birth (person) year is
      after the leak's last year cannot be in it: ``keep`` is False. CLP
      HOLDINGS LIMITED (Jersey, founded 2021) against a Panama Papers
      intermediary (documents through 2015) is the shape.
    * **Jurisdiction** — the party's countries (``_party_facts``) among the
      node's ``country_codes``. The only corroboration an ICIJ match carries,
      so the only way to ``high``, and for entities only: a person match is
      ``medium`` whatever the countries say (Stephen, 24 Sept 2026).

    The evidence records each gate's inputs, outcome and one sentence
    (``gates``), so a reader can see why a match stands at its confidence.
    """
    kind = target["kind"]
    founded = target.get("founded")
    party_countries = list(target.get("countries") or [])
    cutoff, cutoff_source = _leak_cutoff(dataset, collection, node)
    date_word = "incorporation" if kind == _KIND_ENTITY else "birth"
    notes: list[str] = []

    if founded and cutoff is not None:
        year = int(str(founded)[:4])
        if year > cutoff:
            return False, "", {}
        date_status = "passed"
        notes.append(
            f"gate passed: {date_word} {founded} ≤ leak cutoff {cutoff}"
        )
    elif cutoff is None:
        date_status = "not_checked"
        notes.append("date not checked: no cutoff known for this leak")
    else:
        date_status = "not_checked"
        notes.append(f"date not checked: no {date_word} date on the party")

    record_countries: list[str] = list((node or {}).get("country_codes") or [])
    matched = sorted(set(party_countries) & set(record_countries))
    if matched:
        juris_status = "corroborated"
        notes.append(
            "jurisdiction "
            + ", ".join(f"{c} = {c}" for c in matched)
        )
    elif not details_answered:
        juris_status = "not_checked"
        notes.append("jurisdiction not checked: ICIJ node details unavailable")
    elif not party_countries:
        juris_status = "not_checked"
        notes.append("jurisdiction not checked: no country on the party")
    elif not record_countries:
        juris_status = "not_checked"
        notes.append("jurisdiction not checked: ICIJ record carries no country")
    else:
        juris_status = "differs"
        notes.append(
            f"jurisdiction differs: {'/'.join(party_countries)} ≠ "
            f"{'/'.join(record_countries)}"
        )

    corroborated = juris_status == "corroborated" and kind != _KIND_PERSON
    if not corroborated:
        notes.append("name-only match: capped at medium")

    evidence = {
        "gates": notes,
        "date_gate": {
            "status": date_status,
            "party_date": founded,
            "leak_cutoff_year": cutoff,
            "cutoff_source": cutoff_source or None,
        },
        "jurisdiction_gate": {
            "status": juris_status,
            "party_countries": party_countries,
            "record_countries": record_countries,
        },
    }
    return True, ("high" if corroborated else "medium"), evidence


def _signal_from_match(
    match: dict[str, Any],
    target: dict[str, Any],
    *,
    min_score: int,
    min_name_sim: float = _MIN_NAME_SIM,
    node: dict[str, Any] | None = None,
    details_answered: bool = False,
) -> RiskSignal | None:
    """Convert one ICIJ reconciliation result to an OFFSHORE_LEAKS signal.

    Returns ``None`` when the result fails the name gates
    (``_passes_name_gates``) or the date gate (``_gate``). ``node`` is the
    result's extend row (``_fetch_node_details``); ``details_answered`` says
    whether the extend call answered at all, which is what separates "ICIJ
    holds no country for this node" from "we could not ask".
    """
    if not _passes_name_gates(
        match, target, min_score=min_score, min_name_sim=min_name_sim
    ):
        return None
    score: int = int(match.get("score") or 0)
    icij_match: bool = bool(match.get("match"))
    matched_name: str = (match.get("name") or "").strip()

    node_url: str = _node_url(match.get("id"))
    description: str = match.get("description") or ""
    dataset = _parse_dataset(description)
    jurisdiction = _parse_jurisdiction(description)
    collection = _parse_collection(description)
    node_type = _node_type(match)

    keep, confidence, gate_evidence = _gate(
        target,
        dataset=dataset,
        collection=collection,
        node=node,
        details_answered=details_answered,
    )
    if not keep:
        return None

    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
    # Phase 220: a party whose every link has ended is still screened, and
    # the sentence says so — same wording as cross_check.related_party_label.
    if target.get("former"):
        relation = f"Former {relation.lower()}"
    dataset_label = f"the {dataset}" if dataset else "the ICIJ Offshore Leaks database"
    # Legacy descriptions carried a jurisdiction; current ones carry a leak
    # sub-collection. Either narrows the record usefully in the same slot.
    qualifier = jurisdiction or collection
    qual_note = f" ({qualifier})" if qualifier else ""

    # An Intermediary node is not the same finding as an Entity or Officer
    # one: ICIJ uses it for the go-between that ARRANGED an offshore
    # structure — a law firm or company-formation agent (Appleby, Mossack
    # Fonseca). A related party turning up in that role is a materially
    # different fact from being named in the leak as an owner or officer, so
    # it is worded differently rather than folded into "matches a record".
    # The sentence names the finding and the dataset, and stops there. It used
    # to end "(ICIJ score 100/100)" — a retrieval score printed with a
    # denominator, which reads as *this is a perfect match* on the one finding
    # type that is most explicitly not an identity claim. Worse, it is the one
    # input this module deliberately does not trust: ICIJ's own scorer rated
    # the ENERGEN/BIOGAS collision 90/100, which is why every match still has
    # to clear ``min_name_sim`` and the distinctive-token gate below. The
    # number is a coarse first filter, not the reason the signal fired, so
    # handing it to a reader as though it were the finding's strength
    # overstated it in exactly the direction that matters. It stays on
    # ``evidence["icij_score"]`` (Phase 136).
    if target.get("subject"):
        # Phase 235: the looked-up company's own name. Not a related party,
        # and still a name match — the sentence says both.
        if node_type == "Intermediary":
            summary = (
                f"The looked-up company's name '{target['name']}' appears as an "
                f"offshore-services intermediary in {dataset_label}{qual_note}."
            )
        else:
            summary = (
                f"The looked-up company's name '{target['name']}' matches a "
                f"record in {dataset_label}{qual_note}."
            )
    elif node_type == "Intermediary":
        summary = (
            f"{relation} '{target['name']}' appears as an offshore-services "
            f"intermediary in {dataset_label}{qual_note}."
        )
    else:
        summary = (
            f"{relation} '{target['name']}' matches a record in "
            f"{dataset_label}{qual_note}."
        )

    return RiskSignal(
        code=OFFSHORE_LEAKS,
        confidence=confidence,
        summary=summary,
        source_id="icij",
        hit_id=node_url or f"icij:{_slug(target['name'])}",
        evidence={
            # A subject match is anchored like SANCTIONED / PEP
            # (``statement_id``); a related party's like every RELATED_*
            # signal (``subject_statement_id``) — frontend signalScope reads both.
            **(
                {"statement_id": target["statement_id"], "subject": True}
                if target.get("subject")
                else {"subject_statement_id": target["statement_id"]}
            ),
            "search_name": target["name"],
            "matched_name": matched_name,
            "icij_score": score,
            # ICIJ's own flag, recorded — it decides nothing (Phase 237).
            "icij_match": icij_match,
            "dataset": dataset,
            "jurisdiction": jurisdiction,
            "collection": collection,
            "node_type": node_type,
            "node_url": node_url,
            "kind": target["kind"],
            **gate_evidence,
            **({"former": True} if target.get("former") else {}),
        },
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _node_url(raw_id: Any) -> str:
    """Public ICIJ node URL for a reconciliation result ``id``.

    Spec v0.2 returns a bare node identifier (``"12345"``), so the link is
    rebuilt from the template. Values that are already absolute URLs (the
    pre-v0.2 shape, and any future change back) pass through unchanged, so
    the evidence link is correct either way.
    """
    node_id = str(raw_id or "").strip()
    if not node_id:
        return ""
    if node_id.startswith(("http://", "https://")):
        return node_id
    return _NODE_URL_TEMPLATE.format(id=node_id)


def _parse_dataset(description: str) -> str:
    """Extract the leak dataset name from an ICIJ description string.

    Two shapes are handled, because the service changed under us (see
    :func:`_parse_collection`):

    * v0.2 sentence — ``"Entity node extracted from the Panama Papers
      data."`` → ``"Panama Papers"``.
    * Legacy bullet — ``"Panama Papers · British Virgin Islands"`` →
      ``"Panama Papers"``.

    An unrecognised dataset passes through as-is — a future leak should still
    be named — but only once it has been isolated from the surrounding prose.
    Text that still reads as a sentence returns ``""`` instead, so the caller
    falls back to "the ICIJ Offshore Leaks database"; pasting a whole
    description into user-facing copy is what this function used to do, and
    it read as "matches a record in the Entity node extracted from the Panama
    Papers data.".
    """
    if not description:
        return ""
    source = _SENTENCE_RE.search(description)
    raw = source.group(1) if source else re.split(r"[·•|/]", description)[0]
    # "Paradise Papers - Appleby" → leak name, sub-collection.
    leak = raw.split(" - ", 1)[0].strip()
    known = _DATASET_LABELS.get(leak.lower())
    if known:
        return known
    return "" if _PROSE_RE.search(leak) else leak


def _parse_collection(description: str) -> str:
    """Extract the sub-collection an ICIJ node came from, if any.

    ICIJ's reconciliation v0.2 descriptions are free-text sentences of the
    form ``"<NodeType> node extracted from the <Dataset> data."``, where the
    dataset may carry a sub-collection: ``"Paradise Papers - Appleby"``,
    ``"Paradise Papers - Malta corporate registry"``. Confirmed live
    2026-07-30 across the four node types the service returns (Entity,
    Officer, Intermediary, Address).

    Deliberately NOT reported as a jurisdiction: "Appleby" is a law firm and
    "Malta corporate registry" is a leak sub-source, so labelling either as
    the record's jurisdiction would assert something ICIJ has not said. See
    :func:`_parse_jurisdiction`, which stays keyed to the legacy shape that
    really did carry one.
    """
    if not description:
        return ""
    source = _SENTENCE_RE.search(description)
    if source is None:
        return ""
    parts = source.group(1).split(" - ", 1)
    return parts[1].strip() if len(parts) == 2 else ""


def _parse_jurisdiction(description: str) -> str:
    """Extract the jurisdiction part from a legacy ICIJ description string.

    ``"Panama Papers · British Virgin Islands"`` → ``"British Virgin Islands"``

    The current (v0.2) sentence shape carries no jurisdiction, so this
    returns ``""`` for it rather than guessing — the sub-collection goes to
    :func:`_parse_collection` instead.
    """
    if not description or _SENTENCE_RE.search(description):
        return ""
    parts = re.split(r"[·•|/]", description)
    if len(parts) >= 2:
        return parts[1].strip()
    return ""


def _node_type(match: dict[str, Any]) -> str:
    """ICIJ node type for a result — ``Entity``, ``Officer``,
    ``Intermediary`` or ``Address``.

    Spec v0.2 renamed the field from ``type`` to ``types``; both are read so
    the type survives a change back. Recorded as evidence because the four
    types are not equally meaningful as a screening hit — an ``Address``
    match says a name resembles a street address in the leaks, not that a
    related party appears in them.
    """
    raw = match.get("types") or match.get("type") or []
    if not isinstance(raw, list):
        return ""
    for item in raw:
        if isinstance(item, dict):
            name = (item.get("name") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            return name
    return ""


def _normalise(name: str) -> str:
    """Shared comparable form (Phase B) — see ``opencheck/names.py``. The
    verbatim duplicate of cross_check's normaliser (and its second copy of
    the fold table) is gone."""
    return names.normalise_name(name)


def _name_sim(a: str, b: str) -> float:
    """Similarity between a searched name and an ICIJ result name.

    Delegates to the Phase-D shared scorer, ``names.name_similarity`` — the
    same one behind RELATED_PEP / RELATED_SANCTIONED and BackgroundCheck.
    This used to be a bespoke unweighted token-overlap (Jaccard) score, which
    counted legal-form boilerplate as evidence and so could not tell a real
    match from a collision: "CHAUMET INTERNATIONAL SA." vs "BRONTE
    INTERNATIONAL SA" and "MOET HENNESSY INTERNATIONAL" vs "HENNESSY
    INTERNATIONAL LIMITED" both scored exactly 0.500, one false and one true.
    The shared scorer separates them (0.766 / 0.877) and, unlike a second
    private scorer, improves here whenever the shared one improves.

    Kept as a named seam rather than inlined: it is the single place where
    this module's matching semantics can be swapped or instrumented.
    """
    return names.name_similarity(a, b)


def _screenable(sanitised_query: str) -> bool:
    """Whether a sanitised name is specific enough to be worth screening.

    See ``_MIN_COMPARABLE_CHARS`` — this is the guard against a name that
    erodes to one or two characters and then matches the whole database.
    """
    if not sanitised_query:
        return False
    comparable = _normalise(sanitised_query).replace(" ", "")
    return len(comparable) >= _MIN_COMPARABLE_CHARS


def _slug(name: str) -> str:
    import hashlib
    return hashlib.sha256(name.lower().encode()).hexdigest()[:12]


def _dedupe(signals: list[RiskSignal]) -> list[RiskSignal]:
    """Collapse duplicate signals — same ICIJ node matched by the same
    subject statement produces at most one signal."""
    rank = {"high": 3, "medium": 2, "low": 1}
    keyed: dict[tuple, RiskSignal] = {}
    for sig in signals:
        sub = sig.evidence.get("subject_statement_id") or sig.evidence.get(
            "statement_id", ""
        )
        key = (sig.code, sig.source_id, sig.hit_id, sub)
        existing = keyed.get(key)
        if existing is None or rank.get(sig.confidence, 0) > rank.get(existing.confidence, 0):
            keyed[key] = sig
    return list(keyed.values())
