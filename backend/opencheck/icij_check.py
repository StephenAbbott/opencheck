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

Scores are on a 0–100 scale.  ``match: true`` means ICIJ judges it a
high-confidence match.

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
    pick_degradation_reason,
)

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

# ICIJ score threshold (0–100). Matches below this are ignored unless
# ``match: true`` — ICIJ's own high-confidence flag overrides the threshold.
_MIN_SCORE = 70

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
) -> list[RiskSignal]:
    """Return ``OFFSHORE_LEAKS`` risk signals for entities and persons in
    the BODS bundle whose names match a record in the ICIJ Offshore Leaks
    database.

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

    targets = _collect_targets(bods)[:max_targets]
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


def _collect_targets(bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract ``{kind, statement_id, name}`` records from a BODS bundle.

    Mirrors ``cross_check._collect_targets`` but shared here to keep the
    ICIJ module self-contained.  Skips placeholder types
    (``unknownPerson`` / ``anonymousEntity``) and records with empty names.
    """
    out: list[dict[str, Any]] = []
    for stmt in bods:
        record_type = stmt.get("recordType") or ""
        rd = stmt.get("recordDetails") or {}
        sid = stmt.get("statementId") or ""
        if not sid:
            continue
        if record_type == "person":
            person_type = rd.get("personType") or ""
            if person_type and person_type != "knownPerson":
                continue
            name = _person_name(rd)
            if not name:
                continue
            out.append({"kind": _KIND_PERSON, "statement_id": sid, "name": name})
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
            out.append({"kind": _KIND_ENTITY, "statement_id": sid, "name": name})
    return out


def _person_name(rd: dict[str, Any]) -> str:
    names = rd.get("names") or []
    if not isinstance(names, list):
        return ""
    individual = next(
        (n for n in names if isinstance(n, dict) and n.get("type") == "individual"),
        None,
    )
    pick = individual or next(
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

    signals: list[RiskSignal] = []
    for query_key, target in keyed_targets.items():
        query_result = raw.get(query_key) or {}
        results = query_result.get("result") or []
        for match in results:
            sig = _signal_from_match(
                match, target, min_score=min_score, min_name_sim=min_name_sim
            )
            if sig is not None:
                signals.append(sig)
    return signals


# ---------------------------------------------------------------------
# Signal construction
# ---------------------------------------------------------------------


def _signal_from_match(
    match: dict[str, Any],
    target: dict[str, Any],
    *,
    min_score: int,
    min_name_sim: float = _MIN_NAME_SIM,
) -> RiskSignal | None:
    """Convert one ICIJ reconciliation result to an OFFSHORE_LEAKS signal.

    Returns ``None`` when:
    * The score is below threshold AND ``match`` is not ``True``.
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
    score: int = int(match.get("score") or 0)
    is_high_confidence: bool = bool(match.get("match"))

    if not is_high_confidence and score < min_score:
        return None

    matched_name: str = (match.get("name") or "").strip()
    if not matched_name:
        return None

    # Secondary name-similarity sanity check.
    if _name_sim(target["name"], matched_name) < min_name_sim:
        return None

    # Distinctive-token gate. Applied even to ICIJ ``match: true`` results
    # — ICIJ's own scorer rated the ENERGEN/BIOGAS collision 90/100, so
    # its confidence flag earns no bypass. Entity targets always; person
    # targets only when either name carries a legal form, because BODS
    # person statements sometimes hold corporate officers and those are
    # organisations for matching purposes. Real personal names ("NICHOLAS
    # PAUL RATCLIFFE") are all distinctive tokens and rely on the
    # similarity threshold alone.
    entityish = (
        target["kind"] != _KIND_PERSON
        or names.has_org_form_tokens(target["name"])
        or names.has_org_form_tokens(matched_name)
    )
    if entityish and not names.distinctive_token_agreement(
        target["name"], matched_name
    ):
        return None

    node_url: str = _node_url(match.get("id"))
    description: str = match.get("description") or ""
    dataset = _parse_dataset(description)
    jurisdiction = _parse_jurisdiction(description)
    collection = _parse_collection(description)
    node_type = _node_type(match)

    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
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
    if node_type == "Intermediary":
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
        confidence="high" if is_high_confidence else "medium",
        summary=summary,
        source_id="icij",
        hit_id=node_url or f"icij:{_slug(target['name'])}",
        evidence={
            "subject_statement_id": target["statement_id"],
            "search_name": target["name"],
            "matched_name": matched_name,
            "icij_score": score,
            "icij_match": is_high_confidence,
            "dataset": dataset,
            "jurisdiction": jurisdiction,
            "collection": collection,
            "node_type": node_type,
            "node_url": node_url,
            "kind": target["kind"],
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
        sub = sig.evidence.get("subject_statement_id", "")
        key = (sig.code, sig.source_id, sig.hit_id, sub)
        existing = keyed.get(key)
        if existing is None or rank.get(sig.confidence, 0) > rank.get(existing.confidence, 0):
            keyed[key] = sig
    return list(keyed.values())
