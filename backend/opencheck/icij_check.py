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
      "q0": {"query": "ENTITY NAME", "limit": 3},
      "q1": {"query": "PERSON NAME", "limit": 3},
      ...
    }

Response::

    {
      "q0": {
        "result": [
          {
            "id": "12345",
            "name": "ENTITY NAME",
            "score": 90,
            "match": true,
            "type": [{"id": "/type/entity", "name": "Entity"}],
            "description": "Panama Papers · British Virgin Islands"
          }
        ]
      }
    }

Scores are on a 0–100 scale.  ``match: true`` means ICIJ judges it a
high-confidence match.

Reference: https://offshoreleaks.icij.org/docs/reconciliation
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any

import httpx

from .config import get_settings
from .http import build_client
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

# Maximum queries per API batch (stay conservative to avoid request-size issues).
_BATCH_SIZE = 10

# ICIJ score threshold (0–100). Matches below this are ignored unless
# ``match: true`` — ICIJ's own high-confidence flag overrides the threshold.
_MIN_SCORE = 70

# Secondary sanity check: even if ICIJ scores high, the returned name must
# share at least this much similarity with what we searched, to guard against
# false positives when the ICIJ index blends multiple transliterations.
_MIN_NAME_SIM = 0.45

# Human-friendly labels for ICIJ dataset descriptions.  The ``description``
# field in reconciliation results typically looks like
# "Panama Papers · British Virgin Islands" — we extract the dataset part.
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
            batch_signals = await _check_batch(batch, min_score=min_score)
            signals.extend(batch_signals)
        except httpx.HTTPStatusError as exc:
            # The endpoint answered but rejected us. A 404 here means the
            # reconciliation service moved again (it did once already — see
            # the module docstring); 429 means we're being throttled. Loud
            # enough to notice without reading raw access logs.
            failed += 1
            skipped_names += len(batch)
            reason = classify_degradation_reason(exc)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            _LOG.warning(
                "ICIJ Offshore Leaks reconciliation failed: HTTP %s from %s "
                "(%d name(s) in this batch skipped).",
                exc.response.status_code,
                _RECONCILE_URL,
                len(batch),
            )
        except Exception as exc:  # noqa: BLE001
            # Network error, timeout, or unexpected response shape. Still
            # swallowed so one upstream problem can't sink the rest of the
            # risk pipeline — but no longer silent.
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


async def _check_batch(
    targets: list[dict[str, Any]],
    *,
    min_score: int,
) -> list[RiskSignal]:
    """POST one batch of names to the ICIJ reconciliation API and parse
    the results into risk signals."""
    queries: dict[str, Any] = {
        f"q{i}": {"query": t["name"], "limit": 3}
        for i, t in enumerate(targets)
    }

    async with build_client() as client:
        response = await client.post(
            _RECONCILE_URL,
            data={"queries": json.dumps(queries)},
        )
        response.raise_for_status()
        raw = response.json()

    signals: list[RiskSignal] = []
    for i, target in enumerate(targets):
        query_key = f"q{i}"
        query_result = raw.get(query_key) or {}
        results = query_result.get("result") or []
        for match in results:
            sig = _signal_from_match(match, target, min_score=min_score)
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
) -> RiskSignal | None:
    """Convert one ICIJ reconciliation result to an OFFSHORE_LEAKS signal.

    Returns ``None`` when:
    * The score is below threshold AND ``match`` is not ``True``.
    * The returned name is too dissimilar to the searched name
      (secondary sanity check, guards against ICIJ index collisions).
    """
    score: int = int(match.get("score") or 0)
    is_high_confidence: bool = bool(match.get("match"))

    if not is_high_confidence and score < min_score:
        return None

    matched_name: str = (match.get("name") or "").strip()
    if not matched_name:
        return None

    # Secondary name-similarity sanity check.
    if _name_sim(target["name"], matched_name) < _MIN_NAME_SIM:
        return None

    node_url: str = _node_url(match.get("id"))
    description: str = match.get("description") or ""
    dataset = _parse_dataset(description)
    jurisdiction = _parse_jurisdiction(description)

    relation = "Related party" if target["kind"] == _KIND_PERSON else "Related entity"
    dataset_label = f"the {dataset}" if dataset else "the ICIJ Offshore Leaks database"
    jur_note = f" ({jurisdiction})" if jurisdiction else ""

    summary = (
        f"{relation} '{target['name']}' matches a record in {dataset_label}{jur_note} "
        f"(ICIJ score {score}/100)."
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

    ICIJ descriptions look like ``"Panama Papers · British Virgin Islands"``.
    We return a normalised label like ``"Panama Papers"``, or ``""`` if
    unrecognised.
    """
    if not description:
        return ""
    parts = re.split(r"[·•|/]", description)
    first = parts[0].strip().lower()
    return _DATASET_LABELS.get(first, parts[0].strip())


def _parse_jurisdiction(description: str) -> str:
    """Extract the jurisdiction part from an ICIJ description string.

    ``"Panama Papers · British Virgin Islands"`` → ``"British Virgin Islands"``
    """
    if not description:
        return ""
    parts = re.split(r"[·•|/]", description)
    if len(parts) >= 2:
        return parts[1].strip()
    return ""


_NON_DECOMPOSABLE_FOLDS = {
    "ł": "l", "Ł": "L",
    "ø": "o", "Ø": "O",
    "æ": "ae", "Æ": "Ae",
    "œ": "oe", "Œ": "Oe",
    "ð": "d", "Ð": "D",
    "þ": "th", "Þ": "Th",
    "ß": "ss",
}


def _normalise(name: str) -> str:
    if not name:
        return ""
    folded = "".join(_NON_DECOMPOSABLE_FOLDS.get(c, c) for c in name)
    decomposed = unicodedata.normalize("NFKD", folded)
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_only.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _name_sim(a: str, b: str) -> float:
    """Simple token-overlap similarity — more lenient than SequenceMatcher
    for all-caps ICIJ names vs mixed-case BODS names."""
    na, nb = _normalise(a), _normalise(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    tokens_a = set(na.split())
    tokens_b = set(nb.split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


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
