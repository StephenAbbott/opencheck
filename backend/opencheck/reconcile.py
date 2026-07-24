"""Cross-source reconciliation.

Given the hits returned by a single fan-out search, identify which of
them describe the same real-world entity or person. The output drives
the "cross-source links" panel in the report — one of the named GODIN
hooks: every bridge is shown alongside the identifier that made it.

Bridge keys (in order of confidence):

1. ``wikidata_qid`` — primary, applies to both persons and entities
2. ``lei`` — entities only (corporate identity)
3. ``gb_coh`` — UK Companies House number
4. ``opensanctions_id`` — same canonical FtM id across OpenSanctions
   and EveryPolitician (both pull from the same database)

Weak bridges (``possibly-same-as``) — surfaced separately, not merged:

* ``name`` (case-folded, punctuation-stripped) + matching DOB or LEI
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

from . import identifiers, names
from .matching import canonical_identifier, is_matchable_name
from .sources import SearchKind, SourceHit


@dataclass
class CrossSourceLink:
    """A confirmed bridge: ``hits`` all describe the same subject.

    ``key`` and ``key_value`` say *why* — e.g. ``("lei",
    "213800LBDB8WB3QGVN21")``. ``confidence`` is one of:

    * ``"strong"`` — shared structured identifier (Q-ID / LEI / CH /
      OpenSanctions id).
    * ``"possible"`` — name + birth-year match between persons.
    """

    key: str
    key_value: str
    confidence: str
    hits: list[SourceHit] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "key_value": self.key_value,
            "confidence": self.confidence,
            "hits": [
                {
                    "source_id": hit.source_id,
                    "hit_id": hit.hit_id,
                    "name": hit.name,
                }
                for hit in self.hits
            ],
        }


_STRONG_KEYS: tuple[str, ...] = (
    "wikidata_qid",
    "lei",
    "gb_coh",
    "opensanctions_id",
)

# Keys where a single-source hit is still worth surfacing in the panel.
# ``wikidata_qid`` is the canonical cross-source bridge and is worth showing
# even when only Wikidata carries it — GLEIF does not yet publish an official
# Wikidata mapping, so the QID is always Wikidata-sourced.
_SINGLE_SOURCE_OK: frozenset[str] = frozenset({"wikidata_qid"})


def reconcile(hits: Iterable[SourceHit]) -> list[CrossSourceLink]:
    """Group hits by shared identifier and return confirmed bridges.

    For most keys, at least two independent sources must share the identifier
    before a link is emitted — a single-source match is not a *bridge*.

    Exception: keys in ``_SINGLE_SOURCE_OK`` (currently ``wikidata_qid``) are
    surfaced even with only one source because the identifier itself is the
    canonical provenance marker — no corroboration is required.
    """
    hits = list(hits)
    seen: set[tuple[str, str]] = set()
    links: list[CrossSourceLink] = []

    for key in _STRONG_KEYS:
        groups: dict[str, list[SourceHit]] = {}
        for hit in hits:
            value = hit.identifiers.get(key)
            if not value or value.upper() in {"Q0", "STUB000000000000LEI0", "00000000"}:
                continue
            # Malformed identifiers must not become strong bridges: QIDs are
            # shape-checked (``Q`` + digits, no ``Q0``/leading zeros — see
            # identifiers.is_valid_qid), LEIs additionally checksum-checked
            # when enforcement is on.
            if key == "wikidata_qid" and not identifiers.is_valid_qid(value):
                continue
            if (
                key == "lei"
                and identifiers.checksums_enforced()
                and not identifiers.is_valid_lei(value, checksum=True)
            ):
                continue
            groups.setdefault(value, []).append(hit)

        min_group = 1 if key in _SINGLE_SOURCE_OK else 2
        for value, group in groups.items():
            if len(group) < min_group:
                continue
            if (key, value) in seen:
                continue
            seen.add((key, value))
            links.append(
                CrossSourceLink(
                    key=key,
                    key_value=value,
                    confidence="strong",
                    hits=group,
                )
            )

    # Weak fallback: same normalised name across persons.
    person_groups: dict[str, list[SourceHit]] = {}
    for hit in hits:
        if hit.kind != SearchKind.PERSON:
            continue
        # Skip persons already covered by a strong bridge above.
        if any(hit in link.hits for link in links):
            continue
        norm = _normalise_name(hit.name)
        if norm:
            person_groups.setdefault(norm, []).append(hit)

    for norm, group in person_groups.items():
        if len(group) < 2:
            continue
        # Single-token names ("Ivanov") are too generic to bridge two people
        # on name alone (ftmg drops single-token names from matching).
        if not is_matchable_name(norm):
            continue
        # Don't pretend two stubs match.
        if any(hit.is_stub for hit in group):
            continue
        links.append(
            CrossSourceLink(
                key="name",
                key_value=norm,
                confidence="possible",
                hits=group,
            )
        )

    return links


def _normalise_name(name: str) -> str:
    """Shared comparable form (Phase B) — see ``opencheck/names.py``. This
    module's local normaliser had NO fold table, so ``Ørsted`` bridged in
    cross_check but not here; delegating closes that gap."""
    return names.normalise_name(name)



# ---------------------------------------------------------------------
# POSSIBLY_SAME_AS — name-only "likely same" entity candidates.
#
# Operates on the assembled BODS bundle (entity statements carry name +
# jurisdiction + foundingDate, which SourceHits do not). Surfaces the residual
# the identifier bridges above can't: distinct entity statements that share an
# exact normalised name + jurisdiction but no shared identifier. The Splink
# spike (Notion) showed this rule beats fuzzy matching and a trained
# probabilistic model on OpenCheck's data (F1 0.95). These are **suggestions
# for a human** (rendered as a dashed "likely same" edge), never a silent merge.
# A founding-date tiebreaker rejects the same-name/different-entity case (e.g.
# distinct same-named subsidiaries incorporated in different years); address is
# deliberately not used — cross-source formatting is too noisy to require.
#
# Mirror of frontend ``possiblySameAs`` in ``frontend/src/lib/reconcile.ts`` —
# keep the two in sync.
# ---------------------------------------------------------------------


@dataclass
class PossiblySame:
    """A name-only 'likely same' candidate between two entity statements.

    ``a_name`` / ``b_name`` / ``jurisdiction`` are carried so the QuickCheck
    report can render the pair without re-assembling the BODS bundle (it only
    holds SourceHits, not the entity statements these statementIds point at)."""

    a: str  # statementId
    b: str  # statementId
    reason: str
    a_name: str = ""
    b_name: str = ""
    jurisdiction: str = ""
    # Which source asserted each record — the key context for a human
    # reviewing a name-only match ("GLEIF vs OpenCorporates" reads very
    # differently from "OpenAleph vs OpenAleph").
    a_source: str = ""
    b_source: str = ""

    def to_dict(self) -> dict:
        return {
            "a": self.a,
            "b": self.b,
            "reason": self.reason,
            "a_name": self.a_name,
            "b_name": self.b_name,
            "jurisdiction": self.jurisdiction,
            "a_source": self.a_source,
            "b_source": self.b_source,
        }


_LEI_RE = identifiers.LEI_STRICT_SHAPE


def _entity_jurisdiction(rd: dict) -> str:
    # GLEIF entity statements use `incorporatedInJurisdiction`; OpenSanctions
    # (and some others) use `jurisdiction`. Read both.
    jur = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
    code = jur.get("code") if isinstance(jur, dict) else ""
    return str(code or "").strip().upper().split("-")[0]


def _identifier_keys(stmt: dict) -> set[str]:
    """Identifier-merge keys for an entity (mirror of frontend ``identKeys``):
    LEI (scheme-agnostic) + scheme-scoped values + a jurisdiction-scoped key for
    bare national registration numbers (VAT excluded — see frontend note). Two
    statements that share any key are already linked by identifier, so they are
    NOT name-only candidates."""
    rd = stmt.get("recordDetails") or {}
    jur = _entity_jurisdiction(rd)
    keys: set[str] = set()
    for i in rd.get("identifiers") or []:
        raw = str(i.get("id") or "").strip().upper()
        if not raw:
            continue
        if identifiers.classify_lei(raw):
            keys.add(f"LEI:{raw}")
            continue
        # Canonicalise the value the way ftmg does (StrictFormat: strip
        # separators/punctuation) so the SAME registration number written
        # differently across sources ("556056-6258" vs "5560566258") produces
        # one merge key — two records that share it are correctly treated as
        # identifier-linked rather than surfaced as a name-only "possibly same"
        # pair. Falls back to the raw value when StrictFormat yields nothing.
        val = canonical_identifier(raw, min_len=0) or raw
        scheme = str(i.get("scheme") or "?").strip().upper()
        keys.add(f"{scheme}:{val}")
        if jur and (scheme == "" or scheme.startswith(f"{jur}-")) and "VAT" not in scheme and "/" not in val:
            keys.add(f"JUR:{jur}:{val}")
    return keys


def _founding_year(stmt: dict) -> str | None:
    rd = stmt.get("recordDetails") or {}
    m = re.match(r"(\d{4})", str(rd.get("foundingDate") or ""))
    return m.group(1) if m else None


def _founding_compatible(a: dict, b: dict) -> bool:
    """Compatible unless BOTH founding years are present and differ."""
    ya, yb = _founding_year(a), _founding_year(b)
    return not (ya and yb and ya != yb)


def possibly_same_entities(bods: list[dict]) -> list[PossiblySame]:
    """Candidate 'likely same' entity pairs: exact normalised name + jurisdiction,
    passing the founding-date tiebreaker. Run on the assembled BODS bundle."""
    ents = [s for s in (bods or []) if s.get("recordType") == "entity" and s.get("statementId")]
    id_keys = {s["statementId"]: _identifier_keys(s) for s in ents}
    groups: dict[tuple[str, str], list[dict]] = {}
    for s in ents:
        rd = s.get("recordDetails") or {}
        nm = _normalise_name(rd.get("name") or "")
        jur = _entity_jurisdiction(rd)
        if not nm or not jur:  # both required — name alone over-merges
            continue
        groups.setdefault((nm, jur), []).append(s)

    out: list[PossiblySame] = []
    seen: set[tuple[str, str]] = set()
    for group in groups.values():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if a["statementId"] == b["statementId"]:
                    continue
                # Skip pairs already linked by a shared identifier — they are
                # known-same (the bundle just isn't identifier-merged here),
                # not the name-only residual this surfaces.
                if id_keys[a["statementId"]] & id_keys[b["statementId"]]:
                    continue
                if not _founding_compatible(a, b):
                    continue
                pair = tuple(sorted((a["statementId"], b["statementId"])))
                if pair in seen:
                    continue
                seen.add(pair)
                by_id = {a["statementId"]: a, b["statementId"]: b}
                rd_a = by_id[pair[0]].get("recordDetails") or {}
                rd_b = by_id[pair[1]].get("recordDetails") or {}

                def _src(stmt: dict) -> str:
                    return str(((stmt.get("source") or {}).get("description")) or "")

                out.append(
                    PossiblySame(
                        pair[0],
                        pair[1],
                        "same name + jurisdiction",
                        a_name=str(rd_a.get("name") or ""),
                        b_name=str(rd_b.get("name") or ""),
                        jurisdiction=_entity_jurisdiction(rd_a),
                        a_source=_src(by_id[pair[0]]),
                        b_source=_src(by_id[pair[1]]),
                    )
                )
    return out
