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
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable

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


def reconcile(hits: Iterable[SourceHit]) -> list[CrossSourceLink]:
    """Group hits by shared identifier and return one link per group ≥2.

    Single-source matches (one hit carrying a Q-ID with no other source
    matching that Q-ID) are deliberately not returned — the panel shows
    *bridges*, not individual identifiers.
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
            groups.setdefault(value, []).append(hit)

        for value, group in groups.items():
            if len(group) < 2:
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
    """Lower, strip diacritics, collapse whitespace, drop punctuation."""
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_only.lower())
    return re.sub(r"\s+", " ", cleaned).strip()
