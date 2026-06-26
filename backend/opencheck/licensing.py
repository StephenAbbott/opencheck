"""Licensing compatibility matrix for OpenCheck exports.

Classifies each data source's licence into structured re-use terms (commercial
use, attribution, share-alike, redistribution) and assesses a *combined* export:
the most-restrictive terms win, so a single non-commercial source makes the whole
bundle non-commercial. Surfaced in the export manifest, ``LICENSES.md`` and the
``/license-matrix`` endpoint so users can see — at a glance — whether a bundle is
safe for their intended use.

This is an informational summary of well-known open-data licences, **not legal
advice**. Always verify against each source's actual licence before relying on it.
"""

from __future__ import annotations

import re
from typing import Iterable, Literal

from pydantic import BaseModel

# Match "nc"/"sa" only as standalone tokens (delimited by non-letters), so the
# UK spelling "licence" (which contains the substring "nc") is not mistaken for
# a NonCommercial licence.
_NC_TOKEN = re.compile(r"(?<![a-z])nc(?![a-z])")
_SA_TOKEN = re.compile(r"(?<![a-z])sa(?![a-z])")

DISCLAIMER = (
    "This licensing summary is informational only and is not legal advice. "
    "Verify each source's licence terms before commercial use or redistribution."
)

Commercial = Literal["yes", "no", "conditional"]


class LicenseTerms(BaseModel):
    """Structured re-use terms for a single licence."""

    license: str  # the licence identifier as declared by the adapter
    name: str
    url: str | None = None
    commercial_use: Commercial
    attribution_required: bool
    share_alike: bool
    redistribution: Commercial
    color: Literal["green", "amber", "red"]  # green = commercial-safe; amber = restricted/verify
    summary: str


class SourceLicensing(BaseModel):
    """A registered source mapped to its licence terms."""

    source_id: str
    name: str
    license: str
    terms: LicenseTerms


class LicenseAssessment(BaseModel):
    """The combined verdict for a set of contributing sources."""

    commercial_use: Commercial
    attribution_required: bool
    share_alike: bool
    color: Literal["green", "amber", "red"]
    headline: str
    warnings: list[str]
    per_source: list[SourceLicensing]
    disclaimer: str = DISCLAIMER


def _terms(
    license: str,
    name: str,
    *,
    commercial: Commercial,
    attribution: bool,
    share_alike: bool,
    redistribution: Commercial,
    summary: str,
    url: str | None = None,
) -> LicenseTerms:
    return LicenseTerms(
        license=license,
        name=name,
        url=url,
        commercial_use=commercial,
        attribution_required=attribution,
        share_alike=share_alike,
        redistribution=redistribution,
        color="green" if commercial == "yes" else "amber",
        summary=summary,
    )


# Canonical classifications, keyed by the exact licence id declared by adapters
# (matched case-insensitively). classify() falls back to pattern matching for
# descriptive or unfamiliar strings so new adapters are handled gracefully.
_CANONICAL: dict[str, LicenseTerms] = {
    "cc0-1.0": _terms(
        "CC0-1.0", "Creative Commons Zero v1.0 (public domain)",
        commercial="yes", attribution=False, share_alike=False, redistribution="yes",
        url="https://creativecommons.org/publicdomain/zero/1.0/",
        summary="Public-domain dedication; no restrictions, no attribution required.",
    ),
    "public domain": _terms(
        "Public Domain", "Public domain",
        commercial="yes", attribution=False, share_alike=False, redistribution="yes",
        summary="No copyright; free for any use including commercial.",
    ),
    "ogl-3.0": _terms(
        "OGL-3.0", "UK Open Government Licence v3.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        url="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        summary="Commercial use permitted with attribution.",
    ),
    "ogl-uk-3.0": _terms(
        "OGL-UK-3.0", "UK Open Government Licence v3.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        url="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        summary="Commercial use permitted with attribution.",
    ),
    "ogl-canada-2.0": _terms(
        "OGL-Canada-2.0", "Open Government Licence – Canada 2.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        summary="Commercial use permitted with attribution.",
    ),
    "singapore-ogl-1.0": _terms(
        "Singapore-OGL-1.0", "Singapore Open Data Licence v1.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        summary="Commercial use permitted with attribution.",
    ),
    "nlod-2.0": _terms(
        "NLOD-2.0", "Norwegian Licence for Open Government Data 2.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        summary="Commercial use permitted with attribution.",
    ),
    "cc-by-4.0": _terms(
        "CC-BY-4.0", "Creative Commons Attribution 4.0",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        url="https://creativecommons.org/licenses/by/4.0/",
        summary="Commercial use permitted with attribution.",
    ),
    "cc-by-3.0-au": _terms(
        "CC-BY-3.0-AU", "Creative Commons Attribution 3.0 Australia",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        summary="Commercial use permitted with attribution.",
    ),
    "odc-by": _terms(
        "ODC-By", "Open Data Commons Attribution Licence",
        commercial="yes", attribution=True, share_alike=False, redistribution="yes",
        summary="Commercial use permitted with attribution.",
    ),
    "cc-by-nc-4.0": _terms(
        "CC-BY-NC-4.0", "Creative Commons Attribution-NonCommercial 4.0",
        commercial="no", attribution=True, share_alike=False, redistribution="conditional",
        url="https://creativecommons.org/licenses/by-nc/4.0/",
        summary="NON-COMMERCIAL only; attribution required; no commercial re-use.",
    ),
    "cc-by-nc-sa-4.0": _terms(
        "CC-BY-NC-SA-4.0", "Creative Commons Attribution-NonCommercial-ShareAlike 4.0",
        commercial="no", attribution=True, share_alike=True, redistribution="conditional",
        url="https://creativecommons.org/licenses/by-nc-sa/4.0/",
        summary="NON-COMMERCIAL only; derivatives must be shared under the same licence.",
    ),
    "oc-terms": _terms(
        "OC-Terms", "OpenCorporates Terms & Conditions",
        commercial="conditional", attribution=True, share_alike=True, redistribution="conditional",
        url="https://opencorporates.com/legal/terms_and_conditions/",
        summary="Bespoke terms; share-alike and bulk-redistribution restrictions — verify before re-use.",
    ),
    "custom-kbo-reuse": _terms(
        "Custom-KBO-Reuse", "Belgian KBO/BCE re-use conditions",
        commercial="conditional", attribution=True, share_alike=False, redistribution="conditional",
        summary="Free re-use with notification; commercial use requires an agreement with KBO/BCE.",
    ),
    "per-collection": _terms(
        "per-collection", "Per-collection (varies)",
        commercial="conditional", attribution=True, share_alike=False, redistribution="conditional",
        summary="Licence varies per OpenAleph collection; check each collection's stated licence.",
    ),
}


def classify(license_id: str | None) -> LicenseTerms:
    """Return structured re-use terms for a licence id.

    Exact (case-insensitive) match against the canonical table first, then a
    conservative pattern fallback so descriptive or new licence strings still
    get a sensible classification. Unknown licences are treated as
    ``conditional`` (amber) so users are prompted to verify.
    """
    raw = (license_id or "").strip()
    key = raw.lower()
    if key in _CANONICAL:
        return _CANONICAL[key]

    name = raw or "Unknown"
    # Non-commercial wins regardless of other markers.
    if "non-commercial" in key or "noncommercial" in key or _NC_TOKEN.search(key):
        share = "share" in key or bool(_SA_TOKEN.search(key))
        return _terms(
            raw, f"{name} (non-commercial)", commercial="no", attribution=True,
            share_alike=share, redistribution="conditional",
            summary="Non-commercial licence; no commercial re-use permitted.",
        )
    if "cc0" in key or "public domain" in key or "publicdomain" in key:
        return _terms(
            raw, name, commercial="yes", attribution=False, share_alike=False,
            redistribution="yes", summary="Public-domain style; free for any use.",
        )
    # Permissive open-government / open-data attribution licences.
    _PERMISSIVE_MARKERS = (
        "ogl", "psi", "nlod", "odc-by", "cc-by", "open licence", "open license",
        "licence ouverte", "open government", "open data", "opendata", "ogd",
    )
    if any(m in key for m in _PERMISSIVE_MARKERS):
        return _terms(
            raw, name, commercial="yes", attribution=True, share_alike=False,
            redistribution="yes", summary="Open licence; commercial use permitted with attribution.",
        )
    # Bespoke / unrecognised — flag for verification.
    return _terms(
        raw, name, commercial="conditional", attribution=True, share_alike=False,
        redistribution="conditional",
        summary="Bespoke or unrecognised licence — verify terms before re-use.",
    )


def _registry():
    # Imported lazily to avoid a circular import (sources -> licensing not needed).
    from .sources import REGISTRY

    return REGISTRY


def source_licensing(source_id: str) -> SourceLicensing | None:
    """Licence terms for one registered source, or None if unknown."""
    adapter = _registry().get(source_id)
    if adapter is None:
        return None
    info = adapter.info
    return SourceLicensing(
        source_id=info.id, name=info.name, license=info.license, terms=classify(info.license)
    )


def assess(source_ids: Iterable[str]) -> LicenseAssessment:
    """Assess the combined licensing of the given contributing sources.

    The most-restrictive terms win: any non-commercial source makes the whole
    bundle non-commercial; any conditional source makes it conditional.
    """
    per_source: list[SourceLicensing] = []
    for sid in sorted(set(source_ids)):
        sl = source_licensing(sid)
        if sl is not None:
            per_source.append(sl)

    commercial: Commercial = "yes"
    if any(s.terms.commercial_use == "no" for s in per_source):
        commercial = "no"
    elif any(s.terms.commercial_use == "conditional" for s in per_source):
        commercial = "conditional"

    attribution_required = any(s.terms.attribution_required for s in per_source)
    share_alike = any(s.terms.share_alike for s in per_source)

    if commercial == "no":
        color: Literal["green", "amber", "red"] = "red"
    elif commercial == "conditional" or share_alike:
        color = "amber"
    else:
        color = "green"

    warnings: list[str] = []
    nc = [s for s in per_source if s.terms.commercial_use == "no"]
    if nc:
        names = ", ".join(f"{s.name} ({s.license})" for s in nc)
        warnings.append(
            "Not for commercial use — this bundle includes non-commercial source(s): "
            f"{names}. Remove their statements before any commercial use."
        )
    sa = [s for s in per_source if s.terms.share_alike]
    if sa:
        names = ", ".join(f"{s.name} ({s.license})" for s in sa)
        warnings.append(
            "Share-alike obligations apply — derivative works of "
            f"{names} must be released under the same licence."
        )
    cond = [
        s for s in per_source
        if s.terms.commercial_use == "conditional" and s not in nc and s not in sa
    ]
    if cond:
        names = ", ".join(f"{s.name} ({s.license})" for s in cond)
        warnings.append(f"Verify terms — bespoke or per-collection licence(s): {names}.")
    if attribution_required and color == "green":
        warnings.append("Attribution is required for one or more sources (see LICENSES.md).")

    headline = {
        "yes": "Safe for commercial use (attribution may be required).",
        "conditional": "Commercial use may be restricted — verify the flagged sources.",
        "no": "NOT for commercial use — a non-commercial source is included.",
    }[commercial]

    return LicenseAssessment(
        commercial_use=commercial,
        attribution_required=attribution_required,
        share_alike=share_alike,
        color=color,
        headline=headline,
        warnings=warnings,
        per_source=per_source,
    )


def _restrictiveness(t: LicenseTerms) -> int:
    """Rank a licence's re-use restrictiveness (higher = more restrictive).
    Non-commercial dominates; share-alike and attribution add weight."""
    base = {"no": 3, "conditional": 2, "yes": 1}[t.commercial_use]
    return base * 10 + (5 if t.share_alike else 0) + (2 if t.attribution_required else 0)


def most_restrictive(source_ids: Iterable[str]) -> SourceLicensing | None:
    """The most-restrictive contributing source's licence, or None if none of the
    ids resolve to a registered source. Ties broken deterministically by licence
    id. Used to stamp a per-record ``DATA_LICENSE`` on exports (e.g. Senzing JSON)
    so a record that combines a permissive and a non-commercial source carries the
    non-commercial licence."""
    best: SourceLicensing | None = None
    best_score = -1
    for sid in sorted(set(source_ids)):
        sl = source_licensing(sid)
        if sl is None:
            continue
        score = _restrictiveness(sl.terms)
        if score > best_score or (
            score == best_score and best is not None and sl.terms.license < best.terms.license
        ):
            best, best_score = sl, score
    return best


def attribution_for(source_ids: Iterable[str]) -> str:
    """Combined attribution text for the contributing registered sources (distinct,
    order-stable). Empty string when none resolve."""
    reg = _registry()
    out: list[str] = []
    for sid in sorted(set(source_ids)):
        adapter = reg.get(sid)
        attr = getattr(getattr(adapter, "info", None), "attribution", None)
        if attr and attr not in out:
            out.append(attr)
    return " ".join(out)


def full_matrix() -> dict:
    """The complete matrix: every registered source's licence terms + the
    distinct licence catalogue. Backs the /license-matrix endpoint."""
    sources = [source_licensing(sid) for sid in sorted(_registry())]
    sources = [s for s in sources if s is not None]
    distinct: dict[str, LicenseTerms] = {}
    for s in sources:
        distinct.setdefault(s.terms.license, s.terms)
    return {
        "disclaimer": DISCLAIMER,
        "sources": [s.model_dump() for s in sources],
        "licenses": [t.model_dump() for t in distinct.values()],
    }
