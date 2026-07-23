"""Cross-source matching guards, adopted from OpenSanctions' ``ftmg``.

OpenSanctions' ``followthemoney-graph`` normalises property values before
reifying them into shared graph nodes (``transform.reified_node_value``): it
drops identifiers shorter than 7 characters, canonicalises identifiers and
URLs, and refuses to reify single-token names. Those are battle-tested guards
against a coincidental short code or a bare surname manufacturing a false link
across records.

OpenCheck runs the same risk in its own corroboration/matching:

* ``reconcile`` bridges hits by shared identifier and by person name;
* ``cross_check`` raises RELATED_PEP / RELATED_SANCTIONED from fuzzy name
  matches against the ownership graph's neighbours;
* the OpenAleph ``/match`` step corroborates a hit against the subject by
  shared leiCode / registrationNumber / opencorporatesUrl.

This module centralises the guards so they are applied consistently.

It prefers ``rigour`` (the normalisation library inside followthemoney's
dependency tree — so OpenCheck canonicalises identifiers/URLs exactly as
OpenSanctions does) but ``rigour`` pulls in ``pyicu``, which needs the ICU
toolchain to build. Because this module is imported by *core* modules
(``reconcile``, ``cross_check``) that must load in the base install too, the
``rigour`` import is optional with a pure-Python fallback — the same
belt-and-braces pattern ``opencheck/ftm.py`` uses for ``bods-ftm``. Production
and CI install the ``ftm`` extra, so they run the ``rigour`` path; the fallback
keeps ICU-less dev environments working with equivalent results for the
identifier codes and URLs OpenCheck actually compares.
"""

from __future__ import annotations

import re

try:  # pragma: no cover - exercised via the ftm extra in CI/prod
    from rigour.ids import StrictFormat
    from rigour.urls import clean_url_compare

    _HAS_RIGOUR = True
except ImportError:  # pragma: no cover - base install without the ftm extra
    StrictFormat = None  # type: ignore[assignment]
    clean_url_compare = None  # type: ignore[assignment]
    _HAS_RIGOUR = False

# ftmg (`reified_node_value`): identifier values shorter than this collide too
# easily across registries to be a reliable cross-source corroboration key.
MIN_IDENTIFIER_LEN = 7

_NON_ALNUM = re.compile(r"[^A-Z0-9]")
_URL_SCHEME = re.compile(r"^https?://", re.IGNORECASE)


def _normalise_identifier_value(text: str) -> str:
    """StrictFormat when available, else strip to upper-case alphanumerics.

    The fallback matches StrictFormat for the alphanumeric registry codes /
    LEIs OpenCheck compares (``"GB-12 345 678"`` → ``"GB12345678"``).
    """
    if _HAS_RIGOUR:
        return StrictFormat.normalize(text) or ""
    return _NON_ALNUM.sub("", text.upper())


def _normalise_url_value(text: str) -> str:
    """clean_url_compare when available, else lower-case and drop the scheme."""
    if _HAS_RIGOUR:
        return clean_url_compare(text) or ""
    return _URL_SCHEME.sub("", text.strip().lower())


def canonical_identifier(
    value: object, *, min_len: int = MIN_IDENTIFIER_LEN
) -> str | None:
    """Canonicalise an identifier for cross-source comparison.

    Upper-cases and strips separators/punctuation (so ``"GB-12 345 678"`` and
    ``"gb12345678"`` compare equal). Returns ``None`` when the value is empty
    or normalises to fewer than ``min_len`` characters — too short to
    corroborate on.

    Pass ``min_len=0`` for canonical, globally-unique identifiers that are
    legitimately short (e.g. a Wikidata QID like ``Q9545``), where the length
    guard would be wrong.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    norm = _normalise_identifier_value(text)
    if len(norm) < min_len:
        return None
    return norm


def canonical_url(value: object) -> str | None:
    """Canonicalise a URL for comparison (scheme/host case, trailing slash).
    Returns ``None`` when the value is empty or cannot be cleaned.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = _normalise_url_value(text)
    if not cleaned:
        return None
    # Treat ".../12345678" and ".../12345678/" as equal (rigour's
    # clean_url_compare keeps a path's trailing slash).
    return cleaned.rstrip("/")


def is_matchable_name(normalised_name: str | None) -> bool:
    """True when an already-normalised name is specific enough to match on.

    ftmg drops single-token names (no internal space) from reification: a bare
    surname or forename (``"Fernández"``, ``"Ivanov"``) is too generic to base
    a cross-source match on. Callers pass their own normalised form (e.g.
    ``cross_check._normalise`` or ``reconcile._normalise_name``) so this stays
    agnostic to which normaliser produced it.
    """
    return bool(normalised_name) and " " in normalised_name.strip()
