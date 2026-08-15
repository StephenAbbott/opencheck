"""Nominee arrangements — which register codes state one, and what that means.

Companies House publishes ``natures_of_control`` on every PSC record. Six of
those codes, all from the Register of Overseas Entities, say the overseas
entity holds UK land or property **as a nominee** for someone else. That is a
nominee arrangement stated by the register, not inferred from prose.

Why this lives here and not in ``psc_natures.py``
-------------------------------------------------
``psc_natures.py`` is AUTO-GENERATED from Companies House's published
``psc_descriptions.yml`` and says "Do NOT edit by hand" in its docstring. Hand
-adding a derived constant there works locally and then fails the vendored-enum
drift check, because the generator rewrites the whole file from upstream. So
anything *derived from* the enumeration belongs in a hand-maintained module
that imports it — this one.

The set is still derived at import time rather than written out as a literal.
If Companies House adds a seventh nominee code, it is picked up automatically;
a hand-copied list would silently miss it, which is the exact failure mode the
structured nominee signal exists to remove.

Not modelled here (yet)
-----------------------
BODS wants nominee arrangements represented as an intermediary ``arrangement``
entity (``entityType.subtype: nomination``) linked by ``nominator`` / ``nominee``
relationships, rather than as a bare interest on a direct PSC relationship. The
mapper deliberately does not do that — see ``mapper.py`` — because the ROE codes
describe a nominee holding *land*, and BODS has no asset entity type to attach
the arrangement to. That question is on its own ticket.
"""

from __future__ import annotations

from .psc_natures import PSC_NATURE_DESCRIPTIONS

#: Companies House / ROE nature-of-control codes that state a nominee
#: arrangement. Derived from the vendored enumeration, never hand-written.
NOMINEE_NATURE_CODES: frozenset[str] = frozenset(
    code for code in PSC_NATURE_DESCRIPTIONS if "registered-owner-as-nominee" in code
)


def is_nominee_nature(code: str) -> bool:
    """True when a PSC/ROE nature-of-control code states a nominee arrangement."""
    return (code or "").strip().lower() in NOMINEE_NATURE_CODES
