#!/usr/bin/env python3
"""Drift check: the FtM "names" group OpenCheck reads vs the followthemoney model.

``opencheck.names.FTM_NAME_PROPS`` lists the FollowTheMoney properties that
may carry *a name of this party*. It is a gate, not a display list:
``sources/openaleph._bears_name`` drops any hit that does not bear the
searched name in one of them, and ``openaleph_check._best_name_score`` scores
the similarity against them. A name property that exists upstream and appears
in neither ``FTM_NAME_PROPS`` nor ``FTM_NAME_PROPS_EXCLUDED`` is therefore
read by nobody, and the symptom is a *missing* hit — which looks exactly like
a company that is not listed. It fails closed and silently, the same way the
nine wrong RA codes did.

That is not hypothetical. ``abbreviation`` shipped in the FtM model to hold
acronyms apart from full names (OpenSanctions changelog #39), and OpenCheck
read neither it nor the ``weakAlias`` its values were being copied into, so an
acronym-only match could not clear the gate. This check exists so the next one
fails the build instead.

It compares against the *installed* ``followthemoney`` model (the ``ftm``
extra; needs the ICU toolchain — g++, libicu-dev, pkg-config), over the two
schemata OpenCheck actually searches: ``LegalEntity`` and ``Person``, each
including everything they inherit from ``Thing``. Check-only: on drift it
prints what to do and exits 1.

To fix a failure, decide for each named property whether OpenCheck should
*read* it (add it to ``FTM_NAME_PROPS``) or deliberately *not* (add it to
``FTM_NAME_PROPS_EXCLUDED``, with the reason). Both are edits a human makes
on purpose — which is the point.

Usage:
    python backend/scripts/check_ftm_names.py          # same as --check
    python backend/scripts/check_ftm_names.py --check  # CI: fail if stale

CI: the ``ftm-edges`` job in ``.github/workflows/vendored-enum-drift.yml``,
alongside ``check_ftm_edges.py`` — it already installs the ftm extra, and a
second job would pay for the ICU build twice.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.names import FTM_NAME_PROPS, FTM_NAME_PROPS_EXCLUDED  # noqa: E402

#: The schemata OpenCheck searches — ``_schema_for()`` in the OpenSanctions and
#: OpenAleph adapters returns exactly these two. Their inherited properties
#: (everything on ``Thing``) come along, which is where four of the five name
#: properties live.
_SEARCHED_SCHEMATA = ("LegalEntity", "Person")


def model_name_props() -> dict[str, set[str]]:
    """schema name → its name-type property names, from the installed model."""
    import followthemoney as ftm

    found: dict[str, set[str]] = {}
    for name in _SEARCHED_SCHEMATA:
        schema = ftm.model.get(name)
        if schema is None:
            # A renamed or removed schema is itself drift worth reporting.
            found[name] = set()
            continue
        found[name] = {
            prop.name for prop in schema.properties.values() if prop.type.name == "name"
        }
    return found


def main() -> int:
    try:
        by_schema = model_name_props()
    except ImportError:
        print(
            "followthemoney is not installed — install the `ftm` extra "
            "(uv sync --extra ftm) to run this check.",
            file=sys.stderr,
        )
        return 2

    missing_schemata = [name for name, props in by_schema.items() if not props]
    upstream: set[str] = set().union(*by_schema.values()) if by_schema else set()
    accounted = set(FTM_NAME_PROPS) | set(FTM_NAME_PROPS_EXCLUDED)

    problems: list[str] = []
    for name in sorted(missing_schemata):
        problems.append(
            f"SCHEMA {name} declares no name properties (renamed, removed, or "
            "no longer the schema OpenCheck searches) — check the adapters' "
            "_schema_for()."
        )
    for prop in sorted(upstream - accounted):
        problems.append(
            f"NEW upstream name property not accounted for: {prop!r} — add it to "
            "names.FTM_NAME_PROPS to read it, or to names.FTM_NAME_PROPS_EXCLUDED "
            "with the reason not to."
        )
    for prop in sorted(accounted - upstream):
        problems.append(
            f"STALE name property no longer in the model: {prop!r} — remove it "
            "from names.FTM_NAME_PROPS/FTM_NAME_PROPS_EXCLUDED."
        )

    if problems:
        print(
            "The FtM names group OpenCheck reads has drifted from the "
            "followthemoney model:"
        )
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(
        f"OK: {len(FTM_NAME_PROPS)} name properties read "
        f"({', '.join(FTM_NAME_PROPS)}), "
        f"{len(FTM_NAME_PROPS_EXCLUDED)} deliberately excluded "
        f"({', '.join(sorted(FTM_NAME_PROPS_EXCLUDED))}) — together they cover "
        f"every name property on {' + '.join(_SEARCHED_SCHEMATA)}."
    )
    return 0


if __name__ == "__main__":
    # --check accepted for symmetry with the other drift scripts; this script
    # is check-only either way.
    sys.exit(main())
