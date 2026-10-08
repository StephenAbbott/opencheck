"""Former legal names, said as such (Phase 309).

BODS v0.4 gives an entity one ``name`` and an untyped ``alternateNames`` list,
so every mapper that reads a register's *previous* names — GLEIF's
``otherNames`` of type ``PREVIOUS_LEGAL_NAME``, Companies House's
``previous_company_names`` with their ``ceased_on`` dates — flattened them
into alternates beside trading names and transliterations, and nothing
downstream could tell a name the company *had* from one it also *uses*. The
Barrick case: "Barrick Gold Corporation" finds ``0O4KBQCJZX82UKGCBV73``
through GLEIF's fulltext search, the row shows BARRICK MINING CORPORATION, and
the reader cannot see why it matched.

The fix stays inside the standard, the way Phase 305 carried GLEIF's events:
one ``commenting`` annotation per former name, pointing at its entry in
``alternateNames`` (``/recordDetails/alternateNames/<i>``), with a structured
:data:`FORMER_NAME_PROPERTY` beside the sentence so readers need not parse
it. The name itself stays in ``alternateNames`` exactly as before — a
consumer that knows nothing of this phase sees what it always saw.

Rules:

* **Only what the source types as former.** GLEIF's ``PREVIOUS_LEGAL_NAME``,
  Companies House's ``previous_company_names``, ACRA's former-name columns,
  New York's name history. A trading name, a translation, a transliteration
  or an untyped alias is never called former.
* **Dates only where the register gives them.** Companies House dates each
  former name (``ceased_on``, ``effective_from``); GLEIF dates the *change*
  (a ``CHANGE_LEGAL_NAME`` event) but not which former name it closed, so a
  GLEIF former name carries no date of its own — the profile carries the
  change day separately (``name_changed_on``).
* **The annotation points at the name it describes**, and the reader checks
  that the pointer still resolves to that name (:func:`former_names_of`):
  an annotation that drifted from its entry is dropped, never trusted.
"""

from __future__ import annotations

from typing import Any

from .annotations import commenting, pointer

#: The annotation property carrying the former name as data:
#: ``{"name": ..., "until": "YYYY-MM-DD" | absent, "from": ... | absent}``.
FORMER_NAME_PROPERTY = "formerName"


def _day(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw[:10] if len(raw) >= 10 and raw[4] == "-" else None


def former_name_annotation(
    index: int,
    name: str,
    *,
    until: Any = None,
    since: Any = None,
) -> dict[str, Any]:
    """A ``commenting`` annotation on ``alternateNames[index]`` saying the
    entry is a former legal name, dated where the register dates it."""
    from ..findings import human_date

    until_day, since_day = _day(until), _day(since)
    sentence = f"Former legal name, as filed: {name}"
    if since_day and until_day:
        sentence += f" (from {human_date(since_day) or since_day} until {human_date(until_day) or until_day})"
    elif until_day:
        sentence += f" (until {human_date(until_day) or until_day})"
    elif since_day:
        sentence += f" (from {human_date(since_day) or since_day})"
    annotation = commenting(pointer("recordDetails", "alternateNames", index), sentence + ".")
    fields: dict[str, Any] = {"name": name}
    if until_day:
        fields["until"] = until_day
    if since_day:
        fields["from"] = since_day
    annotation[FORMER_NAME_PROPERTY] = fields
    return annotation


def former_name_annotations(
    alternate_names: list[str],
    former: list[tuple[str, Any, Any]],
) -> list[dict[str, Any]]:
    """Annotations for every ``(name, until, since)`` in ``former`` that is
    present in ``alternate_names`` — the list exactly as it is handed to
    ``make_entity_statement``, so the indexes line up. A former name the
    mapper deduplicated away (it equals the current name) gets none."""
    out: list[dict[str, Any]] = []
    for name, until, since in former:
        try:
            index = alternate_names.index(name)
        except ValueError:
            continue
        out.append(former_name_annotation(index, name, until=until, since=since))
    return out


def former_names_of(statement: dict[str, Any]) -> list[dict[str, Any]]:
    """The former names an entity statement carries, in annotation order:
    ``[{"name", "until", "from"}, ...]`` with absent dates as ``None``.
    Reads only annotations whose pointer still resolves to the named entry."""
    rd = statement.get("recordDetails") or {}
    alternates = rd.get("alternateNames") or []
    prefix = pointer("recordDetails", "alternateNames") + "/"
    out: list[dict[str, Any]] = []
    for annotation in statement.get("annotations") or []:
        fields = annotation.get(FORMER_NAME_PROPERTY)
        if not isinstance(fields, dict) or not fields.get("name"):
            continue
        target = str(annotation.get("statementPointerTarget") or "")
        if not target.startswith(prefix):
            continue
        try:
            index = int(target[len(prefix):])
        except ValueError:
            continue
        if not 0 <= index < len(alternates) or alternates[index] != fields["name"]:
            continue
        out.append({"name": fields["name"], "until": fields.get("until"), "from": fields.get("from")})
    return out
