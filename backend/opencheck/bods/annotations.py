"""BODS annotations — recording what the register said, not just what we read.

OpenCheck transforms a lot on the way to BODS: register role labels become
``interestType`` codes, nature-of-control codes become English prose, dates
change format. The output carried none of that. A reader could see
``seniorManagingOfficial`` but not the Estonian label it came from, or a
``birthDate`` of ``1975-08`` without knowing whether the register published only
a month or OpenCheck truncated a full date.

BODS v0.4 has the right construct. Every statement may carry an ``annotations``
array; each entry pins an RFC6901 JSON Pointer at the fragment it describes:

    statementPointerTarget  required  e.g. "/recordDetails/interests/0/type"
    motivation              required  commenting | correcting | identifying
                                      | linking | transformation
    description             free text
    transformedContent      the representation after transformation
    creationDate            YYYY-MM-DD or ISO 8601
    createdBy               {name, uri}

One rule, applied everywhere
----------------------------
**The statement always carries the usable value; the annotation always carries
the register's words.**

``transformedContent`` is defined as the representation *after* transformation,
which read literally would put the original in the target field and the
normalised value in the annotation. That is unworkable for dates — BODS
constrains ``startDate`` and friends to ``YYYY-MM-DD``, so "01 November 2018"
cannot legally sit there — and inconsistent if applied only to free-string
fields. So the target field always holds the value a consumer should use, and
the annotation's ``description`` holds what the source actually said.
``transformedContent`` mirrors the emitted value, which keeps it truthful under
either reading. Worth raising upstream with Open Ownership; noted in the ticket.

What gets annotated
-------------------
Only transformations that are **lossy or non-obvious**. Annotating every field
would multiply bundle size for no gain — and the Render box is memory-bound
(see the Phase 88 OOM). An identity mapping is not worth a sentence; a
two-hop code translation that discards the register's own vocabulary is.
"""

from __future__ import annotations

from typing import Any, Iterable

#: BODS v0.4 annotation motivation codelist.
MOTIVATIONS: frozenset[str] = frozenset(
    {"commenting", "correcting", "identifying", "linking", "transformation"}
)

_PUBLISHER = {"name": "OpenCheck", "uri": "https://opencheck.world"}


def pointer(*parts: Any) -> str:
    """Build an RFC6901 JSON Pointer from path segments.

    Escaping matters: ``~`` becomes ``~0`` and ``/`` becomes ``~1``, or a
    field name containing either would silently address the wrong fragment.

    >>> pointer("recordDetails", "interests", 0, "type")
    '/recordDetails/interests/0/type'
    """
    escaped = [
        str(p).replace("~", "~0").replace("/", "~1") for p in parts
    ]
    return "/" + "/".join(escaped)


def transformation(
    target: str,
    description: str,
    *,
    transformed_content: str | None = None,
    creation_date: str | None = None,
) -> dict[str, Any]:
    """An annotation recording that OpenCheck transformed a source value.

    ``description`` should state what the source actually said, in the source's
    own words — that is the whole point of the record.
    """
    annotation: dict[str, Any] = {
        "statementPointerTarget": target,
        "motivation": "transformation",
        "description": description,
        "createdBy": dict(_PUBLISHER),
    }
    if transformed_content is not None:
        annotation["transformedContent"] = str(transformed_content)
    if creation_date:
        annotation["creationDate"] = creation_date
    return annotation


def commenting(
    target: str, description: str, *, creation_date: str | None = None
) -> dict[str, Any]:
    """An annotation that adds context without claiming a transformation.

    Used where the value is exactly what the source published but a consumer
    could reasonably misread it — an imprecise ``birthDate``, for instance,
    where BODS legitimately permits ``YYYY-MM`` and the reader cannot otherwise
    tell a privacy-limited register from a truncation on our side.
    """
    annotation: dict[str, Any] = {
        "statementPointerTarget": target,
        "motivation": "commenting",
        "description": description,
        "createdBy": dict(_PUBLISHER),
    }
    if creation_date:
        annotation["creationDate"] = creation_date
    return annotation


def annotate(
    statement: dict[str, Any], *annotations: dict[str, Any] | None
) -> dict[str, Any]:
    """Append annotations to *statement*, creating the array if needed.

    ``None`` entries are skipped so callers can pass a conditional result
    inline. Mutates and returns the statement.
    """
    for annotation in annotations:
        if annotation:
            statement.setdefault("annotations", []).append(annotation)
    return statement


def resolve_pointer(statement: dict[str, Any], target: str) -> Any:
    """Resolve an RFC6901 pointer against *statement*.

    Returns the addressed value, or raises KeyError/IndexError if the pointer
    does not resolve. Used by the tests to prove every annotation points at
    something that actually exists — pointers into arrays are fragile if
    statement construction order ever changes.
    """
    if target in ("", "/"):
        return statement
    node: Any = statement
    for raw in target.lstrip("/").split("/"):
        part = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, list):
            node = node[int(part)]
        else:
            node = node[part]
    return node


# ----------------------------------------------------------------------
# Date precision
# ----------------------------------------------------------------------
# BODS is deliberately inconsistent here, and correctly so:
#
#   birthDate                       YYYY | YYYY-MM | YYYY-MM-DD all legal.
#                                   UK PSC publishes only month and year, on
#                                   purpose, for privacy. Rounding it would
#                                   fabricate precision AND defeat that.
#   foundingDate, dissolutionDate,  MUST be YYYY-MM-DD. Where the month or day
#   startDate, endDate,             is unknown the standard sanctions rounding
#   statementDate                   to the first of the month or year — but the
#                                   rounding is then invisible, so it should be
#                                   annotated.


def round_partial_date(value: str | None) -> tuple[str | None, str | None]:
    """Round a partial date to a full one, reporting what was rounded.

    Returns ``(iso_date, precision)`` where precision is ``"year"`` or
    ``"month"`` when rounding occurred, and ``None`` when the input was already
    a full date (or unusable). Follows the BODS rule: unknown day → first of
    month, unknown month → first of year.

    >>> round_partial_date("2022-03")
    ('2022-03-01', 'month')
    >>> round_partial_date("2022")
    ('2022-01-01', 'year')
    >>> round_partial_date("2022-03-15")
    ('2022-03-15', None)
    """
    if not value:
        return value, None
    text = str(value).strip()
    if len(text) == 4 and text.isdigit():
        return f"{text}-01-01", "year"
    if len(text) == 7 and text[4] == "-" and text[:4].isdigit() and text[5:].isdigit():
        return f"{text}-01", "month"
    return text, None


def date_rounding_annotation(
    target: str, original: str, precision: str, *, creation_date: str | None = None
) -> dict[str, Any]:
    """Record that a date was rounded because the source lacked precision.

    The BODS dates guidance asks publishers to communicate their rounding
    practice to data users. Prose in the docs satisfies that; this makes it
    machine-readable, so a consumer can tell a genuine 1 March from a rounded
    March without reading our documentation.
    """
    unit = "month and day" if precision == "year" else "day"
    return transformation(
        target,
        (
            f"Source supplied '{original}' — no {unit}. Rounded to the first "
            "day per the BODS dates guidance; the rounded portion is not a "
            "value the source stated."
        ),
        creation_date=creation_date,
    )


def validate_annotations(statement: dict[str, Any]) -> list[str]:
    """Return a list of problems with a statement's annotations, empty if fine.

    Checks the two things the schema cannot: that every ``motivation`` is in
    the codelist, and that every pointer actually resolves against this
    statement. A dangling pointer validates fine against the JSON schema and is
    useless to a consumer.
    """
    problems: list[str] = []
    for idx, annotation in enumerate(statement.get("annotations") or []):
        motivation = annotation.get("motivation")
        if motivation not in MOTIVATIONS:
            problems.append(
                f"annotations[{idx}]: motivation {motivation!r} not in the codelist"
            )
        target = annotation.get("statementPointerTarget")
        if not target:
            problems.append(f"annotations[{idx}]: missing statementPointerTarget")
            continue
        try:
            resolve_pointer(statement, target)
        except (KeyError, IndexError, ValueError):
            problems.append(
                f"annotations[{idx}]: pointer {target!r} does not resolve"
            )
    return problems


def validate_all(statements: Iterable[dict[str, Any]]) -> list[str]:
    """Run :func:`validate_annotations` across a bundle."""
    problems: list[str] = []
    for statement in statements:
        for problem in validate_annotations(statement):
            problems.append(f"{statement.get('statementId', '?')}: {problem}")
    return problems
