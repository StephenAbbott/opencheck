"""Reading versions — one integer per source, bumped when OpenCheck changes
what it writes for that source's records.

The publication ledger (``bods/publication_ledger.py``) compares each record's
``recordDetails`` with what it last published. A difference is the source's
when the source changed, and OpenCheck's when a mapper did: diffing two
published Estonia editions (19 and 25 Oct 2026) found 46,304 changed records
of which a handful were register changes and the rest two mapper fixes.
The ledger tells the two apart by the reading version each record was last
published under — a record that differs across a version change is published
``updated`` with a ``correcting`` annotation.

So a version here must move whenever a mapper's ``recordDetails`` do.
``tests/test_reading_versions.py`` maps a pinned bundle per source and
compares the digest with ``tests/fixtures/reading_pins/pins.json``: change a
mapper's output and the test fails until the version is bumped and the pin
regenerated (``uv run python scripts/pin_reading_versions.py``).

Only sources in a published series need an entry. A source joins by adding
its version here, a fixture bundle and a pin.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .statements import content_digest

#: source id → the version of OpenCheck's reading of it.
READING_VERSIONS: dict[str, int] = {
    "gleif": 1,
}


def reading_version(source_id: str) -> int:
    """The source's reading version. Raises ``KeyError`` for a source with
    none: it is not ready to be in a series."""
    return READING_VERSIONS[source_id]


def reading_digest(statements: Iterable[dict[str, Any]]) -> str:
    """One digest of what a mapper wrote for a pinned bundle: every
    statement's ``recordId``, ``recordType`` and ``recordDetails``, in order.
    Dates of reading, provenance and annotations are left out — they are
    outside what the ledger compares."""
    return content_digest(
        [
            [s.get("recordId"), s.get("recordType"), s.get("recordDetails")]
            for s in statements
        ],
        "",
    )


__all__ = ["READING_VERSIONS", "reading_digest", "reading_version"]
