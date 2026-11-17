"""The BODS publication ledger — what OpenCheck has already published of each
record, so the next edition can say what changed.

A lookup publishes one statement per record, always ``recordStatus: "new"``:
it has no memory of the last time it described the record. A dated series of
editions needs that memory. BODS v0.4's information-updates rules are:

* a record keeps one ``recordId`` across its statements;
* its first statement is ``new``, a later one that changes it ``updated``,
  the last one ``closed``, and nothing follows a ``closed`` statement;
* a correction is published as a new statement with a ``correcting``
  annotation, never by editing the old one.

The ledger is one SQLite file holding, per record, the last statement
published (its id, a digest of its ``recordDetails``, its status, the edition,
the source and the source's *reading version*) and the last statement's
body, so a record that disappears can be closed with what was last said of
it. It is built for the whole LEI population — some 17 million records — so
ids are held as 12 bytes, editions as integers and bodies deflated against a
dictionary the ledger keeps (``train``). An edition is applied to it source by source::

    with ledger.edition("2026-12-01") as ed:
        for stmt in ed.apply("gleif", parties, reading=1): ...
        for stmt in ed.apply("gleif", relationships, reading=1): ...
        for stmt in ed.close_missing("gleif"): ...

and what comes back is the edition's delta: only the statements that say
something new. Stephen's decisions (17 Nov 2026, the ledger ticket):

1. **Changes only.** A record whose ``recordDetails`` are what the ledger
   holds yields nothing — no confirming statement. ``statementDate``,
   ``source``, ``publicationDetails`` and annotations are outside the
   comparison, as they are outside the content-addressed ``statementId``.
2. **A change of reading is a correction.** Each source has a reading version
   (``bods/reading_versions.py``), bumped when its mapper changes what it
   writes. A record that differs from the ledger *and* was last published
   under another reading version is ``updated`` with a ``correcting``
   annotation saying the difference may be OpenCheck's and not the source's.
   Diffing two published Estonia editions (19 and 25 Oct 2026) found most
   "changes" were mapper fixes; without this they read as register changes.
3. **A closed record that comes back starts a second life** under a new
   ``recordId`` (BODS: no statement follows ``closed``). The mapper's own
   ``recordId`` stays the ledger's key; the published one is derived from it
   and the life number, and every reference to the record in the edition
   (``subject``, ``interestedParty``, ``declarationSubject``,
   ``componentRecords``) is rewritten to the life it names.
4. **``closed`` only from a whole snapshot.** ``close_missing`` closes what a
   source no longer carries, so it may be called only when the caller read
   the source whole. It refuses (``SuspectEditionError``) when the closures
   would pass a tenth of the source's live records — a truncated read looks
   exactly like a mass deletion.

Parties before relationships: a record that returns gets its new ``recordId``
when it is applied, and a relationship applied before it would still name the
old one. ``apply`` raises ``OrderError`` when an entity or person returns
after a relationship has been applied in the same edition.

Statement ids. A ``new`` statement keeps the id a lookup gives the same
content (``content_statement_id``). An ``updated`` or ``closed`` statement's
id also carries the edition: a record that goes A → B → A would otherwise
publish the second A under the first A's id, and a ``statementId`` must be
unique.
"""

from __future__ import annotations

import json
import re
import sqlite3
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import sqlite_schema
from .annotations import commenting, correcting
from .refs import remap_statement_references
from .statements import _stable_id, content_digest, content_statement_id

#: Above this share of a source's live records, ``close_missing`` refuses…
MAX_CLOSE_FRACTION = 0.10
#: …unless the source has fewer live records than this (a small source can
#: lose a tenth of itself honestly).
CLOSE_GUARD_FLOOR = 1_000

_DDL = """
CREATE TABLE IF NOT EXISTS records (
    base_id       PRIMARY KEY,
    record_id,
    life          INTEGER NOT NULL,
    record_type   TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    reading       INTEGER NOT NULL,
    status        TEXT NOT NULL,
    statement_id  NOT NULL,
    digest        BLOB NOT NULL,
    first_edition INTEGER NOT NULL,
    last_edition  INTEGER NOT NULL,
    seen_edition  INTEGER NOT NULL,
    statements    INTEGER NOT NULL,
    body          BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS editions (
    edition   TEXT PRIMARY KEY,
    built_at  TEXT NOT NULL,
    counts    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value)
"""

MIGRATIONS: tuple[sqlite_schema.Migration, ...] = (
    sqlite_schema.Migration(1, "the ledger as first shipped", sqlite_schema.statements(_DDL)),
)

_OWN_ID = re.compile(r"^opencheck-[0-9a-f]{24}$")
#: Statement fields the ledger holds as columns, so not in the stored body.
_COLUMN_FIELDS = frozenset(
    {"statementId", "recordId", "recordStatus", "recordType", "publicationDetails", "annotations"}
)
_PUBLISHER = {"bodsVersion": "0.4", "publisher": {"name": "OpenCheck"}}


def _key(value: str) -> bytes | str:
    """An id as the ledger stores it: OpenCheck's own (``opencheck-`` + 24
    hex) as its 12 bytes, anything else as written."""
    return bytes.fromhex(value[10:]) if _OWN_ID.match(value) else value


def _unkey(value: bytes | str) -> str:
    return "opencheck-" + value.hex() if isinstance(value, bytes) else value


def _day(edition: str) -> int:
    return int(edition.replace("-", ""))


def _iso(day: int) -> str:
    text = f"{day:08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


#: The outcomes ``Edition.counts`` is keyed on, per source: ``<source>|<outcome>``.
OUTCOMES = (
    "new", "returned", "updated", "corrected", "closed", "unchanged", "duplicate",
)


class LedgerError(RuntimeError):
    """The ledger cannot take what it was given."""


class EditionOrderError(LedgerError):
    """Editions are applied in date order, once each."""


class OrderError(LedgerError):
    """A party returned after a relationship was applied in the same edition."""


class SuspectEditionError(LedgerError):
    """``close_missing`` would close more of a source than a real month does."""


def details_digest(record_details: Any) -> str:
    """The digest two statements of one record are compared on: their
    ``recordDetails`` alone (``publicListing`` left out, as in
    ``content_digest``), whatever their status."""
    return content_digest(record_details, "")


def _digest_key(record_details: Any) -> bytes:
    return bytes.fromhex(details_digest(record_details))


def life_record_id(base_id: str, life: int) -> str:
    """The published ``recordId`` of a record's ``life``-th life: the mapper's
    own id for the first, a derived one after each return from ``closed``."""
    return base_id if life <= 1 else _stable_id(base_id, "life", str(life))


def series_statement_id(record_id: str, record_details: Any, status: str, edition: str) -> str:
    """``statementId`` for a statement in a series (see the module docstring)."""
    if status == "new":
        return content_statement_id(record_id, record_details, "new")
    return _stable_id(record_id, "content", content_digest(record_details, status), "edition", edition)


def correction_note(source_id: str, was: int, now: int, edition: str) -> dict[str, Any]:
    return correcting(
        "/recordDetails",
        f"OpenCheck changed how it reads this source ({source_id}) between this record's "
        f"previous statement (reading version {was}) and this one (reading version {now}). "
        "What differs from the previous statement may come from that change and not from "
        "a change in the source's own record.",
        creation_date=edition,
    )


def _parties(stmt: dict[str, Any]) -> tuple[str, str]:
    """A relationship's subject and interested party, as comparable text."""
    details = stmt.get("recordDetails") or {}
    return (
        json.dumps(details.get("subject"), sort_keys=True),
        json.dumps(details.get("interestedParty"), sort_keys=True),
    )


def parties_changed_note(edition: str) -> dict[str, Any]:
    return commenting(
        "/recordStatus",
        f"Closed in the edition of {edition} because the relationship's subject or "
        "interested party changed. BODS ends a relationship record when either party "
        "changes; the new arrangement is published as a new record. The details are the "
        "last ones published.",
        creation_date=edition,
    )


def closure_note(source_id: str, edition: str) -> dict[str, Any]:
    return commenting(
        "/recordStatus",
        f"Closed because the source ({source_id}), read whole for the edition of {edition}, "
        "no longer carries this record. The source gives no end date; the details are the "
        "last ones published.",
        creation_date=edition,
    )


@dataclass
class Edition:
    """One edition being applied. Use through ``PublicationLedger.edition``."""

    ledger: PublicationLedger
    edition: str
    counts: dict[str, int] = field(default_factory=dict)
    _relationships_applied: bool = False

    def __post_init__(self) -> None:
        self._day = _day(self.edition)

    def _count(self, source_id: str, outcome: str, n: int = 1) -> None:
        key = f"{source_id}|{outcome}"
        self.counts[key] = self.counts.get(key, 0) + n

    def _publish(self, stmt: dict[str, Any], status: str, record_id: str) -> dict[str, Any]:
        out = dict(stmt)
        out["recordId"] = record_id
        out["recordStatus"] = status
        out["statementId"] = series_statement_id(
            record_id, out.get("recordDetails"), status, self.edition
        )
        publication = dict(out.get("publicationDetails") or _PUBLISHER)
        publication["publicationDate"] = self.edition
        out["publicationDetails"] = publication
        return out

    def apply(
        self, source_id: str, statements: Iterable[dict[str, Any]], *, reading: int
    ) -> Iterator[dict[str, Any]]:
        """Apply one source's statements; yield the ones this edition
        publishes. Each incoming statement is the mapper's own (``recordId``
        the mapper's, ``recordStatus`` ``new`` or ``closed``)."""
        ledger = self.ledger
        conn = ledger.conn
        aliases = ledger.aliases
        day = self._day
        for incoming in statements:
            base = incoming.get("recordId")
            if not isinstance(base, str) or not base:
                raise LedgerError("a statement without a recordId cannot be in a series")
            # References to records past their first life are rewritten to
            # the life they name; the statement's own recordId is decided
            # below, from the row.
            stmt = remap_statement_references([incoming], aliases)[0] if aliases else incoming
            record_type = str(stmt.get("recordType") or "")
            digest = _digest_key(stmt.get("recordDetails"))
            incoming_closed = str(incoming.get("recordStatus") or "new") == "closed"
            key = _key(base)
            row = conn.execute(
                "SELECT record_id, life, reading, status, digest, seen_edition "
                "FROM records WHERE base_id = ?",
                (key,),
            ).fetchone()

            if row is not None and row["seen_edition"] == day:
                self._count(source_id, "duplicate")
                continue

            if row is None or row["status"] == "closed":
                if row is not None and incoming_closed:
                    # Closed in the ledger and closed at the source: nothing
                    # follows a closed statement.
                    conn.execute(
                        "UPDATE records SET seen_edition = ? WHERE base_id = ?", (day, key)
                    )
                    self._count(source_id, "unchanged")
                    continue
                life = 1 if row is None else int(row["life"]) + 1
                record_id = life_record_id(base, life)
                if row is not None:
                    if record_type != "relationship" and self._relationships_applied:
                        raise OrderError(
                            f"{base} returned after relationships were applied in "
                            f"{self.edition}: apply parties first"
                        )
                    aliases[base] = record_id
                    # Its own references may name itself (declarationSubject).
                    stmt = remap_statement_references([incoming], aliases)[0]
                    digest = _digest_key(stmt.get("recordDetails"))
                status = "closed" if incoming_closed else "new"
                out = self._publish(stmt, status, record_id)
                conn.execute(
                    "INSERT OR REPLACE INTO records (base_id, record_id, life, record_type, "
                    "source_id, reading, status, statement_id, digest, first_edition, "
                    "last_edition, seen_edition, statements, body) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        key, None if life == 1 else _key(record_id), life, record_type,
                        source_id, reading, status, _key(out["statementId"]), digest, day,
                        day, day, 1, ledger.pack(out),
                    ),
                )
                self._count(source_id, "new" if row is None else "returned")
                if record_type == "relationship":
                    self._relationships_applied = True
                yield out
                continue

            if record_type == "relationship":
                self._relationships_applied = True
            if row["digest"] == digest and not incoming_closed:
                if row["reading"] == reading:
                    conn.execute(
                        "UPDATE records SET seen_edition = ? WHERE base_id = ?", (day, key)
                    )
                else:
                    conn.execute(
                        "UPDATE records SET seen_edition = ?, reading = ? WHERE base_id = ?",
                        (day, reading, key),
                    )
                self._count(source_id, "unchanged")
                continue

            record_id = base if row["record_id"] is None else _unkey(row["record_id"])
            if record_type == "relationship" and not incoming_closed:
                # BODS: a relationship whose subject or interested party
                # changes has ended. The old record closes and the new
                # arrangement is a new record (information updates,
                # "full requirements").
                body_row = conn.execute(
                    "SELECT body FROM records WHERE base_id = ?", (key,)
                ).fetchone()
                last = ledger.unpack(body_row["body"])
                if _parties(last) != _parties(stmt):
                    last["recordType"] = record_type
                    last["statementDate"] = self.edition
                    closed = self._publish(last, "closed", record_id)
                    closed["annotations"] = [parties_changed_note(self.edition)]
                    life = int(row["life"]) + 1
                    new_id = life_record_id(base, life)
                    aliases[base] = new_id
                    out = self._publish(stmt, "new", new_id)
                    conn.execute(
                        "INSERT OR REPLACE INTO records (base_id, record_id, life, "
                        "record_type, source_id, reading, status, statement_id, digest, "
                        "first_edition, last_edition, seen_edition, statements, body) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            key, _key(new_id), life, record_type, source_id, reading, "new",
                            _key(out["statementId"]), digest, day, day, day, 1,
                            ledger.pack(out),
                        ),
                    )
                    self._count(source_id, "closed")
                    self._count(source_id, "new")
                    self._count(source_id, "parties_changed")
                    yield closed
                    yield out
                    continue
            status = "closed" if incoming_closed else "updated"
            out = self._publish(stmt, status, record_id)
            corrected = int(row["reading"]) != reading and row["digest"] != digest
            if corrected:
                out["annotations"] = [
                    *(out.get("annotations") or []),
                    correction_note(source_id, int(row["reading"]), reading, self.edition),
                ]
            conn.execute(
                "UPDATE records SET reading = ?, status = ?, statement_id = ?, digest = ?, "
                "last_edition = ?, seen_edition = ?, statements = statements + 1, body = ? "
                "WHERE base_id = ?",
                (
                    reading, status, _key(out["statementId"]), digest, day, day,
                    ledger.pack(out), key,
                ),
            )
            self._count(
                source_id, "closed" if incoming_closed else ("corrected" if corrected else "updated")
            )
            yield out

    def missing(self, source_id: str) -> tuple[int, int]:
        """``(live records this edition did not see, live records)`` for a source."""
        gone, live = self.ledger.conn.execute(
            "SELECT COALESCE(SUM(seen_edition != ?), 0), COUNT(*) FROM records "
            "WHERE source_id = ? AND status != 'closed'",
            (self._day, source_id),
        ).fetchone()
        return int(gone), int(live)

    def close_missing(
        self, source_id: str, *, max_fraction: float = MAX_CLOSE_FRACTION
    ) -> Iterator[dict[str, Any]]:
        """Close every live record of ``source_id`` this edition did not see.
        Only for a source read whole. Relationships are closed before the
        parties they name."""
        gone, live = self.missing(source_id)
        if live >= CLOSE_GUARD_FLOOR and gone > live * max_fraction:
            raise SuspectEditionError(
                f"{source_id}: {gone:,} of {live:,} live records are missing from "
                f"{self.edition} — more than {max_fraction:.0%}. A partial read looks "
                "like this; nothing was closed."
            )
        ledger = self.ledger
        conn = ledger.conn
        rows = conn.execute(
            "SELECT base_id, record_id, record_type, body FROM records WHERE source_id = ? "
            "AND status != 'closed' AND seen_edition != ? "
            "ORDER BY (record_type != 'relationship')",
            (source_id, self._day),
        ).fetchall()
        for row in rows:
            base = _unkey(row["base_id"])
            record_id = base if row["record_id"] is None else _unkey(row["record_id"])
            body = ledger.unpack(row["body"])
            body["recordType"] = row["record_type"]
            body["statementDate"] = self.edition
            out = self._publish(body, "closed", record_id)
            out["annotations"] = [closure_note(source_id, self.edition)]
            conn.execute(
                "UPDATE records SET status = 'closed', statement_id = ?, last_edition = ?, "
                "statements = statements + 1, body = ? WHERE base_id = ?",
                (_key(out["statementId"]), self._day, ledger.pack(out), row["base_id"]),
            )
            self._count(source_id, "closed")
            yield out


class PublicationLedger:
    """The ledger file. Not safe for two writers; an edition is one
    transaction, committed when its ``with`` block ends without an error."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        sqlite_schema.migrate(self.conn, self.path, MIGRATIONS)
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA cache_size = -400000")
        row = self.conn.execute("SELECT value FROM meta WHERE key = 'zdict'").fetchone()
        self._zdict: bytes | None = bytes(row[0]) if row else None
        self._deflater: Any = None
        #: mapper recordId → published recordId, for records past their first life.
        self.aliases: dict[str, str] = {
            _unkey(r["base_id"]): _unkey(r["record_id"])
            for r in self.conn.execute("SELECT base_id, record_id FROM records WHERE life > 1")
        }

    def close(self) -> None:
        self.conn.close()

    # -- bodies ------------------------------------------------------------

    def train(self, samples: Iterable[dict[str, Any]]) -> bool:
        """Give an empty ledger its compression dictionary, from sample
        statements of the kind it will hold (a few hundred is plenty). The
        dictionary is fixed for the file's life: every body is deflated
        against it. Returns False, changing nothing, once the ledger has one
        or holds a record."""
        if self._zdict is not None or self.conn.execute(
            "SELECT 1 FROM records LIMIT 1"
        ).fetchone():
            return False
        zdict = b"\n".join(_body_bytes(s) for s in samples)[-32768:]
        if not zdict:
            return False
        self.conn.execute("INSERT INTO meta (key, value) VALUES ('zdict', ?)", (zdict,))
        self._zdict = zdict
        return True

    def pack(self, stmt: dict[str, Any]) -> bytes:
        data = _body_bytes(stmt)
        if self._zdict is None:
            return zlib.compress(data, 6)
        # Priming a deflater with the dictionary costs more than deflating a
        # body; prime one and copy it.
        if self._deflater is None:
            self._deflater = zlib.compressobj(6, zdict=self._zdict)
        deflater = self._deflater.copy()
        return deflater.compress(data) + deflater.flush()

    def unpack(self, blob: bytes) -> dict[str, Any]:
        if self._zdict is None:
            data = zlib.decompress(blob)
        else:
            inflater = zlib.decompressobj(zdict=self._zdict)
            data = inflater.decompress(blob) + inflater.flush()
        out = json.loads(data.decode("utf-8"))
        assert isinstance(out, dict)
        return out

    # -- editions ----------------------------------------------------------

    def editions(self) -> list[str]:
        return [str(r[0]) for r in self.conn.execute("SELECT edition FROM editions ORDER BY edition")]

    def last_edition(self) -> str | None:
        editions = self.editions()
        return editions[-1] if editions else None

    def edition_counts(self, edition: str) -> dict[str, int]:
        row = self.conn.execute(
            "SELECT counts FROM editions WHERE edition = ?", (edition,)
        ).fetchone()
        return dict(json.loads(row[0])) if row else {}

    def edition(self, edition: str) -> _EditionScope:
        """Open ``edition`` (an ISO day, later than every edition applied)."""
        datetime.strptime(edition, "%Y-%m-%d")
        last = self.last_edition()
        if last is not None and edition <= last:
            raise EditionOrderError(f"edition {edition} is not after {last}, the last one applied")
        return _EditionScope(self, edition)

    def is_live(self, record_id: str) -> bool:
        """Whether ``record_id`` — a published id, in any life — names a
        record whose last statement is not ``closed``."""
        base = record_id
        for mapper_id, published in self.aliases.items():
            if published == record_id:
                base = mapper_id
                break
        row = self.conn.execute(
            "SELECT status, record_id FROM records WHERE base_id = ?", (_key(base),)
        ).fetchone()
        if row is None or row["status"] == "closed":
            return False
        current = base if row["record_id"] is None else _unkey(row["record_id"])
        return current == record_id

    def live_counts(self) -> dict[str, int]:
        return {
            f"{r[0]}|{r[1]}": int(r[2])
            for r in self.conn.execute(
                "SELECT source_id, record_type, COUNT(*) FROM records "
                "WHERE status != 'closed' GROUP BY 1, 2"
            )
        }

    def latest(self, *, include_closed: bool = False) -> Iterator[dict[str, Any]]:
        """The last statement published of every record, parties first — the
        full state as of the last edition (without annotations)."""
        where = "" if include_closed else "WHERE status != 'closed' "
        for row in self.conn.execute(
            "SELECT base_id, record_id, record_type, status, statement_id, last_edition, body "
            f"FROM records {where}ORDER BY (record_type = 'relationship')"
        ):
            base = _unkey(row["base_id"])
            body = self.unpack(row["body"])
            yield {
                "statementId": _unkey(row["statement_id"]),
                "recordId": base if row["record_id"] is None else _unkey(row["record_id"]),
                "recordType": row["record_type"],
                "recordStatus": row["status"],
                **body,
                "publicationDetails": {**_PUBLISHER, "publicationDate": _iso(row["last_edition"])},
            }


def _body_bytes(stmt: dict[str, Any]) -> bytes:
    """What the ledger keeps of a published statement beyond its columns:
    ``declarationSubject``, ``statementDate``, ``recordDetails`` and
    ``source``. Annotations describe one statement, not the record."""
    body = {k: v for k, v in stmt.items() if k not in _COLUMN_FIELDS}
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class _EditionScope:
    def __init__(self, ledger: PublicationLedger, edition: str) -> None:
        self.ledger = ledger
        self.name = edition
        self.edition: Edition | None = None
        self._aliases_before: dict[str, str] = {}

    def __enter__(self) -> Edition:
        self._aliases_before = dict(self.ledger.aliases)
        self.ledger.conn.execute("BEGIN IMMEDIATE")
        self.edition = Edition(self.ledger, self.name)
        return self.edition

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        conn = self.ledger.conn
        if exc_type is not None:
            conn.execute("ROLLBACK")
            self.ledger.aliases.clear()
            self.ledger.aliases.update(self._aliases_before)
            return
        assert self.edition is not None
        conn.execute(
            "INSERT INTO editions (edition, built_at, counts) VALUES (?, ?, ?)",
            (
                self.name,
                datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                json.dumps(self.edition.counts, sort_keys=True),
            ),
        )
        conn.execute("COMMIT")


__all__ = [
    "CLOSE_GUARD_FLOOR",
    "MAX_CLOSE_FRACTION",
    "MIGRATIONS",
    "OUTCOMES",
    "Edition",
    "EditionOrderError",
    "LedgerError",
    "OrderError",
    "PublicationLedger",
    "SuspectEditionError",
    "closure_note",
    "correction_note",
    "details_digest",
    "life_record_id",
    "series_statement_id",
]
