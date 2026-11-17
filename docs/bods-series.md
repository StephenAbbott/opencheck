# The monthly BODS series

OpenCheck publishes a dated series of Beneficial Ownership Data Standard
(BODS) v0.4 statements, one edition a month, so that a reader can see how a
record changed and when OpenCheck learned of it. A lookup cannot show that:
it describes each record as it is today, always as `recordStatus: "new"`.

The first layer is GLEIF: every Legal Entity Identifier in the Golden Copy,
with its direct and ultimate accounting-consolidation parents and the
reporting exceptions filed in their place. Other layers (the registers
OpenCheck holds whole, then records re-read when a change is reported) are
planned and will join the same series.

## Where it is

One GitHub release a month, tagged `bods-series-YYYY-MM`, on
<https://github.com/StephenAbbott/opencheck/releases>. Each holds:

| File | What it is |
|---|---|
| `opencheck-bods-gleif-<edition>-NN.jsonl.gz` | The edition's statements, one JSON object a line. Entity and person statements come before relationships. |
| `manifest.json` | The edition's date, the Golden Copy publish it was built from, counts, and each part's size and SHA-256. |
| `README.md` | The same in words. |
| `ledger.sqlite.gz.part-*` | OpenCheck's publication ledger. Needed to build the next edition; not needed to read this one. |

Licence: CC0 1.0. GLEIF's data is CC0 and OpenCheck adds no restriction.

## What an edition holds

The first edition holds every record, `new`. Each later edition holds **only
what changed**. A record that is unchanged has no statement in it.

| `recordStatus` | When |
|---|---|
| `new` | A record OpenCheck has not published before. |
| `updated` | The record's `recordDetails` differ from its last statement. |
| `closed` | The Golden Copy no longer carries the record: a parent relationship that ended, a reporting exception withdrawn or replaced. The statement repeats the last details published and carries a note. GLEIF gives no end date, so none is written. |

A record keeps one `recordId` through all of these. To read a record's
history, collect its statements across editions and order them by
`publicationDetails.publicationDate`.

**Read the editions together, oldest first.** An edition after the first is
a set of changes, not a dataset on its own: an `updated` relationship names
parties whose statements are in an earlier edition. BODS requires a
relationship's `subject` and `interestedParty` to match the `recordId` of a
statement earlier in the file, and the series meets that rule when the
editions are joined in date order (each edition's parts in number order).
One later edition validated alone reports those references as missing.

Rules worth knowing:

- **`publicationDate` is the edition**, the first of the month. It is when
  OpenCheck published the statement. `statementDate` is GLEIF's own date for
  the record; on a `closed` statement it is the edition.
- **A change in dates alone is not a change.** `statementDate`, the `source`
  block and annotations are not compared, so a record GLEIF re-stamped
  without changing publishes nothing.
- **A `correcting` annotation marks a possible correction by OpenCheck.**
  OpenCheck gives its reading of each source a version number, raised when
  its mapping changes what it writes. An `updated` statement whose record was
  last published under an earlier version carries a `correcting` annotation:
  what differs may be OpenCheck's reading and not GLEIF's record.
- **Nothing follows `closed`.** A record that comes back after closing is
  published `new` under a new `recordId`, and relationships that name it use
  the new one.
- **A relationship whose subject or interested party changes is closed.**
  BODS ends a relationship record there. The old record is published
  `closed` with its last details and the new arrangement is a `new` record.
  A change to its interests alone is `updated`.
- **Unchanged records are not confirmed.** BODS says a publisher should
  issue an `updated` statement when details are confirmed without changing.
  The series does not: the absence of a statement in an edition means the
  record was read and was the same.
- **A `new` statement has the `statementId` a lookup gives the same content.**
  An `updated` or `closed` statement's id also depends on the edition, so a
  record that returns to an earlier state still gets a new id.
- **GLEIF Level 2 is accounting consolidation, not shareholding.** No
  statement states a percentage.
- Each relationship is published once, from the child's side.

## How it is built

`backend/scripts/build_bods_series.py`, run by
`.github/workflows/publish-bods-series.yml` on the 2nd of each month. It
calls no API. It reads the Golden Copy mirror rebuilt on the 1st, maps every
LEI with the mapper a lookup uses, and passes the statements through the
publication ledger (`backend/opencheck/bods/publication_ledger.py`), which
holds the last statement published of every record.

The build refuses to run when the mirror was not built from a Golden Copy
publish of the first of a month, when an edition would close more than a
tenth of the live records (a truncated read looks like that), or when
relationships name parties that have no live record.

## How it is checked against BODS

Open Ownership's validator, lib-cove-bods, was run on 17 Nov 2026 over a
first edition of 192,919 statements (40,000 LEIs) and over three trial
editions holding `new`, `updated`, `closed`, corrected and returning records.
It reported no schema error. Joined in order, the three editions raised no
reference or lifecycle check. Two checks were raised on the data itself.
`entity_identifiers_not_known_scheme` is on registration numbers GLEIF files
under an authority that has no org-id.guide code; OpenCheck writes GLEIF's
own authority code as the scheme. One entity has a dissolution date before
its founding date, as GLEIF records it. The 40,000-LEI trial also named
3,375 parents outside its slice; the full build names none.
The full 16.6 million statement
edition was not put through the validator, which reads a file into memory.

Measured on the Golden Copy of 1 Nov 2026 (four cores):

| | |
|---|---|
| LEIs read | 3,481,100 |
| Statements in the first edition | 16,652,420 (6,172,075 entity, 3,764,182 person, 6,716,163 relationship) |
| Edition size | 1.68 GB gzipped, in five parts |
| Ledger | 4.25 GB (2.96 GB gzipped, two release assets) |
| Mapping | 14 minutes |
| Ledger pass | 40 minutes |
| Relationship references naming no live record | 0 |

Most entity and person statements that are not an LEI's own record are the
unnamed parties of reporting exceptions: GLEIF holds about 6.5 million
exceptions, and each is one party and one relationship.
