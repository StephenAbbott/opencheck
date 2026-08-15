# Dates in OpenCheck's BODS output

The [BODS dates guidance](https://standard.openownership.org/en/main/standard/modelling/dates-guidance.html)
asks publishers to explain their date practice to data users. This is that
explanation.

OpenCheck is a **republisher**: it reads other people's registers and emits BODS
statements about what they say. That makes the dates unusually easy to get
wrong, because four different questions all have date-shaped answers.

## The four clocks

| Field | Question it answers | Where OpenCheck gets it |
|-------|---------------------|--------------------------|
| `interests[].startDate` / `endDate` | When was it true? | The register |
| `statementDate` | When did the source declare it? | The register's own declaration date where published, else the retrieval date, else today |
| `source.retrievedAt` | When did OpenCheck download it? | Observed at fetch time — see [Data currency](sources.md#data-currency) |
| `publicationDetails.publicationDate` | When did OpenCheck publish this statement? | Today |

They are genuinely different, and until Phase 99 they were all `date.today()`.
Two rules follow, and both have been violated in the past:

**A source's date never goes in `publicationDetails`.** That block describes the
publication of *this* statement by the publisher named in the same block, and
that publisher is OpenCheck. A PSC notified in 2016 was once emitting a
statement OpenCheck claimed to have published in 2016. Open Ownership's own
bundles model it correctly — `statementDate` from the source, `publicationDate`
from OO — and so does OpenCheck now.

**An interest's start date is not a declaration date.** A director appointed in
1998 was not *declared* in 1998, and Companies House publishes no per-officer
notification date. `appointed_on` belongs on `interests[].startDate` and nowhere
else.

## Which sources supply their own declaration date

| Source | Field |
|--------|-------|
| `gleif` | `registration.lastUpdateDate` |
| `companies_house` | PSC `notified_on`, or `ceased_on` for a closed record |
| `sec_edgar` | 13D/13G filing date |
| `bods_gleif`, `bods_uk_psc` | Open Ownership's own `statementDate`, passed through verbatim |
| `krs_poland` | `dataOstatniegoWpisu` |
| `ur_latvia` | officer `last_modified_at` |
| `ares` | `datumAktualizace` |
| `brreg` | `rollegrupper[].sistEndret` |
| `ted_eu` | latest notice `publication-date` |

Everything else falls back to the retrieval date, then to today.

Three registers were investigated and have nothing usable, recorded so the
question does not get re-opened: **Estonia** publishes only a founding date,
**Denmark**'s bitemporal CVR is queried for validity time rather than
transaction time, and **Brazil** — probed live against both OpenCNPJ and
BrasilAPI — returns no update stamp at all.

## Precision, and how it is recorded

BODS treats date precision differently depending on the field, and it is right
to.

**`birthDate` may be `YYYY`, `YYYY-MM` or `YYYY-MM-DD`.** Companies House
publishes month and year only for PSCs and officers, deliberately, for privacy.
OpenCheck emits exactly what the register published and **does not round**:
rounding would fabricate a day the register withheld on purpose. Because a
reader seeing `1975-08` cannot otherwise tell a privacy-limited register from a
truncation on our side, an imprecise `birthDate` carries a BODS annotation
(motivation `commenting`) saying which it is.

**`foundingDate`, `dissolutionDate`, `startDate`, `endDate` and `statementDate`
must be `YYYY-MM-DD`.** Where a month or day is genuinely unknown the standard
sanctions rounding to the first of the month or year — but the rounding is then
invisible in the output. OpenCheck's rule:

- Round per the standard: unknown day → first of month, unknown month → first
  of year.
- **Annotate the rounding** (motivation `transformation`), naming what the
  source actually supplied, so a consumer can tell a genuine 1 March from a
  rounded March without reading this page.

No adapter currently emits a partial value into one of those fields, and a
canary test (`test_annotations.py::TestStrictDateFieldCanary`) fails if one
starts. The helper is `bods/annotations.py::round_partial_date`.

## Annotations generally

Where OpenCheck replaces a register's own vocabulary with a BODS code, the
statement carries an `annotations` entry naming what the source said. The rule
is:

> The statement always carries the usable value; the annotation always carries
> the register's words.

`transformedContent` is defined in BODS as the representation *after*
transformation, which read literally would put the original in the target field.
That is unworkable for dates — a `YYYY-MM-DD` field cannot hold
"01 November 2018" — so the target field always holds the value a consumer
should use, and the annotation's `description` holds the source's wording.
Worth raising upstream with Open Ownership.

Since Phase 108 those annotations are **visible in the UI**. Where a rendered
value has an annotation behind it, the results page marks it with a dotted
underline and offers a persistent **"as filed"** toggle: switched on, the
register's own words lead and OpenCheck's value follows in muted text. The
setting is shared across every source card in a lookup — a lookup renders many
cards, and finding one in the register's vocabulary and the next in OpenCheck's
would read as a bug. It defaults to OpenCheck's reading, which is the value
that is always present and always machine-readable. The toggle only renders
where a bundle actually carries annotations; companies served from a stored
Open Ownership bundle bypass the mapper and so never do.

Only **lossy or non-obvious** transformations are annotated. Annotating identity
mappings would multiply bundle size for no gain, and the deployment is
memory-bound. Currently annotated:

- Companies House nature-of-control codes, whose code identity is otherwise
  recoverable only from an English prose descriptor
- Imprecise `birthDate` values, as above

## See also

- [Data currency](sources.md#data-currency) — the liveness taxonomy behind
  `source.retrievedAt`
- [Which date goes where](sources.md#which-date-goes-where) — the same four
  clocks, summarised alongside the source table
