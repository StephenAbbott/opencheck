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
| `statementDate` | When did the source declare it? | The register's own declaration date where published, else the source's cut date for a bulk dataset, else the retrieval date, else (stubs only) today |
| `source.retrievedAt` | When did OpenCheck download it? | Observed at fetch time, or the download / build time of a bulk index — never the register's cut. See [Data currency](sources.md#data-currency) |
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

## Bulk datasets carry two clocks (Phase 314)

A bulk source has two dates and they answer different questions. The
register's **cut** — KBO's `SnapshotDate`, ONRC's export slug, the Golden Copy
publish, GEM's release, data.gov.ua's export — is when the data was true:
that is `statementDate` material. OpenCheck's **download or build** — the
index's `meta.built_at`, the asset download, a delta refresh — is when we
obtained it: that is `source.retrievedAt`.

Until Phase 314 one provenance slot held both, so the cut was published as
`retrievedAt` (a 31 May extract claimed OpenCheck downloaded it on 31 May).
Now `Provenance` has `source_as_of` (the cut) beside `retrieved_at` (ours),
and every bulk adapter declares both by name:

```python
provenance.record_snapshot(
    retrieved_at=<meta.built_at / download time>,
    source_as_of=<the register's cut date>,
    detail="…",
)
```

Either may be `None` where it is genuinely unknown — an index built before
its build time was recorded, a dataset that publishes no cut — and neither is
ever guessed. Resolution takes the oldest of each clock separately. The
source card's chip, and the weekly sweep's snapshot-age check, read the cut
where there is one: rebuilding an old dump does not make it new.

| Source | `source_as_of` (cut) | `retrieved_at` (ours) |
|--------|----------------------|------------------------|
| `gleif` mirror, subsidiaries mirror | Golden Copy publish watermark | `meta.refreshed_at`, else `meta.built_at` |
| `bce_belgium` | KBO `meta.csv` `SnapshotDate` (persisted by `extract_bce.py`) | `meta.built_at`, else the DB file's write time |
| `edr_ukraine` | `UO.xml`'s own timestamp inside the ZIP (`meta.export_date`) | `meta.built_at` |
| `cyprus_drcor` | `--release-date` passed to `extract_cyprus.py` | `meta.built_at` |
| `onrc_romania` | the export slug date | `meta.built_at`, else the downloaded file's write time |
| `asp_moldova` | the weekly export date from the title | `meta.built_at` |
| `apr_serbia` | `DatumPreseka` | `meta.built_at` |
| `chilecompra` | first day of the latest month covered | `meta.built_at` |
| `meip` | the OECD edition | the asset build |
| `eiti_soe` | `meta.source_snapshot` | `meta.built` |
| `eiti` (organisation index) | — (EITI publishes no cut) | `meta.generated`, the moment the crawl started, in UTC to the second (Phase 318; a bare day before) |
| `climatetrace` (GEM / GEOT) | GEM release date; — for GEOT | asset download; GEOT `meta.generated` |
| `bods_gleif`, `bods_uk_psc` | — | the extract directory's write time |
| Open Ownership stored bundles | OO `publicationDate` | — |

`tests/conftest.py`'s mapper guard fails any test in which a mapper, running
under a snapshot or curated provenance, dates a statement today.

**A retrieval is a moment, not a day.** `source.retrievedAt` is a date-time,
so a build stamp kept as a bare day is published as midnight — and midnight on
the statement's own date is exactly what a register cut published as the
download looks like. The BODS quality sweep cannot tell them apart, and nor
can a reader. Index builders record when they read the source to the second:
the EITI organisation index did not until Phase 318 (`2026-07-07`, flagged
`cut_as_retrieval` in production on 9 Oct 2026).

### Statements are built inside their provenance scope

Most mappers are generators. The phase also found that `/deepen` and the
lookup's deepen pass assigned the generator inside `mapping_provenance` and
drained it after the scope had closed, so the statements of 41 sources were
built under the stub default: dated today, with no `retrievedAt`, whatever
the fetch had recorded. Every call site now consumes the mapper's output
inside the scope, and two guards pin it: a static check that each mapper call
is consumed by the expression that makes it, and the conftest guard, which
flags a generator created under a real provenance and drained under none.

## Which sources supply their own declaration date

| Source | Field |
|--------|-------|
| `gleif` | `registration.lastUpdateDate` of each Level 1 record; for a Level 2 relationship, the relationship (RR) record's own `registration.lastUpdateDate` |
| `companies_house` | PSC `notified_on`, or `ceased_on` for a closed record |
| `sec_edgar` | 13D/13G filing date |
| `bods_gleif`, `bods_uk_psc` | Open Ownership's own `statementDate`, passed through verbatim |
| `krs_poland` | `dataOstatniegoWpisu` |
| `ur_latvia` | officer `last_modified_at` |
| `ares` | `datumAktualizace` |
| `brreg` | `rollegrupper[].sistEndret` |
| `ted_eu` | latest notice `publication-date` |
| `climatetrace` | the GEM ownership release date (the dated CSV filename) |
| `opensanctions`, `everypolitician` | each FtM record's own `last_change` — the subject, every nested party and every edge entity separately (`last_seen` is a crawl, never used) |
| `opencorporates` | the company record's `updated_at` (OpenCorporates is the claimant) |
| `wikidata` | an ownership edge: the latest P813 *retrieved* on the claim's references |
| `dlcp_dc` | the entity: `DCS_LAST_MOD_DTTM`; owners: `DATE_LAST_REPORT_FILED`, the biennial report that declares them |
| `ny_dos` | the entity: its latest filing of any kind (none from a truncated history); the CEO: the filing that names them |
| `cac_nigeria` | a current owner: its latest PSC `notified` date; a departed owner keeps the harvest date (no cessation date is published) |
| `ur_latvia` | officers, beneficial owners and members: `last_modified_at`, else `registered_on` |
| `rpvs_slovakia` | each KUV: the latest of its `PlatnostOd` / `PlatnostDo` (RPVS versions an entry by closing it and opening another) |
| `inpi` | the RNE record's `updatedAt`, for the company and its representatives |
| `prh` | YTJ's `lastModified` |
| `zefix` | the latest SOGC (SHAB) publication — every Swiss register change is published there |
| `cro` | the CKAN resource's `last_modified`: the extract the row was served from |
| `eiti_zambia` | the latest portal `last_updated` among the datasets the record was drawn from |
| `anaf_romania` | ANAF's own as-of date (`date_generale.data`) |
| `asp_moldova`, `apr_serbia`, `onrc_romania`, `eiti_soe`, `eiti_assessment` | the snapshot's cut, stated on every statement (`dated_by_cut`) rather than left to the fallback |

Everything else falls back to the source's cut date (bulk datasets — see
above), then the retrieval date, then, for a stub only, today.

### The rule for a source's own date (Phase 315)

A source date becomes `statementDate` only when it is a **record-level** date
the source keeps for the record itself: a last-modified stamp, the latest
filing, or the declaration that names the party. A periodic declaration that
later register changes can postdate is not used for the whole record, because
it would date the current picture to before some of it was true. The two
cases the audit raised and this phase declined:

- **Companies House `confirmation_statement.last_made_up_to`** for the
  entity. A change of name or office filed after the confirmation statement
  is already in the profile, so the profile as served is Companies House's
  claim today. PSC statements keep `notified_on` / `ceased_on`.
- **CRO `last_ar_date`**. On the record checked, the company's name changed
  in May 2026, after its 2025 annual return. The resource refresh dates the
  row instead.

The helpers: `bods/statements.record_date()` reads a full ISO day and nothing looser, and
returns `None` for a date after today (a due date or validity horizon is never
a declaration); `latest_record_date()` takes the latest of several.

Checked against production payloads on 9 Oct 2026 and found to carry no
usable record date: **OpenAleph** (dataset-level timestamps only),
**Firmenbuch** (no entry date in the extract), **CVR** (only *virkning*
effect dates; `registreringFra` is not in the query), **MCA India** (not
configured in production). They keep the retrieval date.

Three registers were investigated and have nothing usable, recorded so the
question does not get re-opened: **Estonia** publishes only a founding date,
**Denmark**'s bitemporal CVR is queried for validity time rather than
transaction time, and **Brazil** — probed live against both OpenCNPJ and
BrasilAPI — returns no update stamp at all.

## GLEIF Level 2: dated by the relationship record (Phase 313)

GLEIF's `/direct-parent`, `/ultimate-parent` and `/direct-children` endpoints
return the *other party's* Level 1 record, which says nothing about the
relationship. A relationship's own update date and its `RELATIONSHIP_PERIOD`
are only on the relationship (RR) record. Until Phase 313 OpenCheck never read
it, so every Level 2 statement carried the looked-up subject's date and no
period — 107 of 107 relationships in Shell plc's export read the same day.

Now the adapter attaches the RR record behind each edge: from the Golden Copy
mirror first (a local read), else from `/{kind}-parent-relationship` and
`/direct-child-relationships`, inside the discretionary GLEIF budget so the
reads never cost a lookup its anchor. The relationship's `statementDate` is
the RR's `registration.lastUpdateDate` and its `RELATIONSHIP_PERIOD` becomes
`interests[].startDate` / `endDate`. Where no RR record could be read the edge
falls back to the **reporter's** own Level 1 date — the subject for a parent
edge, the child for a child edge, because the child is the start node that
files the relationship — and only past that to the retrieval date. The
reporting-exception bridge party is dated like the exception it stands for.

This follows Open Ownership's GLEIF convention
([data-standard#464](https://github.com/openownership/data-standard/issues/464)):
GLEIF is the claimant, so `statementDate` is GLEIF's date; OpenCheck is the
publisher, so `publicationDate` is OpenCheck's. A GLEIF renewal that changes
nothing still moves `lastUpdateDate`, and so moves `statementDate` — which the
information-updates modelling treats as a confirmation, and is intended.

## Ended relationships (Phase 317)

BODS says a relationship has ended in two places: the record
(`recordStatus: "closed"`) and each interest (`endDate`). OpenCheck now uses
both, by one rule that lives in `make_relationship_statement`:

> A relationship whose every interest has an `endDate` on or before today is
> published `closed`, keeping its `recordId` and taking a new `statementId`.

A future `endDate` is a scheduled end (a director's term), so it stays open; a
relationship with one interest still running stays open. A mapper that knows a
relationship ended without a date passes `record_status="closed"` itself (CAC
Nigeria's INACTIVE rows). The rule is the same one the graph and the exports
read (`bods/lifecycle.py`, `relationshipStatus.ts`), so a closed edge is drawn
dashed and treated as a former party by the risk engine.

### Ownership yes, officers no

Ended **ownership and control** is published, not dropped: the
OpenCorporates network, ARES shareholders and partners struck off the register
(`datumVymazu`), NZ Companies, RPVS, SEC EDGAR, Wikidata and Estonia. Someone
who held a company until last year is part of its ownership history and a due
diligence reader needs to see them.

**Officer and board-role lists stay limited to people serving now**: Companies
House officers, OpenCorporates officers, brreg roles (`fratraadt`,
`avregistrert`) and ARES directors. That is a deliberate scope choice (Phase
192, reaffirmed with Stephen on 9 Oct 2026), not a missing date: a former
director is not an owner, the lists would grow by an order of magnitude for
long-lived companies, and the registers already publish the history for anyone
who needs it. The tests in `tests/test_phase317_lifecycle.py` pin both halves.

An ended OpenCorporates network edge is its own record (`…/ended/<date>` on the
local id), so a later holding between the same two companies is a new
relationship rather than the old one reopened.

### When a startDate is an entry date

Some registers publish no date on which an interest began, only the day they
entered the record: ARES `datumZapisu` and UR Latvia `registered_on`. OpenCheck
uses that day as `startDate` — the closest the register comes — and annotates
it (motivation `transformation`, `bods/annotations.py::entry_date_as_start`),
because it can be later than the interest itself: a shareholder since 1995
entered in a 2003 migration.

### Another publisher's clock

When OpenCheck reads a republisher, the chain has a clock BODS has no field
for. Those dates travel as `commenting` annotations rather than being forced
into `statementDate` or `retrievedAt`:

- **OpenCorporates** `company.source.retrieved_at` — when OpenCorporates last
  read the register — on the subject entity's `/source`.
- **Wikidata** P813 "retrieved" on a claim's references, which since Phase 315
  dates the ownership edge's `statementDate`; the annotation says that is what
  the date is.

### Open Ownership's bulk data

Open Ownership publishes BODS itself, and its statements are immutable: their
`statementId`, `publicationDetails` and `source` describe OO's publication.
OpenCheck does not patch them. The stored OO bundles served for GLEIF and UK
PSC lookups are OO's statements verbatim, and the BODS quality sweep holds
OpenCheck's provenance rules only to statements OpenCheck published.

Where OpenCheck re-maps OO's bulk rows (the `bods_gleif` and `bods_uk_psc`
adapters, currently not registered), it publishes **its own** statements:
a new `statementId`, OO's `recordId` kept so the two link, OpenCheck's
`publicationDetails` and `source`, interest `endDate` from OO's data (so ended
ownership closes by the rule above), and a `commenting` annotation on `/source`
naming the OO statement and its publication date.

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
- A `startDate` that is the register's entry date (ARES, UR Latvia), and
  another publisher's clock (OpenCorporates' retrieval, Wikidata P813, an Open
  Ownership statement OpenCheck republishes) — see
  [Ended relationships](#ended-relationships-phase-317)

## See also

- [Data currency](sources.md#data-currency) — the liveness taxonomy behind
  `source.retrievedAt`
- [Which date goes where](sources.md#which-date-goes-where) — the same four
  clocks, summarised alongside the source table
