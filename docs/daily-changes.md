# The daily changes list (shadow experiment)

Every LEI whose GLEIF record or OpenSanctions record changed on a given UTC
day, graded so that the few changes worth reading are not buried under the
thousands that are not. From the Notion ticket *Daily deltas for OpenCheck*
(13 Nov 2026).

**This is a two-week shadow experiment.** The list is computed and served as
JSON. There is no page for readers and no downloadable BODS file yet; whether
to build them is decided from what these two weeks show. The shape of the
JSON may change.

## What it is, and what it is not

It is a list of what **GLEIF and OpenSanctions changed**. No OpenCheck check is
run for any entity on it: building it costs no lookup and no upstream call per
entity. A row says "GLEIF now records a different direct parent for this LEI",
never "OpenCheck found".

- The GLEIF side reads OpenCheck's Golden Copy mirror immediately before and
  after each delta is applied (three times a day) and compares the material
  fields, the ones the watchlist reads.
- The OpenSanctions side reads the entity delta files of the `default`
  collection against a small local table of what each LEI-bearing organisation
  looked like before: its topics and its datasets. It matches on the LEI
  OpenSanctions holds (`leiCode`), never on a name, and reads organisations
  only, never people.

## The bands

Measured over 6–13 Nov 2026, a weekday's GLEIF delta names 7,000–24,000 LEIs.
Between 55% and 80% of those rows are renewal churn, which is counted and
never stored. The rest are graded:

| Band | Label | What is in it | Estimate per weekday, from the 6–13 Nov measurement |
|---|---|---|---|
| `headline` | Ownership and sanctions | A direct or ultimate parent changed; a successor named; a completed merger, absorption, demerger or similar event; on OpenSanctions, a sanctions, export-control, debarment, enforcement or crime topic added, or the organisation added to a list of that kind | 20–90 |
| `ceased` | Ceased | The entity became inactive, or a dissolution, liquidation or bankruptcy event completed, on an LEI that was current | 90–220 |
| `ceased_lapsed` | Ceased, on an LEI that had already lapsed | The same, where the LEI had lapsed before GLEIF recorded the ending. The company ceased some time ago and the record is catching up | 90–230 |
| `record` | Record changed | Legal name, legal form, jurisdiction, address country, another completed event, an ending in progress; on OpenSanctions, a topic removed or a non-list dataset added | hundreds |
| `note` | Note only | Reporting exceptions, the register identifier, the LEI's registration status, category and conformity flags | 1,500+ |
| `new` | New LEI | An LEI the mirror did not hold before | about 1,200 |

A row carries the most notable band any of its reasons reaches. `note` holds
the two things that would drown a site-wide list: reporting-exception moves
(about 1,500 a day) and bulk re-keying of register numbers by an LEI issuer
(1,700 on 13 Nov 2026 alone).

**Sole proprietors are left out** (GLEIF category `SOLE_PROPRIETOR`): the legal
name is a person's. They are counted.

## The routes

- `GET /daily-changes` lists the days held, newest first, with counts by band
  and by source.
- `GET /daily-changes/{YYYY-MM-DD}.json` serves one day. By default it carries
  `headline`, `ceased`, `ceased_lapsed` and `record`; add `?band=note`,
  `?band=new` or `?band=all` for the rest. A day with nothing on it is
  answered with no rows.

Both answer 503 when the feature is off (`OPENCHECK_DAILY_CHANGES_DB_FILE`
unset). `complete` is false for today, whose list grows with each delta.

Each row: `lei`, `name` and `jurisdiction` (GLEIF's), `source`, `band`,
`reasons` (a closed vocabulary), `changes` (for GLEIF, each field's old and new
value; for OpenSanctions, topic codes and dataset names added or removed),
`as_of` (the GLEIF publish or the OpenSanctions version), and for an
OpenSanctions row a `ref` with the OpenSanctions entity id and link.

## OpenSanctions rows are references

OpenSanctions data is licensed CC BY-NC 4.0. A row carries the entity id, topic
codes and dataset names, and links to the record on opensanctions.org. It does
not carry OpenSanctions' names, addresses or any other property, and the store
keeps none. The name on the row is GLEIF's.

The first run takes a baseline from the full export and stores no rows. If a
version can no longer be read (it aged out of the version list, or its delta
is gone), the gap is counted and a new baseline taken, because a difference
across a gap cannot be dated to a day.

## What the experiment is measuring

`/signalstats` carries a `daily_changes` section: the last fourteen days'
counts by band and by counter (`named`, `churn`, `sole_proprietor`,
`not_an_lei_here`, `gap`), and never an LEI or a name. The questions for the
end of the two weeks:

1. Is `headline` small and interesting enough to read every day?
2. Do `ceased` and `ceased_lapsed` belong apart?
3. Does anything in `note` belong higher, or anything in `record` lower?
4. How many OpenSanctions rows are there, and are they the ones a reader would
   want?

Thirty days are kept (`OPENCHECK_DAILY_CHANGES_KEEP_DAYS`).

## Not in the experiment

A page on the site, a downloadable daily BODS file, a feed, changes from any
register other than GLEIF and OpenSanctions, and any change to how the
watchlist grades changes for a watched company.
