# Identifiers in OpenCheck's BODS output

Every BODS statement OpenCheck publishes carries three identifiers that look
alike — `opencheck-` followed by 24 hex characters — and answer three different
questions. This page says what each one is for, what stays the same between
two runs, what changes, and how a data user should read them. It is the
identifier counterpart to [Dates](dates.md).

OpenCheck is a **republisher**: it reads other people's registers and emits
BODS statements about what they say, one bundle per source per lookup. The
rules below follow from that, and from the standard's own guidance on
[record identifiers](https://standard.openownership.org/en/0.4.0/standard/modelling/record-ids.html)
and [information updates](https://standard.openownership.org/en/0.4.0/standard/modelling/information-updates.html).

## The three identifiers

| Field | Question it answers | Stable across runs? | Derived from |
|-------|---------------------|---------------------|--------------|
| `recordId` | Which record of which party is this? | **Yes** — the key everything joins on | The source, the record kind and the source's own local identifier for the record |
| `statementId` | Which statement — which version of that record — is this? | Only while the record's content is unchanged | The `recordId` and a digest of the statement's content (Phase 398) |
| `declarationSubject` | Whose beneficial ownership network is this statement part of? | Yes, for a given source | The `recordId` of that source's own record of the looked-up company (Phase 396) |

### `recordId` — the stable key

A `recordId` names a record: one party or one relationship as one source
records it. It is `opencheck-` + the first 24 hex characters of a SHA-256 over
the source id, the record kind (`entity`, `person` or `relationship`) and the
source's own local identifier — a Companies House company number, a GLEIF LEI,
an OpenSanctions entity id, a register's row key. The same record in the same
source therefore has the same `recordId` on every run, and two sources' records
of the same company have different ones, because they are different records.

BODS asks for exactly this: record identifiers "MUST be unique within the
publisher's system and never reused to refer to a different element", and when a
record is updated "the new Statement generated must contain the same recordId
as in the outdated Statement".

### `statementId` — this version of the record

A `statementId` names one statement. Since Phase 398 it is content-addressed:
the SHA-256 of the `recordId`, the word `content` and a digest of the
statement's `recordDetails` and `recordStatus`. Two consequences:

- **Same content, same statement.** A lookup run twice against a register that
  has not changed publishes the same statement, with the same `statementId`.
  Replay caches, saved reports and exports compare equal.
- **Changed content, new statement.** When a register changes a record — a new
  name, a new address, a holding that ends — the next run publishes a new
  statement about the same record: a new `statementId`, the same `recordId`.
  That is the update model the standard describes; before Phase 398 the two
  ids were equal for entities and people, so a changed record silently kept
  the id of the statement it replaced.

What is **outside** the digest, and so never moves a `statementId`:
`annotations`, `statementDate`, `publicationDetails`, `source.retrievedAt`, and
`recordDetails.publicListing` (the primary listing is stamped on the subject's
statement after the sources settle, and the graph node must keep its id). A
re-read that found nothing new publishes the same statement even though
OpenCheck read it on a different day.

Relationship statements follow the same rule. A relationship that ends is
published `closed` (see [Ended relationships](dates.md#ended-relationships-phase-317)):
same `recordId`, new `statementId`, because `recordStatus` is in the digest.

### `declarationSubject` — whose network

BODS v0.4 defines `declarationSubject` as "a recordId value for the subject of a
beneficial ownership network", and every statement about a declared network
"MUST contain the subject's recordId as its declarationSubject".

OpenCheck publishes **one declared network per source bundle**: every statement
a source contributed — the company, its owners, officers and the relationships
between them — carries the `recordId` of **that source's own record of the
looked-up company**. A Companies House board member names Companies House's
record of the company; a Wikidata owner names Wikidata's. A bundle read on its
own is therefore a declared network in its own right, and the merged export
carries one declaration per source (Phase 396; decided 23 Oct 2026).

Where one source returns several records of the looked-up company — OpenAleph
holds a PSC-derived record of Ørsted A/S for each UK filing that names it — the
source's statements are folded onto one of them, so a source still names one
subject. The subject's own statement names itself.

Before Phase 396 every entity and person statement named itself and a
relationship named its immediate subject; an export from that period shows
`statementId == recordId == declarationSubject` on every party.

### Release files: many networks, one record per party

The bulk datasets — the [Estonia dataset](../backend/scripts/sweep_estonia.py)
(`dataset-estonia-<cut>`) and the laundromat dataset — hold **one declared
network per company mapped**, built by the same per-bundle rule as a lookup, and
**one record per party**. A party can sit in several of those networks: a person
on two boards, or a company that is its own network's subject and a shareholder
of three others. BODS's
[Record identifiers](https://standard.openownership.org/en/0.4.0/standard/modelling/record-ids.html)
guidance would make such a party one record per network. The release files keep
one record, so a person stays one node across companies, and stamp it with one
network (decided 25 Oct 2026):

- a relationship names the subject of the network it was mapped in;
- a network's subject names itself;
- any other party names the first, by `recordId`, of the networks whose
  relationships reference it — so the choice does not depend on the order the
  file was built in.

To find every network a party appears in, follow the relationships that
reference its `recordId`, not its `declarationSubject`. Each build records the
subject of every network it maps and checks the file against that list before
release; the manifest gives the number of networks.

## Relationships reference parties by `recordId`

A relationship's `subject`, `interestedParty` and `componentRecords` hold a
party's **`recordId`**, as BODS v0.4 requires — never its `statementId`. To find
the party a relationship names, look up the `recordId` among the bundle's entity
and person statements; where several statements share a `recordId` (two
versions of one record in one export), the latest is the current reading.
OpenCheck's own consumers resolve references this way (`bods/refs.py` on the
backend, `bodsRefs.ts` on the frontend) and never compare a reference against
`statementId`s directly.

## Another publisher's statements

Statements OpenCheck hands on verbatim — the OECD-UNSD MEIP Global Register,
Open Ownership's stored GLEIF and UK PSC bundles — keep their publisher's own
`statementId`, `recordId` and `declarationSubject`. Their `recordId`s do not
start `opencheck-`, and their references are resolved by the same rule. Where
OpenCheck re-maps Open Ownership bulk rows into its own statements, those carry
OpenCheck's publication and source blocks and OO's `recordId`, with an
annotation naming the OO statement they were drawn from.

## Reading two exports

- To follow **a record** between two exports, compare on `recordId`.
- A `statementId` that **differs** between two exports of the same `recordId`
  means the record's content changed between the two runs — or that OpenCheck's
  reading of it changed (a mapper that now carries a field it did not before
  also produces a new statement).
- The **same** `statementId` on two days means the same content: nothing the
  digest covers changed. The dates and annotations may still differ.
- A `declarationSubject` tells you **which source's network** a statement
  belongs to; it does not identify the merged company across sources. The
  merged node is the referent group the sources' identifiers join on.

## Checks that hold these rules

- The weekly [BODS quality sweep](bods-quality.md) fails `stale_statement_id`
  on a statement whose `statementId` is not the content-addressed id of its
  `recordDetails` and `recordStatus`, and `declaration_subject_not_subject` on
  a declaration that names no party, a source naming more than one subject, or
  a subject that does not name itself.
- The test suite recomputes the id of every statement every mapper yields,
  checks every registered mapper is content-addressed, and runs every consumer
  over a bundle whose `statementId`s differ from its `recordId`s.
- `/signalstats` → `declaration_subject` counts how each source's subject was
  found (`ref`, `hit_id`, `single`) and when it was not (`unresolved`).
- `sweep_estonia.py check` refuses an Estonia release in which a statement
  names no recorded network, a network's subject names another network, or a
  party names a network none of its relationships belong to. The laundromat
  build counts the same breaches in its manifest
  (`declaration_subject_problems`) rather than refusing: its subjects are real
  lookups, and a source a lookup could not resolve to its subject keeps the
  factory default there too.
