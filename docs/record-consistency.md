# Records compared — when the sources describing one company are read against each other

OpenCheck reads a company from many open sources at once: GLEIF, the national register,
OpenCorporates, OpenAleph, OpenSanctions, Wikidata and the rest. Where two or more of
them describe the *same* legal person, OpenCheck compares what they say on a short list
of facts and reports the result as one observation, **"Records compared"**, in the
structural-context block under Risk signals. It is never a risk signal, it is never
counted in "N risk signals", it never appears on the share card and it never badges a
node on the graph. It says what the records contain — how many sources, what they agree
on, what each disagreement is — and nothing about what to conclude.

This page explains the three rules that decide what gets compared, the gate that
decides what gets shown, and how to read the sentence.

## Three rules

**1. Same referent first.** Statements are grouped by a shared *strong* identifier: an
LEI, or a register number within one named register — the same rule the FullCheck
network merges on. A name match is not a referent. Two same-named companies with
different incorporation dates are two companies, not a disagreement, so the comparison
runs only after identity has been established by an identifier.

**2. Same concept only.** A comparison exists only where both sources assert the same
thing. Wikidata's *inception* (P571) is the founding of the business — Novo Nordisk 1923,
Shell 1890 — while a register records the incorporation of the current legal person
(1931, 2002), so the two are never compared. GLEIF's `entity.creationDate` is supplied by
the registrant and, measured across seven registers on 1 Oct 2026, differed from the
register's date on 13–100 % of pairs, so it is a different concept outside the UK and is
not compared either. ANAF Romania's date is the fiscal registration, not the trade
register's; the OECD's MEIP country column is the economy the group register files the
entity under, not its legal jurisdiction; OpenSanctions' `incorporationDate` on a
company is whatever its originating dataset said. Each exclusion is written on the
`Comparison` in `backend/opencheck/consistency.py` with the measurement behind it.

**3. Independent lineage.** A difference between a source and its own upstream —
OpenCorporates behind Companies House, OpenAleph's register copies behind the register,
OpenSanctions' register datasets behind theirs — is *staleness of the copy*, not a fact
about the company. It is counted (`stale`) and, by decision, never shown. Agreement
between them (`mirror`) is not corroboration and is never said as "two sources agree".
`backend/opencheck/sources/lineage.py` declares who republishes whom.

## The gate

Nothing reaches a reader because a rule says it should; it reaches a reader because
production measured it. Since 2 Sept 2026 every comparison outcome has been counted at
`GET /consistencystats` (aggregate only: field names, adapter ids, relation names — no
values, no names, no identifiers), persisted across deploys since 1 Oct. The gate, agreed
before anything was built: a `field|source_a|source_b` row may be shown only when its
measured disagreement rate is under 10 % **and** it has fired at least once, after at
least two weeks of traffic. A row above 10 % is two sources answering different
questions: it is re-aligned or dropped, never shown.

The reading of 15 Oct 2026 (41,100 lookups, 13 days 21 hours) decided what the page
shows today. The lists are in `backend/opencheck/consistency_allowlist.py`, generated
from that reading and pinned by a test:

- **Register status** (liveness: is the company live, or dissolved / struck off?) — the
  comparison this work was started for, and it holds: 17,479 decided pairs, 4.6 %
  disagree. Fifteen register ↔ GLEIF pairs under 10 % are shown, plus Romania's two
  registers against each other. Austria's Firmenbuch (11.1 %), Brazil's CNPJ (10.2 %),
  Corporations Canada (10.3 %) and Switzerland's Zefix (19 pairs) are held until re-read.
- **Jurisdiction** — 36,348 decided, 73 disagree (0.2 %). Every pair with at least
  twenty decided comparisons is shown.
- **Incorporation date** — not shown. Register ↔ OpenAleph split five passing, six above
  10 %; which OpenAleph collection a date came from is not yet known per record, so the
  second party in the comparison is unknown. Counted and logged.
- **Register number** (a clash on a one-per-entity identifier) — not shown. Once the
  identifier that bridged two statements is no longer counted as agreement, this field
  has no agreements to count against, so its rate needs a different denominator
  (`compared`, added 15 Oct); Romanian numbers also needed a canonical form first.
  Counted and logged.

A comparison that is counted but not shown still exists: it is in the counters, every
independent disagreement is in the server log with both values, and the next reading
decides again.

## Reading the sentence

> Four sources describe this entity (three independent). They agree on jurisdiction.
> One disagreement: UK Companies House records the register status as dissolved
> (since 3 Apr 2019); GLEIF records ACTIVE. OpenCorporates did not answer, so its
> records were not compared.

- *Four sources … three independent* — how many adapters contributed a statement about
  this company, and how many of those are not copies of one another.
- *They agree on …* — the shown fields on which every independent pair agreed. Said even
  when there is nothing else to say, so that silence never reads as "checked and clean":
  a company nobody compared and a company whose sources all agree are different things.
- *One disagreement: …* — each disagreement on a shown comparison, with both values as
  the sources wrote them. The register's own word is used where it published one
  ("dissolved", "ACTIVE", "gelöscht"); the date is the one the register gave.
- *… did not answer, so its records were not compared* — a source that failed to answer
  and would otherwise have been compared with one that did. A comparison that could not
  run is said in the same voice as one that did.
- *No field could be compared between independent sources* — two or more sources, but
  each a copy of another, or nothing comparable stated by two independent ones.

Expanding the chip shows the per-field table: field, source A and its value, source B
and its value, disagreements first.

## What a disagreement means, and does not

A register saying a company is dissolved while GLEIF still says ACTIVE is a fact about
two records. It may mean the LEI issuer has not yet retired the LEI (the LEI ROC policy
requires it to), or that the register's status is newer than GLEIF's last check, or that
the two records are not about the same company after all despite sharing an identifier.
OpenCheck does not choose between these. The observation is there so that an analyst
sees that the records differ and can look at both; it is not a finding against the
company and is not weighed in the verdict.

Two things are deliberately **not** compared as current readings. A company in
liquidation (`pending`) against a GLEIF ACTIVE is one situation described at two stages,
not a disagreement. And GLEIF's ACTIVE on an LEI whose registration has lapsed is the
last thing the company declared to its issuer, not a current reading — the profile shows
it as "last declared active" — so such a side is left out of the comparison (one
missing), never set against the register.

## In the exported bundle

Each shown disagreement travels with the BODS export as a `commenting` annotation on
**both** statements, pointed at the field (`/recordDetails/dissolutionDate`,
`/recordDetails/jurisdiction`, or `/recordDetails` when the field is carried only as a
status annotation), naming the other source, its statement and its value. A saved
report exports what its run said; nothing is recomputed at export time.

## For operators

- `GET /consistencystats` — the counters. `pairs` is `field|source_a|source_b` →
  `{agree, disagree, mirror, stale, one_missing, disagree_rate, compared,
  disagree_per_compared}`; `compared` (top level and per row) counts the pairs of
  statements from two sources that were compared at all. `persisted`, `boots` and
  `since` say how long the window is.
- `OPENCHECK_CONSISTENCYSTATS_DB_FILE` — where the counters are written through
  (`render.yaml` puts it on the persistent disk). Unset, the counters reset on deploy.
- The server log — `consistency disagree field=<field> sources=<a>/<b>
  statements=<id>/<id> values=…` — one INFO line per independent disagreement, shown or
  not. `grep "consistency disagree field=liveness" | grep firmenbuch` is how the next
  reading of a held row gets explained rather than hypothesised.

The three production readings, their tables and the reasoning behind each re-alignment
are logged on the Notion plan *Implementation plan: record-consistency observation
(Phases A–E)*; the phases that built this are 150 (lineage), 151 (uniform liveness),
152 (shadow-mode counters), 268 (the instrument, re-aligned), 269 (Firmenbuch's renamed
firms) and 337 (the observation).
