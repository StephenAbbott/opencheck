---
name: opencheck-due-diligence
description: How to use the OpenCheck MCP server (api.opencheck.world/mcp) and how to read what it returns honestly. Use it every time you read, summarise, count, tabulate or write code over any OpenCheck output — an opencheck_* tool result, a saved or recorded OpenCheck response, a BODS export, a subsidiaries list or a bulk dataset — and whenever the user mentions OpenCheck. Also use it for any question about a company's owners, beneficial owners, PSCs, parent, subsidiaries, sanctions or PEP exposure, an LEI, a UK LLP or a company register number, even if OpenCheck is not named, and before exploring many companies (it points to the bulk datasets instead of live lookups).
---

# OpenCheck due diligence

OpenCheck is an open-source demonstration of what an open standard (BODS v0.4)
makes possible: give it one ID and it gathers what open registers and datasets
say about a company's owners, controllers and risk exposure. It is careful to
say what it read and what it did not. Most mistakes agents make with it are not
in calling the tools — they are in reading the answer more confidently than the
answer allows. This skill is mostly about that.

## 1. Call sequence

Work from cheap to expensive. Only the tools marked **(budget)** spend the shared
lookup budget: ten fresh checks a minute per address, and a repeat of the same
check within 15 minutes is free. In a room of thirty people on one Wi-Fi, that
budget is shared by everyone.

1. **Find** — `opencheck_search(query)` returns ranked candidates, each with an
   `lei` (may be null), a `match` tier (`exact` … `fuzzy`), a one-line `summary`
   and `identifiers[]` (scheme + id). Prefer an `exact` match; read every exact
   candidate's summary, because the same name can come back once from GLEIF and
   once from a register with different facts. With a registration number, use
   `opencheck_resolve_national_id`.
2. **Set expectations** — `opencheck_knowability([jurisdiction])` (free) says
   what that country's registers publish about owners and who may see them.
   Call it before a lookup in an unfamiliar jurisdiction, so a thin answer is
   framed as the register's limit rather than as a finding.
3. **Check** —
   - candidate has an LEI → `opencheck_lookup(lei)` **(budget)**;
   - candidate has `lei: null` and an `identifiers[]` entry such as
     `GB-COH OC346224` → `opencheck_register_lookup(scheme, id, name=…)`
     **(budget)**. Do not give up because there is no LEI: most UK LLPs and most
     small companies anywhere have none.
   - then only what the question needs: `opencheck_owners` (who holds above a
     threshold) **(budget)**, `opencheck_ownership_map` **(budget)**,
     `opencheck_export_bods` (full graph) **(budget)**, `opencheck_subsidiaries`
     (free), `opencheck_person_check` / `opencheck_person_positions` for people.
4. **Keep** — `opencheck_save_report` **(budget)** freezes a check as a link
   with a SHA-256; `opencheck_discrepancy_note` **(budget)** drafts a note to a
   register when two records disagree.

**Many companies.** `opencheck_batch_lookup` takes at most 20 LEIs. For
anything bigger — "explore the 200 companies in this list", "find patterns
across a register" — do not loop over live lookups. Use the published bulk
datasets (Estonia, the Azerbaijani Laundromat); see
`references/bulk-datasets.md`. Say so to the user even if they asked for
lookups: it is faster for them and it is what the datasets are for.

**A 429** carries `retry_after_s`. Wait that long, then retry once. Never retry
at once and never fan out more calls to "get around" it.

## 2. Reading the answer

Each rule below exists because an agent (or a person) got it wrong on real
OpenCheck output. When you report, quote OpenCheck's own wording where it is
careful — it usually is.

### Degraded is not clean
`degraded_sources[]` lists checks that did not fully run: a source that did not
answer, a screen cut short (`reason: "truncated"` — e.g. "covered the 25
highest-ranked of 133 related parties"), a rate limit. The absence of a signal
those checks would have raised is **not** evidence of absence. If
`degraded_sources` is non-empty, say the screen is incomplete and name what was
not covered. In a batch, a row whose `degraded` is non-empty, or any `failed`
row, means that company was not fully screened — never report the batch as
"all clear".

### Risk is not context
Every signal has `kind`. `risk` is a finding about a party. `context` is
structural description (`NON_EU_JURISDICTION`, `GLEIF_REPORTING_EXCEPTION`,
`RECORD_CONSISTENCY`, `RELATED_PEP_SUBJECT_ROLE`, …) and is never a finding —
being outside the EU or having no GLEIF parent is not a red flag. Keep the two
lists apart, as OpenCheck's own `summary` does.

### A name match is not an identity
A related-party signal's `summary` says how the match was made. "matches a
record … (birth year and nationality also agree)" is corroborated. "Possible
name match only … with no birth date or nationality in common" is a lead to
check, not a fact: report it as a possible match and say what would confirm it.
`confidence` (`high`/`medium`/`low`) follows the same evidence. The same person
can appear in several signals from several sources — count people, not chips.

### Read the signal's exact words
`COUNTER_SANCTIONED` is a designation by a state such as Russia, not a Western
sanction — do not call it "sanctioned". `EXPORT_RISK` is a trade-risk flag, not
an export-control listing. `SANCTIONS_OWNERSHIP_TEST` is OpenCheck's arithmetic
on filed shareholdings under the OFAC/EU/UK 50% rules — ownership only, control
not computed — never a determination that the company is sanctioned.
`SANCTIONS_OWNERSHIP_TEST_POSSIBLE` is context: it would only be met on the
high end of a band or if a name-only match were confirmed.

### GLEIF consolidation is not ownership
A GLEIF parent (Level 2, `opencheck_subsidiaries`) is an **accounting
consolidation** relationship. A consolidating parent need not be a shareholder,
and GLEIF publishes no percentages. Say "consolidated by" / "consolidates",
never "owns", "holds" or "subsidiary owned x%". Leave percentage columns out of
any table built from it, rather than filling them from your own knowledge.
The list holds only children that have an LEI and report this parent, so it
is never "all subsidiaries": say so whenever you list or count them. A missing GLEIF parent with a
reporting exception (`NO_KNOWN_PERSON`, `NATURAL_PERSONS`, `NON_PUBLIC`…) is a
permitted filing, not a refusal to disclose.

### The LEI's status is not the company's status
`profile.lei_registration` describes the LEI record (ISSUED, LAPSED, RETIRED).
`profile.register_status` describes the company. When the LEI has lapsed,
GLEIF's "ACTIVE" is the last thing anyone declared, possibly years ago —
`register_status.liveness: "declared"` and its `sentence` say so. Never report
a company as active on that basis. If a register record exists (often visible as
a second search candidate with a register identifier), check it: it is the
authority on whether the company still exists.

### Declared owners, calculated owners and investment managers are different things
- **Declared beneficial owners** are people a register itself names
  (`beneficialOwnershipOrControl: true`, PSCs).
- **Calculated / integrated ownership** (`integrated_ownership`,
  `opencheck_owners.calculated_owners`) is arithmetic on filed shareholdings:
  multiplied along a chain, added across paths, **one figure per source, never
  added across sources**. Two sources giving 67% and 67% do not make 134%.
  When sources name different bodies of the same state (a finance ministry
  in one, "Government of X" in another), they are reading one state holding:
  give the figure once and say which source names which body.
  `may_be_above` means unknown, not "probably".
- **Form 13F managers** (`institutional_shareholders`) are investment managers
  reporting positions they manage. They are not owners or beneficial owners;
  positions overlap and must never be totalled.
- `disclosure_status` says per source what it states about owners (declared,
  exempt, not provided, restricted, no record, unreachable). "Exempt" (e.g. a
  listed company exempt from the UK PSC register) is a legal status, not
  missing data.

### Thin answers are often the register's limit
If a lookup finds few owners, check `knowability` before concluding anything:
some countries publish only officers, some restrict beneficial owners to
authorities, and some OpenCheck does not cover. "OpenCheck holds no register
notes for X" means OpenCheck cannot say, not that X publishes nothing.

### Licences travel with the data
`licensing.headline`, `licensing.warnings` and `license_notices` say whether
the result can be used commercially. Several sources (OpenSanctions,
EveryPolitician) are CC-BY-NC: pass the notice on whenever you hand on results
or exports.

## 3. Reporting

A good OpenCheck answer, in this order:
1. Who the company is (name, ID, jurisdiction) and its status — from the
   register when there is one, with the LEI status said separately.
2. Owners and controllers: declared ones first, then calculated figures with
   their source, then anything listed without a figure.
3. Risk findings, each with how firmly it was matched; then context, labelled
   as context.
4. What was **not** read: degraded checks, sources with no record, the
   jurisdiction's limits.
5. Licence notice if the result will be passed on.

Never grade or rank companies ("high-risk", "clean"). OpenCheck does not, and
the evidence rarely supports it. Describe what the records show.

## References

- `references/bods-shapes.md` — the BODS v0.4 statements you meet in
  `opencheck_export_bods` and the bulk datasets, and OpenCheck's extension keys.
  Read it before parsing an export or writing a graph query.
- `references/bulk-datasets.md` — what is published, where, in which formats,
  and how to load it. Read it whenever the task involves more than ~20
  companies or offline analysis.
