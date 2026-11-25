---
name: opencheck-due-diligence
description: Run a company due-diligence check with OpenCheck's tools, from a name or a number to a saved report. Use when asked who owns or controls a company, whether it is sanctioned or linked to a politically exposed person, or to screen a list of companies.
---

# Due diligence with OpenCheck

OpenCheck answers from open registers and datasets, keyed on the company's
Legal Entity Identifier (LEI) or, when it has none, its number on a national
register. Work in this order. The cheap steps come first because the lookup
tools spend a shared budget (ten fresh checks a minute per address).

## 1. Find the company

- A name: `opencheck_search(query)`. Candidates come ranked best first, each
  with `match` (exact, same_name, all_tokens, distinctive_tokens, fuzzy), a
  jurisdiction, an LEI where one exists, and `identifiers[]` for register
  numbers. If two candidates are both plausible, ask the user which one.
  Never pick one silently.
- A registration number: `opencheck_resolve_national_id(number, country)`.
- No LEI (most UK LLPs, most small companies): take a candidate's
  `identifiers[]` entry and call `opencheck_register_lookup(scheme, id)`, for
  example `GB-COH`, `OC346224`.

## 2. Say what the jurisdiction can show, before you look

Call `opencheck_knowability([jurisdiction])`. It is free. It says what the
country's registers publish about owners, who may see it, and which
OpenCheck sources read it. Cyprus publishes officers, not beneficial owners;
an EU register may be closed to the public. Use it to frame the answer: a
missing owner there is the register's limit, not a finding about the company.

## 3. Check

`opencheck_lookup(lei)` (or the register lookup) gives the verdict sentence,
identity and register status, owners and controllers, risk signals and which
sources answered. Read [reading-opencheck-results](../reading-opencheck-results/SKILL.md)
before you report it.

Then only what the question needs:

| Question | Tool |
|---|---|
| Who holds more than a threshold? | `opencheck_owners` |
| What does the structure look like? | `opencheck_ownership_map` |
| What does it own or consolidate? | `opencheck_subsidiaries` |
| The full graph for another system | `opencheck_export_bods` |
| Is this one person sanctioned or a PEP? | `opencheck_person_check` |
| What else does this person hold, by register key? | `opencheck_person_positions` |

For the ownership questions, see
[opencheck-ownership-questions](../opencheck-ownership-questions/SKILL.md).

## 4. Keep it or hand it on

- `opencheck_save_report(lei)` stores the run as a link with a SHA-256 the
  reader can check. It creates a new report each time, so save once.
- `opencheck_discrepancy_note(lei)` drafts a note to a register when its
  record disagrees with another register's. It sends nothing.

## Many companies

`opencheck_batch_lookup(leis)` screens up to 20 LEIs, one row each. A `failed`
row or `degraded: true` means that company was not fully screened; never call
the batch clean while either is present. For hundreds of companies, point the
user to the published bulk datasets
(https://github.com/StephenAbbott/opencheck/releases) rather than looping.

## When the budget runs out

A 429 with `retry_after_s` means wait that many seconds, then retry once. A
repeat of the same check within 15 minutes is free, so do not re-run a lookup
to re-read it.
