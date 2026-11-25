---
name: reading-opencheck-results
description: How to read and report an OpenCheck lookup honestly, covering sources that did not answer, risk versus context signals, name matches, ownership figures and licences. Use before you summarise any opencheck_lookup, opencheck_register_lookup, opencheck_owners or opencheck_batch_lookup answer.
---

# Reading an OpenCheck result

OpenCheck reports what open sources say and is careful about what they do
not say. Keep that care when you pass the answer on. These are the rules.

## A source that did not answer is not a clean result

`degraded_sources` lists every source that failed, timed out, was rate
limited or answered only in part, with the check it was running (`check`)
and why (`reason`). If it is not empty, the screen is **incomplete**. Say
which sources did not answer, and never write "no sanctions found" or
"clean" for a check that one of them would have made. The `summary` and
`verdict` already say so, so do not drop that clause when you shorten them.

`coverage` counts the applicable sources: with data, answered with no
record, did not answer. "Answered with no record" is an answer. "Did not
answer" is not.

## Risk and context are different things

Every signal has `kind`:

- `risk` is a finding against the subject or a related party: sanctioned,
  sanctions-linked, PEP, debarment, export control, offshore leaks, the
  sanctions 50% ownership test.
- `context` describes how the company is put together: a non-EU
  jurisdiction, ownership layers, a trust in the chain, state control, the
  records-compared note. **Context is never a finding.** Do not list it
  among the risks or count it as one. `counts.risk_signals` and
  `counts.context_signals` are kept apart for this reason.

OpenCheck does not grade or rank companies. Do not add a score.

## A name match is evidence, not identity

A signal whose summary says "Possible name match only", or whose confidence
is medium or low, matched on a name and perhaps a birth year. It is not a
claim that the record is the same party. Say it as "a possible match to
…", give the record, and leave the decision to the user. A `former` related
party held a role or stake that has ended; say "former".

## Ownership figures

- Figures are **per source**. Two sources can give the same holder different
  figures. Report them side by side, each with its source, and **never add
  figures across sources**.
- Integrated ownership multiplies along each path and adds across paths
  within one source's records. A range ("at least 25%", "25–50%") is what the
  source stated; do not turn it into a single number.
- "No owner found" from a register that does not publish owners (see
  `opencheck_knowability`) is a limit of that register, not a finding.
- `disclosure_status` says, per source, whether owners were declared, exempt,
  not provided, restricted or unreachable. Exempt (a listed company) and
  restricted (a closed register) are not the same as "none".

## Status and identity

- `profile.register_status` is what the register says the company is. The
  LEI's own registration status (`lei_registration`) is about the LEI record:
  a lapsed LEI does not mean the company is dissolved.
- GLEIF's parents and children are **accounting consolidation**, not
  shareholding. Say "consolidated by", never "owned by".

## Licences

Pass on `license_notices` with anything you reproduce. Some sources are
CC BY-NC (OpenSanctions among them): fine for this user's own due diligence,
not for commercial redistribution. When you quote, name the source each fact
came from.

## Say what was read

End a report by naming the sources that answered and the ones that did not,
and the date you ran the check (or the saved report's date). A reader
should be able to tell an empty answer from an unasked question.
