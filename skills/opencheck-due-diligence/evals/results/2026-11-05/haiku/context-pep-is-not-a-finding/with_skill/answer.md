# Politically exposed persons connected to Ørsted A/S

**Short answer: no confirmed PEP.** The OpenCheck lookup (LEI `W9NG6WMZIYEU8VEDOG48`, recorded 2026-11-05) names 9 related people with a PEP flag, but only 2 of those are risk findings, and both are possible name matches that have not been confirmed. The other 7 are flagged only because of their role at the company. The PEP screen is also incomplete, so this is not a clean result.

## The 9 PEP rows

**Risk findings (2): possible name matches only**

| Person | Source | Basis |
|---|---|---|
| Karl Johnny Hersvik | OpenSanctions (also EveryPolitician, OpenAleph) | Shares a name with a PEP record. No birth date or nationality in common to confirm it is the same person. Listed on the Ørsted board. |
| Julia Elizabeth King | OpenAleph | Shares a name with a record in "Denmark, Faroe Islands and Greenland PEPs". No birth date or nationality in common. Listed on the Ørsted board. |

**Context only (7): PEP status comes from their Ørsted role**

These are not findings against the company. They say who runs it, and OpenSanctions lists them as politically exposed only because of that role.

- Andrew Richard Dingley Brown (Board, Vice Chairman)
- Benny Gøbel (Board)
- Julian David Waldron (Board)
- Rasmus Errboe (Executive Management)
- Lene Skole-Sørensen (Chairman of the Board)
- Ruchit Majmudar (Board)
- Trond Ødegård Westlie (Executive Management)

## Why this is not a clean screen

The lookup reports two truncated checks, so the PEP count is a lower bound:

- **Related-party PEP and sanctions screening** covered 25 of 47 related parties. The other 22 were not screened. Those 22 are current parties with no ownership or board role recorded.
- **Offshore Leaks screening** covered 30 of 47 related parties.

The absence of a PEP signal for the 22 unscreened parties is not evidence that they are not PEPs.

## Other context

- The risk signal `STATE_CONTROLLED` is separate from PEP status. The Government of Denmark holds about 50.12% (Climate TRACE) or 50.1% (Wikidata). This is a possible state-owned enterprise indicator, not a PEP finding.
- No parent is filed with GLEIF. This is a permitted reporting exception, not a risk signal.
- Beneficial owners in the Danish register (CVR) are not public. The lookup reads CVR for officers and holdings only, so the beneficial owner picture is incomplete.

## Licensing

Do not use the PEP data commercially without checking. OpenSanctions and EveryPolitician are CC-BY-NC-4.0. OpenAleph uses per-collection licences that need checking. Attribution is required, and share-alike applies to OpenCorporates data.

## What would settle the two possible matches

Check Karl Johnny Hersvik and Julia Elizabeth King against a primary source, such as their birth dates and nationalities, before treating either as a PEP. Re-running the lookup will not resolve the truncated checks. Those need the full related-party list, which the bulk export or a narrower lookup can provide.
