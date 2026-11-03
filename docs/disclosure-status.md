# Disclosure status by source

What each source says about a company's owners, as one normalised value per
source. It is computed once per lookup (`backend/opencheck/disclosure.py`),
rides its own `disclosure_status` event, and is frozen with the run, so a
saved report shows what the sources said on the day it ran.

It answers a narrower question than the risk signals. Open Ownership's guidance
on usable beneficial ownership data makes the point that **absence from a
dataset is not the same as an exemption**. A company with no owner on a
register may be exempt, may be withholding information, may be outside what
the register covers, or the register may simply not have answered. OpenCheck
already knew most of this through separate channels; this page says how each
channel maps to one vocabulary.

The status **describes and raises nothing**. No value produces a risk signal,
changes the verdict or reaches the AI summary. `OPAQUE_OWNERSHIP` and
`GLEIF_REPORTING_EXCEPTION` are unchanged (see [risk-signals.md](risk-signals.md)).

## The values

| Value | Words on the chip | Meaning |
|---|---|---|
| `declared` | Declared (N) | The source names N current owners or controllers (or, for GLEIF, a consolidating parent). An owner the source says exists but does not name (`informationUnknownToPublisher`) counts as declared. |
| `none_exist` | Source says none exist | The source says there is no owner to declare: a Companies House "no registrable person" statement, a Ukrainian filed absence with a reason, Estonia's fallback (no owner identified; board members named instead). |
| `exempt` | Exempt | A lawful exemption from filing: a Ukrainian statutory or legal-form exemption, Estonia's "need not file", a GLEIF reporting exception that says there is no consolidating parent. |
| `not_provided` | Not provided | Required but not supplied: Companies House "PSC exists but not identified", "contacted but no response", restriction notices; GLEIF's `NON_PUBLIC` family. |
| `none_filed` | None on record | The source holds the company's record but no owner and no reason. Never worded as an exemption or as non-compliance. |
| `restricted` | Not public | The source is the national register of a country whose beneficial-ownership tier is closed to the public under the [knowability](knowability.md) access status. |
| `unreachable` | Not read | OpenCheck could not read the source, or read it only in part (a `source_fetch` / `source_read` degradation; Estonia's REST tier, which has no beneficial owners). |
| `no_record` | No record of the company | An ownership source answered and holds nothing on this company. |
| `not_covered` | Not an ownership source | The source's job does not include owners. Not chipped; named once under the source cards. |

`none_filed` and `no_record` are OpenCheck's additions to the seven values on
the ticket (Stephen, 3 Nov 2026).

## What each value covers

Every entry also says what it is about:

| `covers` | Used by |
|---|---|
| `beneficial_owners` | Beneficial-ownership registers: Companies House PSC, Estonia, Ukraine ЄДР, Latvia, Slovakia RPVS, Nigeria CAC, Washington DC, EITI beneficial-ownership declarations |
| `shareholders` | Shareholder and founder lists: Estonia, Ukraine, Latvia, SEC EDGAR 13D/13G (the SEC's beneficial owners of 5% or more of a class), New Zealand, Czechia ARES, Austria, Brazil, Greece, Moldova, Global Energy Monitor (ClimateTRACE), Wikidata |
| `parent` | GLEIF Level 2 — the accounting consolidating parent, never an owner |

GLEIF's "exempt" is therefore "Parent: Exempt": there is no consolidating
parent under the accounting definition. It is not a beneficial-ownership
exemption, and the chip says which it is.

Taiwan's GCIS is deliberately not an ownership source here: it publishes
directors' and supervisors' holdings, never "the shareholders".

## Every value a source supports is listed

There is no precedence rule. Companies House can name two PSCs and also file
"at least one beneficial owner unidentified"; the card shows both
"Beneficial owners: Declared (2)" and "Beneficial owners: Not provided".

## Mapping from each existing channel

### BODS `unspecifiedReason` (any source)

| `unspecifiedReason` | Value |
|---|---|
| `noBeneficialOwners` | `none_exist` |
| `subjectExemptFromDisclosure` | `exempt` |
| `interestedPartyExemptFromDisclosure` | `exempt` |
| `subjectUnableToConfirmOrIdentifyBeneficialOwner` | `not_provided` |
| `interestedPartyHasNotProvidedInformation` | `not_provided` |
| `informationUnknownToPublisher` | `declared` (an owner exists, unnamed) |
| `unknown`, or anything else | `none_filed` |

### Companies House PSC statements

| PSC statement | `unspecifiedReason` | Value |
|---|---|---|
| `no-individual-or-entity-with-signficant-control`, `no-beneficial-owner-identified` | `noBeneficialOwners` | `none_exist` |
| `psc-exists-but-not-identified`, `psc-details-not-confirmed`, `steps-to-find-psc-not-yet-completed`, `awaiting-confirmation-from-psc`, `at-least-one-beneficial-owner-unidentified` | `subjectUnableToConfirmOrIdentifyBeneficialOwner` | `not_provided` |
| `psc-contacted-but-no-response`, `psc-has-failed-to-confirm-changed-details`, `restrictions-notice-issued-to-psc`, `information-not-provided-for-at-least-one-beneficial-owner`, and the combined unidentified-and-not-provided statement | `interestedPartyHasNotProvidedInformation` | `not_provided` |
| Positive update-period declarations (`all-beneficial-owners-identified`, …) | none (no statement) | — |

The `-partnership` variants map the same way. A company with no PSC and no
statement reads `none_filed`.

### GLEIF reporting exceptions (`covers: parent`)

| Exception | Value |
|---|---|
| `NATURAL_PERSONS`, `NO_KNOWN_PERSON`, `NON_CONSOLIDATING`, `NO_LEI` | `exempt` |
| `NON_PUBLIC` and its deprecated family (`BINDING_LEGAL_COMMITMENTS`, `CONSENT_NOT_OBTAINED`, `DETRIMENT_NOT_EXCLUDED`, `DISCLOSURE_DETRIMENTAL`, `LEGAL_OBSTACLES`) | `not_provided` |
| A named parent | `declared` |

### Ukraine ЄДР filed absences

| Absence reason | `unspecifiedReason` | Value |
|---|---|---|
| No qualifying person; reason in the ownership-structure filing; state or municipal founder | `noBeneficialOwners` | `none_exist` |
| Statutory exemption; legal-form exemption | `subjectExemptFromDisclosure` | `exempt` |
| Reason not stated, blank, other | `unknown` | `none_filed` |

### Estonia (read from the bundle, not the statements)

| e-Business Register | Value |
|---|---|
| Not obliged to file (`esitab_kasusaajad: false`) | `exempt` |
| Obliged, none in the open data ("due, none published") | `none_filed` — an entry can be withheld for a person at risk or a minor, and the open data does not say so |
| Only `J` / `C` rows (board or council members named because no owner was identified) | `none_exist` |
| Read from the REST tier, which has no beneficial owners | `unreachable` |

### OpenCheck's own channels

| Channel | Value |
|---|---|
| `degraded_sources` with `check: source_fetch` for the source | `unreachable` (the only value) |
| `degraded_sources` with `check: source_read` | `unreachable`, beside what was read |
| Coverage: the source answered with no record | `no_record` (ownership sources) or `not_covered` |
| Knowability: the subject's country has `legitimate_interest`, `restricted_no_lia_route_yet` or `authorities_and_obliged_entities_only` access, and the source is that country's register | `restricted`, for beneficial owners the register did not show |

## Where it appears

- **QuickCheck**: chips on each ownership source's card, with the source's own
  words beneath; the sources that are not ownership sources named on one line.
  Not on FullCheck nodes.
- **API**: `disclosure_status` on `/lookup`, `/lookup-register` and saved
  reports — `sources[<id>]` with `values`, `entries` (`value`, `covers`,
  `count`, `details`, `reasons`) and `ownership_source`; `not_covered`;
  `as_of`.
- **MCP**: `disclosure_status` on `opencheck_lookup` and
  `opencheck_register_lookup` — `by_source[]` with a `line` per source.
- **PDF and Markdown**: "What each source says about owners", after "What each
  source found".
- **BODS**: one `commenting` annotation on OpenCheck's own subject statement
  (the one the integrated-ownership note goes on), pointer `/recordDetails`.
  No new statement is written: `unreachable`, `restricted` and `not_covered`
  are about OpenCheck or the jurisdiction, not something the source said about
  the company, and the existing `unspecifiedReason` relationships already carry
  what the sources did say.
