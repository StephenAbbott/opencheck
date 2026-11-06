## OpenCheck onboarding screen: 4 counterparties, none cleared

Batch recorded 2026-11-05 13:35 UTC. No failed rows. Three of four rows are degraded, so none of the four is a clean result.

| Company | LEI status | Screen | Risk signals | Action |
|---|---|---|---|---|
| Shell PLC (GB) | ISSUED | Degraded (`opencheck`) | 3 | Closer look |
| Bentcard Import LLP (AZ) | LAPSED since 2015-02-05 | Not degraded, but thin | 1 | Closer look |
| Heineken N.V. (NL) | ISSUED | Degraded (`kvk` failed) | 0 | Incomplete; re-run |
| Ørsted A/S (DK) | ISSUED | Degraded (`opencheck`) | 4 | Closer look |

**Shell PLC** (`21380068P1DRHMJ8KU70`)
- The `opencheck` source did not fully run, so the screen is incomplete.
- Risk signals:
  - `RELATED_EXPORT_RISK`: a related party carries a trade-risk flag. This is not an export-control listing. The row's verdict says "export-control findings", which overstates the code.
  - `RELATED_COUNTER_SANCTIONED`: a related party is designated by a state. This is not a Western sanction.
  - `RELATED_PEP`: a politically exposed person is among the related parties.
- The batch row does not give match confidence for these. Pull the full lookup.

**Bentcard Import LLP** (`54930007FGRO3F0RZ382`)
- The LEI has lapsed. GLEIF's "ACTIVE" was declared before 5 Feb 2015 and has not been re-checked since, so it is not a current reading of the company.
- No register record was checked, so the company's current status is unknown.
- Risk signal: `OPAQUE_OWNERSHIP`.
- Only GLEIF returned data. OpenSanctions, OpenAleph, Climate TRACE, WikiRate and EveryPolitician returned no record.
- Next: run `opencheck_knowability("AZ")` so the owner-disclosure limit is framed before reading the opacity flag. Then try a register lookup for the company.

**Heineken N.V.** (`724500K5PTPSST86UQ23`)
- Zero risk signals, but the Dutch register (`kvk`) failed. Its beneficial owners are recorded as unreachable.
- "No risk signals" here means the check was not fully run. It does not mean clear.
- Re-run once `kvk` answers.

**Ørsted A/S** (`W9NG6WMZIYEU8VEDOG48`)
- The `opencheck` source did not fully run, so the screen is incomplete.
- Risk signals:
  - `STATE_CONTROLLED`
  - `RELATED_PEP`: the row's verdict says "possible"
  - `OFFSHORE_LEAKS`: the row's verdict says "possible appearance"
- Treat the PEP and offshore-leaks items as leads to confirm, not confirmed matches.
- The Danish register restricts beneficial owners.

**Licensing:** Shell, Heineken and Ørsted include CC-BY-NC sources (OpenSanctions, and EveryPolitician for Shell and Ørsted). Results are not for commercial use. Keep the notices if these results are passed on.

**Ticket line:** 0 of 4 cleared. Shell, Bentcard and Ørsted need closer review of their risk signals. Heineken needs a re-run after the KvK source recovers. All four need the full `opencheck_lookup` report before a decision.
