**None of the four can be marked clear from this batch.** Three rows are `degraded` (not fully screened), and the fourth has a lapsed LEI and an opacity signal. OpenCheck batch lookup, 2026-11-05.

| Counterparty | LEI | Status | Why it needs a closer look |
|---|---|---|---|
| **Heineken N.V.** (NL) | 724500K5PTPSST86UQ23 | Live (GLEIF) | No risk signals, but **the screen is incomplete**: the Dutch register (KvK) did not answer, so beneficial owners were not read (`beneficial_owners=unreachable`). Re-run it before you clear it. |
| **SHELL PLC** (GB) | 21380068P1DRHMJ8KU70 | Live (Companies House) | 6 risk signals on related parties: `RELATED_EXPORT_RISK` (a trade-risk flag, not an export-control listing), `RELATED_COUNTER_SANCTIONED` (designated by a state such as Russia, not a Western sanction) and `RELATED_PEP`. The row is also degraded: one OpenCheck check did not fully run. |
| **ØRSTED A/S** (DK) | W9NG6WMZIYEU8VEDOG48 | Live (CVR) | 4 risk signals: `STATE_CONTROLLED`, a *possible* related PEP and a *possible* offshore-leaks appearance. Both are possible matches only and need confirming. The row is also degraded (one OpenCheck check did not fully run). Danish beneficial owners are `restricted`. |
| **BENTCARD IMPORT LLP** (AZ) | 54930007FGRO3F0RZ382 | **Unknown.** The LEI **lapsed** in Feb 2015, and GLEIF's "ACTIVE" is a 2015 declaration, not a current reading. | `OPAQUE_OWNERSHIP` risk signal. No parent is provided. Only GLEIF returned data, and the other 5 sources had no record. Check the Azerbaijani register to confirm the company still exists and who owns it. |

**Notes**
- Context codes such as `GLEIF_REPORTING_EXCEPTION`, `NON_EU_JURISDICTION` and `RECORD_CONSISTENCY` describe structure. They are not findings, so I have not counted them. A missing GLEIF parent with an `exempt` filing is permitted under the LEI reporting rules.
- Next step: run `opencheck_lookup` on each LEI to see the full signal detail and how firmly each match was made. Run Heineken again once KvK answers.
- **Licence:** the Shell, Ørsted and Heineken results include CC-BY-NC sources (OpenSanctions, plus EveryPolitician for Shell and Ørsted), and OpenCorporates data carries share-alike terms. Remove those statements before using the results commercially, which includes onboarding.
