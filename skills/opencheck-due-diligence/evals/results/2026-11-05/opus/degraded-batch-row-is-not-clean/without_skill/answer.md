**OpenCheck batch screen, 4 counterparties (run 2026-11-05): none can be marked clear yet.**

| Counterparty | LEI | Status | Why |
|---|---|---|---|
| Heineken N.V. (NL) | 724500K5PTPSST86UQ23 | **Incomplete, not clear** | 0 risk signals, but the Dutch register (KvK) **did not answer**, so beneficial owners were never checked. Re-run, or get the UBO extract from KvK directly. |
| BENTCARD IMPORT LLP (AZ) | 54930007FGRO3F0RZ382 | **Closer look (high priority)** | `OPAQUE_OWNERSHIP`. The LEI has been **lapsed since Feb 2015**, so its "ACTIVE" status hasn't been re-checked in over 10 years. No parent filed, and only GLEIF had any data on it. We can't confirm it still exists or who owns it. Ask the counterparty for its current registry extract and UBO declaration. |
| Shell plc (GB) | 21380068P1DRHMJ8KU70 | **Closer look** | 6 risk signals on **related parties**: export-control (`RELATED_EXPORT_RISK`), a sanctioned counterparty (`RELATED_COUNTER_SANCTIONED`) and a PEP (`RELATED_PEP`). These are about connected parties, not Shell itself. Review the full report to see which parties and how they're connected. The row is also flagged degraded. |
| Ørsted A/S (DK) | W9NG6WMZIYEU8VEDOG48 | **Closer look** | `STATE_CONTROLLED` (expected, since Denmark is the majority holder), a **possible** PEP and a **possible** offshore-leaks match. Both are name matches to verify, not confirmed identities. Danish UBO data is restricted. The row is also flagged degraded. |

**Notes**
- 3 of the 4 rows are flagged `degraded`. For Shell and Ørsted every listed source answered, but OpenCheck itself is named as degraded, so re-run those two before closing them.
- **Licensing:** the Shell, Ørsted and Heineken results include CC-BY-NC data (OpenSanctions, plus EveryPolitician for Shell and Ørsted). Check with compliance before using this output in a commercial onboarding record.
- Full reports: `/?lei=<LEI>` on OpenCheck.
