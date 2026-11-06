## OpenCheck batch screen: four onboarding counterparties

Batch recorded 2026-11-05 13:35 UTC (4 requested, 4 done, 0 failed, 3 degraded).

**Mark clear: none.** No row in this batch can be marked clear.

| LEI | Name | Status | Reason |
|---|---|---|---|
| 21380068P1DRHMJ8KU70 | Shell PLC (GB) | Closer look | 6 risk findings: related export risk, related counter-sanctioned party, related PEP. Row is degraded (source `opencheck` did not answer). The risk count (6) is higher than the 3 risk codes listed, so reconcile in the full report. |
| 54930007FGRO3F0RZ382 | Bentcard Import LLP (AZ) | Closer look | 1 risk finding: opaque ownership. LEI registration is LAPSED. The ACTIVE status comes from GLEIF, last declared 5 Feb 2015, so it is not a current register reading. Only GLEIF returned data; the other five sources had no record. |
| 724500K5PTPSST86UQ23 | Heineken N.V. (NL) | Incomplete, not clear | 0 risk findings, but the screen is degraded: KvK failed, so beneficial-owner data was not consulted. "No risk signals" holds only for the sources that answered. |
| W9NG6WMZIYEU8VEDOG48 | ØRSTED A/S (DK) | Closer look | 4 risk findings: state-controlled, possible PEP link, possible offshore-leaks appearance. These are matches to weigh, not confirmed identities. Row is degraded (source `opencheck` did not answer). |

**Next steps**
- Run `opencheck_lookup` on each LEI above for the full report.
- Heineken: re-run once KvK answers, then decide.

**Licensing (applies to anything copied from this batch)**
- Not for commercial use for Shell, Heineken and Ørsted: they include OpenSanctions and EveryPolitician (CC-BY-NC-4.0). Remove those statements before any commercial use.
- Bentcard's bundle is safe for commercial use; attribution may be required.
- Attribution is required for all rows. OpenCorporates is share-alike. Verify EITI and OpenAleph terms.
