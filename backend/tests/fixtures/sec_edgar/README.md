# SEC EDGAR fixtures — Moody's Corporation (CIK 1059556)

Real EDGAR responses, fetched 28 Sept 2026 (Phase 252):

- `moodys_schedule_13g_feed.atom` — `browse-edgar?action=getcompany&CIK=1059556&type=SCHEDULE+13G&output=atom`, verbatim.
- `moodys_13ga_tci_hohn.xml` — accession 0000902664-26-002468, a joint SCHEDULE 13G/A (TCI Fund Management Ltd + Christopher Hohn, 8.21%).
- `moodys_13g_vanguard_capital_management.xml` — accession 0002100119-26-000791 (6.41%).
- `moodys_13ga_vanguard_group_exit.xml` — accession 0000102909-26-001883, The Vanguard Group reporting 0 shares after its January 2026 realignment.

The three `primary_doc.xml` files are trimmed after the cover pages: the `<items>` narrative, the signature block and the free-text `<comments>` are removed. Nothing else is edited. SEC EDGAR filings are public records.

`edgar_state_country_codes.json` (Phase 259) is the code/name table from the SEC's [EDGAR State and Country Codes](https://www.sec.gov/submit-filings/filer-support-resources/edgar-state-country-codes) page, fetched 28 Sept 2026, in page order, with the one duplicated row (Z4, "CANADA (Federal Level)") listed once. `opencheck/sources/edgar_codes.py` must carry every code in it with the same name.
