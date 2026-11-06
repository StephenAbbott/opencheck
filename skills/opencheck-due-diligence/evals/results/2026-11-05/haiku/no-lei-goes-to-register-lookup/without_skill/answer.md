**Next call: `opencheck_knowability`, then `opencheck_register_lookup`. I have not made either call.**

The top candidate is the one to check. `METASTAR INVEST LLP` is an exact name match, but its `lei` is `null`. Its only identifier is Companies House `GB-COH` `OC346224`, and it is dissolved. OpenCheck's handshake says a company with no LEI is checked by its register number, not by `opencheck_lookup`.

1. **First, `opencheck_knowability`** (free, no lookup budget spent)
   ```json
   { "jurisdictions": ["GB"] }
   ```
   The candidate's address is in Birmingham, England, so the jurisdiction is GB. This tells us what the Companies House register publishes about owners before we read the result. A thin answer then reads as the register's limit, not as a finding.

2. **Then, `opencheck_register_lookup`** (spends one shared lookup)
   ```json
   { "scheme": "GB-COH", "id": "OC346224" }
   ```
   This passes the candidate's `identifiers[]` entry exactly as the search returned it.

Things to keep in mind when reading the result:
- The company is **dissolved**, so the record is historical. Say so in the write-up.
- The search matched 15 of 44 candidates (`truncated: true`). Other "Metastar" entities exist, such as `METASTARINC LTD`, `METASTAR PTY LTD` and `METASTAR CONSULTING L.P.`. They are not the same company, and I would not mix their results in.
- If the register lookup reports any `degraded_sources`, the screen is incomplete, not clean.
