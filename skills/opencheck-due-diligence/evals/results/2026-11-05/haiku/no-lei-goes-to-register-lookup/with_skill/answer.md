# Next OpenCheck call for Metastar Invest LLP

**Call to make:**

```
opencheck_register_lookup(
  scheme = "GB-COH",
  id     = "OC346224",
  name   = "METASTAR INVEST LLP"
)
```

**Why this call:**

- The search returned one `exact` match: METASTAR INVEST LLP, Companies House number OC346224. Its `lei` is `null`, so `opencheck_lookup` does not apply. Pass the `GB-COH` identifier to `opencheck_register_lookup` instead.
- The result's `summary` reads "dissolved". Treat that as a claim to verify. The register lookup will show what the register itself says about the company's status.
- The other candidates are different companies. METASTARINC LTD (16583658) and METASTAR CONSULTING L.P. (SL027540) are fuzzy matches on the name. Do not merge them with METASTAR INVEST LLP.

**Optional, free, before the lookup:** `opencheck_knowability(["GB"])`. It says what UK registers publish about owners. For an LLP, this tells you whether a thin owner result reflects the register's limits or a gap in the data.

**Budget:** `opencheck_register_lookup` spends one shared lookup check. A repeat of the same check within 15 minutes is free.

**What to expect and how to read it:**

- If the register confirms dissolution, report the company as dissolved. Do not describe it as active, even if a GLEIF record or LEI shows otherwise.
- If the result shows degraded sources, report the screen as incomplete and name what was not covered. Do not read a missing signal as a clean result.
- Only the search's top 15 of 44 results were shown. If the company is not the one you expect, search again with a more specific query or a higher limit.
