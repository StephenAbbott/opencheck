## Next call

```
opencheck_register_lookup(scheme="GB-COH", id="OC346224")
```

## Why this call

- **The top result is your company, and it has no LEI.** It's `METASTAR INVEST LLP` with `match: "exact"`, from Companies House. Its `lei` is `null` and its only identifier is `{"scheme": "GB-COH", "id": "OC346224"}`. `opencheck_lookup` needs an LEI, so you can't use it here. For a candidate with no LEI, the search `hint` says to use `opencheck_register_lookup(scheme, id)` with the register identifier. Most UK LLPs have no LEI, so this is normal.
- **Ignore the results that do have LEIs.** Grainger Invest No. 1 LLP, Brabo Invest, the Co-Invest L.P.s and the others are all `fuzzy` matches on the word "Invest". They are different entities. Running `opencheck_lookup` on one of them would check the wrong company.
- **Ignore the other "Metastar" names too.** METASTARINC LTD (16583658), METASTAR CONSULTING L.P. (SL027540) and METASTAR PTY LTD (AU-ABN) are also only `fuzzy` matches, each with a different register number. They are not the LLP.

## What to expect

- **The LLP is dissolved.** The summary says "Company OC346224 · dissolved · Cornwall Buildings 45-51 Newhall Street Office 330, Birmingham, B3 3QR". Expect a historical record: past members and designated members, and any persons with significant control (PSC) filed before dissolution. It won't show current control.
- **Optional free step first.** If you want to know in advance what the UK register publishes about LLP owners, run `opencheck_knowability(jurisdictions=["GB"])`. It doesn't use your lookup budget. That way, if the answer is thin, you'll know it's the register's limit and not a finding about the company.
- **Budget.** `opencheck_register_lookup` uses one of the shared lookup budget's ten fresh checks a minute. Repeating the same check within 15 minutes is free.
- **Reading the result.** Check `degraded_sources` first: if it lists any source, the screen is incomplete, not clean. Signals marked `kind: context` are not findings. Pass on any `license_notices`.
