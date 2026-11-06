## Next call

```
opencheck_register_lookup(scheme="GB-COH", id="OC346224", name="METASTAR INVEST LLP")
```

## Why

- **It's the only exact match.** Of the 15 candidates shown (44 in total), only `METASTAR INVEST LLP` has `match: "exact"`. It comes from Companies House with the identifier `GB-COH OC346224`. The other results are fuzzy matches on different entities, such as Grainger Invest No. 1 LLP, METASTARINC LTD, Metastar Pty Ltd (Australia) and Metastar Consulting L.P. (a Scottish LP). Don't use them.
- **It has no LEI, so `opencheck_lookup` won't work.** The candidate has `lei: null`, which is normal for a UK LLP. The search's own hint says what to do in this case: a candidate with no LEI but a register identifier goes to `opencheck_register_lookup(scheme, id)`. Passing `name` labels the result.
- **Cost:** this call uses one check from the shared lookup budget (ten fresh checks a minute). Repeating it within 15 minutes is free.

## What to expect when you read the result

- **The LLP is dissolved.** The search summary reads "Company OC346224 · dissolved · Cornwall Buildings 45-51 Newhall Street Office 330, Birmingham, B3 3QR". Companies House is the authority on whether the company exists, so check that the lookup's register status confirms this. Expect the owner and controller data to be historical: PSCs and members as they were last filed, not a current structure.
- **Check `degraded_sources`.** If it lists anything, the screen is incomplete. Say which checks didn't run, and don't report the result as clean.
- **Read the signals carefully.** Keep `risk` signals separate from `context` signals, which are not findings. Treat any related-party match that is a "possible name match only" as a lead to confirm, not as an identity.
- **Optional, and free:** `opencheck_knowability(["GB"])` tells you what the UK register publishes about LLP members and PSCs. It's useful if the answer turns out thin, but you don't need it before the lookup.
