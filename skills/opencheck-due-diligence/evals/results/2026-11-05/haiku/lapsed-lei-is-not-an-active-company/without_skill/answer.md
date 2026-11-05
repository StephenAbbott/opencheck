**Short answer: OpenCheck can't confirm that BENTCARD IMPORT LLP is active.** The only status it holds for the Azerbaijani entity is stale.

**The entity I checked:** BENTCARD IMPORT LLP, LEI `54930007FGRO3F0RZ382`, jurisdiction AZ (registered address Baku). The lookup was run on 2026-11-05.

**What the record says**

- **LEI registration is lapsed.** The renewal was due on 5 Feb 2015 and was never made. GLEIF last updated the record on 31 Jul 2023. A lapsed LEI means no issuer has re-checked the reference data since 2015.
- **"ACTIVE" is a 2015 declaration, not a current reading.** GLEIF's entity status of ACTIVE was last declared to the LEI issuer before 5 Feb 2015. OpenCheck flags this as "not a current register reading." It is the status of the LEI record, not of the company.
- **No Azerbaijani register was read.** OpenCheck holds no register notes for Azerbaijan, so nothing from the Azerbaijani company register is in this check.

**Risk signal (one)**

- `OPAQUE_OWNERSHIP` (high confidence, from GLEIF): a direct and an ultimate parent exist but are withheld as non-public under the LEI policy. This is a transparency gap to review. It says nothing about whether the company is active.

**A different entity with the same name**

The search also returned BENTCARD IMPORT LLP on the UK Companies House register (company OC369315, Potters Bar, Hertfordshire), with the status "dissolved." It has no LEI in the results. It is a different register and a different jurisdiction, and the data does not show it is the same company. Don't read its dissolved status across to the Azerbaijani LLP, and don't read the AZ LEI status across to it.

**What is not established**

- Whether the Azerbaijani LLP is currently registered or trading. Confirming that needs the Azerbaijani register itself, which OpenCheck does not read.
- Whether the UK dissolved company and the Azerbaijani LLP are related. Nothing in the data links them.

**Suggested next step:** If the question is about the Azerbaijani LLP, check its status with the Azerbaijani register directly. If it turns out to be the UK company, use the Companies House record instead.

*Sources: OpenCheck search (`/home/claude/fixtures/search-bentcard.json`, 2026-11-05) and lookup (`/home/claude/fixtures/lookup-bentcard.json`, 2026-11-05). GLEIF data is CC0-1.0. No license notices apply.*
