**No, OpenCheck doesn't show it as active. The register record it found says it is dissolved.** The "active" you may have seen comes from an LEI that lapsed in 2015, so it isn't a current reading.

**What the lookup says (LEI 54930007FGRO3F0RZ382)**
- **The LEI has lapsed.** Its renewal was due on 5 Feb 2015 and was never made. OpenCheck's wording: *"This is the status of the LEI record, not of the company."*
- **GLEIF still says "ACTIVE", but that is old information.** OpenCheck labels it `liveness: "declared"`: *"GLEIF holds the entity status as ACTIVE, last declared to the LEI issuer before 5 Feb 2015; the LEI has not been renewed since, so no issuer has re-checked it. This is not a current register reading."*
- The search line for this candidate ("AZ · active") repeats the same old GLEIF value. GLEIF lists it with jurisdiction Azerbaijan, at Bakikhanov St. 18, Baku.

**What the register says**
Your search also returned a second exact match from Companies House: **BENTCARD IMPORT LLP, OC369315, dissolved**, at 175 Darkes Lane, Suite B, 2nd Floor, Potters Bar, EN6 1BW. On whether a UK LLP still exists, the register is the authority.

**One caveat:** nothing in either record links the two. The LEI record carries no GB-COH number, and the jurisdictions and addresses differ (Azerbaijan/Baku vs UK/Potters Bar). They are very probably the same LLP (same exact name, and LLP is a UK legal form), but that is my inference, not something either record states.

**To confirm:** run `opencheck_register_lookup("GB-COH", "OC369315")`. It will give the dissolution date and the members/PSCs. I worked only from your two saved files, so that lookup hasn't been run.

---
*Also in the lookup, in case it matters:* one risk signal, `OPAQUE_OWNERSHIP` (high confidence). GLEIF records `NON_PUBLIC` reporting exceptions for both the direct and the ultimate parent. That means a consolidating parent exists but isn't published, which the LEI rules allow. All 6 sources answered (no degraded sources), and only GLEIF had a record. OpenCheck holds no register notes for Azerbaijan. Licence: CC0 (GLEIF), safe for commercial use.
