**Short answer: OpenCheck can't tell you it's active, and the "active" in the search result shouldn't be read that way.**

### Why the "active" label isn't current
The search result's "AZ · active" for LEI `54930007FGRO3F0RZ382` comes from GLEIF, and the lookup explains what it means:

- **The LEI has lapsed.** It was first registered on 7 Feb 2014. Renewal was due on **5 Feb 2015** and was never made. GLEIF last touched the record on 31 Jul 2023, but no LEI issuer has re-checked the details since 2015.
- **"ACTIVE" was declared, not checked.** It's the entity status as last declared to the LEI issuer before Feb 2015. The lookup itself says: *"This is not a current register reading."*
- **No register confirms it.** OpenCheck has no source for an Azerbaijani company register. GLEIF is the only one of the 6 sources that had a record. The other 5 (OpenSanctions, OpenAleph, Climate TRACE, Wikirate, EveryPolitician) all answered with no record, so none were down.

In short, the record says the company was active about 11 years ago. It says nothing about today.

### A UK entity with the same name is dissolved
The same search also returned a **Companies House** record: **BENTCARD IMPORT LLP, OC369315: dissolved** (175 Darkes Lane, Suite B, 2nd Floor, Potters Bar, EN6 1BW). That status comes straight from a live register.

I **can't confirm** that it's the same entity as the LEI holder:
- The LEI record gives an Azerbaijani address: Bakikhanov St. 18, Baku.
- The LEI record lists no Companies House number.

Still, the identical name and the "LLP" legal form, which is a UK form, make a link plausible. Treat it as a lead to check, not a match.

### Also worth knowing
The lookup raised one **risk** signal, **OPAQUE_OWNERSHIP** (high confidence). GLEIF records that the entity has both a direct and an ultimate parent, but the relationship is not published (reason given: NON_PUBLIC).

### Bottom line
There is **no current evidence that BENTCARD IMPORT LLP is active**. The only "active" status is an old, unchecked GLEIF declaration from before 2015. The UK LLP with the same name is dissolved.

To settle it:
1. Run `opencheck_register_lookup` with `GB-COH` / `OC369315` to get the UK LLP's dissolution date and its members, and compare them with the LEI record.
2. If the entity really is Azerbaijani, check with the Azerbaijani state register directly. OpenCheck doesn't read it.

*Sources: GLEIF (CC0-1.0) and Companies House, via OpenCheck search and lookup, recorded 5 Nov 2026.*
