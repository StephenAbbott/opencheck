# Is Persia International Bank PLC sanctioned?

**No, not on this record.** The OpenCheck owners result does not list Persia International Bank PLC (LEI 213800HTIY6D1KMBG757) as a designated party. The three designated matches are its shareholders, not the bank itself.

The result does contain a computed sanctions ownership test that points at those shareholders, and that needs checking before anyone relies on the "no".

## What the record shows

**Designated matches (all three are shareholders, not the bank):**

| Party | Stake (UK Companies House) | Regimes | Match basis |
|---|---|---|---|
| Mellat Bank | 50–75% | OFAC, EU, UK | Identifier |
| Bank Tejarat | 25–50% | OFAC, EU, UK | Name only |
| TEJARAT BANK | No figure (OpenSanctions) | OFAC, EU, UK | Identifier |

**Sanctions ownership test (computed by OpenCheck, as of 2026-11-05):**

- OFAC 50 Percent Rule: meets, on the Companies House reading. Mellat Bank holds more than 50% up to 75%.
- EU 50% rule: meets, on the same reading.
- UK rule: meets, on the same reading.
- On the OpenSanctions reading, each test "may meet", because no share is stated.
- The result is flagged `if_confirmed: false`.

## Why this is not a "yes"

- **The test is arithmetic on filed shareholdings, not a designation.** The record says so itself: "not a determination that the company is sanctioned or blocked." Nothing in it says the bank is on a sanctions list.
- **If the test holds, it is a US-law issue, not a listing.** Under OFAC's 50 Percent Rule, an entity owned 50% or more by blocked persons is generally treated as blocked property. That is why the result matters, but it is a conclusion for sanctions counsel to confirm, not one this record reaches.
- **The UK reading may be overstated.** The UK rule requires *more than* 50%. Mellat's Companies House band starts at exactly 50%, so the lower bound does not clear the UK threshold. Check Mellat's actual stake in the filing before relying on the UK result.
- **One match rests on a name alone.** Bank Tejarat is matched by name only, not by identifier.
- **The chain is incomplete.** Only one layer was read (depth 1). Mellat Bank and Bank Tejarat have no LEI in this result, so their owners were not reached. TEJARAT BANK's owners were not followed either.
- **Control is not tested.** The EU and UK also catch companies that a designated person controls (appoints the board, directs it, or by agreement). OpenCheck computed ownership only. OFAC general licences, holdings that have ended, and the Sectoral Sanctions Identifications list were also not checked.

## What to do next

1. Check the current OFAC SDN, EU Financial Sanctions File and UK sanctions list directly, for both Persia International Bank PLC and each shareholder. Do not rely on this record for the listing status.
2. Confirm the shareholdings in the Companies House filing for Mellat Bank's stake, and whether Bank Tejarat and TEJARAT BANK are the same party.
3. If the ownership test holds on the confirmed figures, refer the matter to sanctions compliance or counsel. Do not treat the bank as clear.

**Licence note:** the OpenSanctions match is licensed CC-BY-NC-4.0, and commercial re-use of that data is not permitted.
