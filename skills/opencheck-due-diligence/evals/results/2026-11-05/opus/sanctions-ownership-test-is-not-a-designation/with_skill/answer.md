**Not established by this result.** The owners result doesn't say whether Persia International Bank PLC is itself on a sanctions list, because that isn't what it checks. It does find that the bank **meets the sanctions ownership test** under the OFAC, EU and UK rules, because a designated bank owns more than 50% of it.

So the honest answer is that "is it designated?" is still open on this evidence. You should still treat the bank as **likely caught through its ownership**, and confirm that before you deal with it.

## What the result shows

**Persia International Bank PLC**, LEI `213800HTIY6D1KMBG757`, United Kingdom. The data is as of 2026-11-05.

**Owners:**

| Holder | UK Companies House | OpenSanctions | Sanctions match |
|---|---|---|---|
| Mellat Bank | **50–75%**, held directly | holder, no share stated | Designated (OFAC SDN, EU, UK FCDO). **Matched on an identifier.** |
| Bank Tejarat | 25–50%, held directly | — | Designated, but **matched on name only** |
| TEJARAT BANK (LEI `5493007MJ3BLX55XSZ63`) | — | holder, no share stated | Designated (OFAC, EU, UK). Matched on an identifier. |

"Bank Tejarat" and "TEJARAT BANK" are probably the same bank. OpenCheck has not linked them, so the 25–50% Companies House stake is not counted towards the test.

**Sanctions ownership test** (`SANCTIONS_OWNERSHIP_TEST`, kind `risk`, confidence `high`):

- **OFAC** (50% or more, all blocked persons' stakes added together): **meets**
- **EU** (50% or more of the proprietary rights, all designated persons' stakes added together): **meets**
- **UK** (more than 50% held by one designated person): **meets**

All three results rest on one Companies House filing: Mellat Bank holds "more than 50% up to 75%". The OpenSanctions records alone only reach "may meet", because they state no share.

## Why that isn't the same as "sanctioned"

OpenCheck describes this test as "arithmetic on the records, not a determination that the company is sanctioned, blocked or frozen." In more detail:

- **It's about ownership, not a listing.** Under these rules, a company that is more than 50% owned by a designated person is treated as blocked or frozen *as if* it were designated. That has real consequences, but it is a different thing from appearing on the list yourself.
- **OpenCheck did not compute several things.** It did not check the EU and UK control tests, OFAC licences or general licences, or holdings that have since ended. It also did not check the Iran programme's rule that blocks on government ownership or control (OFAC FAQ 398). Mellat and Tejarat are Iranian banks, so that last rule may matter here as well.
- **The owners tool does not screen the company itself.** Nothing in this result checks Persia International Bank PLC against the sanctions lists directly. To answer whether it is designated in its own right, run `opencheck_lookup` on the LEI, or check it directly against the UK consolidated list, the EU Financial Sanctions Files and OFAC SDN. From memory (not from this output), the EU and UK have listed this bank in its own right in the past. Check whether that listing is still current.

## Limits of this result

- It is one layer deep. Mellat Bank and Bank Tejarat have no LEI and their owners weren't found. TEJARAT BANK wasn't followed further; raising the depth would do that.
- No register names a beneficial owner for this company.
- The two sources' figures sit side by side and must not be added together.
- **Licence:** the OpenSanctions data is CC-BY-NC-4.0, so you can't re-use it commercially.

Full check: https://opencheck.world/?lei=213800HTIY6D1KMBG757&mode=full
