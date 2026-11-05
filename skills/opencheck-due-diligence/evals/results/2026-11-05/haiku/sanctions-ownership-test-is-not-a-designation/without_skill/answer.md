# Is Persia International Bank PLC sanctioned?

**No.** Nothing in this OpenCheck result designates Persia International Bank PLC (LEI `213800HTIY6D1KMBG757`). Its own name does not appear in the designated list. The three designated parties are its shareholders or possible shareholders: Mellat Bank, Bank Tejarat and TEJARAT BANK.

The result does flag a sanctions ownership exposure, which is a different thing from a designation.

## What the result says

- **Designation:** none for Persia itself. The `designated` list covers only Mellat Bank, Bank Tejarat and TEJARAT BANK, each under OFAC, EU and UK datasets.
- **Ownership test:** OpenCheck computed a `SANCTIONS_OWNERSHIP_TEST` (kind: risk, confidence: high). From UK Companies House, Mellat Bank holds 50–75% of Persia. On that figure the test "meets" for:
  - **OFAC** (50% or more held by blocked persons together)
  - **EU** (50% or more of proprietary rights held by designated persons together)
  - **UK** (more than 50% held by one designated person)
- **Not a finding of sanctions status:** `if_confirmed` is `false`. The result says it is "not a determination that the company is sanctioned or blocked." It is arithmetic on shareholdings that one source filed.
- **OpenSanctions:** gives no share figure for Mellat Bank or TEJARAT BANK, so its reading is only `may_meet`.

## Limits on this reading

- **Depth 1 only.** The chain stops at Mellat Bank and Bank Tejarat, which have no LEI, so their owners were not reached. Raising depth could change the picture.
- **Bank Tejarat is not counted.** Its 25–50% stake is a `may_be_above` figure, and its designation is `name_only`, so it does not enter the test.
- **Not computed:** control (the EU and UK also treat a company as caught when a designated person controls it), OFAC licences, holdings that have ended, the Iran/Cuba/Syria-type programmes that block on government ownership or control without a 50% test, and OFAC's Sectoral Sanctions Identifications list.
- **Ownership only.** The test ignores board control, which the EU and UK also use.

## A discrepancy to check before relying on the UK line

The UK regime says "more than 50%". The Companies House band is 50–75%, with a minimum of exactly 50.0. The result labels that stake "more than 50% up to 75%", but the tool's own method says the lowest reading of a stake decides whether a test is met. At the lowest reading (50%), "more than 50%" is not met, so the UK result should arguably be `may_meet`, not `meets`. The OFAC and EU results are unaffected, because they say "50% or more".

## Suggested next steps

1. Check Mellat Bank's exact stake in Persia against the Companies House filing.
2. Check the OFAC SDN, EU Financial Sanctions File and UK sanctions lists directly for the three named parties, and for Persia.
3. Have a sanctions adviser apply the 50% rule to the verified figures. The result itself says it is not a determination.
4. If the owners of Mellat Bank and Bank Tejarat matter, rerun with greater depth or use `opencheck_ownership_map`.

## Licence

The OpenSanctions data is licensed under CC-BY-NC-4.0. Commercial re-use of that data is not permitted under the source licence. Keep that in mind before passing any OpenSanctions-derived figure on.

Full check: https://opencheck.world/?lei=213800HTIY6D1KMBG757&mode=full
