I wrote `owners.py` and ran it on `export-persia.json`, the BODS export for Persia International Bank PLC (LEI 213800HTIY6D1KMBG757, Companies House 04218020). It finds two owners: **Bank Mellat**, which holds more than 50% and up to 75%, and **Bank Tejarat**, which holds more than 25% and up to 50%. Companies House gives these as bands, not exact figures. OpenSanctions lists the same two banks as shareholders but gives no share.

Run it with:

```
python3 owners.py /home/claude/fixtures/export-persia.json
```

## Output

```
OWNERS (shareholding / voting rights) — 4 interest(s); shares are per source and must not be added across sources

- Mellat Bank (registeredEntity, IR)
    share:  no share stated  (shareholding, direct, current)
    BO flag: not stated
    source: OpenSanctions [opensanctions], statement dated 2026-05-25
    of:     PERSIA INTERNATIONAL BANK PLC (registeredEntity, GB)
- TEJARAT BANK (registeredEntity, IR)
    share:  no share stated  (shareholding, direct, current)
    BO flag: not stated
    source: OpenSanctions [opensanctions], statement dated 2026-05-25
    of:     PERSIA INTERNATIONAL BANK PLC (registeredEntity, GB)
- Bank Mellat (registeredEntity, IR)
    share:  >50% and ≤75%  (shareholding, unknown, current)
    filed:  Ownership of shares – More than 50% but less than 75%
    BO flag: no (not declared as beneficial owner)
    source: UK Companies House [companies_house], statement dated 2016-05-16
    of:     PERSIA INTERNATIONAL BANK PLC (registeredEntity, GB)
- Bank Tejarat (registeredEntity, IR)
    share:  >25% and ≤50%  (shareholding, unknown, current)
    filed:  Ownership of shares – More than 25% but not more than 50%
    BO flag: no (not declared as beneficial owner)
    source: UK Companies House [companies_house], statement dated 2016-05-16
    of:     PERSIA INTERNATIONAL BANK PLC (registeredEntity, GB)

OTHER RELATIONSHIPS (not ownership) — 8

- Natural person(s) (GLEIF reporting exception) (unknownPerson): otherInfluenceOrControl (current) [GLEIF [gleif]] — GLEIF reporting exception NATURAL_PERSONS: the entity is controlled by natural person(s) without any intermediate legal entity meeting the definition of accounting consolidating parent (ExceptionCategory: DIRECT_ACCOUNTING_CONSOLIDATION_PARENT)
- Natural person(s) (GLEIF reporting exception) (unknownPerson): otherInfluenceOrControl (current) [GLEIF [gleif]] — GLEIF reporting exception NATURAL_PERSONS: the entity is controlled by natural person(s) without any intermediate legal entity meeting the definition of accounting consolidating parent (ExceptionCategory: ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT)
- Islamic Republic Of Iran (registeredEntity): unknownInterest (current) [OpenSanctions [opensanctions]]
- AKHONDI- ASL, Aziz, Mr. (knownPerson, IR): seniorManagingOfficial (current) [UK Companies House [companies_house]] — Director, from 2019-03-29
- FAROKHNEJAD, Farshid (knownPerson, IR): seniorManagingOfficial (current) [UK Companies House [companies_house]] — Director, from 2022-09-01
- FEIZASAR, Hadi Akhlaghi, Dr (knownPerson, IR): seniorManagingOfficial (current) [UK Companies House [companies_house]] — Director, from 2022-03-08
- REZAEIPOUR, Mohammad Reza (knownPerson, IR): seniorManagingOfficial (current) [UK Companies House [companies_house]] — Director, from 2022-06-01
- SHAHMIR, Farrokh (knownPerson, GB): seniorManagingOfficial (current) [UK Companies House [companies_house]] — Director, from 2017-06-30

LICENCE NOTICES
- opensanctions: OpenSanctions is licensed under CC-BY-NC-4.0. Commercial re-use of this data is not permitted under the source license.
```

## How the script works
- **Matching owners to records:** relationships point to owners by `recordId`, not `statementId`, so the script looks owners up by `recordId`. All 12 relationships in the file found their owner.
- **What counts as an owner:** only `shareholding` and `votingRights` interests. The script lists these separately:
  - the five directors (`seniorManagingOfficial`);
  - GLEIF's two `NATURAL_PERSONS` reporting-exception links, which concern accounting consolidation and are not ownership;
  - OpenSanctions' `unknownInterest` link to the Islamic Republic of Iran.
- **Shares:** the script prints each source's figure exactly as published and never adds figures across sources. Companies House's bands are ranges with open or closed ends, not single numbers.

## How to read the result
- **The same two banks, counted twice:** "Mellat Bank" (OpenSanctions) and "Bank Mellat" (Companies House) are the same bank, with Iranian register number 38077. "TEJARAT BANK" and "Bank Tejarat" are also the same bank, number 38027. Each source describes them separately, so they appear twice in the output.
- **Beneficial ownership:** Companies House does not mark either bank's stake as beneficial ownership (`beneficialOwnershipOrControl: false`). They are registrable corporate holders, not named beneficial owners. The export's `disclosure_status` says "Beneficial owners: Declared (2)" for Companies House, and those two entries are the two banks.
- **Islamic Republic of Iran:** OpenSanctions links it to the bank with an `unknownInterest`, which means the kind of interest isn't stated. That is not a stated shareholding.
- **GLEIF:** GLEIF records no consolidating parent because the bank filed a `NATURAL_PERSONS` exception. That exception is allowed under GLEIF's rules. It doesn't name anyone and says nothing about shares.
- **Dates:** the Companies House filings for the two banks are dated 2016-05-16, and the export marks both as current.

**Licence:** OpenSanctions data is CC-BY-NC-4.0, so it cannot be reused commercially. Keep that notice with this data if you pass it on.

Files are in this folder:
- `owners.py`
- `owners-output.txt`
