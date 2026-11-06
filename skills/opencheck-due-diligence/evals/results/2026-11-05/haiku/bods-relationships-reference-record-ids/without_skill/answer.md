# Owners of PERSIA INTERNATIONAL BANK PLC (LEI 213800HTIY6D1KMBG757)

I wrote `owners.py` and ran it on `/home/claude/fixtures/export-persia.json`. The export has 28 statements, recorded 2026-11-05.

The script lists a party as an owner when a relationship statement points at one of the bank's records and its interest is a shareholding. Other interests are listed separately and are not counted as owners.

## What the export holds

The bank has its own record in each of four sources. Owners are read per source, and no owner is matched across sources.

| Source | Bank record |
|---|---|
| GLEIF | `opencheck-87b3ba7f4db2637dd2a55f40` |
| OpenSanctions | `opencheck-1645fff766e1dea36edd3b79` |
| UK Companies House | `opencheck-d38f80e900bdf376c85ad686` |
| OpenAleph | `opencheck-42a98b75dcd07b67c0c47c7d` |

## Owners

Output from `owners.py`:

```
LEI: 213800HTIY6D1KMBG757
Bank: PERSIA INTERNATIONAL BANK PLC
Bank records by source:
  gleif            opencheck-87b3ba7f4db2637dd2a55f40
  opensanctions    opencheck-1645fff766e1dea36edd3b79
  companies_house  opencheck-d38f80e900bdf376c85ad686
  openaleph        opencheck-42a98b75dcd07b67c0c47c7d

Owners (4):
- Mellat Bank
    interest:  shareholding; share: no share stated
    source:    OpenSanctions (opensanctions), retrieved 2026-11-05T13:35:07Z
    url:       https://www.opensanctions.org/entities/NK-DC4R48SCEMmbcURFzktAKy/
    statement: opencheck-64f9c986800bb83f1fa75f53
- TEJARAT BANK
    interest:  shareholding; share: no share stated
    source:    OpenSanctions (opensanctions), retrieved 2026-11-05T13:35:07Z
    url:       https://www.opensanctions.org/entities/NK-DC4R48SCEMmbcURFzktAKy/
    statement: opencheck-c78308cd6756baabaeee3e4a
- Bank Mellat
    interest:  shareholding; share: more than 50% and up to 75%
    source:    UK Companies House (companies_house), retrieved 2026-11-05T13:35:05Z
    url:       https://find-and-update.company-information.service.gov.uk/company/04218020
    statement: opencheck-3d55acf51252d92066cd0a30
- Bank Tejarat
    interest:  shareholding; share: more than 25% and up to 50%
    source:    UK Companies House (companies_house), retrieved 2026-11-05T13:35:05Z
    url:       https://find-and-update.company-information.service.gov.uk/company/04218020
    statement: opencheck-b3eac9cabe452a6011ca96a3

Other interests, not counted as owners (8):
- Natural person(s) (GLEIF reporting exception) [otherInfluenceOrControl] via GLEIF (gleif)
- Natural person(s) (GLEIF reporting exception) [otherInfluenceOrControl] via GLEIF (gleif)
- Islamic Republic Of Iran [unknownInterest] via OpenSanctions (opensanctions)
- AKHONDI- ASL, Aziz, Mr. [seniorManagingOfficial] via UK Companies House (companies_house)
- FAROKHNEJAD, Farshid [seniorManagingOfficial] via UK Companies House (companies_house)
- FEIZASAR, Hadi Akhlaghi, Dr [seniorManagingOfficial] via UK Companies House (companies_house)
- REZAEIPOUR, Mohammad Reza [seniorManagingOfficial] via UK Companies House (companies_house)
- SHAHMIR, Farrokh [seniorManagingOfficial] via UK Companies House (companies_house)

Shares are per source and are not added across sources. Party names are as each source publishes them; no matching is applied.
```

## How to read it

- **Companies House** is the only source that states a share. It gives two holders: Bank Mellat at more than 50% and up to 75%, and Bank Tejarat at more than 25% and up to 50%. OpenCheck's integrated-ownership figures match these bands. They are the register's filed figures, not a beneficial-ownership determination.
- **OpenSanctions** names two entities as direct shareholders, Mellat Bank and TEJARAT BANK, but states no share on any path. Its integrated-ownership figure is incomplete for both.
- **Names are not matched across sources.** "Bank Mellat" (Companies House) and "Mellat Bank" (OpenSanctions) may be the same company, and the same question applies to "Bank Tejarat" and "TEJARAT BANK". The export does not establish that they are, so I have not merged them. Do not add their shares together.
- **GLEIF** publishes no owner. It reports the bank as controlled by natural persons under the NATURAL_PERSONS reporting exception, which is why the two GLEIF entries are control interests and not shareholdings.
- **Companies House directors** are officers, not owners, so they are listed under other interests.
- **Islamic Republic Of Iran** is linked to the bank in OpenSanctions with an unknown interest type and no share. I have not counted it as an owner. Treat it as an open question.
- **Companies House says two beneficial owners are declared** (`disclosure_status`). The export does not contain them as beneficial-ownership statements. The two Companies House shareholdings are recorded with `beneficialOwnershipOrControl: false`.

## Licence

The OpenSanctions data is licensed CC-BY-NC-4.0. Commercial re-use is not permitted under that licence. The export carries this notice for hit `NK-DC4R48SCEMmbcURFzktAKy`.

## Files

- Script: `/home/claude/ws/iteration-2-haiku/bods-relationships-reference-record-ids/without_skill/outputs/owners.py`
- Output: `/home/claude/ws/iteration-2-haiku/bods-relationships-reference-record-ids/without_skill/outputs/owners-output.txt`
