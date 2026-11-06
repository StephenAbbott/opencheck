I wrote `owners.py` and ran it on the export. Persia International Bank PLC (LEI 213800HTIY6D1KMBG757) has two shareholders: **Bank Mellat**, which holds more than 50% and up to 75%, and **Bank Tejarat**, which holds more than 25% and up to 50%. Only UK Companies House gives a share figure. OpenSanctions names the same two banks with no figure, and links the bank to the Islamic Republic of Iran through an interest whose kind isn't stated.

```
$ python3 -I owners.py /home/claude/fixtures/export-persia.json
Owners of PERSIA INTERNATIONAL BANK PLC (LEI 213800HTIY6D1KMBG757)
Read from 4 subject records, one per source; shares are per source and never added.

Owner                                          Kind    Interest                 Share          Direct?   Source
---------------------------------------------------------------------------------------------------------------------------
Natural person(s) (GLEIF reporting exception)  person  otherInfluenceOrControl  not stated     direct    GLEIF
Natural person(s) (GLEIF reporting exception)  person  otherInfluenceOrControl  not stated     indirect  GLEIF
Mellat Bank                                    entity  shareholding             not stated     direct    OpenSanctions
TEJARAT BANK                                   entity  shareholding             not stated     direct    OpenSanctions
Islamic Republic Of Iran                       entity  unknownInterest          not stated     -         OpenSanctions
Bank Mellat                                    entity  shareholding             >50% and ≤75%  unknown   UK Companies House
Bank Tejarat                                   entity  shareholding             >25% and ≤50%  unknown   UK Companies House

7 ownership/control interests; 5 director relationships left out (officers, not owners).
Licence: OpenSanctions is licensed under CC-BY-NC-4.0. Commercial re-use of this data is not permitted under the source license.
```

**How the script reads the file:**
- **Links:** In this format (BODS 0.4), each relationship points to its owner and to the company by the record's `recordId`, not its `statementId`. The script looks them up by `recordId`.
- **The bank appears four times:** each source (GLEIF, OpenSanctions, Companies House and OpenAleph) has its own record for the bank. The script counts a relationship if it points to any of these four records. If it only looked at the LEI-keyed record, it would find GLEIF alone. OpenAleph reports no owners.
- **Directors are left out:** the five Companies House directors are officers, not owners, so the script skips them.

**How to read the results:**
- **Duplicates:** "Mellat Bank" and "Bank Mellat" are the same bank with the same Iranian registry number (38077), as reported by two sources. "TEJARAT BANK" and "Bank Tejarat" are likewise one bank (38027). Shares from different sources are separate readings and should not be added together.
- **The GLEIF rows:** these are a reporting exception, not named owners. GLEIF records that the bank is controlled by natural persons with no consolidating parent. That doesn't match the Companies House filing, which names two corporate shareholders. If you want to raise this with a register, OpenCheck's `opencheck_discrepancy_note` drafts the note.
- **Licence:** the OpenSanctions rows are CC-BY-NC-4.0, so you can't reuse them commercially.

Files are in `/home/claude/ws/iteration-2-opus/bods-relationships-reference-record-ids/without_skill/outputs/`:
- `owners.py`
- `answer.md`
