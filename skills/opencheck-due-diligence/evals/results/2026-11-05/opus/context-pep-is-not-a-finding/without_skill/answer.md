## Politically exposed persons linked to Ørsted A/S

**Short answer: none are confirmed.** The OpenCheck lookup names 9 people with a PEP tag, but they fall into two quite different groups. You shouldn't report "9 PEPs".

Source: OpenCheck lookup for ØRSTED A/S, LEI `W9NG6WMZIYEU8VEDOG48`, Denmark, recorded 2026-11-05.

### 1. Seven people tagged as PEPs only because they work at Ørsted (context, not a finding)

OpenSanctions lists these seven as PEPs **only because of their role at Ørsted**. Ørsted is majority state-owned: Denmark holds about 50.1%. That makes its board and executives "PEPs" under state-owned-enterprise rules. OpenCheck marks these as `RELATED_PEP_SUBJECT_ROLE`, `kind: context`. They describe who runs the company and are not a finding against it.

| Person | Role at Ørsted |
|---|---|
| Lene Skole-Sørensen | Chairman of the Board |
| Andrew Richard Dingley Brown | Vice Chairman |
| Benny Gøbel | Board of Directors |
| Julian David Waldron | Board of Directors |
| Ruchit Majmudar | Board of Directors |
| Rasmus Errboe | Executive Management |
| Trond Ødegård Westlie | Executive Management |

The reason they are tagged is something you already know: the Danish state controls the company. That is a real risk signal in its own right (`STATE_CONTROLLED`).

### 2. Two possible name matches (risk signals, but unconfirmed)

These are the only PEP items with `kind: risk`. In both cases the only link is a shared name. No birth date or nationality matches, so they are **not established as the same person**.

- **Karl Johnny Hersvik** (Ørsted board): shares a name with a PEP record on OpenSanctions (also in EveryPolitician and OpenAleph). That record also lists an Ørsted board role, so it may be the same role-based listing. Check it by hand.
- **Julia Elizabeth King** (Ørsted board): shares a name with a record in OpenAleph's "Denmark, Faroe Islands and Greenland PEPs" collection. Again, this is a name match only.

To settle either one, compare dates of birth and the political positions held against the source records.

### Caveat: the PEP screening was incomplete

OpenCheck found **47 related parties but screened only the top 25** for PEPs and sanctions. The other 22 were not checked. They are current parties with no recorded ownership or board role. So "no other PEPs" means *not screened*, not *clear*. Offshore-leaks screening was also cut short, covering 30 of the 47.

### Bottom line

| Category | Count | Nature |
|---|---|---|
| PEP only because of their Ørsted role | 7 | Context (follows from state ownership) |
| Possible PEP name matches | 2 | Risk, unconfirmed |
| Confirmed PEPs with outside political exposure | 0 found | 22 of 47 related parties not screened |

### Licensing

The PEP data comes from OpenSanctions and EveryPolitician, both under **CC-BY-NC-4.0, so it is not for commercial use** without removing those statements. OpenAleph licences vary by collection.
