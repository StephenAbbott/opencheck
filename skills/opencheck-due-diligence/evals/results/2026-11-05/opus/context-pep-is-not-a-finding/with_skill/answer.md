## PEP connections: Ørsted A/S (LEI W9NG6WMZIYEU8VEDOG48, DK, CVR 36213728)

**There is no single number.** The lookup names **9 people** next to a PEP label, but they fall into two different groups. **None of the 9 is a confirmed PEP in their own right.**

### 1. Listed as PEPs only because of their role at Ørsted: 7 people (context, not a finding)

OpenCheck tags these `RELATED_PEP_SUBJECT_ROLE`, with `kind: context`. OpenSanctions lists each of them as politically exposed **"only by virtue of a role at this company"**. Ørsted is about 50% owned by the Danish state, so its board and executives appear on PEP lists as officers of a state-owned enterprise. In OpenCheck's words, this is *"context about who runs the company, not a finding against it."*

| Person | Role at Ørsted (per the PEP record) |
|---|---|
| Lene Skole-Sørensen | Chairman of the Board |
| Andrew Richard Dingley Brown | Board of Directors (Vice Chairman) |
| Benny Gøbel | Board of directors |
| Julian David Waldron | Board of directors |
| Ruchit Majmudar | Board of directors |
| Rasmus Errboe | Executive Management |
| Trond Ødegård Westlie | Executive Management |

### 2. Possible name matches only: 2 people (risk signal, unconfirmed)

OpenCheck tags these `RELATED_PEP`, with `kind: risk` and medium confidence. In both cases the match is on the **name alone**: *"no birth date or nationality in common to confirm they are the same person."*

- **Karl Johnny Hersvik** shares a name with a PEP record on OpenSanctions, also reported by EveryPolitician and OpenAleph. That record also lists a role on Ørsted's board.
- **Julia Elizabeth King** shares a name with a record on OpenAleph, from the "Denmark, Faroe Islands and Greenland PEPs" collection. That record also lists a role on Ørsted's board.

Both records list an Ørsted board seat, so these may turn out to be the same role-based listing as group 1 rather than a separate political role. That needs checking, not assuming. To confirm or rule out each match, compare the date of birth and nationality on the PEP record with the board member's. `opencheck_person_check` is the next step for that.

### What this screen did not cover

- **The screen is incomplete.** Related-party PEP and sanctions screening covered *"the 25 highest-ranked of 47 related parties; 22 were not screened"* (all 22 are current parties with no ownership or board role recorded). So this is a count of what was found, not a clean screen of everyone connected to Ørsted.
- **The Danish register doesn't publish beneficial owners.** CVR access to them has been restricted to authorities, obliged entities and people with a legitimate interest since 1 Sep 2025, and OpenCheck reads only officers and registered holdings from CVR. The related parties come from officers and open datasets, not from a beneficial-owner register.
- **The state shareholding is a separate signal.** The Danish state holds about 50% (Government of Denmark 50.12% per Global Energy Monitor / Climate TRACE; Ministry of Finance 50.1% per Wikidata; these are two sources' figures, not to be added). That is why OpenCheck raises `STATE_CONTROLLED`, a separate risk signal. It is not a PEP. It is also the reason for group 1.

### Licence

The PEP data comes partly from OpenSanctions and EveryPolitician (CC-BY-NC-4.0), so **this result is not for commercial use**. Attribution is required. OpenAleph content is licensed per collection, so check its terms before reusing it.

*Source: OpenCheck `opencheck_lookup` recorded 2026-11-05T13:34:33Z. All 11 sources answered.*
