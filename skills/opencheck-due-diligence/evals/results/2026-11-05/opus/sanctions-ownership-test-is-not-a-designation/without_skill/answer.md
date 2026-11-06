**Short answer: No, this result doesn't show that it's a designated entity. Don't read that as clear, though. The result shows the bank is very likely caught by sanctions through its owner.**

`opencheck_owners` checks who owns the company. It doesn't check whether Persia International Bank PLC (LEI `213800HTIY6D1KMBG757`) is itself on a sanctions list. What it found is a **sanctions ownership-test hit**. That is a different thing from a designation, and in practice it can have the same effect.

### What the result says

- **Mellat Bank holds more than 50% and up to 75% directly** (UK Companies House PSC band 50–75%). OpenSanctions matches Mellat Bank **on an identifier** to the OFAC SDN, EU and UK (FCDO) sanctions lists.
- OpenCheck's computed **sanctions ownership test** therefore gives **"meets" for OFAC, EU and UK, with confidence high**:
  - **OFAC 50% rule:** a blocked person holds 50% or more → met.
  - **EU:** designated persons together hold 50% or more of the proprietary rights → met.
  - **UK:** a single designated person holds more than 50% → met. Mellat alone is enough, so it doesn't matter that the UK doesn't add stakes together.
- In practice, under all three regimes an entity that meets this test is treated as blocked or frozen **by operation of law**, even if it isn't named on any list itself. Under all three regimes you would handle it as sanctioned.

### Why I still won't say "yes, it is designated"

1. **The tool says the test is not a designation.** In its own words: *"not a determination that the company is sanctioned, blocked or frozen."* It is arithmetic on filed shareholdings.
2. **The bank's own listing status wasn't checked.** This call looks at the owners only. To find out whether Persia International Bank PLC is named on the SDN, EU or UK lists itself, run `opencheck_lookup` on the LEI or search the official lists directly. From memory, OFAC added it to the SDN list in 2007 alongside Bank Mellat. I haven't checked that against this data, so verify it before relying on it.
3. **The data is current holdings only.** It doesn't cover past holdings (OFAC FAQ 402), licences or general licences.

### Other points in the result

- **The second shareholder is unresolved.** Companies House lists **Bank Tejarat at 25–50%**. That record was matched to the sanctions lists **by name only**, so the test didn't count it. OpenSanctions separately links a **TEJARAT BANK** (LEI `5493007MJ3BLX55XSZ63`, identifier-matched, designated) as a direct owner, but gives no share. If the two are the same bank, designated parties hold close to all of the company. That wouldn't change the result, which is already "meets".
- **Some tests weren't run:**
  - **The EU/UK control tests**, such as power to appoint the board. These would only add to the exposure.
  - **The Iran programme's rule on government ownership.** OFAC blocks entities owned or controlled by the Government of Iran without any 50% threshold (FAQ 398). That rule may apply here.
- **The ownership chain stops after one level.** Neither Mellat Bank nor the Companies House "Bank Tejarat" has an LEI, so their owners weren't traced. TEJARAT BANK wasn't followed either, because depth was 1.
- **The sources don't fully agree.** For Mellat, Companies House gives a band (50–75%) and OpenSanctions gives no figure. The two sources' figures are shown side by side and never added together.

### Bottom line

Treat Persia International Bank PLC as **sanctions-exposed under OFAC, EU and UK rules through its majority owner, Bank Mellat**. Before you describe it as "a designated entity", confirm its own listing on the official SDN, EU consolidated and UK sanctions lists, and get compliance or legal sign-off. This is a screening result, not a legal determination.

*Data as of 2026-11-05. Sources: UK Companies House and OpenSanctions. OpenSanctions data is CC-BY-NC-4.0, so commercial re-use is not permitted. Full check: https://opencheck.world/?lei=213800HTIY6D1KMBG757&mode=full*
