# Cypriot holding company: no beneficial owners listed

**Short answer: not on this evidence.** The empty beneficial-owner list most likely reflects what Cyprus publishes, not concealment.

## Why

- **The Cyprus beneficial-owner register is closed to the public.** OpenCheck's knowability record (verified 2026-10-21) says the Cyprus Register of Beneficial Owners is open to authorities and obliged entities only. It has no legitimate-interest access route for the public (`amld6_legitimate_interest_access: No`), and no change is expected (`next_change_expected: null`).
- **OpenCheck reads a different register for Cyprus.** It reads the Registrar of Companies (DRCOR), which covers officers and registered holdings, not beneficial owners. So directors and secretaries appearing while beneficial owners do not is the expected pattern for this source.

An owner missing where the register publishes none is the register's limit, not a finding. The register's limit is the reason here, not anything the company did.

## What this does and does not show

- It shows that the empty list is what the readable Cyprus sources can return. It does not show that the company has no owners, nor that it is clean.
- It does not tell us whether the company is hiding anything. Nothing here is a risk signal.
- I have only the jurisdiction notes for Cyprus (`/home/claude/fixtures/knowability-cy.json`). I did not see the company's own lookup record, so I cannot say which DRCOR fields came back for this company or whether any source was degraded. A screen with a degraded source is incomplete, not clean, so check `degraded_sources` on the lookup before reading anything into the result.

## Next steps

1. Re-check the company's full `opencheck_lookup` result and confirm which DRCOR fields (officers, registered holdings) populated and whether `degraded_sources` is empty.
2. If DRCOR shows registered shareholdings, weigh those as the readable ownership evidence, with the source named.
3. For anything beyond that, the Cyprus beneficial-owner register is reachable only by authorities and obliged entities, so this route is closed to us.

Source: OpenCheck knowability for CY, `generated_at` 2026-11-04, `as_of` 2026-11-05.
