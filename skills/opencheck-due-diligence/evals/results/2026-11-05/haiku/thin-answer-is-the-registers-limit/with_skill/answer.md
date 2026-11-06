# Cypriot holding company: no beneficial owners listed

**Short answer: not on this evidence.** The gap is most likely the register's limit, not the company hiding anything.

## Why the register shows no owners

OpenCheck's knowability record for Cyprus (last verified 2026-10-21) says:

- The Cyprus Register of Beneficial Owners is **open to authorities and obliged entities only**.
- Legitimate-interest access under AMLD6 is recorded as **"No"**, so OpenCheck cannot read it for you.
- OpenCheck reads **Cyprus DRCOR (Registrar of Companies)** for officers and registered holdings. It does **not** read beneficial owners.

Directors and a secretary come from DRCOR, which publishes officers. Beneficial owners sit in a register that the public cannot reach through OpenCheck. An empty beneficial-owner section is therefore what you would expect for any Cypriot company, whatever its structure.

## What I could not check

- **I have not seen the lookup itself.** The only recorded OpenCheck output I found is the Cyprus knowability record. Nothing in the fixtures is for this company.
- So I cannot confirm the company's `disclosure_status` for the Cypriot source, which tells you whether the register reported "no record", "restricted" or "not provided".
- I also cannot tell whether `degraded_sources` was non-empty. If it was, the screen is incomplete and the empty owner list says even less.

## What would settle it

1. Send me the lookup output (or the LEI or Cypriot registration number, and I will check it with OpenCheck). I want `disclosure_status`, `degraded_sources` and the DRCOR shareholder entries.
2. Look at **registered holdings** in DRCOR. If shareholders are listed there, they are the nearest thing to owners OpenCheck can read. If a shareholder is itself a company, that is a further register to check in its own jurisdiction.
3. If you need the beneficial owners, the route is a request to the authorities or through an obliged entity (for example a bank or a law firm that holds the company's file). OpenCheck cannot provide them.

## What to say if you report this

"OpenCheck lists the company's directors and secretary from the Cyprus Registrar of Companies. Cyprus restricts its beneficial-owner register to authorities and obliged entities, and OpenCheck cannot read it. The absence of beneficial owners reflects that restriction. It is not a finding about the company."
