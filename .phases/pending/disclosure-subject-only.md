---
title: The disclosure status counts only the looked-up company's owners.
readme: The "what each source says about owners" chips count only the looked-up company's own owners, not those of other companies in a source's bundle.
tests: backend +3
---
BP P.L.C.'s Companies House card showed "Declared (28)" beside "Exempt" (4 Nov 2026). BP files no PSC: it is exempt, with shares on a UK regulated market. The 28 were the PSCs of its subsidiaries, which are in the bundle because BP is a curated example served from Open Ownership's stored UK PSC extract. In that extract every record names itself as its `declarationSubject`. `disclosure.entries_from_statements` treated every `declarationSubject` in a source's statements as "the subject", so every company's owners counted. GLEIF's "Declared (13)" for BP came the same way, from its children's parent links.

The subject is now the lookup's own subject statements (`subject_identity.statement_ids`, passed to `for_lookup` as `subject_ids`). Failing that, it is the one `declarationSubject` a source's stamped statements share. A bundle naming several subjects, none of them the subject, declares no one. "Every value a source supports is listed" is unchanged; BP now reads "Exempt" alone, like Unilever PLC.

Check in production: BP (`213800LH1BZH3DI6G760`) shows one Companies House chip, "Exempt". Its GLEIF chip now reads "None on record": Open Ownership's stored GLEIF extract carries no reporting exception, while GLEIF itself files NO_KNOWN_PERSON for both parents. That is a separate gap in the curated bundle.
