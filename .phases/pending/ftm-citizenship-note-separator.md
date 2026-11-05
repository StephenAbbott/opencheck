---
title: The FtM citizenship annotation separates the nationalities it names with semicolons.
readme: The note on a person's nationalities read from FollowTheMoney citizenship now separates countries with semicolons, so a former state's comma no longer splits it.
---
Found in Phase 458's post-deploy check: pycountry names the Soviet Union "USSR, Union of Soviet Socialist Republics", so Usmanov's note read "Uzbekistan (UZ), USSR, Union of Soviet Socialist Republics are read from…", which looks like three countries. `citizenship_annotation` now joins entries with "; ". Annotations are outside the statementId digest, so no id moves.
