# Companies House identity verification

Since Phase 203, OpenCheck records where Companies House has verified the
identity of a director or an individual person with significant control (PSC),
both in its BODS output and on the page.

Companies House is verifying every director and individual PSC; the transition
runs to November 2026. The register's public data API carries the outcome on
each officer and PSC list item as `identity_verification_details`
([officers](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/appointmentlist?v=latest),
[PSCs](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/notificationlist?v=latest)).

## What counts as verified

Verifying an identity and linking it to a role are separate steps, and
Companies House shows its "Verified" label against each role. OpenCheck mirrors
that, per appointment:

> A role is verified when `appointment_verification_start_on` is present and
> `appointment_verification_end_on` is absent or later than today
> (`9999-12-31` is the register's "still in place").

A person is verified when any of their roles in the output is. A statement
**due** date on its own is not a verification, and neither is
`identity_verified_on` without a statement.

The block arrives in three shapes on the live register:

| Shape | Fields the register publishes | OpenCheck |
|---|---|---|
| Not yet verified | `appointment_verification_statement_due_on` (PSCs also `…_statement_date`) | nothing |
| Verified through an authorised corporate service provider (ACSP) | `identity_verified_on`, `authorised_corporate_service_provider_name`, `anti_money_laundering_supervisory_bodies`, `preferred_name`, statement start and end | annotated, with the provider, its supervisors and the verification date |
| Verified directly with Companies House (GOV.UK One Login) | statement start and end **only** | annotated, with no provider and no verification date |

Corporate PSCs (relevant legal entities) have no verification requirement yet and
are never annotated. Resigned officers are not mapped; ceased PSC notifications
earn no annotation.

## In BODS

A `commenting` annotation carrying a custom `identityVerification` object. The
BODS Annotation schema permits this: "custom properties can be included within
the Annotation object to provide structured data where required".

**On the person statement**, pinned at `/recordDetails`:

```json
{
  "statementPointerTarget": "/recordDetails",
  "motivation": "commenting",
  "description": "Companies House records this person's identity as verified. The identity was verified on 28 July 2025 by DE PINNA LLP ACSP, an authorised corporate service provider supervised for anti-money laundering by Faculty Office of the Archbishop of Canterbury (FO). The first identity verification statement for a role in this data was supplied on 7 January 2026.",
  "createdBy": {"name": "OpenCheck", "uri": "https://opencheck.world"},
  "url": "https://find-and-update.company-information.service.gov.uk/officers/…/appointments",
  "identityVerification": {
    "status": "verified",
    "recordedBy": {"name": "Companies House", "uri": "https://www.gov.uk/government/organisations/companies-house"},
    "route": "authorisedCorporateServiceProvider",
    "verifiedBy": {
      "name": "DE PINNA LLP ACSP",
      "antiMoneyLaunderingSupervisoryBodies": ["Faculty Office of the Archbishop of Canterbury (FO)"]
    },
    "identityVerifiedOn": "2025-07-28",
    "firstVerificationStatementOn": "2026-01-07"
  }
}
```

**On each verified role's relationship statement**, pinned at
`/recordDetails/interestedParty`, the same object with
`verificationStatementSuppliedOn` (that role's statement date) in place of
`firstVerificationStatementOn`.

| Property | Meaning |
|---|---|
| `status` | Always `verified`. Nothing is written for anyone else. |
| `recordedBy` | Companies House, the registrar that records the verification. |
| `route` | `authorisedCorporateServiceProvider` or `companiesHouse`. |
| `verifiedBy` | The ACSP and the anti-money-laundering supervisors it was registered with. ACSP route only. |
| `identityVerifiedOn` | When the ACSP verified the identity. ACSP route only — the register does not publish it for the direct route. |
| `firstVerificationStatementOn` | Person: the earliest statement among the roles in this output — the latest the identity can have been verified. |
| `verificationStatementSuppliedOn` | Role: when the statement was supplied for this appointment. |

Deliberate omissions:

- **No `source.type: "verified"`.** BODS has that code, but it marks the whole
  statement as verified, and the register verified the identity — not the
  service address or the nationality on the same statement.
- **No `preferred_name`.** A second name for the person adds nothing to the
  verification claim.
- **No invented date or verifier for the direct route.** The only inference is
  the route itself: the scheme has two routes, so a verification that names no
  provider was made with Companies House, and the sentence says so in those
  terms.
- **Nothing for an unverified person.** Until November a director without a
  statement is the ordinary case, and a person reached through any other source
  carries no Companies House data at all.

The RDF export carries the annotation's `description`; the custom object is a
JSON-only extension. Companies with a stored Open Ownership bundle bypass the
Companies House mapper and carry no annotation.

## On the page

A green tick, read from the annotation above — never from the raw register
payload — so the page and the export cannot disagree:

- on the person's node in the ownership graph, lower right (the flag is
  north-east, risk badges north-west, the collapse toggle due south), named in
  the graph legend;
- on the person's row in **Read as text**, with the words for a screen reader;
- beside the name on **BackgroundCheck** cards, with a sentence naming the
  provider and date where the register gives them.

There is no "not verified" mark. Code: `backend/opencheck/bods/identity_verification.py`,
`frontend/src/lib/identityVerification.ts`, `frontend/src/components/ui/IdentityTick.tsx`.
