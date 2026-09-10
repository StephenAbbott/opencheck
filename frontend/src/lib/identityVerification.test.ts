import { describe, expect, it } from "vitest";
import {
  IDENTITY_VERIFIED_LABEL,
  ROUTE_ACSP,
  ROUTE_COMPANIES_HOUSE,
  humanDate,
  identityVerificationSentence,
  isIdentityVerified,
  readIdentityVerification,
} from "./identityVerification";

// The person annotations `backend/tests/test_ch_identity_verification.py` pins
// as whole objects — copied, so a change to the backend shape has to be made
// here too, where a reader of the frontend will see it.
const ACSP_ANNOTATION = {
  statementPointerTarget: "/recordDetails",
  motivation: "commenting",
  description: "Companies House records this person's identity as verified. …",
  createdBy: { name: "OpenCheck", uri: "https://opencheck.world" },
  url: "https://find-and-update.company-information.service.gov.uk/officers/ofc-acsp/appointments",
  identityVerification: {
    status: "verified",
    recordedBy: {
      name: "Companies House",
      uri: "https://www.gov.uk/government/organisations/companies-house",
    },
    route: "authorisedCorporateServiceProvider",
    verifiedBy: {
      name: "DE PINNA LLP ACSP",
      antiMoneyLaunderingSupervisoryBodies: ["Faculty Office of the Archbishop of Canterbury (FO)"],
    },
    identityVerifiedOn: "2025-07-28",
    firstVerificationStatementOn: "2026-01-07",
  },
};

const DIRECT_ANNOTATION = {
  statementPointerTarget: "/recordDetails",
  motivation: "commenting",
  description: "Companies House records this person's identity as verified. …",
  identityVerification: {
    status: "verified",
    recordedBy: { name: "Companies House" },
    route: "companiesHouse",
    firstVerificationStatementOn: "2026-01-08",
  },
};

const person = (annotations: unknown[] = []) => ({
  statementId: "p1",
  recordType: "person",
  recordDetails: { personType: "knownPerson", names: [{ fullName: "A PERSON" }] },
  ...(annotations.length ? { annotations } : {}),
});

describe("readIdentityVerification", () => {
  it("reads the ACSP route whole", () => {
    expect(readIdentityVerification(person([ACSP_ANNOTATION]))).toEqual({
      route: ROUTE_ACSP,
      verifierName: "DE PINNA LLP ACSP",
      supervisors: ["Faculty Office of the Archbishop of Canterbury (FO)"],
      identityVerifiedOn: "2025-07-28",
      firstStatementOn: "2026-01-07",
    });
  });

  it("reads the direct route with no verifier and no verification date", () => {
    expect(readIdentityVerification(person([DIRECT_ANNOTATION]))).toEqual({
      route: ROUTE_COMPANIES_HOUSE,
      supervisors: [],
      firstStatementOn: "2026-01-08",
    });
  });

  it("finds it among the other annotations a CH person carries", () => {
    const birthDateNote = {
      statementPointerTarget: "/recordDetails/birthDate",
      motivation: "commenting",
      description: "Companies House publishes month and year only.",
    };
    expect(isIdentityVerified(person([birthDateNote, DIRECT_ANNOTATION]))).toBe(true);
  });

  it("is false with no annotation — which is not the same as unverified", () => {
    expect(readIdentityVerification(person())).toBeNull();
  });

  it("ignores a role-level annotation: that is about a relationship, not this record", () => {
    const roleLevel = { ...ACSP_ANNOTATION, statementPointerTarget: "/recordDetails/interestedParty" };
    expect(readIdentityVerification(person([roleLevel]))).toBeNull();
  });

  it.each([
    ["a status other than verified", { ...DIRECT_ANNOTATION.identityVerification, status: "pending" }],
    ["an unknown route", { ...DIRECT_ANNOTATION.identityVerification, route: "selfDeclared" }],
    ["no structure at all", undefined],
  ])("refuses %s", (_label, structure) => {
    const annotation = { ...DIRECT_ANNOTATION, identityVerification: structure };
    expect(readIdentityVerification(person([annotation]))).toBeNull();
  });

  it("tolerates junk", () => {
    expect(readIdentityVerification(null)).toBeNull();
    expect(readIdentityVerification("p1")).toBeNull();
    expect(readIdentityVerification({ annotations: "nope" })).toBeNull();
  });
});

describe("identityVerificationSentence", () => {
  it("names the provider and the date for the ACSP route", () => {
    expect(identityVerificationSentence(readIdentityVerification(person([ACSP_ANNOTATION]))!)).toBe(
      "Identity verified with Companies House through DE PINNA LLP ACSP on 28 July 2025."
    );
  });

  it("gives no date and no provider for the direct route, because the register gives none", () => {
    const sentence = identityVerificationSentence(readIdentityVerification(person([DIRECT_ANNOTATION]))!);
    expect(sentence).toBe("Identity verified with Companies House; the register names no service provider.");
    expect(sentence).not.toMatch(/\d{4}/);
  });

  it("leaves out a date the ACSP record does not carry", () => {
    expect(
      identityVerificationSentence({ route: ROUTE_ACSP, verifierName: "X ACSP", supervisors: [] })
    ).toBe("Identity verified with Companies House through X ACSP.");
  });

  it("never speaks of an unverified state", () => {
    for (const a of [ACSP_ANNOTATION, DIRECT_ANNOTATION]) {
      const s = identityVerificationSentence(readIdentityVerification(person([a]))!);
      expect(s.startsWith(IDENTITY_VERIFIED_LABEL)).toBe(true);
      expect(s).not.toMatch(/unverified|not verified|pending|due/i);
    }
  });
});

describe("humanDate", () => {
  it("formats a bare date without a timezone shift", () => {
    expect(humanDate("2026-01-01")).toBe("1 January 2026");
    expect(humanDate("2025-12-31")).toBe("31 December 2025");
  });
});
