/**
 * Phase 203 — the tick on a BackgroundCheck card, and the sentence beside it.
 *
 * `lib/identityVerification.test.ts` pins what the sentence says and
 * `lib/backgroundCheck.test.ts` which record a merged row keeps. What only
 * rendering can show: the card that carries the tick also carries the words
 * (the tick is `aria-hidden`, so a card with a tick and no sentence would say
 * nothing to a screen reader and nothing a sighted reader can check), and a
 * card without the annotation says nothing about verification at all.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

const statements = [
  { statementId: "co", recordType: "entity", recordDetails: { name: "EXAMPLE PLC" } },
  {
    statementId: "p-acsp",
    recordType: "person",
    recordDetails: { personType: "knownPerson", names: [{ fullName: "VERIFIED, Alex" }] },
    source: { description: "UK Companies House" },
    annotations: [
      {
        statementPointerTarget: "/recordDetails",
        motivation: "commenting",
        identityVerification: {
          status: "verified",
          route: "authorisedCorporateServiceProvider",
          verifiedBy: { name: "EXAMPLE LLP ACSP" },
          identityVerifiedOn: "2025-07-28",
        },
      },
    ],
  },
  {
    statementId: "p-none",
    recordType: "person",
    recordDetails: { personType: "knownPerson", names: [{ fullName: "PENDING, Robin" }] },
    source: { description: "UK Companies House" },
  },
  ...["p-acsp", "p-none"].map((p) => ({
    statementId: `r-${p}`,
    recordType: "relationship",
    recordDetails: { interestedParty: p, subject: "co", interests: [{ type: "seniorManagingOfficial" }] },
    source: { description: "UK Companies House" },
  })),
];

vi.mock("../../lib/api", () => ({
  lookup: () => Promise.resolve({ bods: statements }),
  personCheck: vi.fn(),
  personAppointments: vi.fn(),
  personPositions: vi.fn(),
}));

import BackgroundCheckPanel from "./BackgroundCheckPanel";

describe("BackgroundCheck identity verification", () => {
  it("puts the sentence on the verified person's card and nowhere else", async () => {
    render(<BackgroundCheckPanel lei="213800EXAMPLE0000000" legalName="EXAMPLE PLC" />);
    const sentence = await screen.findByText(
      "Identity verified with Companies House through EXAMPLE LLP ACSP on 28 July 2025."
    );
    expect(sentence.closest("div")!.textContent).toContain("VERIFIED, Alex");
    expect(screen.getAllByText(/Identity verified/)).toHaveLength(1);
    expect(screen.getByText("PENDING, Robin").closest("div")!.textContent).not.toMatch(/verif/i);
  });
});
