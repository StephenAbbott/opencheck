/**
 * Phase 203 — the tree row is the canvas tick's text equivalent.
 *
 * The canvas is `role="img"` and its tick is decorative, so the only place a
 * screen-reader user can learn that Companies House verified a person is the
 * "Read as text" tree. This pins that the words are there for a ticked row and
 * absent for every other row — an unticked row must not say anything about
 * verification at all.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import BodsTree from "./BodsTree";
import { buildTree, bodsToGraph } from "../lib/bodsGraph";
import { IDENTITY_VERIFIED_LABEL } from "../lib/identityVerification";

const statements = [
  {
    statementId: "co",
    recordType: "entity",
    recordDetails: { name: "EXAMPLE PLC" },
  },
  {
    statementId: "verified",
    recordType: "person",
    recordDetails: { personType: "knownPerson", names: [{ fullName: "VERIFIED, Alex" }] },
    annotations: [
      {
        statementPointerTarget: "/recordDetails",
        motivation: "commenting",
        identityVerification: { status: "verified", route: "companiesHouse" },
      },
    ],
  },
  {
    statementId: "pending",
    recordType: "person",
    recordDetails: { personType: "knownPerson", names: [{ fullName: "PENDING, Robin" }] },
  },
  ...["verified", "pending"].map((p) => ({
    statementId: `r-${p}`,
    recordType: "relationship",
    recordDetails: { interestedParty: p, subject: "co", interests: [{ type: "seniorManagingOfficial" }] },
  })),
];

function renderTree() {
  const rows = buildTree(bodsToGraph(statements), new Set());
  render(
    <BodsTree rows={rows} selectedId={null} onSelect={() => {}} onToggleCollapse={() => {}} />
  );
}

describe("BodsTree identity verification", () => {
  it("says it in words on the verified person's row, once", () => {
    renderTree();
    const row = screen.getAllByRole("treeitem").find((r) => r.textContent?.includes("VERIFIED, Alex"))!;
    expect(within(row).getAllByText(IDENTITY_VERIFIED_LABEL)).toHaveLength(1);
  });

  it("says nothing about verification anywhere else", () => {
    renderTree();
    expect(screen.getAllByText(IDENTITY_VERIFIED_LABEL)).toHaveLength(1);
    const other = screen.getAllByRole("treeitem").find((r) => r.textContent?.includes("PENDING, Robin"))!;
    expect(other.textContent).not.toMatch(/verif/i);
  });
});

describe("BodsTree ended relationships (Phase 219)", () => {
  // The canvas draws an ended edge faint. That fade has no text equivalent, so
  // the tree row is the only place a screen-reader user can learn it.
  const bundle = [
    { statementId: "co", recordType: "entity", recordDetails: { name: "EXAMPLE PLC" } },
    { statementId: "former", recordType: "person", recordDetails: { personType: "knownPerson", names: [{ fullName: "FORMER, Sam" }] } },
    { statementId: "current", recordType: "person", recordDetails: { personType: "knownPerson", names: [{ fullName: "CURRENT, Jo" }] } },
    {
      statementId: "r-former", recordType: "relationship", recordStatus: "closed",
      recordDetails: { interestedParty: "former", subject: "co", interests: [{ type: "shareholding", share: { minimum: 25, maximum: 50 }, endDate: "2024-11-30" }] },
    },
    {
      statementId: "r-current", recordType: "relationship", recordStatus: "new",
      recordDetails: { interestedParty: "current", subject: "co", interests: [{ type: "shareholding", share: { minimum: 50, maximum: 75 } }] },
    },
  ];

  it("says 'ended' with the date on the ended edge's row and nowhere else", () => {
    const rows = buildTree(bodsToGraph(bundle, { asOf: "2026-09-16" }), new Set());
    render(<BodsTree rows={rows} selectedId={null} onSelect={() => {}} onToggleCollapse={() => {}} />);
    // The subject hangs under each owner, so find the row by its interest.
    const items = screen.getAllByRole("treeitem");
    const ended = items.filter((r) => /ended/.test(r.textContent ?? ""));
    expect(ended).toHaveLength(1);
    expect(ended[0].textContent).toContain("Owns 25–50% · ended 30 November 2024");
    const current = items.find((r) => r.textContent?.includes("Owns 50–75%"))!;
    expect(current.textContent).not.toMatch(/ended/);
  });
});
