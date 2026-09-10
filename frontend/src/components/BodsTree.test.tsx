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
