/**
 * IntegratedOwnershipList (Phase 339) — one row per source's figure, the
 * status note for a holder with no figure, the method in plain words, and no
 * threshold or highlighting of any kind.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";

import table from "../../lib/integratedOwnershipCases.json";
import { METHOD_DESCRIPTION, computeIntegratedOwnership } from "../../lib/integratedOwnership";
import { IntegratedOwnershipList } from "./IntegratedOwnershipList";

type Case = { name: string; as_of: string; subject_ids: string[]; canon?: Record<string, string>; statements: Record<string, unknown>[] };
const byName = (n: string) => {
  const c = (table as { cases: Case[] }).cases.find((x) => x.name === n)!;
  return computeIntegratedOwnership(c.statements, c.subject_ids, c.as_of, c.canon ?? {});
};

describe("IntegratedOwnershipList", () => {
  it("lists each source's figure side by side, never a sum", () => {
    render(<IntegratedOwnershipList payload={byName("the Equinor shape: three sources, one stake, three figures never summed")} />);
    const rows = screen.getAllByRole("row").slice(1);
    expect(rows).toHaveLength(3);
    expect(rows.map((r) => within(r).getAllByRole("cell")[1].textContent)).toEqual(["67%", "67%", "67%"]);
    expect(screen.queryByText("201%")).toBeNull();
    expect(screen.getByText(/never added/)).toBeTruthy();
  });

  it("gives a voting-only holder a note, not a figure", () => {
    render(<IntegratedOwnershipList payload={byName("a voting path above a shareholder")} />);
    expect(screen.getByText("Voting rights, not shares, on this path")).toBeTruthy();
    expect(screen.getByText("50%")).toBeTruthy();
  });

  it("names the route and marks a person", () => {
    render(<IntegratedOwnershipList payload={byName("a band carries both ends")} />);
    expect(screen.getByText("75–100%")).toBeTruthy();
    expect(screen.getByText("· person", { exact: false })).toBeTruthy();
    expect(screen.getByText("directly")).toBeTruthy();
  });

  it("states the method in plain words and dates a saved copy", () => {
    const payload = { ...byName("one path multiplies (T-Rank 3.1)"), as_of: "2026-01-02" };
    render(<IntegratedOwnershipList payload={payload} readOnly />);
    expect(screen.getByText(new RegExp(METHOD_DESCRIPTION.slice(0, 40)))).toBeTruthy();
    expect(screen.getByText(/as they stood on 2026-01-02/)).toBeTruthy();
  });

  it("says so when no share is stated anywhere", () => {
    render(<IntegratedOwnershipList payload={byName("roles and control are not read")} />);
    expect(screen.getByText(/No shareholding is stated/)).toBeTruthy();
  });
});
