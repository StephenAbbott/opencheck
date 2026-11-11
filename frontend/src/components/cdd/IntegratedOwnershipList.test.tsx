/**
 * IntegratedOwnershipList (Phase 339) — one row per source's figure, the
 * status note for a holder with no figure, the method in plain words, and no
 * threshold or highlighting of any kind.
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";

import table from "../../lib/integratedOwnershipCases.json";
import type { IntegratedOwnershipPayload } from "../../lib/integratedOwnership";
import {
  METHOD_DESCRIPTION,
  UNDATED_MARK,
  UNDATED_MARK_SPOKEN,
  UNDATED_NOTE,
  computeIntegratedOwnership,
  ownershipByNode,
} from "../../lib/integratedOwnership";
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
    // The fixture's holdings carry no dates, so each figure also carries the mark.
    expect(rows.map((r) => within(r).getAllByRole("cell")[1].textContent?.split(UNDATED_MARK)[0])).toEqual(["67%", "67%", "67%"]);
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

describe("IntegratedOwnershipList — the sanctions note (Phase 342)", () => {
  it("prints the note under the table only when given one", () => {
    const payload = byName("the Equinor shape: three sources, one stake, three figures never summed");
    const { unmount } = render(<IntegratedOwnershipList payload={payload} sanctionsNote="Sanctions ownership test, computed separately — OFAC (US): meets." />);
    expect(screen.getByTestId("sanctions-ownership-note").textContent).toMatch(/OFAC \(US\): meets/);
    unmount();
    render(<IntegratedOwnershipList payload={payload} />);
    expect(screen.queryByTestId("sanctions-ownership-note")).toBeNull();
  });
});

describe("IntegratedOwnershipList — undated figures (the undated-marker follow-up)", () => {
  const holder = (party: string, undated: boolean) => ({
    party,
    name: party,
    kind: "entity",
    status: "current",
    sources: ["OpenSanctions"],
    figures: [
      { source: "OpenSanctions", text: "40%", source_id: "opensanctions", minimum: 40, maximum: 40, direct: true, via_names: [], ...(undated ? { undated: true as const } : {}) },
    ],
  });
  const payload = {
    method: "integrated-ownership",
    as_of: "2026-11-11",
    holders: [holder("Undated Holder", true), holder("Dated Holder", false)],
    undated_note: UNDATED_NOTE,
  } as unknown as IntegratedOwnershipPayload;

  it("marks only the undated figure, says it to a screen reader, and opens the note with the mark", () => {
    render(<IntegratedOwnershipList payload={payload} />);
    const [undatedRow, datedRow] = screen.getAllByRole("row").slice(1);
    const cell = within(undatedRow).getAllByRole("cell")[1];
    expect(within(cell).getByTestId("integrated-ownership-undated-mark").textContent).toBe(UNDATED_MARK);
    expect(cell.textContent).toContain(UNDATED_MARK_SPOKEN);
    expect(within(datedRow).queryByTestId("integrated-ownership-undated-mark")).toBeNull();
    expect(screen.getByTestId("integrated-ownership-undated").textContent).toBe(`${UNDATED_MARK} ${UNDATED_NOTE}`);
  });

  it("says it in words on a tree row, which has no note beneath it", () => {
    const rows = ownershipByNode(payload);
    expect(rows.get("Undated Holder")).toBe("40% (OpenSanctions, no date given)");
    expect(rows.get("Dated Holder")).toBe("40% (OpenSanctions)");
  });
});
