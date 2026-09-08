/**
 * SubsidiariesPanel — the claims that live in the markup (Phase 185).
 *
 * The values (resolution, ordering, the sentences) are pinned in
 * `lib/subsidiariesMode.test.ts`. What is here can only be seen by rendering:
 *
 * - a row that can be opened is a link to that company's own Subsidiaries
 *   tab, and a name-derived one is chipped as a name match;
 * - a refused GLEIF network is named as such in the coverage strip and never
 *   enters the comparison — the claims that used to live in
 *   `EitiAssessmentCard.test.tsx`;
 * - a failed declared-lists fetch is an alert beside a still-working GLEIF
 *   band, not an empty tab;
 * - a source that does not cover the company says why.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const getSubsidiaries = vi.fn();
const getDeclaredSubsidiaries = vi.fn();
vi.mock("../../lib/api", () => ({
  getSubsidiaries: (...args: unknown[]) => getSubsidiaries(...args),
  getDeclaredSubsidiaries: (...args: unknown[]) => getDeclaredSubsidiaries(...args),
}));

import SubsidiariesPanel from "./SubsidiariesPanel";
import type { DeclaredSource, DeclaredSubsidiariesResponse } from "../../lib/api";

const LEI = "21380068P1DRHMJ8KU70";
const NORSKE = "213800F4ETX85XLF5K47";

function gleif(children: { lei: string; name: string }[], over: Record<string, unknown> = {}) {
  return {
    lei: LEI,
    available: children.length > 0,
    reason: null,
    children_available: true,
    direct_available: true,
    ultimate_available: true,
    snapshot_fallback: false,
    snapshot_date: null,
    degraded_detail: null,
    direct_total: children.length,
    ultimate_total: 0,
    distinct_fetched: children.length,
    indirect_only: 0,
    node_estimate: children.length,
    render_mode: "table" as const,
    truncated: false,
    jurisdictions: [],
    children: children.map((c) => ({ ...c, jurisdiction: "GB", status: "ACTIVE", relation: "direct" as const, link: null })),
    bods: null,
    ...over,
  };
}

function source(id: DeclaredSource["id"], rows: { name: string; lei?: string | null; country?: string }[], over: Partial<DeclaredSource> = {}): DeclaredSource {
  return {
    id,
    label: id,
    measures: `what ${id} measures`,
    homepage: "https://example.org",
    available: true,
    covered: true,
    reason: null,
    total: null,
    listed: rows.length,
    with_lei: rows.filter((r) => r.lei).length,
    context: null,
    rows: rows.map((r) => ({
      name: r.name,
      lei: r.lei ?? null,
      country: r.country ?? null,
      relation: id === "eiti_assessment" ? "declared" : "direct",
      percent: null,
      years: id === "eiti_assessment" ? ["2024"] : [],
      via: null,
    })),
    ...over,
  };
}

function declared(sources: DeclaredSource[]): DeclaredSubsidiariesResponse {
  return {
    lei: LEI,
    sources,
    covered: sources.filter((s) => s.covered).length,
    listed: sources.reduce((n, s) => n + s.rows.length, 0),
    with_lei: sources.reduce((n, s) => n + s.with_lei, 0),
  };
}

const THREE = declared([
  source("meip", [{ name: "A/S NORSKE SHELL", lei: NORSKE, country: "NOR" }], { total: 1865 }),
  source("eiti_assessment", [
    { name: "A/S Norske Shell", country: "NOR" },
    { name: "Atlantic 1 Holdings LLC", country: "TTO" },
  ]),
  source("climatetrace", [], { covered: false, reason: "not a GEM entity" }),
]);

beforeEach(() => {
  getSubsidiaries.mockReset();
  getDeclaredSubsidiaries.mockReset();
  getSubsidiaries.mockResolvedValue(gleif([{ lei: "5493001KJTIIGC8Y1R12", name: "Shell Energy North America" }]));
  getDeclaredSubsidiaries.mockResolvedValue(THREE);
});

describe("the coverage strip", () => {
  it("counts the lists, names the disagreement, and says who does not cover the company", async () => {
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    expect(await screen.findByText(/^Three sources list what Shell plc owns/)).toBeVisible();
    expect(screen.getByText(/only 1 name appears in more than one of them/)).toBeVisible();
    const pills = within(screen.getByRole("list", { name: "Lists available" })).getAllByRole("listitem");
    expect(pills.map((p) => p.textContent)).toEqual([
      "GLEIF Level 2 · 1 · 1 can be opened",
      "OECD-UNSD MEIP · 1 · 1 can be opened",
      "EITI · 2 · 1 can be opened",
    ]);
    expect(screen.getByText("Global Energy Monitor — not a GEM entity.")).toBeVisible();
    // The fetches happened once each.
    expect(getSubsidiaries).toHaveBeenCalledTimes(1);
    expect(getDeclaredSubsidiaries).toHaveBeenCalledWith(LEI);
  });

  it("names a refused GLEIF network and keeps it out of the comparison", async () => {
    getSubsidiaries.mockResolvedValue(
      gleif([], { children_available: false, degraded_detail: "GLEIF rate-limited the direct-children call" }),
    );
    const onPanelError = vi.fn();
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" onPanelError={onPanelError} />);
    expect(await screen.findByText(/^Two sources list what Shell plc owns/)).toBeVisible();
    expect(screen.getByText(/GLEIF Level 2 — did not answer/)).toBeVisible();
    expect(onPanelError).toHaveBeenCalledWith(
      expect.objectContaining({ panel: "subsidiaries", detail: "GLEIF rate-limited the direct-children call" }),
    );
  });

  it("reports a failed declared-lists fetch beside a working GLEIF band", async () => {
    getDeclaredSubsidiaries.mockRejectedValue(new Error("503 Service Unavailable"));
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    expect(await screen.findByRole("alert")).toHaveTextContent(/declared lists could not be fetched/);
    expect(await screen.findByText("Shell Energy North America")).toBeVisible();
    expect(screen.getByText(/^One source lists what Shell plc owns/)).toBeVisible();
  });
});

describe("the rows", () => {
  it("links every openable row to its own Subsidiaries tab and chips a name match", async () => {
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    const eiti = await screen.findByTestId("declared-list-eiti_assessment");
    const rows = within(eiti).getAllByRole("listitem");
    expect(rows).toHaveLength(2);
    // Openable first: the EITI name that MEIP holds an LEI for.
    const link = within(rows[0]).getByRole("link", { name: "A/S Norske Shell" });
    expect(link).toHaveAttribute("href", `/?lei=${NORSKE}&mode=subsidiaries`);
    expect(within(rows[0]).getByText(/from OECD-UNSD MEIP/)).toBeVisible();
    expect(within(rows[0]).getByText("Also in OECD-UNSD MEIP")).toBeVisible();
    // ●◐○ mean "corroborated by two or more sources". A name match is not that.
    expect(rows[0].textContent ?? "").not.toMatch(/[●◐○]/);
    // The other declared name has nothing to open and says so.
    expect(within(rows[1]).queryByRole("link")).toBeNull();
    expect(within(rows[1]).getByText("no LEI published")).toBeVisible();

    // MEIP's own row carries its LEI and links directly.
    const meip = screen.getByTestId("declared-list-meip");
    expect(within(meip).getByRole("link", { name: "A/S NORSKE SHELL" })).toHaveAttribute(
      "href",
      `/?lei=${NORSKE}&mode=subsidiaries`,
    );
    // GLEIF's child links to its report too, with GLEIF's record one click away.
    expect(screen.getByRole("link", { name: "Shell Energy North America" })).toHaveAttribute(
      "href",
      "/?lei=5493001KJTIIGC8Y1R12&mode=subsidiaries",
    );
  });

  it("shows twelve rows, then all of them when asked", async () => {
    const many = Array.from({ length: 20 }, (_, i) => ({ name: `Declared ${i + 1} Ltd` }));
    getDeclaredSubsidiaries.mockResolvedValue(declared([source("eiti_assessment", many)]));
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    const list = await screen.findByTestId("declared-list-eiti_assessment");
    expect(within(list).getAllByRole("listitem")).toHaveLength(12);
    await userEvent.click(screen.getByRole("button", { name: "Show all 20 rows" }));
    expect(within(list).getAllByRole("listitem")).toHaveLength(20);
  });

  it("collapses the GLEIF network to twelve rows like the declared lists", async () => {
    // The GLEIF band used to print every child — 154 of them for Shell — while
    // the three declared lists stopped at twelve. Same band, same behaviour.
    // Names are zero-padded so the list's own A–Z ordering is the numbering.
    const many = Array.from({ length: 20 }, (_, i) => ({
      lei: `LEI${String(i).padStart(17, "0")}`,
      name: `Child ${String(i + 1).padStart(2, "0")} Ltd`,
    }));
    getSubsidiaries.mockResolvedValue(gleif(many));
    getDeclaredSubsidiaries.mockResolvedValue(declared([]));
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    expect(await screen.findByText("Child 01 Ltd")).toBeVisible();
    expect(screen.getByText("Child 12 Ltd")).toBeVisible();
    expect(screen.queryByText("Child 13 Ltd")).toBeNull();
    await userEvent.click(screen.getByRole("button", { name: "Show all 20 rows" }));
    expect(screen.getByText("Child 20 Ltd")).toBeVisible();
  });

  it("says out loud that MEIP holds only part of what the register publishes", async () => {
    render(<SubsidiariesPanel lei={LEI} legalName="Shell plc" />);
    expect(await screen.findByText(/1 of 1,865 listed here/)).toBeVisible();
  });
});
