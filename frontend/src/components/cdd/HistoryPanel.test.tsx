/**
 * HistoryPanel — the claims that live in the markup (Phase 190).
 *
 * The values (the sentence, the ordering, the links, the labels) are pinned in
 * `lib/historyMode.test.ts`. What is here can only be seen by rendering:
 *
 * - the tab asks `/history` once, for the whole entity, rather than once per
 *   source card as the button it replaces did;
 * - a long timeline stops at ten rows behind a secondary control;
 * - the administrative stream is additive and stays behind the same cap —
 *   GSK's is 1,121 rows;
 * - a register that holds the company and published nothing is named;
 * - a failed fetch is an alert that says so, and reports upwards.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const getHistory = vi.fn();
vi.mock("../../lib/api", () => ({
  getHistory: (...args: unknown[]) => getHistory(...args),
}));

import HistoryPanel from "./HistoryPanel";
import type { HistoryEntry, HistoryRawChange, HistoryResponse } from "../../lib/api";

const LEI = "213800IN6LSRGTZSOS29";

function entry(over: Partial<HistoryEntry> = {}): HistoryEntry {
  return {
    change_type: "LEGAL_NAME_CHANGE",
    label: "Legal name changed",
    tier: 2,
    record_type: "entity",
    date: "2022-01-11",
    date_basis: "effective",
    date_confidence: "high",
    value_old: "OLD LTD",
    value_new: "NEW LTD",
    sources: ["companies_house"],
    corroborating_sources: [],
    counterparty: null,
    interest_start_date: null,
    interest_end_date: null,
    boosted: false,
    ...over,
  };
}

function noise(date: string): HistoryRawChange {
  return {
    source_id: "companies_house",
    record_type: "entity",
    raw_change_type: "CS01",
    raw_field: "confirmation-statement",
    value_old: null,
    value_new: null,
    change_type: null,
    tier: 3,
    event_date: date,
    date_basis: "effective",
  };
}

function response(over: Partial<HistoryResponse> = {}): HistoryResponse {
  const notable = over.notable ?? [entry()];
  return {
    lei: LEI,
    company_number: "00358949",
    available: true,
    sources: ["gleif", "companies_house"],
    notable,
    events: [],
    gleif_record_available: true,
    gleif_events_available: true,
    registry_sources_blocked: false,
    company_number_basis: "live",
    registry_numbers: { companies_house: "00358949" },
    ...over,
    // Last, so an override of `notable` cannot leave a count contradicting it.
    notable_count: notable.length,
  };
}

beforeEach(() => {
  getHistory.mockReset();
  getHistory.mockResolvedValue(response());
});

describe("the coverage band", () => {
  it("asks once for the whole entity and counts what answered", async () => {
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    expect(await screen.findByText(/^Two registers publish a change log for Morrisons/)).toBeVisible();
    // One fetch, with the raw stream riding along so the toggle needs no
    // second round trip. The button this replaces fired one of these per
    // source card that could show a timeline.
    expect(getHistory).toHaveBeenCalledTimes(1);
    expect(getHistory).toHaveBeenCalledWith(LEI, true);
  });

  it("says most registers keep no history, so a short timeline is not a quiet company", async () => {
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    expect(await screen.findByText(/Most registers publish no history at all/)).toBeVisible();
    expect(screen.getByText(/not a change that did not happen/)).toBeVisible();
  });

  it("names a register that holds the company and published nothing", async () => {
    getHistory.mockResolvedValue(
      response({
        sources: ["gleif"],
        registry_numbers: { companies_house: "00358949", cvr_denmark: "12345678" },
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    const list = await screen.findByRole("list", {
      name: "Registers with no change log for this company",
    });
    const rows = within(list).getAllByRole("listitem");
    expect(rows.map((r) => r.textContent)).toEqual([
      "Companies House — holds this company, and published no change history for it here.",
      "CVR (DK) — holds this company, and published no change history for it here.",
    ]);
  });

  it("reports a failed fetch upwards and refuses to call it an absence of change", async () => {
    getHistory.mockRejectedValue(new Error("503 Service Unavailable"));
    const onPanelError = vi.fn();
    render(<HistoryPanel lei={LEI} legalName="Morrisons" onPanelError={onPanelError} />);
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /change history could not be fetched.*not a finding that nothing about Morrisons has changed/,
    );
    expect(onPanelError).toHaveBeenCalledWith(
      expect.objectContaining({ panel: "history" }),
    );
  });
});

describe("the timeline", () => {
  it("shows the ten most recent changes, then all of them when asked", async () => {
    const many = Array.from({ length: 14 }, (_, i) =>
      entry({ date: `20${String(10 + i).padStart(2, "0")}-06-01`, value_new: `NAME ${i + 1}` }),
    );
    getHistory.mockResolvedValue(response({ notable: many }));
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    const rail = await screen.findByTestId("history-rail");
    expect(within(rail).getAllByRole("listitem")).toHaveLength(10);
    // Newest first: the endpoint returns oldest-first, which is right for an
    // audit trail and wrong for a reader.
    expect(within(rail).getAllByRole("listitem")[0]).toHaveTextContent("2023-06-01");

    await userEvent.click(screen.getByRole("button", { name: "Show all 14 rows" }));
    expect(within(rail).getAllByRole("listitem")).toHaveLength(14);

    await userEvent.click(screen.getByRole("button", { name: "Show the first 10" }));
    expect(within(rail).getAllByRole("listitem")).toHaveLength(10);
  });

  it("links a row to the record in the register that published it", async () => {
    getHistory.mockResolvedValue(
      response({
        notable: [entry({ sources: ["companies_house", "gleif"] })],
        registry_numbers: { companies_house: "00358949" },
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    const rail = await screen.findByTestId("history-rail");
    expect(within(rail).getByRole("link", { name: /Companies House/ })).toHaveAttribute(
      "href",
      "https://find-and-update.company-information.service.gov.uk/company/00358949/filing-history",
    );
    expect(within(rail).getByRole("link", { name: /GLEIF/ })).toHaveAttribute(
      "href",
      `https://search.gleif.org/#/record/${LEI}`,
    );
  });

  it("keeps the administrative stream additive, behind a control and the same cap", async () => {
    getHistory.mockResolvedValue(
      response({
        notable: [entry({ date: "2022-01-11" })],
        events: Array.from({ length: 30 }, (_, i) =>
          noise(`2019-${String((i % 12) + 1).padStart(2, "0")}-01`),
        ),
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    const rail = await screen.findByTestId("history-rail");
    expect(within(rail).getAllByRole("listitem")).toHaveLength(1);

    await userEvent.click(
      screen.getByRole("button", { name: "Add the 30 administrative changes" }),
    );
    // 31 rows exist; the cap still holds, and the control to lift it appears.
    expect(within(rail).getAllByRole("listitem")).toHaveLength(10);
    expect(screen.getByRole("button", { name: "Show all 31 rows" })).toBeVisible();
    expect(screen.getByText(/suppressed, never dropped/)).toBeVisible();

    await userEvent.click(screen.getByRole("button", { name: "Hide administrative changes" }));
    expect(within(rail).getAllByRole("listitem")).toHaveLength(1);
  });

  it("draws no timeline band at all when nothing notable was recorded", async () => {
    getHistory.mockResolvedValue(response({ notable: [], available: true }));
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    expect(await screen.findByText(/none of them records a notable change/)).toBeVisible();
    expect(screen.queryByTestId("history-rail")).toBeNull();
  });
});
