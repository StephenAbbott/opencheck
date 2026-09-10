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
// Phase 200: the panel also asks `/lookup` — replay-cached, and only to learn
// which people the FullCheck network draws, so a board row offers a link
// exactly when there is a node to reach. Defaults to a graph with no people,
// which is the pre-Phase-200 rail; the tests that care set their own.
const lookup = vi.fn();
vi.mock("../../lib/api", () => ({
  getHistory: (...args: unknown[]) => getHistory(...args),
  lookup: (...args: unknown[]) => lookup(...args),
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
    label: null,
    counterparty: null,
    tier: 3,
    event_date: date,
    date_basis: "effective",
  };
}

function boardChange(
  date: string,
  name: string | null = "Mr Kelly Brian Bennett",
  personSid: string | null = null,
): HistoryRawChange {
  return {
    source_id: "companies_house",
    record_type: "relationship",
    raw_change_type: "appointed_on",
    raw_field: "officers/director",
    value_old: null,
    value_new: null,
    change_type: "OFFICER_APPOINTED",
    label: "Officer appointed",
    counterparty: name,
    tier: 4,
    event_date: date,
    date_basis: "effective",
    party_id: personSid ? "ch-officer-key" : null,
    party_statement_id: personSid,
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
  lookup.mockReset();
  lookup.mockResolvedValue({ bods: [] });
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

  it("keeps board turnover in a stream of its own, additive like the noise", async () => {
    getHistory.mockResolvedValue(
      response({
        notable: [entry({ date: "2022-01-11" })],
        events: [
          boardChange("2026-09-01"),
          boardChange("1996-07-11", null),
          noise("2019-01-01"),
        ],
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    const rail = await screen.findByTestId("history-rail");
    expect(within(rail).getAllByRole("listitem")).toHaveLength(1);

    // Two controls, two streams: adding one must not add the other.
    await userEvent.click(screen.getByRole("button", { name: "Add the 2 board changes" }));
    expect(within(rail).getAllByRole("listitem")).toHaveLength(3);
    expect(within(rail).getByText("Mr Kelly Brian Bennett")).toBeVisible();
    expect(screen.queryByText("CS01")).toBeNull();

    await userEvent.click(screen.getByRole("button", { name: "Hide board changes" }));
    expect(within(rail).getAllByRole("listitem")).toHaveLength(1);
  });

  it("says a filing that names nobody names nobody", async () => {
    getHistory.mockResolvedValue(
      response({ notable: [entry()], events: [boardChange("1996-07-11", null)] }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await screen.findByTestId("history-rail");
    await userEvent.click(screen.getByRole("button", { name: "Add the 1 board changes" }));
    expect(screen.getByText("no name on the filing")).toBeVisible();
    // And the stream states the gap rather than leaving it to be noticed.
    expect(screen.getByText(/none of which name the officer/)).toBeVisible();
  });

  it("says so when the register holds more filings than the view read", async () => {
    getHistory.mockResolvedValue(response({ filings_truncated: true }));
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    expect(
      await screen.findByText(/holds more filings than this view reads/),
    ).toBeVisible();
    expect(screen.getByText(/oldest/)).toBeVisible();
  });

  it("does not mention truncation when the whole history was read", async () => {
    getHistory.mockResolvedValue(response());
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await screen.findByTestId("history-rail");
    expect(screen.queryByText(/more filings than this view reads/)).toBeNull();
  });

  it("draws no timeline band at all when nothing notable was recorded", async () => {
    getHistory.mockResolvedValue(response({ notable: [], available: true }));
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    expect(await screen.findByText(/none of them records a notable change/)).toBeVisible();
    expect(screen.queryByTestId("history-rail")).toBeNull();
  });
});

// ---------------------------------------------------------------------
// Phase 200 — a board row reaching the person the graph draws
// ---------------------------------------------------------------------

describe("linking a board row to a person", () => {
  const SERVING = "opencheck-serving-director";

  function withGraphPeople(...ids: string[]) {
    lookup.mockResolvedValue({
      bods: ids.map((statementId) => ({ recordType: "person", statementId })),
    });
  }

  async function openBoardStream() {
    const rail = await screen.findByTestId("history-rail");
    await userEvent.click(
      screen.getByRole("button", { name: /Add the .* board changes?/ }),
    );
    return rail;
  }

  it("offers a labelled link on a row whose person the network draws", async () => {
    withGraphPeople(SERVING);
    getHistory.mockResolvedValue(
      response({
        notable: [entry()],
        events: [boardChange("2019-01-01", "BENNETT, Kelly Brian", SERVING)],
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await openBoardStream();

    // A named control, not an underline on the name: only a minority of rows
    // can link, and a bare link would read as arbitrary.
    const link = await screen.findByRole("link", { name: /in the network/ });
    expect(link).toHaveAttribute(
      "href",
      `/?lei=${LEI}&mode=full&focus=${SERVING}`,
    );
    // The name itself stays plain text.
    expect(
      screen.getByText("BENNETT, Kelly Brian").closest("a"),
    ).toBeNull();
  });

  it("offers nothing on a row whose person the network does not draw", async () => {
    // A resigned director: keyed by the register, on this stream, and
    // deliberately absent from a graph of serving managing officials.
    // Linking to a node that is not there is worse than not linking.
    withGraphPeople(SERVING);
    getHistory.mockResolvedValue(
      response({
        notable: [entry()],
        events: [boardChange("2004-09-30", "PRIOR, Jane", "opencheck-resigned")],
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await openBoardStream();

    expect(screen.getByText("PRIOR, Jane")).toBeVisible();
    expect(screen.queryByRole("link", { name: /in the network/ })).toBeNull();
  });

  it("offers nothing on a row the source identified nobody on", async () => {
    withGraphPeople(SERVING);
    getHistory.mockResolvedValue(
      response({ notable: [entry()], events: [boardChange("1996-07-11", null)] }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await openBoardStream();

    expect(screen.queryByRole("link", { name: /in the network/ })).toBeNull();
  });

  it("says how many rows link and why the rest do not", async () => {
    withGraphPeople(SERVING);
    getHistory.mockResolvedValue(
      response({
        notable: [entry()],
        events: [
          boardChange("2019-01-01", "BENNETT, Kelly Brian", SERVING),
          boardChange("2004-09-30", "PRIOR, Jane", "opencheck-resigned"),
        ],
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await openBoardStream();

    expect(
      await screen.findByText(/One row reaches a person in the network/),
    ).toBeVisible();
    expect(screen.getByText(/currently serving/)).toBeVisible();
  });

  it("hands the click up rather than reloading, and still resolves the href", async () => {
    // The mode switch belongs to the page above — the graph lives in another
    // mode and this tab cannot mount it.
    withGraphPeople(SERVING);
    const onFocusPerson = vi.fn();
    getHistory.mockResolvedValue(
      response({
        notable: [entry()],
        events: [boardChange("2019-01-01", "BENNETT, Kelly Brian", SERVING)],
      }),
    );
    render(
      <HistoryPanel lei={LEI} legalName="Morrisons" onFocusPerson={onFocusPerson} />,
    );
    await openBoardStream();

    await userEvent.click(await screen.findByRole("link", { name: /in the network/ }));
    expect(onFocusPerson).toHaveBeenCalledWith(SERVING);
  });

  it("offers no link at all when the network could not be read", async () => {
    // A missing link is a smaller wrong than a broken one, so a failed
    // /lookup silently means "no links", never "link everything".
    lookup.mockRejectedValue(new Error("network down"));
    getHistory.mockResolvedValue(
      response({
        notable: [entry()],
        events: [boardChange("2019-01-01", "BENNETT, Kelly Brian", SERVING)],
      }),
    );
    render(<HistoryPanel lei={LEI} legalName="Morrisons" />);
    await openBoardStream();

    expect(screen.getByText("BENNETT, Kelly Brian")).toBeVisible();
    expect(screen.queryByRole("link", { name: /in the network/ })).toBeNull();
  });
});
