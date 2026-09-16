/**
 * The saved report, rendered (Phase 217).
 *
 * The values — the banner's sentences, when Save is honest, what a saved
 * report holds — are pinned in `lib/savedReport.test.ts`. What is pinned here
 * is the markup those values must survive into: the banner is there and says
 * the page is not live, the licence panel reads the saved assessment and
 * offers the saved JSON instead of today's downloads, the summary is shown
 * read-only with its frozen sign-off, and the Save item keeps its reason
 * reachable when it cannot be pressed.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { SavedReportBanner, SavedReportExcluded } from "./SavedReportBanner";
import { NarrativePanel } from "./NarrativePanel";
import { ExportPanel } from "../export/ExportPanel";
import { ExportMenu } from "../export/ExportMenu";
import type { LicenseAssessment, NarrativeResponse, SavedReportMeta } from "../../lib/api";

const META: SavedReportMeta = {
  report_id: "SU82_KMkQo2QbEv3Kcfm8A",
  lei: "254900RT9QQBQZVH8O89",
  legal_name: "BIRTLEY INVESTMENT LIMITED",
  content_hash: "8b73a92b55d18b963f700550d467194c17721b51756dcc98005bf7220a1cca21",
  saved_at: "2026-09-16T15:18:42Z",
  expires_at: "2026-12-15T15:18:42Z",
  extended_at: null,
  size_bytes: 19330,
  report_path: "/report/SU82_KMkQo2QbEv3Kcfm8A",
  url: "https://opencheck.world/report/SU82_KMkQo2QbEv3Kcfm8A",
};

describe("SavedReportBanner", () => {
  it("names the saved report, both clocks, the expiry and the full hash", () => {
    render(<SavedReportBanner meta={META} runCompletedAt="2026-09-16T15:18:42+00:00" onRunLive={() => {}} onExtended={() => {}} />);
    const band = screen.getByRole("region", { name: "Saved report" });
    expect(band).toHaveTextContent("Saved 16 Sept 2026, 15:18 UTC, from a check that finished at 15:18 UTC.");
    expect(band).toHaveTextContent("Nothing on this page has been re-checked since it was saved.");
    expect(band).toHaveTextContent("Kept until 15 Dec 2026.");
    expect(band).toHaveTextContent(META.content_hash);
    expect(within(band).getByRole("link", { name: "Download the saved JSON" })).toHaveAttribute(
      "href",
      expect.stringContaining(`/saved-reports/${META.report_id}.json`),
    );
  });

  it("has no control that hides it", () => {
    render(<SavedReportBanner meta={META} runCompletedAt="2026-09-16T15:18:42+00:00" onRunLive={() => {}} onExtended={() => {}} />);
    const band = screen.getByRole("region", { name: "Saved report" });
    const names = within(band).queryAllByRole("button").map((b) => b.getAttribute("aria-label") ?? b.textContent);
    expect(names.some((n) => /hide|close|dismiss|collapse/i.test(n ?? ""))).toBe(false);
  });

  it("offers a live check of the same company", async () => {
    const onRunLive = vi.fn();
    render(<SavedReportBanner meta={META} runCompletedAt="2026-09-16T15:18:42+00:00" onRunLive={onRunLive} onExtended={() => {}} />);
    await userEvent.click(screen.getByRole("button", { name: "Run a live check" }));
    expect(onRunLive).toHaveBeenCalledOnce();
  });

  it("offers Keep for another 90 days only to the browser holding the manage token", () => {
    render(<SavedReportBanner meta={META} runCompletedAt="2026-09-16T15:18:42+00:00" onRunLive={() => {}} onExtended={() => {}} />);
    expect(screen.queryByRole("button", { name: "Keep for another 90 days" })).toBeNull();
  });

  it("puts a sentence in place of a tab the report does not hold", () => {
    render(<SavedReportExcluded sentence="History is not part of a saved report." onRunLive={() => {}} />);
    expect(screen.getByText("History is not part of a saved report.")).toBeInTheDocument();
  });
});

const LICENSING: LicenseAssessment = {
  commercial_use: "no",
  attribution_required: true,
  share_alike: false,
  color: "red",
  headline: "Non-commercial use only — OpenSanctions is CC-BY-NC.",
  warnings: [],
  per_source: [],
  disclaimer: "Informational only.",
};

describe("ExportPanel on a saved report", () => {
  it("builds every format from the saved report, with the saved licence and no subsidiary option", () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ExportPanel
          lei={META.lei}
          legalName={META.legal_name}
          contributingSourceIds={["opensanctions"]}
          saved={{ reportId: META.report_id, licensing: LICENSING }}
        />
      </QueryClientProvider>,
    );
    expect(screen.getByText(LICENSING.headline)).toBeInTheDocument();
    expect(screen.getByRole("radiogroup", { name: "Export format" })).toBeInTheDocument();
    const download = screen.getByRole("link", { name: "Download" });
    expect(download.getAttribute("href")).toContain(`saved_report_id=${META.report_id}`);
    expect(screen.getByRole("link", { name: "Download the saved JSON" })).toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /subsidiary network/ })).toBeNull();
    expect(screen.getByText(/built from the saved report — the same records, not a new check/)).toBeInTheDocument();
    // The licence matrix is not asked again: the saved assessment is the one that applied.
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
  });

  it("on a live check, says a download can differ later instead of calling it reproducible", () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("{}", { status: 503 }));
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ExportPanel lei={META.lei} legalName={META.legal_name} contributingSourceIds={[]} />
      </QueryClientProvider>,
    );
    expect(screen.queryByText(/Reproducible/)).toBeNull();
    expect(screen.getByText(/so one made later can differ/)).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: /subsidiary network/ })).toBeInTheDocument();
    vi.restoreAllMocks();
  });
});

const NARRATIVE: NarrativeResponse = {
  lei: META.lei,
  subject_name: META.legal_name ?? "",
  summary: "BIRTLEY INVESTMENT LIMITED's LEI lapsed on 15 September 2026.",
  claims: [{ id: "c1", text: "The LEI lapsed.", fact_ids: [], confidence: "high" }],
  limitations: [],
  overall_confidence: "high",
  model: "m",
  prompt_version: "p",
  run_id: "0123456789abcdef",
  generated_at: "2026-09-16T15:18:00Z",
  packet: { lei: META.lei, subject_name: "B", facts: [], risks: [], gaps: [] },
  validation_ok: true,
  dropped_claims: [],
  validation_issues: [],
  uncited_gaps: [],
} as unknown as NarrativeResponse;

describe("NarrativePanel on a saved report", () => {
  it("shows the saved summary and its frozen sign-off, with nothing to generate or edit", () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    render(
      <NarrativePanel
        lei={META.lei}
        saved={{
          narrative: NARRATIVE,
          dispositions: {
            lei: META.lei,
            run_id: "0123456789abcdef",
            prompt_version: "p",
            model: "m",
            reviewer: null,
            reviewed: true,
            dispositions: [{ claim_id: "c1", status: "disputed", comment: "Check the date" }],
          },
        }}
      />,
    );
    expect(screen.getByText(NARRATIVE.summary)).toBeInTheDocument();
    expect(screen.getByText("Marked as reviewed when saved")).toBeInTheDocument();
    expect(screen.getByText("Dispute")).toBeInTheDocument();
    expect(screen.getByText("Check the date")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Generate summary|Regenerate|Mark as reviewed|Accept/ })).toBeNull();
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
  });

  it("says when no summary was saved", () => {
    render(<NarrativePanel lei={META.lei} saved={{ narrative: null, dispositions: null }} />);
    expect(screen.getByText("No summary was saved with this report.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Generate summary" })).toBeNull();
  });
});

describe("the Save item in Share and export", () => {
  it("keeps the reason for a disabled Save reachable, and does nothing when pressed", async () => {
    const onSelect = vi.fn();
    render(
      <ExportMenu
        pdfBusy={false}
        mdBusy={false}
        onPdf={() => {}}
        onMarkdown={() => {}}
        onShare={() => {}}
        save={{ label: "Save this report", description: "Available when the check finishes.", disabled: true, onSelect }}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: /Export/ }));
    const item = screen.getByRole("menuitem", { name: /Save this report/ });
    expect(item).toHaveAttribute("aria-disabled", "true");
    expect(item).toHaveTextContent("Available when the check finishes.");
    expect(item).not.toBeDisabled();
    await userEvent.click(item);
    expect(onSelect).not.toHaveBeenCalled();
  });
});
