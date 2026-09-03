/**
 * The /sources catalogue with the weekly sweep's verdict on it (Phase 168,
 * testing Phase 161).
 *
 * `lib/sourceHealth.test.ts` pins the wording — Degraded is never Failed, a
 * skipped source is never Healthy. What it cannot pin is whether any of that
 * reaches the page: which cards open their details unasked, whether the
 * disclosure is a real control, and whether a page with no sweep behind it
 * renders exactly as it did before health existed. Those are the claims here.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SourcesPage } from "./SourcesPage";
import type { SourceHealthReport, SourceHealthRow, SourceInfo } from "../lib/api";

const fetchSourceHealth = vi.hoisted(() => vi.fn());
vi.mock("../lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/api")>()),
  fetchSourceHealth,
}));

const source = (id: string, name: string, overrides: Partial<SourceInfo> = {}): SourceInfo => ({
  id,
  name,
  homepage: `https://example.invalid/${id}`,
  description: `What ${name} is.`,
  license: "CC-BY-4.0",
  attribution: name,
  supports: ["entity"],
  requires_api_key: false,
  live_available: true,
  category: "cdd",
  is_national_register: true,
  ...overrides,
});

const SOURCES = [
  source("gleif", "GLEIF", { license: "CC0-1.0" }),
  source("jar_lithuania", "JAR — Lithuanian Register of Legal Entities"),
  source("bolagsverket", "Bolagsverket", { requires_api_key: true, live_available: false }),
  source("newcomer", "A source the sweep has never seen"),
];

const row = (overrides: Partial<SourceHealthRow> = {}): SourceHealthRow => ({
  status: "ok",
  reason: "",
  known_gap: "",
  liveness: "live",
  retrieved_at: "2026-08-31T07:31:02Z",
  latency_ms: 412,
  attempts: 1,
  statement_total: 12,
  statement_collapse: null,
  history: ["ok", "ok", "ok"],
  ...overrides,
});

const REPORT: SourceHealthReport = {
  available: true,
  generated_at: "2026-08-31T07:31:04Z",
  compared_against: "2026-08-24T07:30:58Z",
  registry_size: 40,
  probed: 40,
  counts: { ok: 34, degraded: 2, fail: 0, skipped: 4 },
  sweeps: [],
  sources: {
    gleif: row(),
    jar_lithuania: row({
      status: "degraded",
      reason: "register unreachable from CI (HTTP 403)",
      known_gap: "the register refuses datacentre IPs",
      liveness: null,
      retrieved_at: null,
      latency_ms: 1180,
      attempts: 2,
      statement_total: null,
      history: ["ok", "degraded", "degraded"],
    }),
    bolagsverket: row({
      status: "skipped",
      reason: "not configured: BOLAGSVERKET_API_KEY",
      liveness: null,
      retrieved_at: null,
      latency_ms: null,
      statement_total: null,
      history: ["skipped"],
    }),
  },
};

function renderPage(health: SourceHealthReport = REPORT) {
  fetchSourceHealth.mockResolvedValue(health);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <SourcesPage sources={SOURCES} loading={false} />
    </QueryClientProvider>,
  );
}

/** The `<li>` for a source, which is what "the card" means here. */
function card(name: string | RegExp): HTMLElement {
  const heading = screen.getByRole("link", { name });
  return heading.closest("li")!;
}

describe("SourcesPage", () => {
  beforeEach(() => fetchSourceHealth.mockReset());

  it("summarises the sweep once it has loaded, in the four statuses", async () => {
    renderPage();
    expect(await screen.findByText(/Last sweep · Mon 31 Aug 2026, 07:31 UTC/)).toBeInTheDocument();
    const strip = screen.getByText(/Last sweep ·/).parentElement!;
    for (const [count, word] of [["34", "healthy"], ["2", "degraded"], ["0", "failed"], ["4", "not tested"]]) {
      expect(within(strip).getByText(count)).toBeInTheDocument();
      expect(within(strip).getByText(word)).toBeInTheDocument();
    }
  });

  it("opens the details of a card with something to explain, and leaves a healthy one shut", async () => {
    renderPage();
    await screen.findByText(/Last sweep ·/);

    const degraded = card(/Lithuanian Register/);
    const degradedToggle = within(degraded).getByRole("button", { name: /Health details/ });
    expect(degradedToggle).toHaveAttribute("aria-expanded", "true");
    expect(within(degraded).getByText("register unreachable from CI (HTTP 403)")).toBeVisible();
    expect(
      within(degraded).getByText("Known gap: the register refuses datacentre IPs"),
    ).toBeVisible();

    const healthy = card("GLEIF");
    expect(within(healthy).getByRole("button", { name: /Health details/ })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });

  it("is a real disclosure: the panel it names appears and disappears", async () => {
    renderPage();
    await screen.findByText(/Last sweep ·/);
    const healthy = card("GLEIF");
    const toggle = within(healthy).getByRole("button", { name: /Health details/ });
    const panel = document.getElementById(toggle.getAttribute("aria-controls")!)!;

    expect(panel).not.toBeVisible();
    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(panel).toBeVisible();
    expect(within(healthy).getByText("Answered live")).toBeInTheDocument();
    expect(within(healthy).getByText("0.4 s")).toBeInTheDocument();
    expect(within(healthy).getByText("12 statements")).toBeInTheDocument();

    await userEvent.click(toggle);
    expect(panel).not.toBeVisible();
  });

  it("says Not tested of a source the sweep skipped — never healthy, never blank", async () => {
    renderPage();
    await screen.findByText(/Last sweep ·/);
    const skipped = card("Bolagsverket");
    expect(within(skipped).getByText("Not tested")).toBeInTheDocument();
    expect(within(skipped).queryByText("Healthy")).not.toBeInTheDocument();
    expect(within(skipped).getByText("not configured: BOLAGSVERKET_API_KEY")).toBeVisible();
  });

  it("renders nothing at all for a source the sweep has never seen", async () => {
    renderPage();
    await screen.findByText(/Last sweep ·/);
    const unknown = card(/never seen/);
    // Not "unknown": on a page where every other card carries a verdict, a
    // blank is the honest shape for "no verdict".
    expect(within(unknown).queryByRole("button", { name: /Health details/ })).not.toBeInTheDocument();
    expect(within(unknown).getByText(/Supports: entity/)).toBeInTheDocument();
  });

  it("renders the catalogue exactly as before when no sweep has published", async () => {
    renderPage({ available: false, reason: "no sweep has published a report yet" });
    expect(await screen.findByRole("link", { name: "GLEIF" })).toBeInTheDocument();
    expect(screen.queryByText(/Last sweep ·/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Health details/ })).not.toBeInTheDocument();
    expect(screen.queryByText("no sweep has published a report yet")).not.toBeInTheDocument();
  });

  it("says when it is showing the last copy it holds rather than the latest", async () => {
    renderPage({ ...REPORT, stale: true } as SourceHealthReport);
    expect(
      await screen.findByText(/The latest sweep could not be read just now/),
    ).toBeInTheDocument();
  });

  it("keeps the catalogue's own facts — licence, support, live-readiness", async () => {
    renderPage();
    await screen.findByText(/Last sweep ·/);
    const gleif = card("GLEIF");
    expect(within(gleif).getByText("CC0-1.0")).toBeInTheDocument();
    expect(within(gleif).getByText(/Supports: entity · live ready/)).toBeInTheDocument();
    // "live ready" is configuration, and the sweep's verdict is health: both
    // appear, because neither substitutes for the other.
    expect(within(gleif).getByText("Healthy")).toBeInTheDocument();
    expect(within(card("Bolagsverket")).getByText(/placeholder data/)).toBeInTheDocument();
  });
});
