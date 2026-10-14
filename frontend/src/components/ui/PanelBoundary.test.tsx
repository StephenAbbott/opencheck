/**
 * The error boundary around each lazy mode panel (Phase 246).
 *
 * Before it, a mode chunk that failed to load — or a panel that threw while
 * rendering — escaped `<Suspense>` with nothing to catch it and React
 * unmounted the whole app. What this pins is the markup: the failure is
 * contained, said in a sentence with a way out, and the rest of the page
 * stays on screen.
 */
import { lazy, Suspense } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { act } from "react";
import { PanelBoundary, PanelLoading, boundaryMessage } from "./PanelBoundary";
import { checkForNewVersion, resetBuildVersionForTests } from "../../lib/buildVersion";

function Boom(): never {
  throw new Error("render failed");
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  resetBuildVersionForTests();
});

describe("PanelBoundary", () => {
  it("renders its children when nothing fails", () => {
    render(
      <PanelBoundary label="FullCheck">
        <p>the network</p>
      </PanelBoundary>,
    );
    expect(screen.getByText("the network")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("contains a panel that throws, and names the tab", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    render(
      <div>
        <h1>The subject</h1>
        <PanelBoundary label="History">
          <Boom />
        </PanelBoundary>
      </div>,
    );
    // Phase 331: a render error is a bug, not a deploy — worded as one.
    expect(screen.getByRole("alert").textContent).toContain("The History tab hit an error");
    expect(screen.getByRole("button", { name: "Reload the page" })).toBeTruthy();
    // The page around it survives — the failure used to blank everything.
    expect(screen.getByRole("heading", { name: "The subject" })).toBeTruthy();
  });

  it("contains a lazy chunk that fails to load", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const Missing = lazy(() =>
      Promise.reject(new TypeError("Failed to fetch dynamically imported module")),
    );
    render(
      <PanelBoundary label="Subsidiaries">
        <Suspense fallback={<PanelLoading label="Subsidiaries" />}>
          <Missing />
        </Suspense>
      </PanelBoundary>,
    );
    expect(screen.getByText("Loading Subsidiaries…")).toBeTruthy();
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("The Subsidiaries tab could not be loaded");
  });

  it("contains a lazy graph without taking the tab with it (Phase 331)", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const Missing = lazy(() =>
      Promise.reject(new TypeError("Failed to fetch dynamically imported module")),
    );
    render(
      <PanelBoundary label="QuickCheck">
        <h2>Source records</h2>
        <PanelBoundary label="ownership diagram" kind="part" compact>
          <Suspense fallback={<p>Loading graph…</p>}>
            <Missing />
          </Suspense>
        </PanelBoundary>
      </PanelBoundary>,
    );
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("The ownership diagram could not be loaded");
    expect(screen.getByRole("heading", { name: "Source records" })).toBeTruthy();
  });

  it("says OpenCheck was updated once a newer build is known", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const Missing = lazy(() =>
      Promise.reject(new TypeError("Failed to fetch dynamically imported module")),
    );
    render(
      <PanelBoundary label="FullCheck">
        <Suspense fallback={<PanelLoading label="FullCheck" />}>
          <Missing />
        </Suspense>
      </PanelBoundary>,
    );
    await screen.findByRole("alert");
    const fetcher = vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ entry: "assets/index-B.js" }) });
    await act(async () => {
      await checkForNewVersion(fetcher, "assets/index-A.js");
    });
    expect(screen.getByRole("alert").textContent).toContain(
      "OpenCheck has been updated since this page was opened, and the FullCheck tab belongs to the new version",
    );
  });
});

describe("boundaryMessage", () => {
  it("names a tab and a part differently", () => {
    expect(boundaryMessage("History", "tab", "chunk", false)).toMatch(/^The History tab could not be loaded/);
    expect(boundaryMessage("person report", "part", "chunk", false)).toMatch(/^The person report could not be loaded/);
  });

  it("always says the rest of the report is unaffected and how to recover", () => {
    for (const cause of ["chunk", "render"] as const) {
      for (const fresh of [true, false]) {
        const m = boundaryMessage("History", "tab", cause, fresh);
        expect(m).toContain("The rest of this report is unaffected.");
        expect(m).toMatch(/Reload/);
      }
    }
  });

  it("never blames a deploy for a render error", () => {
    expect(boundaryMessage("History", "tab", "render", true)).not.toMatch(/updated/);
  });
});
