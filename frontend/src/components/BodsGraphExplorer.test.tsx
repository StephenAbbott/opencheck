/**
 * The FullCheck header's two states — the claim that lives in the markup.
 *
 * Which control is on screen is not a value a `lib/` test can see: the header
 * before a run and the header after one are different trees, and the bug this
 * replaced was precisely that they were the same one. So this tier asserts the
 * switch itself — that the run control gives way to a summary of what it built,
 * that "Go deeper" brings it back, and that Reset returns the panel to the
 * network the subject's own lookup produced.
 *
 * It also pins the wiring of the single-layer control into the canvas toolbar.
 * BODSGraph is stubbed: Cytoscape measures a canvas jsdom does not have, and
 * the claim here is what the explorer HANDS the toolbar, not how the toolbar
 * draws it. What the control says is pinned in `lib/fullCheckHeader.test.ts`.
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const expandLayer = vi.fn();
vi.mock("../lib/api", () => ({
  expandLayer: (...args: unknown[]) => expandLayer(...args),
  fetchExpandSchemes: () => Promise.resolve(new Set<string>()),
  downloadNetwork: vi.fn(),
}));

vi.mock("./BODSGraph", () => ({
  default: ({
    layer,
    onAddLayer,
  }: {
    layer?: { label: string; count: number | null; ariaLabel: string; disabled: boolean };
    onAddLayer?: () => void;
  }) => (
    <div data-testid="canvas">
      {layer && onAddLayer && (
        <button
          type="button"
          disabled={layer.disabled}
          aria-label={layer.ariaLabel}
          onClick={onAddLayer}
        >
          {layer.label}
          {layer.count !== null ? ` ${layer.count}` : ""}
        </button>
      )}
    </div>
  ),
}));

import BodsGraphExplorer from "./BodsGraphExplorer";

type Stmt = Record<string, unknown>;

function entity(id: string, lei: string): Stmt {
  return {
    statementId: id,
    recordType: "entity",
    recordDetails: {
      entityType: { type: "registeredEntity" },
      name: id,
      identifiers: [{ id: lei, scheme: "XI-LEI", schemeName: "LEI" }],
    },
    // `stmtSources` reads the source description; without one the network has
    // no provenance and the legend under the canvas never renders.
    source: { description: "GLEIF" },
  };
}

const A = entity("A", "5493001KJTIIGC8Y1R12");
const B = entity("B", "5493004YR8F4DUF6C453");
const C = entity("C", "213800MBWEIJDM5CU638");

function renderPanel() {
  return render(<BodsGraphExplorer statements={[A, B]} fullCheck signals={[]} />);
}

/** The summary's own live region. `expandNote` is a status too — a bare
 *  `getByRole("status")` matches both and throws. */
const summary = () => screen.getByRole("status", { name: "FullCheck run summary" });
const noSummary = () => screen.queryByRole("status", { name: "FullCheck run summary" });

/** Walk one layer, the way a reader does: the toolbar control. */
async function addLayer(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: /Add the next layer/ }));
  await waitFor(() => expect(summary()).toBeInTheDocument());
}

beforeEach(() => {
  expandLayer.mockReset();
  expandLayer.mockResolvedValue({
    bods: [C],
    risk_signals: [],
    expanded: ["A", "B"],
    count: 2,
    truncated: false,
  });
});

describe("before a run", () => {
  it("offers one primary action and no summary of a network nobody has built", () => {
    renderPanel();
    expect(screen.getByRole("button", { name: /Run FullCheck/ })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Go deeper" })).not.toBeInTheDocument();
    expect(noSummary()).not.toBeInTheDocument();
  });

  it("hands the toolbar the frontier count, named for what it would resolve", () => {
    renderPanel();
    const control = screen.getByRole("button", { name: /Add the next layer/ });
    expect(control).toHaveAccessibleName(/2 companies/);
    expect(control).toHaveAccessibleName(/owners and controllers/);
    expect(control).toHaveTextContent("+1 layer 2");
  });

  it("says what FullCheck would add to the screening, not what it found", () => {
    renderPanel();
    expect(screen.getByText(/Run FullCheck to screen the wider network for risk/)).toBeInTheDocument();
  });
});

describe("after a layer lands", () => {
  it("collapses the run control into a summary of what was built", async () => {
    const user = userEvent.setup();
    renderPanel();
    await addLayer(user);

    expect(summary()).toHaveTextContent("FullCheck");
    expect(summary()).toHaveTextContent("1 layer by hand");
    expect(summary()).toHaveTextContent("3 companies");
    // The header stops offering a decision the reader has already made.
    expect(screen.queryByRole("button", { name: /Run FullCheck/ })).not.toBeInTheDocument();
  });

  it("keeps the network reachable: Go deeper restores the control", async () => {
    const user = userEvent.setup();
    renderPanel();
    await addLayer(user);

    await user.click(screen.getByRole("button", { name: "Go deeper" }));
    expect(screen.getByRole("button", { name: /Go deeper$/ })).toBeInTheDocument();
    expect(screen.getByLabelText("Depth")).toBeInTheDocument();
  });

  it("Reset returns to the network the subject's own lookup produced", async () => {
    const user = userEvent.setup();
    renderPanel();
    await addLayer(user);

    await user.click(screen.getByRole("button", { name: "Reset" }));
    await waitFor(() => expect(noSummary()).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: /Run FullCheck/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Add the next layer/ })).toHaveAccessibleName(
      /2 companies/,
    );
  });
});

describe("the provenance legend", () => {
  it("sits under the canvas it describes, never above it", async () => {
    const user = userEvent.setup();
    renderPanel();
    await addLayer(user);

    const canvas = screen.getByTestId("canvas");
    const legend = screen.getByText(/Sources in this network/);
    // Above the canvas it was source chips and a corroboration count standing
    // between the reader and a diagram they had not seen yet.
    expect(canvas.compareDocumentPosition(legend) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});
