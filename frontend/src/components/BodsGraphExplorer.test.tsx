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

// ---------------------------------------------------------------------
// Phase 200 — focusing a statement, including one that was merged away
// ---------------------------------------------------------------------

function person(id: string, fullName: string, source: string): Stmt {
  return {
    statementId: id,
    recordType: "person",
    recordDetails: {
      personType: "knownPerson",
      names: [{ type: "individual", fullName }],
      birthDate: "1968-04",
    },
    source: { description: source },
  };
}

function directorship(id: string, subject: string, personId: string): Stmt {
  return {
    statementId: id,
    recordType: "relationship",
    recordDetails: {
      subject: { describedByEntityStatement: subject },
      interestedParty: { describedByPersonStatement: personId },
      interests: [{ type: "seniorManagingOfficial", directOrIndirect: "direct" }],
    },
    source: { description: "Companies House" },
  };
}

/** Open the text equivalent — the tree is where selection is legible; the
 *  canvas is stubbed because Cytoscape measures a canvas jsdom does not have. */
async function openTree(_user: ReturnType<typeof userEvent.setup>) {
  // jsdom does not implement <details> toggling, so open it directly rather
  // than clicking the <summary> (which is not a button and has no role here).
  const details = screen.getByText(/Read as text/).closest("details");
  if (details) details.open = true;
}

describe("focusing a statement from outside the graph", () => {
  const CH_PERSON = "opencheck-ch-kelly";
  const OC_PERSON = "opencheck-oc-kelly";

  it("selects the node a raw statement id resolves to after reconciliation", async () => {
    // The Phase 200 bug in one test. Phase 195 merges these two records of one
    // person into a canonical `recon:PERSON:...` node, so the Companies House
    // id a History board row carries names no node any more. Before the remap
    // was applied the click appeared to work and focused nothing at all —
    // worse than an error, because nothing said so.
    const user = userEvent.setup();
    render(
      <BodsGraphExplorer
        statements={[
          A,
          person(CH_PERSON, "Kelly Brian Bennett", "Companies House"),
          person(OC_PERSON, "BENNETT, Kelly Brian", "OpenCorporates"),
          directorship("rel-1", "A", CH_PERSON),
          directorship("rel-2", "A", OC_PERSON),
        ]}
        fullCheck
        signals={[]}
        focusStatementId={CH_PERSON}
      />,
    );
    await openTree(user);

    const selected = await screen.findAllByRole("treeitem", { selected: true });
    expect(selected).toHaveLength(1);
    // One node, under neither source's id.
    expect(selected[0]).toHaveTextContent(/Bennett/i);
  });

  it("selects an unmerged statement under its own id", async () => {
    const user = userEvent.setup();
    render(
      <BodsGraphExplorer
        statements={[A, person(CH_PERSON, "Kelly Brian Bennett", "Companies House"),
          directorship("rel-1", "A", CH_PERSON)]}
        fullCheck
        signals={[]}
        focusStatementId={CH_PERSON}
      />,
    );
    await openTree(user);

    expect(await screen.findAllByRole("treeitem", { selected: true })).toHaveLength(1);
  });

  it("selects nothing, and does not throw, for a statement this graph lacks", async () => {
    // A board row can only offer the link when the graph holds the person, but
    // a hand-edited or stale `?focus=` must fail quietly rather than loudly.
    const user = userEvent.setup();
    render(
      <BodsGraphExplorer
        statements={[A, B]}
        fullCheck
        signals={[]}
        focusStatementId="opencheck-not-here"
      />,
    );
    await openTree(user);

    expect(screen.queryAllByRole("treeitem", { selected: true })).toHaveLength(0);
  });

  it("selects nothing when no focus was asked for", async () => {
    const user = userEvent.setup();
    renderPanel();
    await openTree(user);

    expect(screen.queryAllByRole("treeitem", { selected: true })).toHaveLength(0);
  });
});
