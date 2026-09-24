import { MODE_ACCENT, TOPIC_MODES, modeForKey, type CheckMode } from "../../lib/checkMode";
import { Icon, type IconName } from "../ui";
import PanelSection from "../ui/PanelSection";

/*
 * The report's check-mode tablist and each mode's one-sentence blurb. Moved
 * out of App.tsx in Phase 246, unchanged; App still owns the mode itself and
 * `selectMode`, the single entry point that writes `?mode=`, fires the
 * analytics event and moves focus into the new panel.
 */

/**
 * The five checks, in the order they escalate: the subject alone, its
 * network, the people in it — then the two topics, Subsidiaries (what it
 * owns, Phase 185) and Climate & ESG, which are different questions rather
 * than further depths, and are separated in the strip to say so. Accents
 * come from `MODE_ACCENT` in lib/checkMode.ts — the `oo.node.*` brand tier
 * that already names each mode's badge, plus the graph's control colour
 * for subsidiaries — so the tab, the badge and the graph edge are one
 * colour rather than three.
 */
export const MODE_TABS: {
  id: CheckMode;
  label: string;
  icon: IconName;
  accent: string;
  blurb: string;
  topic?: boolean;
}[] = [
  {
    id: "quick",
    label: "QuickCheck",
    icon: "quickcheck",
    accent: MODE_ACCENT.quick,
    blurb: "Screening this company on its own — sanctions, control, structure. The fastest answer.",
  },
  {
    id: "full",
    label: "FullCheck",
    icon: "fullcheck",
    accent: MODE_ACCENT.full,
    blurb: "Following the ownership chain outwards, then screening everything it reaches.",
  },
  {
    id: "background",
    label: "BackgroundCheck",
    icon: "backgroundcheck",
    accent: MODE_ACCENT.background,
    blurb: "Screening the officers, directors and beneficial owners named in the records.",
  },
  {
    id: "subsidiaries",
    label: "Subsidiaries",
    icon: "subsidiaries",
    accent: MODE_ACCENT.subsidiaries,
    blurb: "What this company owns, from every source that publishes a list — they disagree, and the tab says why.",
    topic: TOPIC_MODES.has("subsidiaries"),
  },
  {
    id: "history",
    label: "History",
    icon: "history",
    accent: MODE_ACCENT.history,
    blurb: "How this company's records changed, merged from every register that keeps a change log — most keep none.",
    topic: TOPIC_MODES.has("history"),
  },
  {
    id: "esg",
    label: "Climate & ESG",
    icon: "esg",
    accent: MODE_ACCENT.esg,
    blurb: "Emissions and asset records published about this company — what it does, rather than who owns it.",
    topic: TOPIC_MODES.has("esg"),
  },
];

/**
 * The mode tab strip. `onSelect` is App's `selectMode`; arrow keys pass
 * `focusPanel: false` so focus stays on the tab they moved to (Phase 241).
 */
export function ModeTabs({
  mode,
  onSelect,
}: {
  mode: CheckMode;
  onSelect: (next: CheckMode, opts?: { focusPanel?: boolean }) => void;
}) {
  return (
    <div
      role="tablist"
      aria-label="Check mode"
      /* No bottom margin: the tab strip claims the card beneath it, and
         a 24px gap between them breaks the claim — the active tab's
         white edge has to meet the card's. The honesty notices that can
         sit between the two carry their own top margin instead, so they
         are the exception rather than the default spacing. */
      /* Phase 157: below `sm` the strip is a 2×2 grid of stacked
         icon-over-label cells — the pattern the search-method tablist
         already uses. The one-row strip needs ~657px, so on a 390px
         phone a reader saw "QuickCheck · FullCheck · Ba…" and two of
         the four modes did not exist unless they knew to swipe. From
         `sm` up the strip is unchanged (padding eases to px-3 until
         `md` so it still fits at 700), and the wrappers collapse to
         `contents` on phones so the buttons are the grid cells. */
      className="grid grid-cols-2 overflow-hidden rounded-oo border border-oo-rule bg-oo-bg mb-3 sm:mb-0 sm:flex sm:items-end sm:gap-1 sm:overflow-x-auto sm:overflow-y-auto sm:rounded-none sm:border-0 sm:border-b sm:bg-transparent"
    >
      {MODE_TABS.map((tab, i) => {
        const active = mode === tab.id;
        // Grid lines for the phone layout: a left rule on the right-hand
        // column, a top rule on the second row. Reset at `sm`, where the
        // button's own tab border takes over.
        // Phase 185: five tabs. An odd count leaves the last cell alone
        // on its row, so it spans both columns rather than sitting beside
        // an empty one.
        const lastAlone = MODE_TABS.length % 2 === 1 && i === MODE_TABS.length - 1;
        const cellRules = `${i % 2 === 1 ? "border-l" : ""} ${i >= 2 ? "border-t" : ""} ${lastAlone ? "col-span-2 sm:col-span-1" : ""}`.trim();
        return (
          <div
            key={tab.id}
            className={
              // The divider marks where the depths end and the topics
              // begin: on the first topic tab only, not on every one.
              tab.topic && !MODE_TABS[i - 1]?.topic
                ? "contents sm:flex sm:items-end sm:pl-2 sm:ml-1 md:pl-3 md:ml-2 sm:border-l sm:border-oo-rule"
                : "contents sm:flex sm:items-end"
            }
          >
            <button
              type="button"
              role="tab"
              id={`tab-${tab.id}`}
              aria-selected={active}
              aria-controls={`panel-${tab.id}`}
              tabIndex={active ? 0 : -1}
              onClick={() => onSelect(tab.id)}
              onKeyDown={(e) => {
                // Left/Right/Home/End move between tabs (WAI-ARIA tabs
                // pattern); the roving tabIndex above keeps one stop in
                // the sequence rather than six. Focus stays on the tab
                // (Phase 241) so →→→ walks the whole strip.
                const next = modeForKey(e.key, tab.id, MODE_TABS.map((t) => t.id));
                if (!next) return;
                e.preventDefault();
                onSelect(next, { focusPanel: false });
                document.getElementById(`tab-${next}`)?.focus();
              }}
              className={`relative flex flex-col items-center justify-center gap-1 px-2 py-2.5 min-h-[56px] text-oo-meta border-0 border-oo-rule ${cellRules} transition-colors sm:flex-row sm:shrink-0 sm:justify-start sm:gap-2 sm:rounded-t-oo sm:px-3 md:px-4 sm:pb-3 sm:pt-3 sm:text-oo-body sm:min-h-[44px] sm:border ${
                active
                  ? "bg-white font-bold text-oo-ink sm:-mb-px sm:border-oo-rule sm:border-b-white"
                  : "font-medium text-oo-muted hover:text-oo-ink sm:border-transparent"
              }`}
            >
              {active && (
                <span
                  aria-hidden="true"
                  className="absolute inset-x-0 top-0 h-[3px] sm:inset-x-[-1px] sm:top-[-1px] sm:rounded-t-oo"
                  style={{ background: tab.accent }}
                />
              )}
              {/* The glyph takes the mode accent when active and the
                  muted text colour otherwise, via currentColor on a
                  wrapper — Icon itself never takes a colour prop, so
                  there is exactly one way to colour an icon. */}
              <span
                className="inline-flex shrink-0"
                style={active ? { color: tab.accent } : undefined}
              >
                <Icon name={tab.icon} size={17} />
              </span>
              {tab.label}
            </button>
          </div>
        );
      })}
    </div>
  );
}

/**
 * The mode's own sentence, as its panel card's first band.
 *
 * The strings have been on `MODE_TABS` since Phase 122 and rendered nowhere.
 * A tab labelled "QuickCheck" says what it is called, not what it does, and a
 * reader arriving on a shared link has no other way to find out which of the
 * four they are looking at. Three of the four panels used to state it
 * themselves — in their own coloured strip, in their own words, at a heading
 * level of their own choosing — which is three chances to disagree with the
 * tab above them; this is one.
 */
export function ModeBlurb({ mode }: { mode: CheckMode }) {
  const blurb = MODE_TABS.find((t) => t.id === mode)?.blurb;
  if (!blurb) return null;
  return (
    <PanelSection>
      <p className="text-oo-small text-oo-muted">{blurb}</p>
    </PanelSection>
  );
}
