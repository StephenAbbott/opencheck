/**
 * ActionChip and DataTile — the source row's control vocabulary (Phase 126).
 *
 * They started life private to `SourceBucketCard`, which was correct while one
 * component drew source rows. Three now do, and the alternative to sharing
 * them is each restyling its own chip -- which is exactly how the audit found
 * nine button styles for one meaning. They live in `ui/` for the same reason
 * `Button` and `Chip` do.
 */

/**
 * The compact action group on a source row (Phase 126).
 *
 * v1 gave the graph a full-width blue call-to-action strip and demoted
 * everything else to 11px mono text links — `12 statements`, `Raw JSON`. The
 * v2 design makes all three equal-weight outline chips in one right-aligned
 * group, labelled by **what they contain** rather than by the data model: how
 * many companies, how many changes, and one neutral `Data` disclosure standing
 * in for the rest.
 *
 * The counts matter more than they look. "14 companies" tells a reader whether
 * opening the diagram is worth it; "Explore the ownership graph" does not.
 */
export function ActionChip({
  onClick,
  expanded,
  controls,
  tone = "neutral",
  icon,
  children,
}: {
  onClick: () => void;
  expanded: boolean;
  controls: string;
  tone?: "network" | "timeline" | "neutral";
  icon?: React.ReactNode;
  children: React.ReactNode;
}) {
  const tones = {
    network: "border-oo-graph-ownership/40 bg-oo-graph-ownershipTint text-oo-graph-ownershipText",
    timeline: "border-oo-rule bg-oo-bg text-oo-burst",
    neutral: "border-oo-softBorder bg-white text-oo-blue",
  } as const;
  return (
    <button
      type="button"
      onClick={onClick}
      aria-expanded={expanded}
      aria-controls={controls}
      className={`inline-flex items-center gap-1.5 rounded-oo border px-3 py-1.5 text-oo-small font-medium transition-colors hover:brightness-95 ${tones[tone]}`}
    >
      {icon}
      {children}
    </button>
  );
}

/**
 * One tile in the Data drawer.
 *
 * The drawer is a **menu of things to open**, not the things themselves. v1
 * rendered the Cytoscape graph and two raw JSON dumps inline under headings
 * reading `BODS · N statements`, `Mapped statements`, `BODS statements` and
 * `Raw source payload` — the data model as the interface. Each tile here says
 * what it holds in a human clause and opens it on click.
 */
export function DataTile({
  title,
  meta,
  action,
  tone = "neutral",
  open = false,
  onClick,
  controls,
}: {
  title: string;
  meta: string;
  /** Present only on tiles that reveal something; a tile that merely describes
   *  what the download contains has no action and is not a button. */
  action?: string;
  tone?: "network" | "timeline" | "neutral";
  open?: boolean;
  onClick?: () => void;
  controls?: string;
}) {
  const tones = {
    network: "border-oo-graph-ownership/40 bg-oo-graph-ownershipTint",
    timeline: "border-oo-rule bg-oo-bg",
    neutral: "border-oo-rule bg-oo-bg",
  } as const;
  const body = (
    <>
      <p className="text-oo-small font-bold text-oo-ink">{title}</p>
      <p className="text-oo-small text-oo-muted">{meta}</p>
      {action && (
        <p className="mt-1.5 text-oo-small font-bold text-oo-blue">
          {open ? "Close" : action}
        </p>
      )}
    </>
  );
  const cls = `rounded-oo border px-3.5 py-3 text-left ${tones[tone]}`;
  if (!onClick) return <div className={cls}>{body}</div>;
  return (
    <button
      type="button"
      onClick={onClick}
      aria-expanded={open}
      aria-controls={controls}
      className={`${cls} transition-colors hover:brightness-95`}
    >
      {body}
    </button>
  );
}
