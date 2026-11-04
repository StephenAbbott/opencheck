/**
 * FilterChip — a set filter, with its own remove control (the
 * advanced-search-filters ticket).
 *
 * The accent tone, because a filter is the reader's own choice and asserts
 * nothing about the subject (`Chip`'s rule: tone by what a chip asserts).
 * The remove control is a real button with a name that says which filter it
 * removes ("Remove jurisdiction filter: Germany") and a 24px target, the
 * size the graph's overlay controls hold to (WCAG 2.5.8).
 */

import { chipClasses } from "./Chip";
import { Icon } from "./Icon";

export function FilterChip({
  label,
  removeLabel,
  onRemove,
}: {
  label: string;
  removeLabel: string;
  onRemove: () => void;
}) {
  return (
    <span className={chipClasses("accent", "sm", "max-w-full !pr-0.5 text-oo-ink")}>
      <span className="min-w-0 truncate">{label}</span>
      <button
        type="button"
        onClick={onRemove}
        aria-label={removeLabel}
        className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-oo-burst hover:bg-oo-softBorder focus-visible:outline focus-visible:outline-2 focus-visible:outline-oo-blue"
      >
        <Icon name="close" size={12} strokeWidth={2.5} />
      </button>
    </span>
  );
}
