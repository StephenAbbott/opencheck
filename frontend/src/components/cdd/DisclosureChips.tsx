/**
 * What one source says about the subject's owners, as chips plus the source's
 * own words — the disclosure-status ticket's card treatment, shared by the
 * QuickCheck source cards (`SourceBucketCard`) and the Climate TRACE card on
 * the Climate & ESG tab, so the two cannot be styled apart.
 */
import { Chip } from "../ui";
import {
  chipEntries,
  entryDetails,
  entryLabel,
  entryTone,
  type SourceDisclosure,
} from "../../lib/disclosureStatus";

export function DisclosureChips({
  disclosure,
  cardFailed = false,
  className = "px-5 pt-3",
}: {
  disclosure?: SourceDisclosure;
  /** The card already says "Did not answer": no "Not read" chip on top. */
  cardFailed?: boolean;
  className?: string;
}) {
  const chips = chipEntries(disclosure, { cardFailed });
  if (chips.length === 0) return null;
  const words = entryDetails(chips);
  return (
    <div className={className}>
      <ul className="flex flex-wrap items-center gap-1.5" aria-label="Owners disclosed by this source">
        {chips.map((e) => (
          <li key={`${e.value}:${e.covers ?? ""}`}>
            <Chip tone={entryTone(e)}>{entryLabel(e)}</Chip>
          </li>
        ))}
      </ul>
      {words.length > 0 && (
        <p className="mt-1.5 text-oo-meta text-oo-muted">
          As the source puts it: {words.join("; ")}
        </p>
      )}
    </div>
  );
}
