/**
 * The design-system primitives, in one place.
 *
 * Follows the `components/icons/index.tsx` precedent: named exports, no
 * default. Import as `"./components/ui"` from App.tsx and `"../ui"` from
 * `cdd/*`.
 */

export { Button, buttonClasses } from "./Button";
export type { ButtonVariant, ButtonSize } from "./Button";

export {
  Chip,
  chipClasses,
  CONFIDENCE_GLYPH,
  CONFIDENCE_LABEL,
} from "./Chip";
export type { ChipTone, ChipSize } from "./Chip";

export { SectionLabel, SectionHeading, sectionLabelClasses } from "./SectionLabel";
export type { SectionLabelTone } from "./SectionLabel";

export { Icon, ICON_NAMES, ICON_PATHS } from "./Icon";
export type { IconName } from "./Icon";
export { ActionChip, DataTile } from "./ActionChip";
export { RowList } from "./RowList";
export type { RowListItem } from "./RowList";
