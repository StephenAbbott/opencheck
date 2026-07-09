"""og_image — render the shareable social card (og:image) for an entity.

Design: split panel, 1200×630. Left (white): OpenCheck logo + wordmark,
entity name (Bitter Bold, auto-shrunk to fit), LEI, "Visit opencheck.world"
CTA. Right (brand indigo #3d30d4): risk-signal count and the first three
signal chips in the exact RiskChip colours from the frontend, plus a
"+N more" line. Everything is drawn with Pillow at 2× and downsampled —
no headless browser (Render free tier can't afford one).

Fonts are bundled under ``assets/fonts`` (Bitter + DM Sans, both SIL OFL —
see ATTRIBUTIONS.md).

Two variants:

* **full** — name + signal count + chips (rendered when a completed lookup
  for the LEI is available, normally because the sharer just ran it).
* **teaser** — name (or just the LEI) + "34 open sources, one query"
  when no completed lookup is cached; invites the viewer to run the check.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

_FONT_DIR = Path(__file__).resolve().parent / "assets" / "fonts"

# Canvas (before 2× supersampling).
W, H = 1200, 630
_SPLIT = 700  # left/white panel width; right panel = brand indigo

# Brand colours.
_INDIGO = "#3d30d4"
_NAVY = "#0d1b3e"
_CHECK_BLUE = "#2563eb"
_INK = "#191d23"
_MUTED = "#8a8a99"
_LAVENDER = "#cecbf6"  # secondary text on indigo

#: Signal code → (label, chip background, chip text). Mirrors
#: RISK_PRESENTATION in frontend/src/components/risk/RiskChip.tsx —
#: keep the two in sync when adding signals.
SIGNAL_STYLE: dict[str, tuple[str, str, str]] = {
    "PEP": ("PEP", "#f5f3ff", "#6d28d9"),
    "SANCTIONED": ("Sanctioned", "#fff1f2", "#be123c"),
    "SANCTIONED_SECURITY": ("Sanctioned securities", "#fff1f2", "#be123c"),
    "SANCTIONS_LINKED": ("Sanctions-linked", "#fffbeb", "#92400e"),
    "DEBARMENT": ("Debarred", "#ffedd5", "#7c2d12"),
    "OFFSHORE_LEAKS": ("Offshore leaks", "#fffbeb", "#92400e"),
    "OPAQUE_OWNERSHIP": ("Opaque ownership", "#f1f5f9", "#334155"),
    "TRUST_OR_ARRANGEMENT": ("Trust / arrangement", "#eef2ff", "#4338ca"),
    "NON_EU_JURISDICTION": ("Non-EU jurisdiction", "#fff7ed", "#c2410c"),
    "STATE_CONTROLLED": ("State-controlled", "#fff7ed", "#c2410c"),
    "NOMINEE": ("Nominee", "#fdf4ff", "#a21caf"),
    "COMPLEX_OWNERSHIP_LAYERS": ("≥3 ownership layers", "#f0f9ff", "#0369a1"),
    "COMPLEX_CORPORATE_STRUCTURE": ("Complex structure (AMLA)", "#fef2f2", "#b91c1c"),
    "POSSIBLE_OBFUSCATION": ("Possible obfuscation", "#fefce8", "#854d0e"),
    "RELATED_PEP": ("Related PEP", "#f5f3ff", "#6d28d9"),
    "RELATED_SANCTIONED": ("Related sanctioned", "#fff1f2", "#be123c"),
    "RELATED_SANCTIONS_LINKED": ("Related sanctions-linked", "#fffbeb", "#92400e"),
    "RELATED_DEBARMENT": ("Related debarred", "#fff7ed", "#9a3412"),
    "FATF_BLACK_LIST": ("FATF black list", "#fee2e2", "#991b1b"),
    "FATF_GREY_LIST": ("FATF grey list", "#fff7ed", "#9a3412"),
}
_DEFAULT_STYLE = ("#f1f5f9", "#334155")


def _font(name: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(_FONT_DIR / f"{name}.ttf"), size)


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
          max_width: int) -> list[str]:
    """Greedy word wrap; single overlong words are hard-truncated."""
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=font) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        while draw.textlength(word, font=font) > max_width and len(word) > 1:
            word = word[:-1]
        current = word
    if current:
        lines.append(current)
    return lines


def _truncate(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
              max_width: int) -> str:
    if draw.textlength(text, font=font) <= max_width:
        return text
    while text and draw.textlength(f"{text}…", font=font) > max_width:
        text = text[:-1].rstrip()
    return f"{text}…"


def _draw_logo(draw: ImageDraw.ImageDraw, x: int, y: int, s: float) -> None:
    """The OpenCheck magnifier mark, from the social-card SVG geometry
    (120-unit viewBox), scaled by ``s`` and offset to (x, y)."""
    def pt(px: float, py: float) -> tuple[float, float]:
        return (x + px * s, y + py * s)

    r = 44 * s
    cx, cy = pt(54, 54)
    lw = max(2, round(10 * s))
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=_NAVY, width=lw)
    draw.line([pt(86, 86), pt(114, 114)], fill=_NAVY, width=max(2, round(11 * s)))
    draw.polygon([pt(38, 34), pt(38, 74), pt(74, 54)], fill=_NAVY)
    for (dx, dy), colour in [((34, 22), "#22c55e"), ((16, 54), "#3b82f6"), ((34, 86), "#7c3aed")]:
        dcx, dcy = pt(dx, dy)
        dr = 8 * s
        draw.ellipse([dcx - dr, dcy - dr, dcx + dr, dcy + dr], fill=colour)


def _draw_confidence_dot(draw: ImageDraw.ImageDraw, cx: float, cy: float,
                         r: float, colour: str, confidence: str) -> None:
    """● high — filled; ◐ medium — left half; ○ low — outline."""
    box = [cx - r, cy - r, cx + r, cy + r]
    if confidence == "high":
        draw.ellipse(box, fill=colour)
    elif confidence == "medium":
        draw.ellipse(box, outline=colour, width=max(2, int(r / 2.5)))
        draw.pieslice(box, 90, 270, fill=colour)
    else:
        draw.ellipse(box, outline=colour, width=max(2, int(r / 2.5)))


def render_share_card(
    name: str | None,
    lei: str,
    signals: list[dict[str, Any]] | None,
) -> bytes:
    """Render the share card PNG. ``signals=None`` renders the teaser
    variant (no completed lookup available); ``[]`` means a completed
    lookup with zero signals."""
    s = 2  # supersample factor
    img = Image.new("RGB", (W * s, H * s), "#ffffff")
    draw = ImageDraw.Draw(img)
    draw.rectangle([_SPLIT * s, 0, W * s, H * s], fill=_INDIGO)

    f_word = _font("dmsans-700", 36 * s)
    f_name = None  # chosen by fit loop
    f_lei = _font("dmsans-400", 24 * s)
    f_cta = _font("dmsans-400", 25 * s)
    f_cta_b = _font("dmsans-700", 25 * s)
    f_count_label = _font("dmsans-400", 28 * s)
    f_pill = _font("dmsans-500", 27 * s)
    f_more = _font("dmsans-400", 24 * s)

    # ── Left panel ──────────────────────────────────────────────────────
    _draw_logo(draw, 70 * s, 56 * s, 0.4 * s)
    wx = (70 + 60) * s
    draw.text((wx, 62 * s), "Open", font=f_word, fill=_NAVY)
    wx += draw.textlength("Open", font=f_word)
    draw.text((wx, 62 * s), "Check", font=f_word, fill=_CHECK_BLUE)

    display_name = (name or "").strip() or f"LEI {lei}"
    name_max_w = (_SPLIT - 70 - 60) * s
    longest_word = max(display_name.split(), key=len, default="")
    for size in (60, 54, 48, 42, 36, 30):
        f_name = _font("bitter-700", size * s)
        lines = _wrap(draw, display_name, f_name, name_max_w)
        # Fit = at most 3 lines AND no single word needed hard-chopping.
        if len(lines) <= 3 and draw.textlength(longest_word, font=f_name) <= name_max_w:
            break
    lines = _wrap(draw, display_name, f_name, name_max_w)[:3]
    y = 196 * s
    for line in lines:
        draw.text((70 * s, y), line, font=f_name, fill=_INK)
        y += int(f_name.size * 1.18)

    if name:  # only show the LEI line when it isn't already the headline
        draw.text((70 * s, y + 20 * s), f"LEI {lei}", font=f_lei, fill=_MUTED)

    cta_y = (H - 44 - 32) * s
    cx = 70 * s
    for text, font, colour in [
        ("Visit ", f_cta, _INK),
        ("opencheck.world", f_cta_b, _INDIGO),
        (" for more details", f_cta, _INK),
    ]:
        draw.text((cx, cta_y), text, font=font, fill=colour)
        cx += draw.textlength(text, font=font)

    # ── Right panel ─────────────────────────────────────────────────────
    px = (_SPLIT + 56) * s
    panel_w = (W - _SPLIT - 2 * 56) * s

    if signals is None:
        f_count = _font("bitter-700", 120 * s)
        draw.text((px, 44 * s), "34", font=f_count, fill="#ffffff")
        nx = px + draw.textlength("34", font=f_count) + 18 * s
        draw.text((nx, 116 * s), "open sources,", font=f_count_label, fill=_LAVENDER)
        draw.text((nx, 152 * s), "one query", font=f_count_label, fill=_LAVENDER)
        ty = 260 * s
        for line in ["Run the live check for", "risk signals, ownership", "and source-linked data."]:
            draw.text((px, ty), line, font=_font("dmsans-500", 30 * s), fill="#ffffff")
            ty += 44 * s
    else:
        total = len(signals)
        f_count = _font("bitter-700", 120 * s)
        count_text = str(total)
        draw.text((px, 44 * s), count_text, font=f_count, fill="#ffffff")
        nx = px + draw.textlength(count_text, font=f_count) + 18 * s
        draw.text((nx, 116 * s), "risk signal" + ("s" if total != 1 else ""),
                  font=f_count_label, fill=_LAVENDER)
        draw.text((nx, 152 * s), "found", font=f_count_label, fill=_LAVENDER)

        # First three distinct signal codes, in pipeline order.
        chips: list[dict[str, Any]] = []
        seen: set[str] = set()
        for sig in signals:
            code = str(sig.get("code") or "")
            if code and code not in seen:
                seen.add(code)
                chips.append(sig)
            if len(chips) == 3:
                break

        cy = 236 * s
        pill_h = 56 * s
        for sig in chips:
            code = str(sig.get("code") or "")
            label, bg, fg = SIGNAL_STYLE.get(code, (code, *_DEFAULT_STYLE))
            confidence = str(sig.get("confidence") or "high")
            dot_r = 7 * s
            text_max = panel_w - 30 * s * 2 - dot_r * 2 - 12 * s
            # Long labels: step the font down before resorting to "…".
            pill_font = f_pill
            if draw.textlength(label, font=pill_font) > text_max:
                pill_font = _font("dmsans-500", 23 * s)
            label = _truncate(draw, label, pill_font, text_max)
            text_w = draw.textlength(label, font=pill_font)
            pill_w = 30 * s + dot_r * 2 + 12 * s + text_w + 30 * s
            draw.rounded_rectangle(
                [px, cy, px + pill_w, cy + pill_h], radius=pill_h / 2, fill=bg
            )
            _draw_confidence_dot(draw, px + 30 * s + dot_r, cy + pill_h / 2, dot_r, fg, confidence)
            draw.text(
                (px + 30 * s + dot_r * 2 + 12 * s, cy + (pill_h - pill_font.size * 1.16) / 2 + 2 * s),
                label, font=pill_font, fill=fg,
            )
            cy += pill_h + 18 * s

        remaining = total - len(chips)
        if remaining > 0:
            draw.text((px, cy + 4 * s), f"+ {remaining} more on opencheck.world",
                      font=f_more, fill=_LAVENDER)
        elif total == 0:
            draw.text((px, 250 * s), "No risk signals surfaced",
                      font=_font("dmsans-500", 30 * s), fill="#ffffff")
            draw.text((px, 296 * s), "across 34 open sources",
                      font=f_more, fill=_LAVENDER)

    img = img.resize((W, H), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
