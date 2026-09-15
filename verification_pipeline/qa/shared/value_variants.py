"""Generate plausible source-text representations of a (value, unit) pair.

Used by source_text_loader.slice_for_item for the value-anchor pass.
Example: generate(4, "%") -> ["4%", "4 %", "4,0%", "4.0%", "4 percent",
"4 procent", "vier procent"].

See PLAN.md §4.5 (value-anchor pass) and qa/shared/source_text_loader.py.
"""

from __future__ import annotations

from typing import Optional


# Spelled-out low integers in Dutch + English.
_DUTCH_NUMERALS = {
    1: "een", 2: "twee", 3: "drie", 4: "vier", 5: "vijf",
    6: "zes", 7: "zeven", 8: "acht", 9: "negen", 10: "tien",
    11: "elf", 12: "twaalf",
}
_ENGLISH_NUMERALS = {
    1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
    6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
    11: "eleven", 12: "twelve",
}

# Unit synonym tables — Dutch/English. Each key is normalized lowercase.
# Values are the unit-suffix strings to append after the number.
_UNIT_VARIANTS: dict[str, list[str]] = {
    "%":       ["%", " %", " percent", " procent"],
    "percent": ["%", " %", " percent", " procent"],
    "procent": ["%", " %", " percent", " procent"],

    "hours":   [" uur", " uren", " u", " hours", " hour", " h"],
    "hour":    [" uur", " uren", " u", " hours", " hour", " h"],
    "uur":     [" uur", " uren", " u", " hours", " hour", " h"],
    "uren":    [" uur", " uren", " u", " hours", " hour", " h"],

    "weeks":   [" weken", " wk", " weeks", " week"],
    "week":    [" weken", " wk", " weeks", " week"],
    "weken":   [" weken", " wk", " weeks", " week"],

    "days":    [" dagen", " dgn", " days", " day"],
    "day":     [" dagen", " dgn", " days", " day"],
    "dagen":   [" dagen", " dgn", " days", " day"],

    "months":  [" maanden", " mnd", " months", " month"],
    "month":   [" maanden", " mnd", " months", " month"],
    "maanden": [" maanden", " mnd", " months", " month"],

    "years":   [" jaar", " jaren", " jr", " years", " year", " y"],
    "year":    [" jaar", " jaren", " jr", " years", " year", " y"],
    "jaar":    [" jaar", " jaren", " jr", " years", " year", " y"],
    "jaren":   [" jaar", " jaren", " jr", " years", " year", " y"],

    "eur":          ["€", " EUR", " euro", " euros", " eur"],
    "euro":         ["€", " EUR", " euro", " euros", " eur"],
    "eur one-off":  [" EUR eenmalig", " EUR one-off", "€ eenmalig",
                     " EUR per maand eenmalig"],
    "eur per year": [" EUR per jaar", " EUR/jaar", " EUR per year",
                     " EUR/year"],
    "eur per month":[" EUR per maand", " EUR/maand", " EUR per month",
                     " EUR/month"],
    "eur per km":   [" EUR per km", " ct/km", " cent per km"],

    "fte":     [" fte", " FTE"],
    "km":      [" km", " kilometer", " kilometers"],
    "minutes": [" minuten", " min", " minutes"],
}


def _normalize_unit(unit: Optional[str]) -> str:
    """Lowercase + strip; map common variants to the canonical key."""
    if unit is None:
        return ""
    u = str(unit).strip().lower()
    # Normalize common variants
    u = u.replace("€", "eur")  # € symbol (in case stored as text)
    return u


def _format_number(n: float) -> list[str]:
    """Return number-only variants: '4', '4.0', '4,0' etc.

    Integer values: '4', 'four', 'vier'.
    Decimal values: '4.5', '4,5', '4.50', '4,50'.
    """
    out = []
    if n == int(n):
        i = int(n)
        out.append(str(i))
        # Decimal-zero forms only if value is small (else over-anchors)
        if abs(i) < 1000:
            out.append(f"{i}.0")
            out.append(f"{i},0")
        # Spelled-out only for 1-12
        if 1 <= i <= 12:
            out.append(_DUTCH_NUMERALS[i])
            out.append(_ENGLISH_NUMERALS[i])
    else:
        # Float
        # Two-decimal forms
        s_dot = f"{n:.2f}".rstrip("0").rstrip(".")
        if not s_dot or s_dot == "-":
            s_dot = str(n)
        s_comma = s_dot.replace(".", ",")
        out.append(s_dot)
        out.append(s_comma)
        # Also the strict-2-decimal form for cases like 0.43
        s_dot2 = f"{n:.2f}"
        s_comma2 = s_dot2.replace(".", ",")
        out.append(s_dot2)
        out.append(s_comma2)
    return out


def generate(value, unit: Optional[str]) -> list[str]:
    """Generate plausible source-text representations of (value, unit).

    Variants include:
      - Decimal-comma swaps (4.5 <-> 4,5)
      - With/without trailing zero (4 <-> 4.0 <-> 4,0)
      - Spelled-out low integers (1..12 in Dutch + English)
      - Unit synonyms (uur/uren/u/hour/hours, weken/wk/weeks/week, etc.)
      - Currency symbols (€ vs ' EUR')

    Returns up to ~20 distinct strings. Empty list if value is empty/None or
    cannot be normalized to a number.

    Empty/None value -> []. Unrecognized unit -> only number-only variants
    (no unit-suffixed forms).
    """
    if value is None or value == "":
        return []
    # Try to parse as number
    try:
        s = str(value).strip().replace(",", ".")
        n = float(s)
    except (ValueError, TypeError):
        return []
    if n != n:  # NaN
        return []

    number_forms = _format_number(n)
    unit_key = _normalize_unit(unit)
    unit_suffixes = _UNIT_VARIANTS.get(unit_key, [])

    variants: list[str] = []
    # Plain number forms (no unit) — useful when unit is unrecognized or
    # source text doesn't repeat the unit
    for nf in number_forms:
        variants.append(nf)
    # Number + unit-suffix combinations (only for digit-form numbers, not
    # spelled-out, to avoid "vier uur" looking weird — let spelled-out
    # forms anchor on their own)
    digit_forms = [nf for nf in number_forms
                   if nf and nf[0].isdigit() or (nf and nf[0] == "-")]
    for nf in digit_forms:
        for suffix in unit_suffixes:
            variants.append(f"{nf}{suffix}")

    # Dedupe preserving order
    seen = set()
    out = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out
