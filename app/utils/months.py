from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

MONTHS_FULL: Tuple[str, ...] = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
)

MONTHS_FULL_INDEX = {name: idx + 1 for idx, name in enumerate(MONTHS_FULL)}
MONTHS_ABBR = {
    "Ene": 1,
    "Feb": 2,
    "Mar": 3,
    "Abr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Ago": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dic": 12,
}
MONTH_NUM_TO_ABBR = {value: key for key, value in MONTHS_ABBR.items()}


def dedupe_preserve_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def parse_instar_label(label: str) -> Tuple[int, int]:
    try:
        month_name, year_str = label.split("/")
        month_index = MONTHS_FULL_INDEX.get(month_name.strip().capitalize())
        if month_index is None:
            return (9999, 99)
        return int(year_str.strip()), month_index
    except (ValueError, AttributeError):
        return (9999, 99)


def sort_instar_months(months: Iterable[str]) -> List[str]:
    return sorted(dedupe_preserve_order(months), key=parse_instar_label)


def format_admedia_stored(value: str) -> str:
    raw = value.strip().replace("-", " ")
    parts = [part for part in raw.split() if part]
    if not parts:
        return value.strip()
    try:
        year = parts[0]
        month_num = int(parts[1]) if len(parts) > 1 else 0
    except (IndexError, ValueError):
        return value.strip()

    abbr = parts[2] if len(parts) > 2 else MONTH_NUM_TO_ABBR.get(month_num)
    if abbr is None:
        abbr = MONTH_NUM_TO_ABBR.get(month_num, f"{month_num:02d}")
    return f"{year} {month_num:02d} {abbr}"


def parse_admedia_stored(value: str) -> Tuple[int, int]:
    formatted = format_admedia_stored(value)
    parts = formatted.split()
    try:
        year = int(parts[0])
        month_num = int(parts[1])
        return year, month_num
    except (IndexError, ValueError):
        return (9999, 99)


def sort_admedia_months(months: Iterable[str]) -> List[str]:
    formatted = [format_admedia_stored(value) for value in months]
    return sorted(dedupe_preserve_order(formatted), key=parse_admedia_stored)


def admedia_label_to_stored(label: str) -> str:
    raw = label.strip()
    if "/" not in raw:
        return format_admedia_stored(raw)
    try:
        month_abbr, year = raw.split("/")
        month_num = MONTHS_ABBR.get(month_abbr.strip().capitalize())
        if month_num is None:
            return format_admedia_stored(raw)
        return f"{year.strip()} {month_num:02d} {month_abbr.strip().capitalize()}"
    except ValueError:
        return format_admedia_stored(raw)


def admedia_stored_to_label(value: str) -> str:
    formatted = format_admedia_stored(value)
    parts = formatted.split()
    try:
        year = parts[0]
        month_num = int(parts[1])
        abbr = parts[2] if len(parts) > 2 else MONTH_NUM_TO_ABBR.get(month_num, parts[1])
        return f"{abbr}/{year}"
    except (IndexError, ValueError):
        return value.strip()


def normalize_admedia_months(months: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    for value in months:
        raw = value.strip()
        if not raw:
            continue
        if "/" in raw:
            normalized.append(admedia_label_to_stored(raw))
        else:
            normalized.append(format_admedia_stored(raw))
    return dedupe_preserve_order(normalized)



def normalize_instar_months(months: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    for value in months:
        raw = value.strip()
        if raw:
            normalized.append(raw)
    return dedupe_preserve_order(normalized)

