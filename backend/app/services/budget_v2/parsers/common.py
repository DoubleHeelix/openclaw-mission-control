"""Common parser helpers for ANZ/NAB statement adapters."""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation

from app.services.budget_v2.config import (
    LEAKAGE_TOKENS,
    NOISE_SECTION_PREFIXES,
    PROVISIONAL_DOCUMENT_MARKERS,
    TERMINAL_SECTION_PREFIXES,
)

MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
MONTH_NAMES = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}

DATE_DD_MMM = re.compile(r"^(\d{1,2})\s+([A-Z]{3})(?:\s+(\d{4}))?$")
DATE_DD_MMM_YY = re.compile(r"^(\d{1,2})\s+([A-Z]{3})\s+(\d{2})$")
DATE_DD_MONTH_YYYY = re.compile(r"^(\d{1,2})\s+([A-Z]{3,9})\s+(\d{4})$")
TRAILING_AMOUNT = re.compile(r"(-?\$?[\d,]+(?:\.\d{2})?)\s*$")
AMOUNT_TOKEN = re.compile(r"^\(?-?\$?[\d,]+(?:\.\d{2})?\)?$")
AMOUNT_WITHOUT_BLANK = re.compile(
    r"(?:^|.*\s)(\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s+(\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s*(CR|DR)?\s*$",
    re.IGNORECASE,
)
SINGLE_AMOUNT_LINE = re.compile(r"^\s*(\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s*$", re.IGNORECASE)
AMOUNT_WITH_BLANK = re.compile(
    r"(?:^|.*\s)(BLANK|\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s+"
    r"(BLANK|\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s+"
    r"(\(?-?\$?[\d,]+(?:\.\d{2})?\)?)\s*$",
    re.IGNORECASE,
)
TABLE_FOOTER_PREFIXES = (
    "TOTALS AT END OF PAGE",
)
SUMMARY_SECTION_PREFIXES = (
    "OPENING BALANCE",
    "CLOSING BALANCE",
    "TOTAL CREDITS",
    "TOTAL DEBITS",
    "TOTAL FEES",
    "SUMMARY OF",
    "STATEMENT SUMMARY",
)


def _parse_two_digit_year(yy: str) -> int:
    return 2000 + int(yy)


def parse_decimal(value: str) -> Decimal | None:
    clean = value.strip().replace("$", "").replace(",", "")
    if clean.startswith("(") and clean.endswith(")"):
        clean = f"-{clean[1:-1]}"
    if not clean:
        return None
    try:
        return Decimal(clean)
    except InvalidOperation:
        return None


def parse_dd_mmm(token: str, *, fallback_year: int | None = None) -> date | None:
    normalized = token.strip().upper()

    yy_match = DATE_DD_MMM_YY.match(normalized)
    if yy_match:
        day = int(yy_match.group(1))
        month = MONTHS.get(yy_match.group(2))
        if month is None:
            return None
        try:
            return date(_parse_two_digit_year(yy_match.group(3)), month, day)
        except ValueError:
            return None

    match = DATE_DD_MMM.match(normalized)
    if not match:
        return None
    day = int(match.group(1))
    month = MONTHS.get(match.group(2))
    if month is None:
        return None
    year = int(match.group(3)) if match.group(3) else fallback_year
    if year is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_date_token(token: str, *, fallback_year: int | None = None) -> date | None:
    cleaned = re.sub(r"\s+", " ", token.strip().upper())
    parsed = parse_dd_mmm(cleaned, fallback_year=fallback_year)
    if parsed is not None:
        return parsed

    match = DATE_DD_MONTH_YYYY.match(cleaned)
    if not match:
        return None
    day = int(match.group(1))
    month_token = match.group(2)
    month = MONTHS.get(month_token[:3]) if len(month_token) >= 3 else None
    if month is None:
        month = MONTH_NAMES.get(month_token)
    if month is None:
        return None
    year = int(match.group(3))
    try:
        return date(year, month, day)
    except ValueError:
        return None


def normalize_spaced_text(line: str) -> str:
    normalized = line
    prev = None
    while prev != normalized:
        prev = normalized
        normalized = re.sub(r"(?<=\b[A-Z])\s+(?=[A-Z]\b)", "", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"(?<=\b\d)\s+(?=\d\b)", "", normalized)
        normalized = re.sub(r"(?<=\d)\s+\.\s*(?=\d)", ".", normalized)
    normalized = re.sub(r"\s{2,}", " ", normalized)
    return normalized.strip()


def extract_statement_period(lines: list[str]) -> tuple[date | None, date | None]:
    text = " ".join(lines).upper()
    text = re.sub(r"\s+", " ", text)
    start_match = re.search(r"STATEMENT\\s+STARTS\\s+(\\d{1,2}\\s+[A-Z]{3,9}\\s+\\d{4})", text)
    end_match = re.search(r"STATEMENT\\s+ENDS\\s+(\\d{1,2}\\s+[A-Z]{3,9}\\s+\\d{4})", text)

    if start_match and end_match:
        return parse_date_token(start_match.group(1)), parse_date_token(end_match.group(1))

    range_match = re.search(
        r"STATEMENT\\s+PERIOD\\s+(\\d{1,2}\\s+[A-Z]{3,9}\\s+\\d{4})\\s+(?:TO|-)\\s+(\\d{1,2}\\s+[A-Z]{3,9}\\s+\\d{4})",
        text,
    )
    if range_match:
        return parse_date_token(range_match.group(1)), parse_date_token(range_match.group(2))

    fuzzy_matches = re.findall(r"(\d{1,2})\s+([A-Z\s]{3,15})\s+(20\d{2})", text)
    parsed: list[date] = []
    for day_raw, month_raw, year_raw in fuzzy_matches:
        month_token = re.sub(r"[^A-Z]", "", month_raw)
        month = MONTH_NAMES.get(month_token) or MONTHS.get(month_token[:3])
        if month is None:
            continue
        try:
            parsed.append(date(int(year_raw), month, int(day_raw)))
        except ValueError:
            continue
        if len(parsed) >= 2:
            break
    if len(parsed) >= 2:
        return parsed[0], parsed[1]
    return None, None


def extract_trailing_amount(line: str) -> Decimal | None:
    match = TRAILING_AMOUNT.search(line)
    if not match:
        return None
    return parse_decimal(match.group(1))


def parse_amount_token(token: str) -> Decimal | None:
    cleaned = token.strip().upper()
    if cleaned == "BLANK":
        return None
    if not AMOUNT_TOKEN.match(cleaned):
        return None
    return parse_decimal(cleaned)


def parse_single_amount_line(line: str) -> Decimal | None:
    match = SINGLE_AMOUNT_LINE.match(line.strip())
    if not match:
        return None
    return parse_amount_token(match.group(1))


def parse_three_column_amount_line(
    line: str,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None] | None:
    normalized = re.sub(r"\s+", " ", line.strip())
    if not normalized:
        return None

    with_blank = AMOUNT_WITH_BLANK.match(normalized.upper())
    if with_blank:
        left = parse_amount_token(with_blank.group(1))
        middle = parse_amount_token(with_blank.group(2))
        balance = parse_amount_token(with_blank.group(3))
        return left, middle, balance, None

    no_blank = AMOUNT_WITHOUT_BLANK.match(normalized.upper())
    if no_blank:
        left = parse_amount_token(no_blank.group(1))
        middle = parse_amount_token(no_blank.group(2))
        if left is None or middle is None:
            return None
        crdr = (no_blank.group(3) or "").upper() or None
        return left, None, middle, crdr
    return None


def is_noise_section(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return normalized.startswith(NOISE_SECTION_PREFIXES)


def is_terminal_section(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return normalized.startswith(TERMINAL_SECTION_PREFIXES)


def should_stop_parsing(line: str) -> bool:
    return is_terminal_section(line)


def is_table_footer(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return normalized.startswith(TABLE_FOOTER_PREFIXES)


def is_summary_section(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return normalized.startswith(SUMMARY_SECTION_PREFIXES)


def classify_section(line: str) -> str:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    if not normalized:
        return "empty"
    if is_page_marker(normalized):
        return "page_marker"
    if is_transaction_table_header(normalized):
        return "table_header"
    if is_table_footer(normalized):
        return "table_footer"
    if is_summary_section(normalized):
        return "summary_section"
    if is_noise_section(normalized):
        return "legal_noise"
    if is_terminal_section(normalized):
        return "terminal_section"
    return "transaction_candidate"


def detect_leakage_tokens(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", text.upper())
    hits = [token for token in LEAKAGE_TOKENS if token in normalized]
    return hits


def detect_document_signals(lines: list[str]) -> dict[str, object]:
    joined = re.sub(r"\s+", " ", " ".join(lines).upper())
    matched_markers = [marker for marker in PROVISIONAL_DOCUMENT_MARKERS if marker in joined]
    if matched_markers:
        return {
            "document_type": "transaction_listing",
            "document_reconcilable": False,
            "document_warnings": [
                "This document appears to be a provisional transaction listing rather than a strict statement ledger."
            ],
            "document_markers": matched_markers,
        }
    return {
        "document_type": "statement",
        "document_reconcilable": None,
        "document_warnings": [],
        "document_markers": [],
    }


PAGE_MARKER = re.compile(r"^PAGE\s+\d+\s+OF\s+\d+$", re.IGNORECASE)
TABLE_HEADER = re.compile(r"^DATE\s+PARTICULARS\s+DEBITS\s+CREDITS\s+BALANCE$", re.IGNORECASE)


def is_page_marker(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return bool(PAGE_MARKER.match(normalized))


def is_transaction_table_header(line: str) -> bool:
    normalized = re.sub(r"\s+", " ", line.strip().upper())
    return bool(TABLE_HEADER.match(normalized)) or normalized == "TRANSACTION DETAILS"


def parse_amount_columns(
    line: str,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None, str | None] | None:
    normalized = re.sub(r"\s+", " ", line.strip())
    if not normalized:
        return None

    amount_tokens = re.findall(r"\$?[\d,]+\.\d{2}", normalized)
    crdr_match = re.search(r"\b(CR|DR)\s*$", normalized, re.IGNORECASE)
    crdr = (crdr_match.group(1).upper() if crdr_match else None)

    if len(amount_tokens) >= 3:
        debit = parse_decimal(amount_tokens[-3])
        credit = parse_decimal(amount_tokens[-2])
        balance = parse_decimal(amount_tokens[-1])
        return debit, credit, balance, crdr, "explicit_column"

    parsed = parse_three_column_amount_line(normalized)
    if parsed is not None:
        left, middle, balance, crdr = parsed
        if left is not None and middle is not None:
            return left, middle, balance, crdr, "explicit_column"
        if left is not None:
            return left, None, balance, crdr, None

    if len(amount_tokens) == 2:
        amount = parse_decimal(amount_tokens[-2])
        balance = parse_decimal(amount_tokens[-1])
        return amount, None, balance, crdr, None

    if len(amount_tokens) == 1:
        amount = parse_decimal(amount_tokens[-1])
        return amount, None, None, crdr, None

    return None


def resolve_direction_source(flags: list[str], *, fallback: str = "fallback") -> str:
    if "direction_source_explicit_column" in flags:
        return "explicit_column"
    if "direction_source_balance_delta" in flags:
        return "balance_delta"
    return fallback


def assess_row_quality(
    *,
    description: str,
    amount_found: bool,
    date_found: bool,
    section_kind: str,
    direction_source: str,
    parser_flags: list[str],
    debit_found: bool = False,
    credit_found: bool = False,
    balance_found: bool = False,
) -> tuple[float, dict[str, object]]:
    score = Decimal("0.92")
    shape_score = Decimal("0.90")
    amount_score = Decimal("0.92") if amount_found else Decimal("0.25")
    date_score = Decimal("0.92") if date_found else Decimal("0.25")
    section_score = Decimal("0.90")

    if section_kind in {"legal_noise", "terminal_section", "summary_section"}:
        section_score = Decimal("0.30")
        score -= Decimal("0.22")
    elif section_kind == "table_footer":
        section_score = Decimal("0.55")
        score -= Decimal("0.10")

    if detect_leakage_tokens(description):
        score -= Decimal("0.28")
        shape_score -= Decimal("0.20")

    if "malformed_amount" in parser_flags or "missing_amount" in parser_flags:
        amount_score = Decimal("0.20")
        score -= Decimal("0.30")
    if "malformed_date" in parser_flags:
        date_score = Decimal("0.20")
        score -= Decimal("0.22")
    if "both_amount_columns_present" in parser_flags or (debit_found and credit_found):
        amount_score = min(amount_score, Decimal("0.55"))
        shape_score = min(shape_score, Decimal("0.55"))
        score -= Decimal("0.28")
    if "direction_inference_uncertain" in parser_flags:
        score -= Decimal("0.12")
    if direction_source == "fallback":
        score -= Decimal("0.06")
    elif direction_source == "balance_delta":
        score -= Decimal("0.03")

    if len(description.strip()) < 3:
        shape_score = min(shape_score, Decimal("0.40"))
        score -= Decimal("0.10")
    if balance_found:
        shape_score += Decimal("0.03")

    final = max(Decimal("0.05"), min(Decimal("0.99"), score)).quantize(Decimal("0.01"))
    quality_bucket = "high" if final >= Decimal("0.80") else "medium" if final >= Decimal("0.55") else "low"
    return float(final), {
        "row_shape_confidence": float(max(Decimal("0.05"), min(Decimal("0.99"), shape_score)).quantize(Decimal("0.01"))),
        "amount_parse_confidence": float(max(Decimal("0.05"), min(Decimal("0.99"), amount_score)).quantize(Decimal("0.01"))),
        "date_parse_confidence": float(max(Decimal("0.05"), min(Decimal("0.99"), date_score)).quantize(Decimal("0.01"))),
        "section_context_confidence": float(max(Decimal("0.05"), min(Decimal("0.99"), section_score)).quantize(Decimal("0.01"))),
        "direction_source": direction_source,
        "row_quality_bucket": quality_bucket,
    }


def summarize_row_quality(rows: list[object]) -> tuple[dict[str, int], dict[str, int]]:
    quality_counts = {"high": 0, "medium": 0, "low": 0}
    direction_counts = {"explicit_column": 0, "balance_delta": 0, "fallback": 0}
    for row in rows:
        metadata = getattr(row, "metadata", {}) or {}
        bucket = str(metadata.get("row_quality_bucket", "medium"))
        direction_source = str(metadata.get("direction_source", resolve_direction_source(list(getattr(row, "parser_flags", []) or []))))
        quality_counts[bucket] = quality_counts.get(bucket, 0) + 1
        direction_counts[direction_source] = direction_counts.get(direction_source, 0) + 1
    return quality_counts, direction_counts
