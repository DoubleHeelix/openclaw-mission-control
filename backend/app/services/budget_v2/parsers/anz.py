"""ANZ PDF statement parser adapter."""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal

from app.services.budget_v2.parsers.base import BankStatementParser
from app.services.budget_v2.parsers.common import (
    assess_row_quality,
    classify_section,
    detect_leakage_tokens,
    detect_document_signals,
    extract_statement_period,
    is_noise_section,
    is_page_marker,
    is_terminal_section,
    normalize_spaced_text,
    parse_date_token,
    parse_decimal,
    parse_single_amount_line,
    parse_three_column_amount_line,
    parse_dd_mmm,
    summarize_row_quality,
)
from app.services.budget_v2.types import ParsedStatementResult, ParsedTransaction

ANZ_DATE_LINE = re.compile(r"^(\d{1,2}\s+[A-Z]{3})(?:\s+(.*))?$")
ANZ_YEAR = re.compile(r"\b(20\d{2})\b")
ANZ_TRANSACTION_HEADER = re.compile(r"^DATE\s+TRANSACTION\s+DETAILS", re.IGNORECASE)
ANZ_TABLE_HEADER = re.compile(r"^DATE\s+TRANSACTION\s+DETAILS\s+WITHDRAWALS\s+\(\$\)\s+DEPOSITS\s+\(\$\)\s+BALANCE\s+\(\$\)$", re.IGNORECASE)
ANZ_OPENING_BALANCE = re.compile(r"OPENING BALANCE", re.IGNORECASE)
ANZ_TOTALS_LINE = re.compile(r"TOTALS AT END OF PAGE", re.IGNORECASE)
ANZ_TRAILING_MONEY = re.compile(r"\$?[\d,]+\.\d{2}")
ANZ_COMPACT_HEADER = "DATETRANSACTIONTYPEANDDETAILS WITHDRAWALS DEPOSITS BALANCE"
ANZ_COMPACT_PERIOD = re.compile(
    r"STATEMENTPERIOD\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\s*-\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})",
    re.IGNORECASE,
)
ANZ_COMPACT_ROW_START = re.compile(r"\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", re.IGNORECASE)
ANZ_COMPACT_PAGE = re.compile(r"PAGE\s+(\d+)\s+OF\s+\d+", re.IGNORECASE)
ANZ_COMPACT_MONEY = re.compile(r"\$?[\d,]+\.\d{2}")
ANZ_COMPACT_TYPES = ("DD", "DC", "BP", "EP", "VT", "AP", "CQ", "ED", "FX", "IP", "IF", "AT", "IA")
ANZ_PAGE_MARKER = re.compile(r"PAGE\s+(\d+)\s+OF\s+(\d+)", re.IGNORECASE)
ANZ_PERIOD_TOTALS_BLOCK = re.compile(
    r"TOTALS AT END OF PERIOD.*?WITHDRAWALS\s+\(\$\)\s*\$?\s*([\d,]+\.\d{2}).*?"
    r"DEPOSITS\s+\(\$\)\s*\$?\s*([\d,]+\.\d{2}).*?"
    r"BALANCE\s+\(\$\)\s*\$?\s*([\d,]+\.\d{2})",
    re.IGNORECASE | re.DOTALL,
)


class AnzPdfParser(BankStatementParser):
    name = "anz_pdf_v2"
    banks = ("ANZ",)

    def can_parse(self, text: str, filename: str) -> float:
        score = 0.0
        upper = text.upper()
        if "ANZ ACCESS ADVANTAGE" in upper or "ANZ" in upper:
            score += 0.45
        if "TRANSACTION DETAILS" in upper and "WITHDRAWALS ($) DEPOSITS ($)" in upper:
            score += 0.35
        if "VISA DEBIT PURCHASE CARD" in upper or "ANZ INTERNET BANKING PAYMENT" in upper:
            score += 0.2
        if "TRANSACTION LISTING" in upper:
            score -= 0.3
        return min(score, 1.0)

    def parse(self, text: str, filename: str) -> ParsedStatementResult:
        lines = [normalize_spaced_text(ln.rstrip()) for ln in text.splitlines() if ln.strip()]
        fallback_year = self._detect_year(lines)
        statement_start_date, statement_end_date = extract_statement_period(lines)
        document_signals = detect_document_signals(lines)
        opening_balance = self._extract_labeled_amount(lines, "Opening Balance", search_limit=120)
        closing_balance = self._extract_labeled_amount(lines, "Closing Balance", search_limit=120)
        statement_total_credits = self._extract_labeled_amount(lines, "Total Deposits", search_limit=120)
        statement_total_debits = self._extract_labeled_amount(lines, "Total Withdrawals", search_limit=120)
        footer_debits, footer_credits, footer_balance = self._extract_period_totals(lines)
        if statement_total_debits is None:
            statement_total_debits = footer_debits
        if statement_total_credits is None:
            statement_total_credits = footer_credits
        if closing_balance is None:
            closing_balance = footer_balance
        page_count = self._extract_page_count(lines)

        transactions: list[ParsedTransaction] = []
        parser_flags: list[str] = []
        excluded_rows: list[dict[str, object]] = []
        suspicious_rows: list[dict[str, object]] = []
        page_row_counts: dict[int, int] = {}
        seen_transaction_table = False
        resume_on_next_table_header = False
        table_detected = False
        page_resume_count = 0
        first_stop_reason: str | None = None
        first_stop_line: str | None = None
        current_page = 1
        current_date: date | None = None
        current_desc: list[str] = []
        current_block_lines: list[str] = []
        row_index = 0

        def flush_block(line_no: int, *, missing_amount: bool = False) -> None:
            nonlocal row_index, current_date, current_desc, current_block_lines
            if current_date is None:
                return

            withdraw: Decimal | None = None
            deposit: Decimal | None = None
            balance: Decimal | None = None

            for block_line in reversed(current_block_lines):
                parsed = parse_three_column_amount_line(block_line)
                if parsed is None:
                    continue
                left, middle, bal, _crdr = parsed
                withdraw = left
                deposit = middle
                balance = bal
                break
            if withdraw is None and deposit is None:
                for block_line in reversed(current_block_lines):
                    single = parse_single_amount_line(block_line)
                    if single is None:
                        continue
                    withdraw = single
                    break

            description_lines = [
                part.strip()
                for part in current_desc
                if part.strip()
                and not ANZ_TOTALS_LINE.search(part)
                and not parse_three_column_amount_line(part)
            ]
            description = self._strip_transaction_type_prefix(" ".join(description_lines).strip()) or "UNKNOWN"

            if ANZ_OPENING_BALANCE.search(description):
                current_date = None
                current_desc = []
                current_block_lines = []
                return

            row_flags: list[str] = []

            leakage_hits = detect_leakage_tokens(description)
            if leakage_hits:
                row_flags.append("suspected_footer_merge")
                parser_flags.append("suspected_footer_merge")
                suspicious_rows.append(
                    {
                        "line_no": line_no,
                        "reason": "suspected_footer_merge",
                        "row_preview": description[:200],
                        "tokens": leakage_hits,
                    }
                )

            if withdraw is None and deposit is None:
                if missing_amount:
                    row_flags.append("missing_amount")
                    amount_value = Decimal("0.00")
                    direction = "debit"
                    excluded_rows.append(
                        {
                            "line_no": line_no,
                            "reason": "missing_amount",
                            "row_preview": description[:200],
                        }
                    )
                else:
                    current_date = None
                    current_desc = []
                    current_block_lines = []
                    return
            elif withdraw is not None and deposit is not None:
                if withdraw == 0 and deposit != 0:
                    direction = "credit"
                    amount_value = abs(deposit)
                elif deposit == 0 and withdraw != 0:
                    direction = "debit"
                    amount_value = abs(withdraw)
                elif abs(withdraw) >= abs(deposit):
                    direction = "debit"
                    amount_value = abs(withdraw)
                    row_flags.append("both_amount_columns_present")
                else:
                    direction = "credit"
                    amount_value = abs(deposit)
                    row_flags.append("both_amount_columns_present")
            elif withdraw is not None:
                direction = "debit"
                amount_value = abs(withdraw)
            else:
                direction = "credit"
                amount_value = abs(deposit or Decimal("0"))

            direction_source = "explicit_column" if (withdraw is not None or deposit is not None) else "fallback"
            if direction_source == "explicit_column":
                row_flags.append("direction_source_explicit_column")
            else:
                row_flags.append("direction_source_fallback")
            section_kind = classify_section(description)
            row_confidence, row_meta = assess_row_quality(
                description=description,
                amount_found=withdraw is not None or deposit is not None,
                date_found=current_date is not None,
                section_kind=section_kind,
                direction_source=direction_source,
                parser_flags=row_flags,
                debit_found=withdraw is not None,
                credit_found=deposit is not None,
                balance_found=balance is not None,
            )

            transactions.append(
                ParsedTransaction(
                    row_index=row_index,
                    transaction_date=current_date,
                    effective_date=None,
                    amount=amount_value,
                    direction=direction,
                    raw_description=description,
                    balance_after=balance,
                    page_number=current_page,
                    source_line_refs=[line_no],
                    parser_flags=row_flags,
                    parser_confidence=row_confidence,
                    metadata=row_meta,
                )
            )
            page_row_counts[current_page] = page_row_counts.get(current_page, 0) + 1
            row_index += 1
            current_date = None
            current_desc = []
            current_block_lines = []

        for idx, raw_line in enumerate(lines):
            line = raw_line.strip()
            upper = line.upper()
            section_kind = classify_section(line)

            if section_kind == "page_marker":
                match = re.search(r"PAGE\s+(\d+)\s+OF\s+\d+", upper)
                if match:
                    current_page = int(match.group(1))
                if seen_transaction_table:
                    resume_on_next_table_header = True
                continue

            if ANZ_TRANSACTION_HEADER.match(line) or ANZ_TABLE_HEADER.match(line) or section_kind == "table_header":
                if seen_transaction_table and resume_on_next_table_header:
                    page_resume_count += 1
                seen_transaction_table = True
                table_detected = True
                resume_on_next_table_header = False
                continue

            if not seen_transaction_table:
                if section_kind in {"legal_noise", "summary_section"} or is_noise_section(line):
                    continue
                if section_kind == "terminal_section" or is_terminal_section(line):
                    suspicious_rows.append(
                        {"line_no": idx, "reason": "pre_table_terminal_ignored", "row_preview": line[:200]}
                    )
                    continue
            else:
                if section_kind == "summary_section":
                    if current_date is not None:
                        flush_block(idx, missing_amount=True)
                    parser_flags.append("unsupported_section_encountered")
                    first_stop_reason = "terminal_section_encountered"
                    first_stop_line = line[:200]
                    suspicious_rows.append(
                        {"line_no": idx, "reason": "unsupported_section_encountered", "row_preview": line[:200]}
                    )
                    break
                if section_kind == "legal_noise" or is_noise_section(line):
                    if current_date is not None:
                        flush_block(idx, missing_amount=True)
                    resume_on_next_table_header = True
                    suspicious_rows.append(
                        {"line_no": idx, "reason": "noise_section_encountered", "row_preview": line[:200]}
                    )
                    continue
                if section_kind == "terminal_section" or is_terminal_section(line):
                    if current_date is not None:
                        flush_block(idx, missing_amount=True)
                    parser_flags.append("unsupported_section_encountered")
                    first_stop_reason = "terminal_section_encountered"
                    first_stop_line = line[:200]
                    suspicious_rows.append(
                        {"line_no": idx, "reason": "unsupported_section_encountered", "row_preview": line[:200]}
                    )
                    break

            if ANZ_TOTALS_LINE.search(line) or section_kind == "table_footer":
                if current_date is not None:
                    flush_block(idx)
                resume_on_next_table_header = True
                continue

            m = ANZ_DATE_LINE.match(line.upper())
            if m:
                candidate_date = parse_dd_mmm(m.group(1), fallback_year=fallback_year)
                if candidate_date:
                    if current_date is not None:
                        flush_block(idx, missing_amount=True)
                    current_date = candidate_date
                    current_desc = []
                    current_block_lines = []
                    trailing = (m.group(2) or "").strip()
                    if trailing:
                        current_block_lines.append(trailing)
                        current_desc.append(trailing)
                    continue

            if current_date is not None:
                current_block_lines.append(line)
                current_desc.append(line)
                parsed = parse_three_column_amount_line(line)
                if parsed is not None:
                    left, middle, _bal, _crdr = parsed
                    if left is not None or middle is not None:
                        flush_block(idx)
                    continue
                if parse_single_amount_line(line) is not None:
                    flush_block(idx)
                    continue
                if ANZ_TRAILING_MONEY.search(line) and (" IDR " in f" {line.upper()} " or " USD " in f" {line.upper()} "):
                    continue

        if current_date is not None:
            flush_block(len(lines), missing_amount=True)

        if table_detected and not transactions:
            parser_flags.append("parser_failed_after_table_detection")
            if first_stop_reason is None:
                first_stop_reason = "table_detected_but_no_transactions"
                first_stop_line = None
        elif not table_detected and lines:
            parser_flags.append("transaction_table_not_found")
            if first_stop_reason is None:
                first_stop_reason = "transaction_table_not_found"
                first_stop_line = None

        if not transactions:
            compact_result = self._parse_compact_nz_layout(
                text=text,
                filename=filename,
                fallback_year=fallback_year,
                document_signals=document_signals,
            )
            if compact_result is not None:
                return compact_result

        parser_failure_reason = first_stop_reason
        row_quality_counts, direction_source_counts = summarize_row_quality(transactions)

        return ParsedStatementResult(
            statement_id=f"anz-{fallback_year or 'unk'}-{abs(hash(filename))}",
            bank_name="ANZ",
            account_name=None,
            account_ref_masked=None,
            statement_start_date=statement_start_date,
            statement_end_date=statement_end_date,
            parser_name=self.name,
            parser_confidence=0.82,
            parser_flags=sorted(set(parser_flags)),
            metadata={
                "line_count": len(lines),
                "page_count": page_count,
                "table_header_count": page_resume_count + (1 if table_detected else 0),
                "page_transaction_counts": page_row_counts,
                "page_row_counts": page_row_counts,
                "table_detected": table_detected,
                "page_resume_count": page_resume_count,
                "parser_failure_reason": parser_failure_reason,
                "first_stop_reason": first_stop_reason,
                "first_stop_line": first_stop_line,
                "excluded_rows": excluded_rows,
                "suspicious_rows": suspicious_rows,
                "row_quality_counts": row_quality_counts,
                "direction_source_counts": direction_source_counts,
                "opening_balance": opening_balance,
                "closing_balance": closing_balance,
                "statement_total_debits": statement_total_debits,
                "statement_total_credits": statement_total_credits,
                **document_signals,
            },
            transactions=transactions,
        )

    def _detect_year(self, lines: list[str]) -> int | None:
        for line in lines[:120]:
            match = ANZ_YEAR.search(line)
            if match:
                return int(match.group(1))
        return None

    def _extract_labeled_amount(self, lines: list[str], label: str, *, search_limit: int = 160) -> Decimal | None:
        joined = "\n".join(lines[:search_limit])
        match = re.search(
            rf"{re.escape(label)}\s*:?\s*(?:\$\s*)?([\d,]+\.\d{{2}})",
            joined,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return None
        return parse_decimal(match.group(1))

    def _extract_period_totals(self, lines: list[str]) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        joined = "\n".join(lines)
        match = ANZ_PERIOD_TOTALS_BLOCK.search(joined)
        if not match:
            return None, None, None
        debits = parse_decimal(match.group(1))
        credits = parse_decimal(match.group(2))
        balance = parse_decimal(match.group(3))
        return debits, credits, balance

    def _extract_page_count(self, lines: list[str]) -> int:
        page_markers = [ANZ_PAGE_MARKER.search(line) for line in lines]
        counts = [int(match.group(2)) for match in page_markers if match is not None]
        if counts:
            return max(counts)
        current_pages = [int(match.group(1)) for match in page_markers if match is not None]
        if current_pages:
            return max(current_pages)
        return 1

    def _parse_compact_nz_layout(
        self,
        *,
        text: str,
        filename: str,
        fallback_year: int | None,
        document_signals: dict[str, object],
    ) -> ParsedStatementResult | None:
        page_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        compact_text = text.replace(" ", "")
        if "Statementperiod" not in text or "DateTransactiontypeanddetails Withdrawals Deposits Balance" not in text:
            return None

        period_match = ANZ_COMPACT_PERIOD.search(text)
        statement_start_date = parse_date_token(period_match.group(1)) if period_match else None
        statement_end_date = parse_date_token(period_match.group(2)) if period_match else None

        opening_balance = None
        closing_balance = None
        statement_total_debits = None
        statement_total_credits = None

        opening_match = re.search(r"Openingbalance\s+(\$?[\d,]+\.\d{2})", text, re.IGNORECASE)
        if opening_match:
            opening_balance = parse_decimal(opening_match.group(1))

        period_totals_match = re.search(
            r"Totalsatendofperiod\s+\$?([\d,]+\.\d{2}).*?\$?([\d,]+\.\d{2})\$?([\d,]+\.\d{2})\s*$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if period_totals_match:
            closing_balance = parse_decimal(period_totals_match.group(1))
            statement_total_debits = parse_decimal(period_totals_match.group(2))
            statement_total_credits = parse_decimal(period_totals_match.group(3))

        transactions: list[ParsedTransaction] = []
        page_row_counts: dict[int, int] = {}
        suspicious_rows: list[dict[str, object]] = []
        parser_flags = ["compact_nz_layout_fallback"]
        prev_balance = opening_balance
        row_index = 0

        for raw_page in page_lines:
            page_match = ANZ_COMPACT_PAGE.search(raw_page)
            current_page = int(page_match.group(1)) if page_match else max(page_row_counts.keys(), default=0) + 1
            upper_page = raw_page.upper()
            header_index = upper_page.find(ANZ_COMPACT_HEADER)
            if header_index < 0:
                continue
            segment = raw_page[header_index + len(ANZ_COMPACT_HEADER):]
            totals_index = re.search(r"Totalsatendofpage|Totalsatendofperiod", segment, re.IGNORECASE)
            if totals_index:
                segment = segment[: totals_index.start()]

            starts = list(ANZ_COMPACT_ROW_START.finditer(segment))
            for idx, match in enumerate(starts):
                start = match.start()
                end = starts[idx + 1].start() if idx + 1 < len(starts) else len(segment)
                chunk = segment[start:end].strip()
                if not chunk:
                    continue

                date_token = match.group(0)
                tx_date = self._compact_row_date(
                    date_token=date_token,
                    fallback_year=fallback_year,
                    statement_start_date=statement_start_date,
                    statement_end_date=statement_end_date,
                )
                if tx_date is None:
                    continue

                body = chunk[len(date_token) :].strip()
                body_upper = body.upper()
                if body_upper.startswith("OPENINGBALANCE"):
                    maybe_balance = self._last_compact_amount(body)
                    if maybe_balance is not None:
                        opening_balance = maybe_balance
                        prev_balance = maybe_balance
                    continue
                if "BALANCEBROUGHTFORWARDFROMPREVIOUSPAGE" in body_upper:
                    body = re.sub(r"^Balancebroughtforwardfrompreviouspage", "", body, flags=re.IGNORECASE).strip()
                    body_upper = body.upper()

                balance = self._last_compact_amount(body)
                if balance is None:
                    suspicious_rows.append({"line_no": row_index, "reason": "compact_balance_missing", "row_preview": body[:200]})
                    continue
                if prev_balance is None:
                    suspicious_rows.append({"line_no": row_index, "reason": "compact_previous_balance_missing", "row_preview": body[:200]})
                    prev_balance = balance
                    continue

                delta = balance - prev_balance
                direction = "credit" if delta > 0 else "debit"
                amount_value = abs(delta).quantize(Decimal("0.01"))
                description = self._compact_description(body=body, amount=amount_value, balance=balance)
                if not description:
                    description = "UNKNOWN"

                row_flags = ["compact_nz_layout_fallback", "direction_source_balance_delta"]
                section_kind = classify_section(description)
                row_confidence, row_meta = assess_row_quality(
                    description=description,
                    amount_found=True,
                    date_found=True,
                    section_kind=section_kind,
                    direction_source="balance_delta",
                    parser_flags=row_flags,
                    debit_found=direction == "debit",
                    credit_found=direction == "credit",
                    balance_found=True,
                )
                transactions.append(
                    ParsedTransaction(
                        row_index=row_index,
                        transaction_date=tx_date,
                        effective_date=None,
                        amount=amount_value,
                        direction=direction,
                        raw_description=description,
                        balance_after=balance,
                        page_number=current_page,
                        source_line_refs=[row_index],
                        parser_flags=row_flags,
                        parser_confidence=row_confidence,
                        metadata=row_meta,
                    )
                )
                page_row_counts[current_page] = page_row_counts.get(current_page, 0) + 1
                row_index += 1
                prev_balance = balance

        if not transactions:
            return None

        row_quality_counts, direction_source_counts = summarize_row_quality(transactions)
        compact_page_markers = {
            int(match.group(1))
            for line in page_lines
            for match in [ANZ_COMPACT_PAGE.search(line)]
            if match is not None
        }
        compact_page_count = len(compact_page_markers) or len(page_row_counts) or 1
        return ParsedStatementResult(
            statement_id=f"anz-{fallback_year or 'unk'}-{abs(hash(filename))}",
            bank_name="ANZ",
            account_name=None,
            account_ref_masked=None,
            statement_start_date=statement_start_date,
            statement_end_date=statement_end_date,
            parser_name=self.name,
            parser_confidence=0.78,
            parser_flags=sorted(set(parser_flags)),
            metadata={
                "line_count": len(page_lines),
                "page_count": compact_page_count,
                "table_header_count": compact_page_count,
                "page_transaction_counts": page_row_counts,
                "page_row_counts": page_row_counts,
                "table_detected": True,
                "page_resume_count": max(compact_page_count - 1, 0),
                "parser_failure_reason": None,
                "first_stop_reason": None,
                "first_stop_line": None,
                "excluded_rows": [],
                "suspicious_rows": suspicious_rows,
                "row_quality_counts": row_quality_counts,
                "direction_source_counts": direction_source_counts,
                "opening_balance": opening_balance,
                "closing_balance": closing_balance or prev_balance,
                "statement_total_debits": statement_total_debits,
                "statement_total_credits": statement_total_credits,
                **document_signals,
            },
            transactions=transactions,
        )

    def _last_compact_amount(self, text: str) -> Decimal | None:
        matches = list(ANZ_COMPACT_MONEY.finditer(text))
        if not matches:
            return None
        return parse_decimal(matches[-1].group(0))

    def _compact_row_date(
        self,
        *,
        date_token: str,
        fallback_year: int | None,
        statement_start_date: date | None,
        statement_end_date: date | None,
    ) -> date | None:
        if statement_start_date and statement_end_date and statement_start_date.year != statement_end_date.year:
            parsed_end_year = parse_dd_mmm(date_token, fallback_year=statement_end_date.year)
            if parsed_end_year is None:
                return None
            if parsed_end_year.month >= statement_start_date.month:
                return parse_dd_mmm(date_token, fallback_year=statement_start_date.year)
            return parsed_end_year
        return parse_dd_mmm(date_token, fallback_year=fallback_year)

    def _compact_description(self, *, body: str, amount: Decimal, balance: Decimal) -> str:
        working = body
        balance_text = f"{balance:,.2f}"
        if working.endswith(balance_text):
            working = working[: -len(balance_text)]
        else:
            plain_balance = f"{balance:.2f}"
            if working.endswith(plain_balance):
                working = working[: -len(plain_balance)]
        amount_text = f"{amount:,.2f}"
        plain_amount = f"{amount:.2f}"
        for candidate in (amount_text, plain_amount, f"0{plain_amount}"):
            if working.endswith(candidate):
                working = working[: -len(candidate)]
                break
        working = self._strip_transaction_type_prefix(working)
        working = re.sub(r"(\d{2}/\d{2}/\d{4})(?=\d)", r"\1 ", working)
        working = re.sub(r"([A-Za-z])(\d)", r"\1 \2", working)
        working = re.sub(r"([a-z])([A-Z])", r"\1 \2", working)
        working = re.sub(r"\s{2,}", " ", working).strip(" -")
        return working.strip()

    def _strip_transaction_type_prefix(self, text: str) -> str:
        working = text.strip()
        upper = working.upper()
        for prefix in ANZ_COMPACT_TYPES:
            token = f"{prefix} "
            if upper.startswith(token):
                return working[len(token):].strip()
        return working
