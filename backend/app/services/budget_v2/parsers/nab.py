"""NAB PDF statement parser adapter."""

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
    is_page_marker,
    is_transaction_table_header,
    normalize_spaced_text,
    parse_amount_columns,
    parse_decimal,
    parse_dd_mmm,
    summarize_row_quality,
)
from app.services.budget_v2.types import ParsedStatementResult, ParsedTransaction

NAB_DATE_LINE = re.compile(r"^(\d{1,2}\s+[A-Z]{3}(?:\s+\d{2,4})?)(?:\s+(.*))?$")
NAB_YEAR = re.compile(r"\b(20\d{2})\b")
NAB_OPENING_BALANCE = re.compile(r"OPENING BALANCE\s+\$?([\d,]+\.\d{2})\s*(CR|DR)?", re.IGNORECASE)
NAB_CLOSING_BALANCE = re.compile(r"CLOSING BALANCE\s+\$?([\d,]+\.\d{2})\s*(CR|DR)?", re.IGNORECASE)
NAB_TOTAL_CREDITS = re.compile(r"TOTAL CREDITS\s+\$?([\d,]+\.\d{2})", re.IGNORECASE)
NAB_TOTAL_DEBITS = re.compile(r"TOTAL DEBITS\s+\$?([\d,]+\.\d{2})", re.IGNORECASE)
NAB_DATE_HEADER = re.compile(r"^DATE\s+PARTICULARS\s+DEBITS\s+CREDITS\s+BALANCE$", re.IGNORECASE)
NAB_DATE_WITH_LINE = re.compile(
    r"^\d{1,2}\s+[A-Z]{3}(?:\s+\d{2,4})?\s+.*\$\s*[\d,]+\.\d{2}\s+\$\s*[\d,]+\.\d{2}\s*(CR|DR)\s*$",
    re.IGNORECASE,
)


class NabPdfParser(BankStatementParser):
    name = "nab_pdf_v2"
    banks = ("NAB",)

    def can_parse(self, text: str, filename: str) -> float:
        score = 0.0
        upper = text.upper()
        if "NATIONAL AUSTRALIA BANK" in upper or "NAB" in upper:
            score += 0.45
        if "TRANSACTION LISTING" in upper:
            score += 0.35
        if "DATE PARTICULARS DEBITS CREDITS BALANCE" in upper:
            score += 0.2
        if "ANZ ACCESS ADVANTAGE" in upper:
            score -= 0.3
        return min(score, 1.0)

    def parse(self, text: str, filename: str) -> ParsedStatementResult:
        lines = [normalize_spaced_text(ln.rstrip()) for ln in text.splitlines() if ln.strip()]
        fallback_year = self._detect_year(lines)
        statement_start_date, statement_end_date = extract_statement_period(lines)
        document_signals = detect_document_signals(lines)
        opening_balance = self._extract_opening_balance(lines)
        closing_balance = self._extract_closing_balance(lines)
        statement_total_debits = self._extract_total(lines, NAB_TOTAL_DEBITS)
        statement_total_credits = self._extract_total(lines, NAB_TOTAL_CREDITS)
        page_count = self._extract_page_count(lines)

        transactions: list[ParsedTransaction] = []
        parser_flags: list[str] = []
        current_date: date | None = None
        current_lines: list[str] = []
        row_index = 0
        inside_transaction_table = False
        resume_on_next_header = False
        saw_transaction_listing = False
        table_header_count = 0
        current_page = 1
        page_row_counts: dict[int, int] = {}
        excluded_rows: list[dict[str, object]] = []
        suspicious_rows: list[dict[str, object]] = []

        def flush(line_no: int) -> None:
            nonlocal row_index, current_date, current_lines
            if current_date is None:
                return
            joined = " ".join(current_lines).strip()
            amount, balance, explicit_direction, direction_source, both_amounts = self._extract_amount_and_balance(current_lines)
            if amount is None:
                excluded_rows.append(
                    {
                        "page_number": current_page,
                        "line_no": line_no,
                        "reason": "malformed_amount",
                        "row_preview": joined[:200],
                    }
                )
                parser_flags.append("malformed_amount")
                current_date = None
                current_lines = []
                return
            description = self._strip_amount_from_description(joined) or "UNKNOWN"
            row_flags: list[str] = []

            leakage_hits = detect_leakage_tokens(description)
            if leakage_hits:
                row_flags.append("suspected_footer_merge")
                parser_flags.append("suspected_footer_merge")
                suspicious_rows.append(
                    {
                        "page_number": current_page,
                        "line_no": line_no,
                        "reason": "suspected_footer_merge",
                        "row_preview": description[:200],
                        "tokens": leakage_hits,
                    }
                )

            direction = explicit_direction or "debit"
            if both_amounts:
                row_flags.append("both_amount_columns_present")
            if direction_source == "explicit_column":
                row_flags.append("direction_source_explicit_column")
            elif direction_source == "balance_delta":
                row_flags.append("direction_source_balance_delta")
            else:
                row_flags.append("direction_source_fallback")
            if explicit_direction and direction_source != "explicit_column":
                row_flags.append("direction_marked_without_explicit_columns")
            if amount is not None and balance is None:
                row_flags.append("balance_missing")

            section_kind = classify_section(description)
            row_confidence, row_meta = assess_row_quality(
                description=description,
                amount_found=amount is not None,
                date_found=current_date is not None,
                section_kind=section_kind,
                direction_source=direction_source or "fallback",
                parser_flags=row_flags,
                debit_found=direction == "debit",
                credit_found=direction == "credit" and explicit_direction == "credit",
                balance_found=balance is not None,
            )

            transactions.append(
                ParsedTransaction(
                    row_index=row_index,
                    transaction_date=current_date,
                    effective_date=None,
                    amount=abs(amount),
                    direction=direction,
                    raw_description=description,
                    raw_reference=None,
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
            current_lines = []

        for idx, raw_line in enumerate(lines):
            line = raw_line.strip()
            upper = line.upper()
            section_kind = classify_section(line)

            if upper == "TRANSACTION LISTING":
                saw_transaction_listing = True
                continue

            if section_kind == "table_header" or is_transaction_table_header(line) or NAB_DATE_HEADER.match(upper):
                inside_transaction_table = True
                resume_on_next_header = False
                if NAB_DATE_HEADER.match(upper):
                    table_header_count += 1
                continue

            if section_kind in {"page_marker", "legal_noise", "summary_section"} or upper.startswith("IMPORTANT") or upper in {
                "IMPORTANT",
                "THIS PROVISIONAL LIST IS NOT A STATEMENT OF ACCOUNT.",
                "IT MAY INCLUDE TRANSACTIONS WHICH MAY APPEAR ON PREVIOUS STATEMENTS.",
                "IT MAY NOT INCLUDE ALL TRANSACTIONS PROCESSED SINCE LAST STATEMENT WAS ISSUED.",
                "WITH THE EXCEPTION OF CHEQUE SERIAL NUMBERS, THE DETAILS SHOWN IN THE PARTICULARS COLUMN MAY BE AN ABBREVIATION.",
                "INCLUSION OF A DEBIT DOES NOT ALWAYS INDICATE PAYMENT BY THE BANK.",
            } or upper.startswith("NATIONAL AUSTRALIA BANK LIMITED"):
                if current_date is not None:
                    flush(idx)
                parser_flags.append("unsupported_section_encountered")
                suspicious_rows.append(
                    {
                        "page_number": current_page,
                        "line_no": idx,
                        "reason": "unsupported_section_encountered",
                        "row_preview": line[:200],
                    }
                )
                inside_transaction_table = False
                resume_on_next_header = True
                if section_kind == "page_marker" and current_page < page_count:
                    current_page += 1
                continue

            if resume_on_next_header:
                continue

            if not inside_transaction_table:
                if saw_transaction_listing and NAB_DATE_LINE.match(upper):
                    inside_transaction_table = True
                else:
                    continue

            if resume_on_next_header:
                continue

            m = NAB_DATE_LINE.match(upper)
            if m:
                if current_date is not None:
                    flush(idx)
                tx_date = parse_dd_mmm(m.group(1), fallback_year=fallback_year)
                if tx_date is None:
                    parser_flags.append("malformed_date")
                    excluded_rows.append(
                        {
                            "page_number": current_page,
                            "line_no": idx,
                            "reason": "malformed_date",
                            "row_preview": line[:200],
                        }
                    )
                    continue
                current_date = tx_date
                rest = (m.group(2) or "").strip()
                current_lines = [rest] if rest else []
                continue

            if current_date is not None:
                current_lines.append(line)
                if parse_amount_columns(line) is not None:
                    flush(idx)
                    continue
                if NAB_DATE_WITH_LINE.match(f"{current_date.strftime('%d %b %y')} {line}"):
                    flush(idx)

        if current_date is not None:
            flush(len(lines))

        if not transactions:
            transactions = self._fallback_parse_dense_layout(lines, fallback_year)
            if transactions:
                parser_flags.append("dense_layout_fallback")
        transactions = self._infer_direction_from_balances(transactions, opening_balance)
        row_quality_counts, direction_source_counts = summarize_row_quality(transactions)

        return ParsedStatementResult(
            statement_id=f"nab-{fallback_year or 'unk'}-{abs(hash(filename))}",
            bank_name="NAB",
            account_name=None,
            account_ref_masked=None,
            statement_start_date=statement_start_date,
            statement_end_date=statement_end_date,
            parser_name=self.name,
            parser_confidence=0.80,
            parser_flags=sorted(set(parser_flags)),
            metadata={
                "line_count": len(lines),
                "page_count": page_count,
                "table_header_count": table_header_count,
                "opening_balance": opening_balance,
                "closing_balance": closing_balance,
                "statement_total_debits": statement_total_debits,
                "statement_total_credits": statement_total_credits,
                "page_row_counts": page_row_counts,
                "excluded_rows": excluded_rows,
                "suspicious_rows": suspicious_rows,
                "row_quality_counts": row_quality_counts,
                "direction_source_counts": direction_source_counts,
                **document_signals,
            },
            transactions=transactions,
        )

    def _extract_opening_balance(self, lines: list[str]) -> Decimal | None:
        for line in lines[:80]:
            match = NAB_OPENING_BALANCE.search(line)
            if not match:
                continue
            value = parse_decimal(match.group(1))
            if value is None:
                continue
            if (match.group(2) or "").upper() == "DR":
                return -abs(value)
            return abs(value)
        return None

    def _extract_amount_and_balance(
        self, block_lines: list[str]
    ) -> tuple[Decimal | None, Decimal | None, str | None, str | None, bool]:
        for line in reversed(block_lines):
            parsed = parse_amount_columns(line)
            if parsed is None:
                continue
            debit, credit, balance, _crdr, source = parsed
            if debit is not None and credit is not None:
                return debit, balance, "debit", source, True
            if debit is not None:
                return debit, balance, None, source, False
            if credit is not None:
                return credit, balance, "credit", source, False
        joined = " ".join(block_lines)
        matches = re.findall(r"\$?([\d,]+\.\d{2})", joined)
        if len(matches) >= 2:
            amount = parse_decimal(matches[-2])
            balance = parse_decimal(matches[-1])
            return amount, balance, None, "fallback", False
        if matches:
            return parse_decimal(matches[-1]), None, None, "fallback", False
        return None, None, None, None, False

    def _strip_amount_from_description(self, joined: str) -> str:
        cleaned = re.sub(
            r"\s+\$?[\d,]+\.\d{2}\s+\$?[\d,]+\.\d{2}\s*(?:CR|DR)?\s*$",
            "",
            joined,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        return cleaned

    def _infer_direction_from_balances(
        self,
        transactions: list[ParsedTransaction],
        opening_balance: Decimal | None,
    ) -> list[ParsedTransaction]:
        prev_balance = opening_balance
        tolerance = Decimal("0.05")
        for tx in transactions:
            if "direction_source_explicit_column" in tx.parser_flags:
                if tx.balance_after is not None:
                    prev_balance = tx.balance_after
                continue
            if prev_balance is not None and tx.balance_after is not None:
                delta = tx.balance_after - prev_balance
                if abs(delta + tx.amount) <= tolerance:
                    tx.direction = "debit"
                    tx.parser_flags = [flag for flag in tx.parser_flags if flag != "direction_source_fallback"]
                    tx.parser_flags.append("direction_source_balance_delta")
                    tx.metadata["direction_source"] = "balance_delta"
                elif abs(delta - tx.amount) <= tolerance:
                    tx.direction = "credit"
                    tx.parser_flags = [flag for flag in tx.parser_flags if flag != "direction_source_fallback"]
                    tx.parser_flags.append("direction_source_balance_delta")
                    tx.metadata["direction_source"] = "balance_delta"
                else:
                    tx.parser_flags.append("direction_inference_uncertain")
                prev_balance = tx.balance_after
                continue
            tx.direction = "debit"
            tx.parser_flags.append("direction_source_fallback")
            tx.metadata["direction_source"] = "fallback"
            if tx.balance_after is not None:
                prev_balance = tx.balance_after
        return transactions

    def _extract_total(self, lines: list[str], pattern: re.Pattern[str]) -> Decimal | None:
        for line in lines[:40]:
            match = pattern.search(line)
            if match:
                return parse_decimal(match.group(1))
        return None

    def _extract_closing_balance(self, lines: list[str]) -> Decimal | None:
        for line in lines[:40]:
            match = NAB_CLOSING_BALANCE.search(line)
            if not match:
                continue
            value = parse_decimal(match.group(1))
            if value is None:
                continue
            if (match.group(2) or "").upper() == "DR":
                return -abs(value)
            return abs(value)
        return None

    def _extract_page_count(self, lines: list[str]) -> int:
        max_page = 1
        for line in lines:
            match = re.match(r"^PAGE\s+(\d+)\s+OF\s+(\d+)$", line.strip().upper())
            if match:
                max_page = max(max_page, int(match.group(2)))
        return max_page

    def _fallback_parse_dense_layout(self, lines: list[str], fallback_year: int | None) -> list[ParsedTransaction]:
        blob = " ".join(lines)
        pattern = re.compile(r"(\d{1,2}\s+[A-Z]{3}\s+20\d{2})(.*?)(\d[\d,]*\.\d{2})(?:\s*(CR|DR))?", re.IGNORECASE)
        transactions: list[ParsedTransaction] = []
        row_index = 0

        for match in pattern.finditer(blob):
            date_token, raw_desc, amount_token, crdr = match.groups()
            tx_date = parse_dd_mmm(date_token.upper(), fallback_year=fallback_year)
            if tx_date is None:
                continue
            amount = Decimal(amount_token.replace(",", ""))
            if amount == 0:
                continue
            description = re.sub(r"\s+", " ", raw_desc).strip(" .")
            if len(description) < 3:
                continue
            row_flags: list[str] = ["dense_layout_fallback"]
            if detect_leakage_tokens(description):
                row_flags.append("suspected_footer_merge")
            direction = "credit" if (crdr or "").upper() == "CR" else "debit"
            transactions.append(
                ParsedTransaction(
                    row_index=row_index,
                    transaction_date=tx_date,
                    effective_date=None,
                    amount=abs(amount),
                    direction=direction,
                    raw_description=description[:300],
                    raw_reference=None,
                    balance_after=None,
                    source_line_refs=[],
                    parser_flags=row_flags,
                    parser_confidence=0.62,
                    metadata={
                        "row_shape_confidence": 0.58,
                        "amount_parse_confidence": 0.64,
                        "date_parse_confidence": 0.72,
                        "section_context_confidence": 0.52,
                        "direction_source": "fallback",
                        "row_quality_bucket": "medium",
                    },
                )
            )
            row_index += 1
        return transactions

    def _detect_year(self, lines: list[str]) -> int | None:
        for line in lines[:140]:
            match = NAB_YEAR.search(line)
            if match:
                return int(match.group(1))
        return None
