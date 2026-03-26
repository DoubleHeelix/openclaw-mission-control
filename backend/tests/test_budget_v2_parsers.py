from decimal import Decimal

from app.services.budget_v2.parsers.anz import AnzPdfParser
from app.services.budget_v2.parsers.nab import NabPdfParser
from app.services.budget_v2.parsers.registry import pick_parser


def test_anz_multiline_rows_and_effective_lines_parse_without_blank_placeholders() -> None:
    text = """
ANZ
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
2026
21 JUL EFTPOS
PAYPAL *APPLE.COM/BILL SYDNEY AU
EFFECTIVE DATE 19 JUL 2025
36,500.00 IDR INC O/S FEE $0.10
25.99
22 JUL PAYMENT
TO HASHINI SATHARAS TRANSFER
120.00
TOTAL FEES
"""
    parser = AnzPdfParser()
    result = parser.parse(text, "anz.pdf")
    assert len(result.transactions) >= 2
    assert all(" blank" not in tx.raw_description.lower() for tx in result.transactions)
    assert any("PAYPAL" in tx.raw_description.upper() for tx in result.transactions)
    assert result.transactions[0].amount == Decimal("25.99")
    assert result.transactions[0].direction == "debit"
    assert "unsupported_section_encountered" in result.parser_flags


def test_anz_fx_line_does_not_override_actual_withdrawal_amount() -> None:
    text = """
ANZ ACCESS ADVANTAGE STATEMENT
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
2025
04 JUL VISA DEBIT PURCHASE CARD 9056
TRAVELOKA3DS*1265567835 JAKARTA
640,000.00 IDR INC O/S FEE $1.81
EFFECTIVE DATE 01 JUL 2025
62.36 blank 1,200.42
"""
    parser = AnzPdfParser()
    result = parser.parse(text, "anz.pdf")
    assert len(result.transactions) == 1
    assert result.transactions[0].amount == Decimal("62.36")


def test_anz_parser_resumes_after_cover_pages_and_page_markers() -> None:
    text = """
ANZ ACCESS ADVANTAGE STATEMENT
WELCOME TO YOUR ANZ ACCOUNT AT A GLANCE
ACCOUNT DETAILS
NEED TO GET IN TOUCH
Page 2 of 29
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
2025
03 JUL VISA DEBIT PURCHASE CARD 1234
GOODLIFE FOUNTAIN GATE
15.29 blank 1,500.00
04 JUL VISA DEBIT PURCHASE CARD 1234
GOODLIFE FOUNTAIN GATE
15.29 blank 1,484.71
blank TOTALS AT END OF PAGE $30.58 $0.00
Page 3 of 29
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
10 JUL ANZ INTERNET BANKING PAYMENT
FROM ACCENTURE AUSTRALIA
blank 5,314.81 6,799.52
Page 4 of 29
Summary of ANZ Transaction Fees Transactions Fee Per
"""
    parser = AnzPdfParser()
    result = parser.parse(text, "anz.pdf")
    assert len(result.transactions) == 3
    assert result.metadata["table_detected"] is True
    assert result.metadata["page_resume_count"] >= 1
    assert result.metadata["page_transaction_counts"][2] == 2
    assert result.metadata["page_transaction_counts"][3] == 1
    assert result.metadata["parser_failure_reason"] == "terminal_section_encountered"
    assert result.metadata["row_quality_counts"]["high"] >= 1
    assert result.metadata["direction_source_counts"]["explicit_column"] >= 1
    assert any("ACCENTURE" in tx.raw_description.upper() for tx in result.transactions)
    assert all("PAGE 2 OF 29" not in tx.raw_description.upper() for tx in result.transactions)
    assert all("NEED TO GET IN TOUCH" not in tx.raw_description.upper() for tx in result.transactions)


def test_anz_parser_flags_table_detected_but_no_transactions_as_parser_flow_failure() -> None:
    text = """
ANZ ACCESS ADVANTAGE STATEMENT
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
Page 2 of 29
"""
    result = AnzPdfParser().parse(text, "anz.pdf")
    assert len(result.transactions) == 0
    assert result.metadata["table_detected"] is True
    assert result.metadata["parser_failure_reason"] == "table_detected_but_no_transactions"


def test_anz_parser_extracts_cover_summary_and_end_of_period_totals() -> None:
    text = """
ANZ ACCESS ADVANTAGE STATEMENT
STATEMENT NUMBER 51
03 JULY 2025 TO 02 JANUARY 2026
Opening Balance:
$
1,320.07
Total Deposits:
$53,192.04
Total Withdrawals:
$50,596.08
Closing Balance:
$
3,916.03
Page 2 of 29
Transaction Details
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
2025
04 JUL PAYMENT
TO HASHINI SATHARAS TRANSFER
50.00 blank 1,262.78
11 JUL PAYMENT FROM MR RAMITH D ACHCHIGE SATHAR blank 400.00 1,318.15
Page 26 of 29
Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)
02 JAN VISA DEBIT PURCHASE CARD 9056
APPLE.COM/AU SYDNEY
1,874.40 blank 3,916.03
blank TOTALS AT END OF PAGE $1,874.40 $0.00
blank
TOTALS AT END OF PERIOD
Withdrawals ($)
$50,596.08
Deposits ($)
$53,192.04
Balance ($)
$3,916.03
Page 27 of 29
Fee Summary
"""
    result = AnzPdfParser().parse(text, "anz.pdf")
    assert result.metadata["opening_balance"] == Decimal("1320.07")
    assert result.metadata["closing_balance"] == Decimal("3916.03")
    assert result.metadata["statement_total_debits"] == Decimal("50596.08")
    assert result.metadata["statement_total_credits"] == Decimal("53192.04")
    assert result.metadata["page_count"] == 29
    assert result.metadata["table_header_count"] >= 2


def test_anz_parser_handles_compact_nz_statement_layout() -> None:
    text = """
StatementofAccountsYour accounts at a glance as at 7 March 2025
Page 1 of 3ANZ Bank New Zealand Limited
Today'sstatementsAccounttype Accountnumber BalanceGo 06-0569-0242151-00138.16GoAccountnameMR S P KUMARAccountnumber06-0569-0242151-00Statementnumber00046Statementperiod10 Dec 2024 - 07 Mar 2025DateTransactiontypeanddetails Withdrawals Deposits Balance10 DecOpeningbalance 222.1010 DecDDCityfitness Group CityFitnessG 0O100DKQ00507.20214.9010 DecDD9554-1094-1075-7132 DEBIT TRANSFER 01134871.20143.7011 DecDDCityfitness Group CityFitnessG 0O100DKS00GO7.20136.5012 DecDCAccenture NZ Ltd Pay DIRECT CREDIT4,239.664,376.16TotalsatendofpageTexttextAPAutomaticPayment BPBillPayment
Page 2 of 3Page 2 of 3
Go- continuedDateTransactiontypeanddetails Withdrawals Deposits BalanceBalancebroughtforwardfrompreviouspage13 DecDD06-0569-0242151-52 DEBIT TRANSFER 1101211,000.003,376.16TotalsatendofpageTextTotalsatendofperiod $3,376.16Your available credit is $3,376.16 as at the closing date of this statement.text$1,085.60$4,239.66
"""
    result = AnzPdfParser().parse(text, "anz-nz.pdf")
    assert len(result.transactions) == 5
    assert result.metadata["table_detected"] is True
    assert "compact_nz_layout_fallback" in result.parser_flags
    assert str(result.statement_start_date) == "2024-12-10"
    assert str(result.statement_end_date) == "2025-03-07"
    assert result.metadata["opening_balance"] == Decimal("222.10")
    assert result.metadata["closing_balance"] == Decimal("3376.16")
    assert result.metadata["statement_total_debits"] == Decimal("1085.60")
    assert result.metadata["statement_total_credits"] == Decimal("4239.66")
    assert result.metadata["page_count"] == 2
    assert result.metadata["table_header_count"] == 2
    assert result.transactions[0].amount == Decimal("7.20")
    assert result.transactions[0].direction == "debit"
    assert not result.transactions[0].raw_description.upper().startswith("DD ")
    assert "CITYFITNESS" in result.transactions[0].raw_description.upper()
    assert result.transactions[3].amount == Decimal("4239.66")
    assert result.transactions[3].direction == "credit"
    assert not result.transactions[3].raw_description.upper().startswith("DC ")
    assert "ACCENTURE" in result.transactions[3].raw_description.upper()


def test_nab_multiline_rows_parse_until_boundary() -> None:
    text = """
NAB
Statement starts 15 August 2025
Statement ends 18 February 2026
Transaction Listing
2026
11 MAR V0628 11/03 MYKI PAYMENTS MELBOURNE 74940526070
10.00
13 MAR V0628 13/03 GOOGLE YOUTUBEPREMIUM BARANGAROO
25.99
IMPORTANT INFORMATION
Do not parse below this line
"""
    parser = NabPdfParser()
    result = parser.parse(text, "nab.pdf")
    assert len(result.transactions) == 2
    assert result.transactions[0].raw_description.upper().startswith("V0628")
    assert str(result.statement_start_date) == "2025-08-15"


def test_nab_parser_does_not_strip_real_bp_merchant_prefix() -> None:
    text = """
NAB
Statement starts 15 August 2025
Statement ends 18 February 2026
Transaction Listing
2026
11 MAR BP RICHMOND EAST MELBOURNE
45.67
"""
    parser = NabPdfParser()
    result = parser.parse(text, "nab.pdf")
    assert len(result.transactions) == 1
    assert result.transactions[0].raw_description.upper().startswith("BP ")


def test_nab_parser_selected_for_transaction_listing_layout() -> None:
    text = """
Transaction Listing
Date Particulars Debits Credits Balance
16 FEB 26 SOME MERCHANT $15.00 $14,626.21 CR
"""
    parser = pick_parser(text, "statement.pdf")
    assert parser.name == "nab_pdf_v2"


def test_nab_direction_inferred_from_balance_movement() -> None:
    text = """
NAB
Opening Balance $1,000.00 CR
Transaction Listing
Date Particulars Debits Credits Balance
16 FEB 26 PAYMENT TO SOMEONE $100.00 $900.00 CR
17 FEB 26 SALARY PAYMENT $500.00 $1,400.00 CR
"""
    parser = NabPdfParser()
    result = parser.parse(text, "nab.pdf")
    assert len(result.transactions) == 2
    assert result.transactions[0].direction == "debit"
    assert result.transactions[1].direction == "credit"


def test_nab_parser_resumes_after_page_footer_and_captures_credit_rows() -> None:
    text = """
NAB
Opening Balance $1,000.00 CR
Total Credits $5,314.81
Total Debits $100.00
Closing Balance $6,214.81 CR
Transaction Listing starts 16 February 2026
Transaction Listing ends 17 March 2026
Transaction Details
Date Particulars Debits Credits Balance
16 Feb 26 SOME MERCHANT $100.00 $900.00 CR
Page 1 Of 3
Important
This provisional list is not a statement of account.
Transaction Details
Date Particulars Debits Credits Balance
12 Mar 26 140516401 ACCENTURE AUSTRA $5,314.81 $6,214.81 CR
Page 2 Of 3
Important
Transaction Details
Date Particulars Debits Credits Balance
17 Mar 26 NPP-ANZBAU3LXXXN20260317020005979143890 $51.00 $6,265.81 CR
"""
    parser = NabPdfParser()
    result = parser.parse(text, "nab.pdf")
    assert len(result.transactions) == 3
    assert any("ACCENTURE" in tx.raw_description.upper() for tx in result.transactions)
    accenture = next(tx for tx in result.transactions if "ACCENTURE" in tx.raw_description.upper())
    assert accenture.direction == "credit"
    assert result.metadata["page_count"] == 3
    assert result.metadata["table_header_count"] == 3
    assert result.metadata["document_type"] == "transaction_listing"
    assert result.metadata["document_reconcilable"] is False
    assert result.metadata["row_quality_counts"]["high"] >= 1
    assert result.metadata["direction_source_counts"]["balance_delta"] >= 1
    assert result.metadata["page_row_counts"][1] == 1
    assert result.metadata["page_row_counts"][2] == 1
    assert result.metadata["page_row_counts"][3] == 1
    assert all("IMPORTANT" not in tx.raw_description.upper() for tx in result.transactions)


def test_nab_parser_records_suspicious_unsupported_sections_without_polluting_rows() -> None:
    text = """
NAB
Opening Balance $1,000.00 CR
Total Credits $0.00
Total Debits $100.00
Closing Balance $900.00 CR
Transaction Listing
Transaction Details
Date Particulars Debits Credits Balance
16 Feb 26 SOME MERCHANT $100.00 $900.00 CR
Page 1 Of 1
Important
This provisional list is not a statement of account.
National Australia Bank Limited ABN 12 004 044 937 AFSL and Australian Credit Licence 230686
"""
    result = NabPdfParser().parse(text, "nab.pdf")
    assert len(result.transactions) == 1
    assert "IMPORTANT" not in result.transactions[0].raw_description.upper()
    suspicious = result.metadata["suspicious_rows"]
    assert any(item["reason"] == "unsupported_section_encountered" for item in suspicious)


def test_nab_ambiguous_amount_row_is_downgraded_in_row_quality() -> None:
    text = """
NAB
Opening Balance $1,000.00 CR
Transaction Listing
Transaction Details
Date Particulars Debits Credits Balance
16 Feb 26 SOME WEIRD ROW 100.00 100.00 900.00 CR
"""
    result = NabPdfParser().parse(text, "nab.pdf")
    assert len(result.transactions) == 1
    tx = result.transactions[0]
    assert "both_amount_columns_present" in tx.parser_flags
    assert tx.metadata["row_quality_bucket"] in {"medium", "low"}
