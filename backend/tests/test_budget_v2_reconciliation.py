from decimal import Decimal

from app.services.budget_v2.engine import _classify_reconciliation, _coverage_warnings


def test_reconciliation_reconciled_when_balances_match() -> None:
    status, difference, expected, reason, warning_reasons = _classify_reconciliation(
        metadata={"document_type": "statement", "document_reconcilable": True},
        opening_balance=Decimal("1000.00"),
        closing_balance=Decimal("900.00"),
        parsed_debit_total=Decimal("100.00"),
        parsed_credit_total=Decimal("0.00"),
    )
    assert status == "reconciled"
    assert difference == Decimal("0.00")
    assert expected == Decimal("900.00")
    assert warning_reasons == []
    assert "reconcile" in reason.lower()


def test_reconciliation_parser_incomplete_when_statement_totals_do_not_match() -> None:
    status, difference, expected, reason, warning_reasons = _classify_reconciliation(
        metadata={
            "document_type": "statement",
            "document_reconcilable": True,
            "statement_total_debits": Decimal("120.00"),
            "statement_total_credits": Decimal("0.00"),
        },
        opening_balance=Decimal("1000.00"),
        closing_balance=Decimal("900.00"),
        parsed_debit_total=Decimal("100.00"),
        parsed_credit_total=Decimal("0.00"),
    )
    assert status == "parser_incomplete"
    assert expected == Decimal("900.00")
    assert "missing, duplicated, truncated, or misclassified" in reason
    assert "statement_totals_mismatch" in warning_reasons
    assert "parser_coverage_gap" in warning_reasons


def test_reconciliation_source_non_reconcilable_when_totals_match_but_document_is_provisional() -> None:
    status, difference, expected, reason, warning_reasons = _classify_reconciliation(
        metadata={
            "document_type": "transaction_listing",
            "document_reconcilable": False,
            "statement_total_debits": Decimal("9975.28"),
            "statement_total_credits": Decimal("5365.81"),
        },
        opening_balance=Decimal("14641.21"),
        closing_balance=Decimal("10056.11"),
        parsed_debit_total=Decimal("9975.28"),
        parsed_credit_total=Decimal("5365.81"),
    )
    assert status == "source_non_reconcilable"
    assert difference == Decimal("24.37")
    assert expected == Decimal("10031.74")
    assert "provisional transaction listing" in reason.lower()
    assert "provisional_document_non_reconcilable" in warning_reasons


def test_reconciliation_unknown_when_balances_missing() -> None:
    status, difference, expected, reason, warning_reasons = _classify_reconciliation(
        metadata={"document_type": "unknown"},
        opening_balance=None,
        closing_balance=None,
        parsed_debit_total=Decimal("10.00"),
        parsed_credit_total=Decimal("5.00"),
    )
    assert status == "unknown"
    assert difference == Decimal("0.00")
    assert expected is None
    assert "could not be completed" in reason.lower()
    assert warning_reasons == ["reconciliation_inputs_missing"]


def test_reconciliation_extraction_degraded_when_text_is_too_weak_and_no_rows_exist() -> None:
    status, difference, expected, reason, warning_reasons = _classify_reconciliation(
        metadata={"document_type": "unknown", "text_extraction_length": 120},
        opening_balance=None,
        closing_balance=None,
        parsed_debit_total=Decimal("0.00"),
        parsed_credit_total=Decimal("0.00"),
    )
    assert status == "extraction_degraded"
    assert expected is None
    assert "too weak for reliable parsing" in reason.lower()
    assert warning_reasons == ["extraction_quality_low"]


def test_coverage_warning_calls_out_parser_flow_failure_when_table_detected_but_no_rows() -> None:
    warnings = _coverage_warnings(
        parser_name="anz_pdf_v2",
        metadata={
            "table_detected": True,
            "parser_failure_reason": "table_detected_but_no_transactions",
            "text_extraction_length": 2500,
        },
        transactions=[],
        parsed_debit_total=Decimal("0.00"),
        parsed_credit_total=Decimal("0.00"),
        parsed_credit_count=0,
        parsed_debit_count=0,
        reconciliation_status="unknown",
        reconciliation_reason="",
    )
    assert any("failed to traverse transaction rows" in warning.lower() for warning in warnings)
    assert any("table_detected_but_no_transactions" in warning for warning in warnings)


def test_coverage_warning_calls_out_extraction_quality_when_text_is_too_small() -> None:
    warnings = _coverage_warnings(
        parser_name="anz_pdf_v2",
        metadata={
            "table_detected": False,
            "text_extraction_length": 120,
        },
        transactions=[],
        parsed_debit_total=Decimal("0.00"),
        parsed_credit_total=Decimal("0.00"),
        parsed_credit_count=0,
        parsed_debit_count=0,
        reconciliation_status="unknown",
        reconciliation_reason="",
    )
    assert any("may require ocr" in warning.lower() for warning in warnings)


def test_coverage_warning_is_suppressed_for_verified_compact_nz_layout() -> None:
    warnings = _coverage_warnings(
        parser_name="anz_pdf_v2",
        metadata={
            "table_detected": True,
            "page_count": 3,
            "table_header_count": 3,
            "page_row_counts": {"1": 10, "2": 20, "3": 5},
            "parser_flags": ["compact_nz_layout_fallback"],
            "statement_total_debits": Decimal("120.00"),
            "statement_total_credits": Decimal("80.00"),
        },
        transactions=[object(), object()],
        parsed_debit_total=Decimal("120.00"),
        parsed_credit_total=Decimal("80.00"),
        parsed_credit_count=1,
        parsed_debit_count=1,
        reconciliation_status="reconciled",
        reconciliation_reason="",
    )
    assert not any("one or more pages may not have been parsed fully" in warning.lower() for warning in warnings)


def test_coverage_warning_is_suppressed_for_any_reconciled_multi_page_statement_with_full_page_rows() -> None:
    warnings = _coverage_warnings(
        parser_name="some_bank_pdf_v1",
        metadata={
            "table_detected": True,
            "page_count": 3,
            "table_header_count": 1,
            "page_row_counts": {"1": 12, "2": 9, "3": 14},
            "statement_total_debits": Decimal("250.00"),
            "statement_total_credits": Decimal("100.00"),
        },
        transactions=[object(), object(), object()],
        parsed_debit_total=Decimal("250.00"),
        parsed_credit_total=Decimal("100.00"),
        parsed_credit_count=2,
        parsed_debit_count=5,
        reconciliation_status="reconciled",
        reconciliation_reason="",
    )
    assert not any("one or more pages may not have been parsed fully" in warning.lower() for warning in warnings)
