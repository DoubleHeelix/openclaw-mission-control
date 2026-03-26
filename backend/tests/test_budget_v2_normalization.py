from datetime import date
from decimal import Decimal

from app.services.budget_v2.normalization import normalize_transactions
from app.services.budget_v2.types import ParsedTransaction


def _parsed(row: int, desc: str) -> ParsedTransaction:
    return ParsedTransaction(
        row_index=row,
        transaction_date=date(2026, 3, 1),
        effective_date=None,
        amount=Decimal("15.29"),
        direction="debit",
        raw_description=desc,
        parser_flags=[],
        parser_confidence=0.9,
    )


def test_reference_codes_are_removed_from_goodlife_grouping_candidate() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "A00LEQ2007CT GOODLIFE FOUNTAI SHAHEEL KUMAR"),
            _parsed(2, "A00LGSH307RB GOODLIFE FOUNTAI SHAHEEL KUMAR"),
        ]
    )
    assert rows[0].merchant_candidate == "GOODLIFE"
    assert rows[1].merchant_candidate == "GOODLIFE"


def test_reference_codes_are_removed_but_unknown_merchant_name_is_preserved() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "V0628 ELIOS PLACE MELBOURNE"),
        ]
    )
    assert rows[0].merchant_candidate == "ELIOS PLACE MELBOURNE"


def test_housing_aliases_collapse_to_canonical_merchant() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "UPM TRUST H4152678927 CLARKE ST"),
            _parsed(2, "UPM TRUST M7573372610 CLARKE ST"),
        ]
    )
    assert rows[0].merchant_candidate == "UPM TRUST"
    assert rows[1].merchant_candidate == "UPM TRUST"


def test_address_like_suffixes_are_trimmed_from_merchant_candidate() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "V0628 14/02 WW METRO 245 CITY ROAD SOUTHBANK 74278246045"),
        ]
    )
    assert rows[0].merchant_candidate == "WW METRO"


def test_split_alias_tokens_still_collapse_to_known_merchant_family() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "CARD PURCHASE WOOL WORTHS SUPERMARKET 1234"),
        ]
    )
    assert rows[0].merchant_candidate == "WOOLWORTHS"


def test_anz_compact_direct_credit_strips_rail_and_credit_boilerplate() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "DC Accenture NZ Ltd Pay DIRECT CREDIT"),
        ]
    )
    assert rows[0].payment_rail == "DC"
    assert rows[0].merchant_candidate == "ACCENTURE"


def test_anz_compact_transfer_rows_group_as_internal_transfer_not_dd() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "DD 06-0569-0242151-52 DEBIT TRANSFER 110121"),
        ]
    )
    assert rows[0].payment_rail == "DD"
    assert rows[0].merchant_candidate == "INTERNAL TRANSFER"


def test_anz_compact_bill_payment_strips_bp_prefix_and_card_digits() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "BP American Express Kumar SK 3798 79238552004"),
        ]
    )
    assert rows[0].payment_rail == "BP"
    assert rows[0].merchant_candidate == "AMERICAN EXPRESS KUMAR"


def test_anz_compact_vt_strips_orig_date_suffix() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "VT Google One 483561******5278 Orig date 24/12/2024"),
        ]
    )
    assert rows[0].payment_rail == "VT"
    assert rows[0].merchant_candidate == "GOOGLE ONE"


def test_anz_compact_glued_prefixes_are_still_recognized() -> None:
    rows = normalize_transactions(
        [
            _parsed(1, "DCAccenture NZ Ltd Pay DIRECT CREDIT"),
            _parsed(2, "BPAmerican Express Kumar SK 3798 79238552004"),
            _parsed(3, "VTGoogle One 483561******5278 Orig date 24/12/2024"),
        ]
    )
    assert rows[0].payment_rail == "DC"
    assert rows[0].merchant_candidate == "ACCENTURE"
    assert rows[1].payment_rail == "BP"
    assert rows[1].merchant_candidate == "AMERICAN EXPRESS KUMAR"
    assert rows[2].payment_rail == "VT"
    assert rows[2].merchant_candidate == "GOOGLE ONE"
