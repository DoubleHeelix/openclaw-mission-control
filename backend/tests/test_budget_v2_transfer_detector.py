from decimal import Decimal

from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.transfer_detector import assess_transfer
from app.services.budget_v2.types import NormalizedTransaction


def _tx(desc: str, *, direction: str = "debit", payment_rail: str | None = None) -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=1,
        transaction_date=None,
        effective_date=None,
        amount=Decimal("100.00"),
        direction=direction,
        raw_description=desc,
        normalized_description=desc.upper(),
        payment_rail=payment_rail,
        merchant_candidate=desc.upper(),
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash="h",
        signed_amount=Decimal("-100.00") if direction == "debit" else Decimal("100.00"),
        metadata={},
    )


def test_internal_transfer_confirmed_from_self_account_hints() -> None:
    result = assess_transfer(tx=_tx("OSKO transfer to savings", payment_rail="OSKO"), memory=MerchantMemoryHints())
    assert result.state == "transfer_confirmed_internal"


def test_ambiguous_npp_person_payment_stays_likely_not_confirmed_internal() -> None:
    result = assess_transfer(tx=_tx("NPP payment to john smith", payment_rail="NPP"), memory=MerchantMemoryHints())
    assert result.state in {"transfer_likely", "transfer_confirmed_external"}


def test_account_number_debit_transfer_is_confirmed_internal() -> None:
    result = assess_transfer(
        tx=_tx("DD 06-0569-0242151-52 DEBIT TRANSFER 110121", direction="debit", payment_rail="DD"),
        memory=MerchantMemoryHints(),
    )
    assert result.state == "transfer_confirmed_internal"
