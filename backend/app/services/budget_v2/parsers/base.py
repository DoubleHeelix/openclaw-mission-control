"""Parser adapter interface for bank statement ingestion."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.services.budget_v2.types import ParsedStatementResult


@dataclass(frozen=True)
class ParserProbe:
    parser_name: str
    bank_name: str | None
    confidence: float


class BankStatementParser(ABC):
    name: str
    banks: tuple[str, ...]

    @abstractmethod
    def can_parse(self, text: str, filename: str) -> float:
        """Return parser confidence score (0-1)."""

    @abstractmethod
    def parse(self, text: str, filename: str) -> ParsedStatementResult:
        """Parse statement text into canonical parsed statement result."""
