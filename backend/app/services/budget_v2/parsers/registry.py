"""Parser registry and PDF text extraction."""

from __future__ import annotations

import io

from pypdf import PdfReader

from app.services.budget_v2.parsers.anz import AnzPdfParser
from app.services.budget_v2.parsers.base import BankStatementParser
from app.services.budget_v2.parsers.nab import NabPdfParser

PARSERS: list[BankStatementParser] = [AnzPdfParser(), NabPdfParser()]


def extract_pdf_pages(raw_bytes: bytes) -> list[str]:
    reader = PdfReader(io.BytesIO(raw_bytes))
    return [page.extract_text() or "" for page in reader.pages]


def extract_pdf_text(raw_bytes: bytes) -> str:
    return "\n".join(extract_pdf_pages(raw_bytes))


def parser_scorecard(text: str, filename: str) -> list[tuple[BankStatementParser, float]]:
    return sorted(
        ((parser, parser.can_parse(text, filename)) for parser in PARSERS),
        key=lambda item: item[1],
        reverse=True,
    )


def pick_parser(text: str, filename: str) -> BankStatementParser:
    scored = parser_scorecard(text, filename)
    parser, score = scored[0]
    if score < 0.2:
        return parser
    return parser
