# ruff: noqa: INP001
from __future__ import annotations

from decimal import Decimal
from uuid import uuid4
from urllib.parse import quote

import pytest
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.control_center_budget import router as budget_router
from app.api.deps import require_org_member
from app.db.session import get_session
from app.models.organization_members import OrganizationMember
from app.models.organizations import Organization
from app.models.users import User
from app.services.organizations import OrganizationContext


def _pdf_escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _make_pdf(lines: list[str]) -> bytes:
    content = ["BT", "/F1 12 Tf", "50 780 Td", "14 TL"]
    first = True
    for line in lines:
        escaped = _pdf_escape(line)
        if first:
            content.append(f"({escaped}) Tj")
            first = False
        else:
            content.append("T*")
            content.append(f"({escaped}) Tj")
    content.append("ET")
    stream = "\n".join(content).encode()

    objects: list[tuple[int, bytes]] = []

    def obj(number: int, body: bytes) -> None:
        objects.append((number, body))

    obj(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    obj(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    obj(
        3,
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
    )
    obj(4, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    obj(5, b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream))

    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, body in objects:
        offsets.append(len(out))
        out.extend(f"{number} 0 obj\n".encode())
        out.extend(body)
        out.extend(b"\nendobj\n")
    xref_start = len(out)
    out.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    out.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.extend(f"{offset:010d} 00000 n \n".encode())
    out.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode())
    return bytes(out)


async def _make_engine() -> AsyncEngine:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    return engine


async def _seed_org(session_maker: async_sessionmaker[AsyncSession]) -> OrganizationContext:
    org = Organization(name="Budget Import Test Org")
    user = User(clerk_user_id=f"local-{uuid4().hex}", email="budget-test@example.com", name="Budget Test")
    async with session_maker() as session:
        session.add(org)
        session.add(user)
        await session.flush()
        member = OrganizationMember(organization_id=org.id, user_id=user.id, role="owner")
        session.add(member)
        await session.commit()
        await session.refresh(org)
        await session.refresh(member)
        return OrganizationContext(organization=org, member=member)


def _build_test_app(
    session_maker: async_sessionmaker[AsyncSession],
    ctx: OrganizationContext,
) -> FastAPI:
    app = FastAPI()
    api_v1 = APIRouter(prefix="/api/v1")
    api_v1.include_router(budget_router)
    app.include_router(api_v1)

    async def _override_get_session() -> AsyncSession:
        async with session_maker() as session:
            yield session

    async def _override_require_org_member() -> OrganizationContext:
        return ctx

    app.dependency_overrides[get_session] = _override_get_session
    app.dependency_overrides[require_org_member] = _override_require_org_member
    return app


FULL_WINDOW_NAB_LINES = [
    "NAB",
    "Opening Balance $1,000.00 CR",
    "Total Credits $5,314.81",
    "Total Debits $166.57",
    "Closing Balance $6,148.24 CR",
    "Transaction Listing starts 01 February 2026",
    "Transaction Listing ends 28 February 2026",
    "Transaction Details",
    "Date Particulars Debits Credits Balance",
    "03 Feb 26 UBER TRIP MELBOURNE $25.99 $974.01 CR",
    "10 Feb 26 140516401 ACCENTURE AUSTRA $5,314.81 $6,288.82 CR",
    "12 Feb 26 GOODLIFE FOUNTAIN GATE $15.29 $6,273.53 CR",
    "19 Feb 26 GOODLIFE FOUNTAIN GATE $15.29 $6,258.24 CR",
    "25 Feb 26 MYKI PAYMENTS MELBOURNE $10.00 $6,248.24 CR",
    "26 Feb 26 NPP-ANZBAU3LXXXN20260226020005979143890 $100.00 $6,148.24 CR",
]

SHORT_WINDOW_NAB_LINES = [
    "NAB",
    "Opening Balance $1,000.00 CR",
    "Total Credits $2,000.00",
    "Total Debits $51.99",
    "Closing Balance $2,948.01 CR",
    "Transaction Listing starts 01 March 2026",
    "Transaction Listing ends 05 March 2026",
    "Transaction Details",
    "Date Particulars Debits Credits Balance",
    "01 Mar 26 PAYROLL ACME PTY LTD $2,000.00 $3,000.00 CR",
    "02 Mar 26 UBER TRIP MELBOURNE $15.00 $2,985.00 CR",
    "03 Mar 26 GOOGLE YOUTUBEPREMIUM BARANGAROO $26.99 $2,958.01 CR",
    "05 Mar 26 MYKI PAYMENTS MELBOURNE $10.00 $2,948.01 CR",
]


def _decimal(payload: dict[str, object], key: str) -> Decimal:
    return Decimal(str(payload[key]))


@pytest.mark.asyncio
async def test_budget_v2_pdf_import_populates_all_views_for_full_statement() -> None:
    engine = await _make_engine()
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ctx = await _seed_org(session_maker)
    app = _build_test_app(session_maker, ctx)
    pdf_bytes = _make_pdf(FULL_WINDOW_NAB_LINES)

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            imported = await client.post(
                "/api/v1/control-center/budget/imports",
                files={"file": ("nab-full-window.pdf", pdf_bytes, "application/pdf")},
            )
            assert imported.status_code == 200, imported.text
            import_payload = imported.json()
            import_id = import_payload["import_id"]
            assert import_payload["parser_name"] == "nab_pdf_v2"
            assert import_payload["transaction_count"] == 6
            assert import_payload["parser_confidence"] > 0

            summary = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}")).json()
            lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            review = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/needs-review")).json()
            transactions = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/transactions")).json()
            snapshot = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/snapshot")).json()

            assert summary["statement_start_date"] == "2026-02-01"
            assert summary["statement_end_date"] == "2026-02-28"
            assert _decimal(summary, "parsed_credit_total") == Decimal("5314.81")
            assert _decimal(summary, "parsed_debit_total") == Decimal("166.57")
            assert summary["parsed_credit_count"] == 1
            assert summary["parsed_debit_count"] == 5
            assert summary["transaction_count"] == 6
            assert summary["reconciliation_status"] == "reconciled"
            assert _decimal(summary, "reconciliation_difference") == Decimal("0.00")
            assert summary["statement_truth"]["reconciliation_status"] == "reconciled"
            assert Decimal(str(summary["statement_truth"]["opening_balance"])) == Decimal("1000.00")
            assert Decimal(str(summary["statement_truth"]["closing_balance"])) == Decimal("6148.24")
            assert Decimal(str(summary["statement_truth"]["expected_closing_balance"])) == Decimal("6148.24")
            assert Decimal(str(summary["statement_truth"]["total_credits"])) == Decimal("5314.81")
            assert Decimal(str(summary["statement_truth"]["total_debits"])) == Decimal("166.57")
            assert Decimal(str(summary["statement_truth"]["net_movement"])) == Decimal("5148.24")
            assert summary["trust"]["modeling_allowed"] is True
            assert summary["budget_model"]["modeling_allowed"] is True
            assert summary["trust"]["totals_trust_level"] == "verified"
            assert Decimal(str(summary["budget_model"]["recurring_income_monthly"])) == Decimal("5314.81")
            assert Decimal(str(summary["budget_model"]["irregular_income_total"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["variable_discretionary_monthly"])) == Decimal("35.99")
            assert Decimal(str(summary["budget_model"]["observed_transfer_total"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["core_net"])) == Decimal("5314.81")
            assert Decimal(str(summary["budget_model"]["observed_net"])) == Decimal("5278.82")
            assert summary["budget_model"]["classification_reconciliation_status"] == "reconciled"
            assert Decimal(str(summary["budget_model"]["classification_reconciliation_difference"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["classified_section_totals"]["uncategorized"])) == Decimal("100.00")

            assert transactions["total"] == 6
            assert review["total"] >= 1
            assert any(item["final_bucket"] == "uncategorized" for item in transactions["items"])
            assert any("ACCENTURE" in item["raw_description"].upper() for item in transactions["items"])
            assert any(item["merchant_fingerprint"] for item in transactions["items"])
            assert all(item["signed_amount"] != "0" for item in transactions["items"])

            assert snapshot["statement_truth"] == summary["statement_truth"]
            assert snapshot["budget_model"] == summary["budget_model"]
            assert snapshot["trust"] == summary["trust"]

            goodlife_line = next(item for item in lines["items"] if "GOODLIFE" in item["group_label"].upper())
            assert goodlife_line["transaction_count"] == 2
            assert Decimal(str(goodlife_line["observed_amount"])) == Decimal("30.58")
            assert goodlife_line["observational_monthly_estimate"] is not None
            assert goodlife_line["movement_type"] == "expense"
            assert goodlife_line["group_key"]

            transfer_review_line = next(item for item in lines["items"] if item["final_bucket"] == "uncategorized")
            assert transfer_review_line["bucket_assignment"] == "variable_discretionary"
            assert transfer_review_line["included"] is True
            assert Decimal(str(transfer_review_line["normalized_monthly"])) == Decimal("0.00")

            salary_line = next(item for item in lines["items"] if item["line_type"] == "income")
            assert salary_line["bucket_assignment"] == "income_recurring"
            assert Decimal(str(salary_line["observed_amount"])) == Decimal("5314.81")
            assert Decimal(str(salary_line["observed_amount"])) == Decimal("5314.81")

            goodlife_group_key = quote(goodlife_line["group_key"], safe="")
            drilldown = (
                await client.get(
                    f"/api/v1/control-center/budget/imports/{import_id}/lines/{goodlife_group_key}/transactions"
                )
            ).json()
            assert drilldown["total"] == 2
            assert sum(Decimal(str(item["amount"])) for item in drilldown["items"]) == Decimal("30.58")

            grouped_observed_total = sum(Decimal(str(item["observed_amount"])) for item in lines["items"])
            assert grouped_observed_total == Decimal("5481.38")
            assert lines["statement_truth"] == summary["statement_truth"]
            assert lines["budget_model"] == summary["budget_model"]
            assert lines["trust"] == summary["trust"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_budget_v2_pdf_import_short_statement_populates_truth_and_blocks_modeling() -> None:
    engine = await _make_engine()
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ctx = await _seed_org(session_maker)
    app = _build_test_app(session_maker, ctx)
    pdf_bytes = _make_pdf(SHORT_WINDOW_NAB_LINES)

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            imported = await client.post(
                "/api/v1/control-center/budget/imports",
                files={"file": ("nab-short-window.pdf", pdf_bytes, "application/pdf")},
            )
            assert imported.status_code == 200, imported.text
            import_id = imported.json()["import_id"]

            summary = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}")).json()
            lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            transactions = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/transactions")).json()

            assert summary["statement_start_date"] == "2026-03-01"
            assert summary["statement_end_date"] == "2026-03-05"
            assert summary["transaction_count"] == 4
            assert _decimal(summary, "parsed_credit_total") == Decimal("2000.00")
            assert _decimal(summary, "parsed_debit_total") == Decimal("51.99")
            assert summary["statement_truth"]["reconciliation_status"] == "reconciled"
            assert summary["trust"]["modeling_allowed"] is False
            assert any("too short" in item.lower() for item in summary["trust"]["modeling_restrictions"])
            assert summary["budget_model"]["modeling_allowed"] is False
            assert Decimal(str(summary["budget_model"]["recurring_income_monthly"])) == Decimal("2000.00")
            assert Decimal(str(summary["budget_model"]["recurring_baseline_monthly"])) == Decimal("0")
            assert Decimal(str(summary["budget_model"]["variable_discretionary_monthly"])) == Decimal("51.99")
            assert Decimal(str(summary["budget_model"]["observed_net"])) == Decimal("1948.01")
            assert Decimal(str(summary["budget_model"]["irregular_income_total"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["observed_one_off_total"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["classified_section_totals"]["uncategorized"])) == Decimal("0.00")
            assert Decimal(str(summary["budget_model"]["classified_section_totals"]["variable_spending"])) == Decimal("51.99")

            assert transactions["total"] == 4
            assert all(item["movement_type"] in {"income", "expense", "other_needs_review"} for item in transactions["items"])
            assert all(item["merchant_fingerprint"] for item in transactions["items"])

            review_line = next(
                item
                for item in lines["items"]
                if item.get("final_bucket") == "variable_spending" and Decimal(str(item["observed_amount"])) > Decimal("0")
            )
            assert review_line["observational_monthly_estimate"] is None
            assert review_line["modeling_allowed"] is False
            assert lines["trust"] == summary["trust"]
            assert lines["budget_model"] == summary["budget_model"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_budget_v2_pdf_import_one_off_and_excluded_visibility_updates_totals() -> None:
    engine = await _make_engine()
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ctx = await _seed_org(session_maker)
    app = _build_test_app(session_maker, ctx)
    pdf_bytes = _make_pdf(FULL_WINDOW_NAB_LINES)

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            imported = await client.post(
                "/api/v1/control-center/budget/imports",
                files={"file": ("nab-full-window.pdf", pdf_bytes, "application/pdf")},
            )
            assert imported.status_code == 200, imported.text
            import_id = imported.json()["import_id"]

            initial_lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            uber_line = next(item for item in initial_lines["items"] if item["group_label"] == "UBER")
            assert uber_line["bucket_assignment"] == "variable_discretionary"
            assert Decimal(str(initial_lines["budget_model"]["variable_discretionary_monthly"])) == Decimal("35.99")

            mark_one_off = await client.patch(
                f"/api/v1/control-center/budget/imports/{import_id}/overrides",
                json={
                    "operations": [
                        {
                            "target_type": "group",
                            "target_id": uber_line["group_key"],
                            "operation": "mark_one_off",
                            "payload": {},
                        }
                    ]
                },
            )
            assert mark_one_off.status_code == 200, mark_one_off.text
            recompute = await client.post(f"/api/v1/control-center/budget/imports/{import_id}/recompute")
            assert recompute.status_code == 200, recompute.text

            after_one_off = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            uber_one_off = next(item for item in after_one_off["items"] if item["group_label"] == "UBER")
            assert uber_one_off["bucket_assignment"] == "one_off_exceptional"
            assert uber_one_off["final_bucket"] == "one_off_spending"
            assert uber_one_off["included"] is True
            assert Decimal(str(after_one_off["budget_model"]["observed_one_off_total"])) == Decimal("25.99")
            assert Decimal(str(after_one_off["budget_model"]["variable_discretionary_monthly"])) == Decimal("10.00")
            assert Decimal(str(after_one_off["budget_model"]["observed_net"])) == Decimal("5304.81")

            exclude_visible = await client.patch(
                f"/api/v1/control-center/budget/imports/{import_id}/overrides",
                json={
                    "operations": [
                        {
                            "target_type": "group",
                            "target_id": uber_line["group_key"],
                            "operation": "set_include",
                            "payload": {"included": False},
                        }
                    ]
                },
            )
            assert exclude_visible.status_code == 200, exclude_visible.text
            recompute_again = await client.post(f"/api/v1/control-center/budget/imports/{import_id}/recompute")
            assert recompute_again.status_code == 200, recompute_again.text

            final_lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            final_uber = next(item for item in final_lines["items"] if item["group_label"] == "UBER")
            assert final_uber["bucket_assignment"] == "one_off_exceptional"
            assert final_uber["final_bucket"] == "one_off_spending"
            assert final_uber["included"] is False
            assert Decimal(str(final_uber["observed_amount"])) == Decimal("25.99")
            assert Decimal(str(final_lines["budget_model"]["observed_one_off_total"])) == Decimal("0.00")
            assert Decimal(str(final_lines["budget_model"]["variable_discretionary_monthly"])) == Decimal("10.00")
            assert Decimal(str(final_lines["budget_model"]["observed_net"])) == Decimal("5304.81")
    finally:
        await engine.dispose()



@pytest.mark.asyncio
async def test_budget_v2_pdf_import_group_save_line_override_updates_amount_and_bucket() -> None:
    engine = await _make_engine()
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ctx = await _seed_org(session_maker)
    app = _build_test_app(session_maker, ctx)
    pdf_bytes = _make_pdf(FULL_WINDOW_NAB_LINES)

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            imported = await client.post(
                "/api/v1/control-center/budget/imports",
                files={"file": ("nab-full-window.pdf", pdf_bytes, "application/pdf")},
            )
            assert imported.status_code == 200, imported.text
            import_id = imported.json()["import_id"]

            initial_lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            myki_line = next(item for item in initial_lines["items"] if item["group_label"] == "MYKI")

            save_line = await client.patch(
                f"/api/v1/control-center/budget/imports/{import_id}/overrides",
                json={
                    "operations": [
                        {
                            "target_type": "group",
                            "target_id": myki_line["group_key"],
                            "operation": "set_bucket_assignment",
                            "payload": {"bucket_assignment": "recurring_baseline"},
                        },
                        {
                            "target_type": "group",
                            "target_id": myki_line["group_key"],
                            "operation": "set_base_amount_period",
                            "payload": {"base_amount": 60, "base_period": "monthly"},
                        },
                    ]
                },
            )
            assert save_line.status_code == 200, save_line.text

            recompute = await client.post(f"/api/v1/control-center/budget/imports/{import_id}/recompute")
            assert recompute.status_code == 200, recompute.text

            updated_lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            updated_myki = next(item for item in updated_lines["items"] if item["group_label"] == "MYKI")
            assert updated_myki["bucket_assignment"] == "recurring_baseline"
            assert Decimal(str(updated_myki["base_amount"])) == Decimal("60.00")
            assert Decimal(str(updated_myki["normalized_monthly"])) == Decimal("60.00")
    finally:
        await engine.dispose()

ANZ_MULTILINE_LINES = [
    "ANZ ACCESS ADVANTAGE STATEMENT",
    "Statement starts 21 July 2025",
    "Statement ends 22 July 2025",
    "Transaction Details",
    "Date Transaction Details Withdrawals ($) Deposits ($) Balance ($)",
    "2025",
    "21 JUL EFTPOS",
    "PAYPAL *APPLE.COM/BILL SYDNEY AU",
    "EFFECTIVE DATE 19 JUL 2025",
    "36,500.00 IDR INC O/S FEE $0.10",
    "25.99",
    "22 JUL PAYMENT",
    "TO HASHINI SATHARAS TRANSFER",
    "120.00",
    "TOTAL FEES",
]


@pytest.mark.asyncio
async def test_budget_v2_pdf_import_populates_views_for_anz_multiline_statement() -> None:
    engine = await _make_engine()
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ctx = await _seed_org(session_maker)
    app = _build_test_app(session_maker, ctx)
    pdf_bytes = _make_pdf(ANZ_MULTILINE_LINES)

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            imported = await client.post(
                "/api/v1/control-center/budget/imports",
                files={"file": ("anz-multiline.pdf", pdf_bytes, "application/pdf")},
            )
            assert imported.status_code == 200, imported.text
            payload = imported.json()
            import_id = payload["import_id"]
            assert payload["parser_name"] == "anz_pdf_v2"
            assert payload["transaction_count"] == 2
            assert payload["parser_confidence"] > 0

            summary = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}")).json()
            lines = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/lines")).json()
            transactions = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/transactions")).json()
            review = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/needs-review")).json()
            snapshot = (await client.get(f"/api/v1/control-center/budget/imports/{import_id}/snapshot")).json()

            assert summary["statement_start_date"] == "2025-07-21"
            assert summary["statement_end_date"] == "2025-07-22"
            assert _decimal(summary, "parsed_debit_total") == Decimal("145.99")
            assert _decimal(summary, "parsed_credit_total") == Decimal("0.00")
            assert summary["parsed_debit_count"] == 2
            assert summary["parsed_credit_count"] == 0
            assert summary["transaction_count"] == 2
            assert summary["source_bank"] == "ANZ"
            assert summary["statement_truth"]["reconciliation_status"] == "unknown"
            assert summary["trust"]["modeling_allowed"] is False
            assert summary["budget_model"]["modeling_allowed"] is False

            assert transactions["total"] == 2
            assert review["total"] == 1
            assert any("PAYPAL" in item["raw_description"].upper() for item in transactions["items"])
            assert any("HASHINI" in item["raw_description"].upper() for item in transactions["items"])
            assert all(not item["raw_description"].upper().startswith(("DD ", "DC ", "BP ")) for item in transactions["items"])
            assert all(item["merchant_fingerprint"] for item in transactions["items"])
            assert all(item["signed_amount"] != "0" for item in transactions["items"])

            assert len(lines["items"]) == 2
            assert all(Decimal(str(item["observed_amount"])) > Decimal("0") for item in lines["items"])
            assert all(item["group_key"] for item in lines["items"])
            assert {item["movement_type"] for item in lines["items"]} == {"fee", "expense"}
            assert {item["final_bucket"] for item in lines["items"]} == {"fees", "variable_spending"}
            assert lines["statement_truth"] == summary["statement_truth"]
            assert lines["budget_model"] == summary["budget_model"]
            assert snapshot["statement_truth"] == summary["statement_truth"]
            assert snapshot["budget_model"] == summary["budget_model"]
    finally:
        await engine.dispose()
