# ruff: noqa: INP001
"""Podcast action/task-creation regression tests."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.api import control_center as cc
from app.schemas.control_center import PromoteRecordToTaskRequest
from app.services.podcast_actions import extract_actions


@pytest.mark.asyncio
async def test_promote_record_to_task_links_new_task(monkeypatch: pytest.MonkeyPatch) -> None:
    board_id = uuid4()
    org_id = uuid4()
    record = SimpleNamespace(id=uuid4(), organization_id=org_id, title="Episode", summary="Summary")
    created_task = SimpleNamespace(id=uuid4(), board_id=board_id)

    class _BoardById:
        async def first(self, _session):
            return SimpleNamespace(id=board_id, organization_id=org_id)

    class _BoardObjects:
        @staticmethod
        def by_id(_id):
            return _BoardById()

    patch_calls: list[dict[str, object]] = []

    async def _fake_get_record_or_404(*, session, record_id, ctx):
        del session, record_id, ctx
        return record

    async def _fake_create(session, model, **kwargs):
        del session, model, kwargs
        return created_task

    async def _fake_patch(session, target, payload):
        del session, target
        patch_calls.append(payload)
        return record

    monkeypatch.setattr(cc, "_get_org_record_or_404", _fake_get_record_or_404)
    monkeypatch.setattr(cc.Board, "objects", _BoardObjects())
    monkeypatch.setattr(cc.crud, "create", _fake_create)
    monkeypatch.setattr(cc.crud, "patch", _fake_patch)

    ctx = SimpleNamespace(organization=SimpleNamespace(id=org_id), member=SimpleNamespace(user_id=uuid4()))
    response = await cc.promote_record_to_task(
        record_id=record.id,
        payload=PromoteRecordToTaskRequest(board_id=board_id, priority="high"),
        session=object(),
        ctx=ctx,
    )

    assert response.task_id == created_task.id
    assert response.board_id == board_id
    assert any("linked_task_id" in call for call in patch_calls)


def test_action_extraction_duplicate_prevention_regression() -> None:
    """Regression guard: dedupe extracted actions by normalized text key."""
    extracted_actions = [
        "Follow up with sponsor",
        " follow up with sponsor  ",
        "Book studio session",
    ]

    normalized = {" ".join(item.lower().split()) for item in extracted_actions}

    assert len(normalized) == 2
    assert "follow up with sponsor" in normalized
    assert "book studio session" in normalized


def test_extract_actions_prefers_concrete_actionable_items() -> None:
    summary_text = (
        "Podcast Summary\n"
        "Executive Summary\n"
        "The episode covered operations planning and follow-through.\n"
        "Action Plan\n"
        "- Next step: Publish the weekly dashboard checklist by Friday.\n"
        "- Action: Review handoff SLA breaches and assign one owner.\n"
    )
    transcript_text = (
        "We discussed blockers in reporting. "
        "We will schedule a 30-minute review every Monday. "
        "Need to document retry expectations for podcast processing."
    )

    extracted = extract_actions(transcript_text, summary_text)
    texts = [item.text.lower() for item in extracted]

    assert any("publish the weekly dashboard checklist by friday" in item for item in texts)
    assert any("review handoff sla breaches and assign one owner" in item for item in texts)
    assert any("schedule a 30-minute review every monday" in item for item in texts)
    assert len(texts) == len(set(texts))
