# ruff: noqa: INP001
"""Podcast pipeline tests for ingest/transcribe/classify/route behavior."""

from __future__ import annotations

from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.responses import FileResponse
from fastapi import HTTPException, UploadFile

from app.api.control_center import (
    _normalize_podcast_record_state,
    get_podcast_record_audio,
    get_podcast_record_view,
    run_control_center_record_pipeline,
)
from app.services import podcast_ingest, podcast_summary, podcast_transcription
from app.services.podcast_classification import classify_podcast_category
from app.services.podcast_storage import route_completed_artifacts


@pytest.mark.asyncio
async def test_ingest_rejects_unsupported_extension(tmp_path: Path) -> None:
    upload = UploadFile(filename="episode.wav", file=BytesIO(b"abc"))
    with pytest.raises(HTTPException, match="Only .mp3 and .m4a"):
        await podcast_ingest.store_podcast_audio_upload(upload, inbox_dir=tmp_path)


@pytest.mark.asyncio
async def test_ingest_stores_supported_audio(tmp_path: Path) -> None:
    upload = UploadFile(filename="episode.mp3", file=BytesIO(b"abc123"))
    stored = await podcast_ingest.store_podcast_audio_upload(upload, inbox_dir=tmp_path)

    assert stored.extension == ".mp3"
    assert stored.size_bytes == 6
    assert Path(stored.source_path).exists()


def test_classification_prefers_matching_keywords() -> None:
    category = classify_podcast_category(
        title="Deep Work Habit Systems",
        summary="Build productivity routines",
        transcript_text="This lesson teaches a focus routine and time management system.",
    )
    assert category == "habits-productivity"


def test_route_completed_artifacts_moves_all_existing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    audio = tmp_path / "sample.mp3"
    transcript = tmp_path / "sample.txt"
    summary = tmp_path / "sample.md"
    audio.write_text("a", encoding="utf-8")
    transcript.write_text("t", encoding="utf-8")
    summary.write_text("s", encoding="utf-8")

    monkeypatch.setattr("app.services.podcast_storage.BASE_COMPLETED_DIR", tmp_path / "completed")

    routed = route_completed_artifacts(
        category="teaching",
        audio_path=str(audio),
        transcript_path=str(transcript),
        summary_path=str(summary),
    )

    assert routed.category == "teaching"
    assert routed.audio_path and Path(routed.audio_path).exists()
    assert routed.transcript_path and Path(routed.transcript_path).exists()
    assert routed.summary_path and Path(routed.summary_path).exists()
    assert Path(routed.audio_path).parent == tmp_path / "completed" / "teaching"


@pytest.mark.asyncio
async def test_transcription_updates_status_flow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    record_id = uuid4()
    patches: list[dict[str, object]] = []
    record = SimpleNamespace(id=record_id, data={"notes": "existing"})

    class _ById:
        async def first(self, _session):
            return record

    class _Objects:
        @staticmethod
        def by_id(_record_id):
            return _ById()

    async def _fake_patch(_session, _record, payload):
        patches.append(payload)
        return _record

    monkeypatch.setattr("app.services.podcast_transcription.ControlCenterRecord.objects", _Objects())
    monkeypatch.setattr("app.services.podcast_transcription.crud.patch", _fake_patch)
    monkeypatch.setattr("app.services.podcast_transcription.TRANSCRIPTS_DIR", tmp_path)
    monkeypatch.setattr(
        "app.services.podcast_transcription._transcribe_with_whisper_cli",
        lambda _path: "This is a mocked transcript.",
    )

    audio_path = tmp_path / "audio.mp3"
    audio_path.write_text("audio", encoding="utf-8")

    result = await podcast_transcription.process_transcription_for_record(
        session=object(),
        record_id=record_id,
        audio_path=str(audio_path),
    )

    assert result["transcript_status"] == "completed"
    assert Path(str(result["transcript_path"])).exists()
    statuses = [p["data"]["transcript_status"] for p in patches if "data" in p]
    assert statuses[0] == "processing"
    assert statuses[-1] == "completed"


def test_summary_text_contains_required_sections() -> None:
    summary_text = podcast_summary._build_summary_text(
        title="Weekly Leadership Sync",
        transcript_text=(
            "Key update: Team cleared onboarding backlog and improved handoff speed.\n"
            "Decision: We will move all urgent escalations to a single triage owner.\n"
            "Risk: Reporting lag still depends on manual spreadsheet updates.\n"
            "Next step: Product ops owner to publish dashboard checklist by Friday.\n"
        ),
    )

    assert "Key Points" in summary_text
    assert "Decisions" in summary_text
    assert "Risks" in summary_text
    assert "Action Plan" in summary_text


@pytest.mark.asyncio
async def test_summary_processing_persists_path_and_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    record_id = uuid4()
    transcript_path = tmp_path / "episode-transcript.txt"
    transcript_path.write_text(
        "Decision: Keep async review flow.\nRisk: flaky retries in pipeline.\nNext step: add retry metrics.",
        encoding="utf-8",
    )

    record = SimpleNamespace(
        id=record_id,
        title="Episode 42",
        summary="",
        data={"transcript_path": str(transcript_path), "summary_status": "pending"},
    )
    patches: list[dict[str, object]] = []

    class _ById:
        async def first(self, _session):
            return record

    class _Objects:
        @staticmethod
        def by_id(_record_id):
            return _ById()

    async def _fake_patch(_session, _record, payload):
        patches.append(payload)
        if "data" in payload:
            _record.data = payload["data"]
        return _record

    monkeypatch.setattr("app.services.podcast_summary.ControlCenterRecord.objects", _Objects())
    monkeypatch.setattr("app.services.podcast_summary.crud.patch", _fake_patch)
    monkeypatch.setattr("app.services.podcast_summary.SUMMARIES_DIR", tmp_path / "summaries")

    result = await podcast_summary.process_summary_for_record(session=object(), record_id=record_id)

    assert result["summary_status"] == "completed"
    assert Path(str(result["summary_path"])).exists()
    statuses = [p["data"]["summary_status"] for p in patches if "data" in p]
    assert statuses[0] == "processing"
    assert statuses[-1] == "completed"
    final_data = patches[-1]["data"]
    assert isinstance(final_data, dict)
    assert str(final_data.get("summary_path", "")).endswith(".txt")
    assert final_data.get("summary_generated_at")
    assert final_data.get("summary_format")


def test_normalize_podcast_record_state_repairs_fake_completed_state(tmp_path: Path) -> None:
    record = SimpleNamespace(
        stage="completed",
        data={
            "audio_path": str(tmp_path / "episode.mp3"),
            "transcript_status": "failed",
            "summary_status": "completed",
            "task_extraction_status": "completed",
            "created_task_ids": [],
            "action_hashes": [],
            "action_points": [],
        },
        module_category="podcasts",
    )
    Path(record.data["audio_path"]).write_text("audio", encoding="utf-8")

    normalized, next_stage, changed = _normalize_podcast_record_state(record)

    assert changed is True
    assert next_stage == "uploaded"
    assert normalized["summary_status"] == "pending"
    assert normalized["task_extraction_status"] == "pending"
    assert normalized["transcript_error"] == "Transcript artifact is missing. Re-run transcription."


@pytest.mark.asyncio
async def test_get_podcast_record_view_returns_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    transcript = tmp_path / "episode.txt"
    summary = tmp_path / "episode-summary.txt"
    words = tmp_path / "episode.words.json"
    vtt = tmp_path / "episode.vtt"
    transcript.write_text("Hello from the transcript.", encoding="utf-8")
    summary.write_text(
        "Podcast Summary\n\nKey Points\n- Point one\n\nDecisions\n- Do the thing\n\nRisks\n- Missing follow-up\n",
        encoding="utf-8",
    )
    words.write_text('[{"text":"Hello","start":0.0,"end":0.3}]', encoding="utf-8")
    vtt.write_text("WEBVTT\n\n00:00.000 --> 00:00.300\nHello\n", encoding="utf-8")

    record = SimpleNamespace(
        id=uuid4(),
        title="Episode",
        summary="Stored summary",
        module_category="podcasts",
        data={
            "category": "teaching",
            "transcript_path": str(transcript),
            "summary_path": str(summary),
            "transcript_words_path": str(words),
            "transcript_vtt_path": str(vtt),
            "action_points": ["Call the lead", "Draft the outline"],
        },
    )

    async def _fake_get(**_kwargs):
        return record

    async def _fake_repair(**_kwargs):
        return record

    monkeypatch.setattr("app.api.control_center._get_org_record_or_404", _fake_get)
    monkeypatch.setattr("app.api.control_center._repair_podcast_record_if_needed", _fake_repair)

    payload = await get_podcast_record_view(
        record_id=record.id,
        session=object(),
        ctx=SimpleNamespace(),
    )

    assert payload.record_id == record.id
    assert payload.transcript_text == "Hello from the transcript."
    assert payload.summary_text is not None and "Podcast Summary" in payload.summary_text
    assert payload.action_points == ["Call the lead", "Draft the outline"]
    assert payload.key_points == ["Point one"]
    assert payload.decisions == ["Do the thing"]
    assert payload.risks == ["Missing follow-up"]
    assert payload.transcript_words[0]["text"] == "Hello"


@pytest.mark.asyncio
async def test_get_podcast_record_audio_returns_file_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    audio = tmp_path / "episode.mp3"
    audio.write_bytes(b"audio-bytes")
    record = SimpleNamespace(
        id=uuid4(),
        module_category="podcasts",
        data={"audio_path": str(audio)},
    )

    async def _fake_get(**_kwargs):
        return record

    async def _fake_repair(**_kwargs):
        return record

    monkeypatch.setattr("app.api.control_center._get_org_record_or_404", _fake_get)
    monkeypatch.setattr("app.api.control_center._repair_podcast_record_if_needed", _fake_repair)

    response = await get_podcast_record_audio(
        record_id=record.id,
        session=object(),
        ctx=SimpleNamespace(),
    )

    assert isinstance(response, FileResponse)
    assert response.path == str(audio)


@pytest.mark.asyncio
async def test_run_pipeline_route_executes_real_stages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    org_id = uuid4()
    record_id = uuid4()
    audio = tmp_path / "episode.mp3"
    transcript = tmp_path / "episode.txt"
    summary = tmp_path / "episode-summary.txt"
    audio.write_bytes(b"audio-bytes")

    record = SimpleNamespace(
        id=record_id,
        organization_id=org_id,
        title="Episode",
        summary="",
        stage="uploaded",
        module_category="podcasts",
        data={"source_path": str(audio), "ingest_status": "uploaded"},
    )

    async def _fake_get(**_kwargs):
        return record

    async def _fake_repair(**_kwargs):
        return record

    async def _fake_patch(_session, target, payload):
        if "data" in payload:
            target.data = payload["data"]
        if "stage" in payload:
            target.stage = payload["stage"]
        return target

    async def _fake_transcribe(*, session, record_id, audio_path):
        assert session is object() or session is not None
        assert record_id == record.id
        assert audio_path == str(audio)
        record.data = {
            **record.data,
            "audio_path": str(audio),
            "transcript_status": "completed",
            "transcript_path": str(transcript),
        }
        transcript.write_text("Transcript text", encoding="utf-8")
        return {
            "transcript_status": "completed",
            "transcript_path": str(transcript),
            "audio_path": str(audio),
        }

    async def _fake_summarize(*, session, record_id):
        assert record_id == record.id
        record.data = {
            **record.data,
            "summary_status": "completed",
            "summary_path": str(summary),
        }
        summary.write_text("Podcast Summary", encoding="utf-8")
        return {"summary_status": "completed", "summary_path": str(summary)}

    async def _fake_extract(*, session, record, board):
        assert board.organization_id == org_id
        record.data = {
            **record.data,
            "action_points": ["Follow up with team"],
            "created_task_ids": [str(uuid4())],
            "action_hashes": ["hash-1"],
            "task_extraction_status": "completed",
        }
        return SimpleNamespace()

    async def _fake_classify(*, session, record):
        record.data = {
            **record.data,
            "category": "teaching",
            "completed_audio_path": str(audio),
            "completed_transcript_path": str(transcript),
            "completed_summary_path": str(summary),
        }
        record.stage = "completed"
        return SimpleNamespace()

    class _ById:
        async def first(self, _session):
            return SimpleNamespace(id=uuid4(), organization_id=org_id)

    class _Objects:
        @staticmethod
        def by_id(_board_id):
            return _ById()

    monkeypatch.setattr("app.api.control_center._get_org_record_or_404", _fake_get)
    monkeypatch.setattr("app.api.control_center._repair_podcast_record_if_needed", _fake_repair)
    monkeypatch.setattr("app.api.control_center.crud.patch", _fake_patch)
    monkeypatch.setattr("app.api.control_center.process_transcription_for_record", _fake_transcribe)
    monkeypatch.setattr("app.api.control_center.process_summary_for_record", _fake_summarize)
    monkeypatch.setattr("app.api.control_center._extract_podcast_actions_for_record", _fake_extract)
    monkeypatch.setattr("app.api.control_center._classify_podcast_record", _fake_classify)
    monkeypatch.setattr("app.api.control_center.Board.objects", _Objects())

    response = await run_control_center_record_pipeline(
        record_id=record.id,
        max_retries=1,
        fail_stage=None,
        session=object(),
        ctx=SimpleNamespace(organization=SimpleNamespace(id=org_id)),
    )

    assert response.pipeline_status == "completed"
    assert response.failed_stage is None
    assert response.completed_stages == ["ingest", "transcribe", "summarize", "actions", "categorize"]
    assert response.retries == {
        "ingest": 0,
        "transcribe": 0,
        "summarize": 0,
        "actions": 0,
        "categorize": 0,
    }
