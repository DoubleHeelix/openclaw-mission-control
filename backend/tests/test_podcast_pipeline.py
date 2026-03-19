# ruff: noqa: INP001
"""Podcast pipeline tests for ingest/transcribe/classify/route behavior."""

from __future__ import annotations

from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException, UploadFile

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

    monkeypatch.setattr(
        "app.services.podcast_storage.podcast_record_folder",
        lambda *, category, record_id, title: tmp_path / "completed" / category / f"{record_id}-{title or 'untitled'}",
    )

    routed = route_completed_artifacts(
        category="teaching",
        record_id=str(uuid4()),
        title="episode",
        audio_path=str(audio),
        transcript_path=str(transcript),
        summary_path=str(summary),
    )

    assert routed.category == "teaching"
    assert routed.audio_path and Path(routed.audio_path).exists()
    assert routed.transcript_path and Path(routed.transcript_path).exists()
    assert routed.summary_path and Path(routed.summary_path).exists()


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
