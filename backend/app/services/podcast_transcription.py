"""Podcast transcription flow and transcript artifact persistence."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from uuid import UUID

from app.core.logging import get_logger
from app.core.config import settings
from app.core.time import utcnow
from app.db import crud
from app.models.control_center import ControlCenterRecord
from app.services.podcast_paths import podcast_transcripts_dir

TRANSCRIPTS_DIR = podcast_transcripts_dir()
logger = get_logger(__name__)


def _safe_filename(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value.strip())
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "audio"


def _extract_word_timestamps(json_payload: dict[str, object]) -> list[dict[str, object]]:
    segments = json_payload.get("segments")
    if not isinstance(segments, list):
        return []
    words: list[dict[str, object]] = []
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        segment_words = segment.get("words")
        if not isinstance(segment_words, list):
            continue
        for item in segment_words:
            if not isinstance(item, dict):
                continue
            text = str(item.get("word") or item.get("text") or "").strip()
            start = item.get("start")
            end = item.get("end")
            if not text:
                continue
            try:
                start_f = float(start)
                end_f = float(end)
            except (TypeError, ValueError):
                continue
            if end_f <= start_f:
                continue
            words.append(
                {
                    "start": round(start_f, 3),
                    "end": round(end_f, 3),
                    "text": text,
                }
            )
    return words


def _transcribe_with_whisper_cli(audio_path: str) -> tuple[str, str | None, list[dict[str, object]]]:
    """Run local whisper CLI and return transcript text, optional VTT text, and word timestamps."""
    whisper_bin = shutil.which("whisper")
    if whisper_bin is None:
        raise RuntimeError("Whisper CLI not found. Install it and ensure `whisper` is on PATH.")

    source = Path(audio_path)
    if not source.exists():
        raise RuntimeError(f"Audio file not found: {audio_path}")

    model = str(getattr(settings, "podcast_whisper_model", "medium") or "medium").strip() or "medium"
    language = str(getattr(settings, "podcast_whisper_language", "en") or "").strip()

    with tempfile.TemporaryDirectory(prefix="podcast-whisper-") as temp_dir:
        output_dir = Path(temp_dir)
        cmd = [
            whisper_bin,
            str(source),
            "--model",
            model,
            "--output_format",
            "all",
            "--word_timestamps",
            "True",
            "--output_dir",
            str(output_dir),
            "--fp16",
            "False",
        ]
        if language:
            cmd.extend(["--language", language])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"Whisper transcription failed (exit {result.returncode}): {detail}")

        txt_path = output_dir / f"{source.stem}.txt"
        if not txt_path.exists():
            raise RuntimeError("Whisper finished without producing transcript text output.")

        transcript_text = txt_path.read_text(encoding="utf-8").strip()
        if not transcript_text:
            raise RuntimeError("Whisper produced an empty transcript.")
        vtt_path = output_dir / f"{source.stem}.vtt"
        vtt_text: str | None = None
        if vtt_path.exists():
            parsed_vtt = vtt_path.read_text(encoding="utf-8").strip()
            vtt_text = parsed_vtt or None
        words_path = output_dir / f"{source.stem}.json"
        transcript_words: list[dict[str, object]] = []
        if words_path.exists():
            try:
                payload = json.loads(words_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    transcript_words = _extract_word_timestamps(payload)
            except Exception:
                transcript_words = []
        return transcript_text, vtt_text, transcript_words


async def save_uploaded_audio(record_id: UUID, filename: str, audio_bytes: bytes) -> Path:
    """Persist uploaded audio to the transcripts workspace."""
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    stem = _safe_filename(Path(filename).stem)
    suffix = Path(filename).suffix or ".bin"
    timestamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    audio_path = TRANSCRIPTS_DIR / f"{record_id}-{stem}-{timestamp}{suffix}"
    audio_path.write_bytes(audio_bytes)
    return audio_path


async def process_transcription_for_record(
    *,
    session,
    record_id: UUID,
    audio_path: str,
) -> dict[str, object]:
    """Entry-point: update record status and write transcript artifact for an audio file."""
    record = await ControlCenterRecord.objects.by_id(record_id).first(session)
    if record is None:
        raise ValueError("Control-center record not found")

    existing_data = dict(record.data or {})
    notes = str(existing_data.get("notes") or "")

    processing_data = {
        **existing_data,
        "audio_path": audio_path,
        "transcript_status": "processing",
        "transcript_error": None,
    }
    await crud.patch(
        session,
        record,
        {
            "data": processing_data,
            "updated_at": utcnow(),
        },
    )

    try:
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        source_path = Path(audio_path)
        artifact_stem = _safe_filename(source_path.stem)
        timestamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
        transcript_path = TRANSCRIPTS_DIR / f"{record_id}-{artifact_stem}-{timestamp}.txt"

        transcribe_result = await asyncio.to_thread(_transcribe_with_whisper_cli, audio_path)
        if isinstance(transcribe_result, tuple):
            if len(transcribe_result) == 3:
                transcript_text, transcript_vtt_text, transcript_words = transcribe_result
            elif len(transcribe_result) == 2:
                transcript_text, transcript_vtt_text = transcribe_result
                transcript_words = []
            elif len(transcribe_result) == 1:
                transcript_text = str(transcribe_result[0])
                transcript_vtt_text = None
                transcript_words = []
            else:
                raise RuntimeError("Unexpected transcription return shape")
        else:
            # Backwards compatibility for tests/mocks returning transcript text only.
            transcript_text = str(transcribe_result)
            transcript_vtt_text = None
            transcript_words = []

        transcript_path.write_text(transcript_text, encoding="utf-8")
        transcript_vtt_path: Path | None = None
        if transcript_vtt_text:
            transcript_vtt_path = TRANSCRIPTS_DIR / f"{record_id}-{artifact_stem}-{timestamp}.vtt"
            transcript_vtt_path.write_text(transcript_vtt_text, encoding="utf-8")
        transcript_words_path: Path | None = None
        if transcript_words:
            transcript_words_path = TRANSCRIPTS_DIR / f"{record_id}-{artifact_stem}-{timestamp}.words.json"
            transcript_words_path.write_text(json.dumps(transcript_words, ensure_ascii=False), encoding="utf-8")

        completed_data = {
            **processing_data,
            "transcript_status": "completed",
            "transcript_path": str(transcript_path),
            "transcript_error": None,
        }
        if transcript_vtt_path is not None:
            completed_data["transcript_vtt_path"] = str(transcript_vtt_path)
        else:
            completed_data.pop("transcript_vtt_path", None)
        if transcript_words_path is not None:
            completed_data["transcript_words_path"] = str(transcript_words_path)
        else:
            completed_data.pop("transcript_words_path", None)
        await crud.patch(
            session,
            record,
            {
                "data": completed_data,
                "updated_at": utcnow(),
            },
        )
        return {
            "transcript_status": "completed",
            "transcript_path": str(transcript_path),
            "transcript_vtt_path": str(transcript_vtt_path) if transcript_vtt_path is not None else None,
            "transcript_words_path": str(transcript_words_path) if transcript_words_path is not None else None,
            "audio_path": audio_path,
        }
    except BaseException as exc:
        # Important: client disconnects/timeouts can raise cancellation exceptions
        # (e.g. asyncio.CancelledError), which do not inherit from Exception.
        # If we do not catch those, records can remain stuck in `processing` forever.
        logger.exception("podcast.transcription.failed record_id=%s error=%s", record_id, exc)
        failed_notes = notes.strip()
        failed_notes = (
            f"{failed_notes}\nTranscription failure: {exc}" if failed_notes else f"Transcription failure: {exc}"
        )
        failed_data = {
            **processing_data,
            "transcript_status": "failed",
            "transcript_error": str(exc),
            "notes": failed_notes,
        }
        await crud.patch(
            session,
            record,
            {
                "data": failed_data,
                "updated_at": utcnow(),
            },
        )
        return {
            "transcript_status": "failed",
            "audio_path": audio_path,
            "error": str(exc),
        }


async def ensure_transcript_vtt_for_record(*, session, record: ControlCenterRecord) -> str | None:
    """Backfill VTT timestamps for older records that only have transcript text."""
    data = dict(record.data or {})
    existing_vtt_path = str(data.get("transcript_vtt_path") or "").strip()
    if existing_vtt_path and Path(existing_vtt_path).exists():
        return existing_vtt_path

    audio_path = (
        str(data.get("audio_path") or "").strip()
        or str(data.get("completed_audio_path") or "").strip()
        or str(data.get("source_path") or "").strip()
    )
    if not audio_path or not Path(audio_path).exists():
        return None

    try:
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        source_path = Path(audio_path)
        artifact_stem = _safe_filename(source_path.stem)
        timestamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
        transcript_text, transcript_vtt_text, transcript_words = await asyncio.to_thread(
            _transcribe_with_whisper_cli, audio_path
        )
        if not transcript_vtt_text:
            return None

        transcript_vtt_path = TRANSCRIPTS_DIR / f"{record.id}-{artifact_stem}-{timestamp}.vtt"
        transcript_vtt_path.write_text(transcript_vtt_text, encoding="utf-8")
        transcript_words_path: Path | None = None
        if transcript_words:
            transcript_words_path = TRANSCRIPTS_DIR / f"{record.id}-{artifact_stem}-{timestamp}.words.json"
            transcript_words_path.write_text(json.dumps(transcript_words, ensure_ascii=False), encoding="utf-8")

        # Keep existing transcript text untouched unless path is missing.
        if not str(data.get("transcript_path") or "").strip():
            transcript_path = TRANSCRIPTS_DIR / f"{record.id}-{artifact_stem}-{timestamp}.txt"
            transcript_path.write_text(transcript_text, encoding="utf-8")
            data["transcript_path"] = str(transcript_path)

        data["transcript_vtt_path"] = str(transcript_vtt_path)
        if transcript_words_path is not None:
            data["transcript_words_path"] = str(transcript_words_path)
        await crud.patch(
            session,
            record,
            {
                "data": data,
                "updated_at": utcnow(),
            },
        )
        return str(transcript_vtt_path)
    except Exception as exc:
        logger.warning("podcast.vtt.backfill.failed record_id=%s error=%s", record.id, exc)
        return None
