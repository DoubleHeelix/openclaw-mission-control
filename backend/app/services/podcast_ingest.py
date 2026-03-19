"""Helpers for validating and storing uploaded podcast audio files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re

from fastapi import HTTPException, UploadFile, status

_ALLOWED_SUFFIXES = {".mp3", ".m4a"}
_FILENAME_CLEAN_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(slots=True)
class StoredPodcastAudio:
    """Metadata for a persisted uploaded podcast audio file."""

    source_path: str
    original_name: str
    stored_name: str
    size_bytes: int
    content_type: str | None
    extension: str


async def store_podcast_audio_upload(
    upload: UploadFile,
    *,
    inbox_dir: Path | None = None,
) -> StoredPodcastAudio:
    """Validate supported extensions and persist upload to local inbox directory."""
    original_name = (upload.filename or "audio").strip() or "audio"
    extension = Path(original_name).suffix.lower()
    if extension not in _ALLOWED_SUFFIXES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported file type. Only .mp3 and .m4a are allowed.",
        )

    target_inbox = inbox_dir or (Path.home() / ".openclaw" / "podcasts" / "inbox")
    target_inbox.mkdir(parents=True, exist_ok=True)

    safe_original = _sanitize_filename(original_name)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    stored_name = f"{timestamp}_{safe_original}"
    target_path = target_inbox / stored_name

    total = 0
    with target_path.open("wb") as out:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)

    await upload.close()

    return StoredPodcastAudio(
        source_path=str(target_path),
        original_name=original_name,
        stored_name=stored_name,
        size_bytes=total,
        content_type=upload.content_type,
        extension=extension,
    )


def _sanitize_filename(filename: str) -> str:
    """Normalize filename while retaining extension-like readability."""
    cleaned = _FILENAME_CLEAN_RE.sub("_", filename).strip("._")
    return cleaned or "audio"
