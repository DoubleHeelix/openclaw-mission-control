"""Filesystem path helpers for podcast artifacts."""

from __future__ import annotations

from pathlib import Path

from app.core.config import BACKEND_ROOT


def _podcast_root() -> Path:
    root = (BACKEND_ROOT / ".data" / "podcasts").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def podcast_transcripts_dir() -> Path:
    path = _podcast_root() / "transcripts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def podcast_summaries_dir() -> Path:
    path = _podcast_root() / "summaries"
    path.mkdir(parents=True, exist_ok=True)
    return path
