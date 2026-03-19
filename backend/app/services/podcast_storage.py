"""Podcast completed-artifact routing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

CATEGORY_FOLDERS = {
    "motivational",
    "teaching",
    "self-confidence-mindset",
    "general",
    "habits-productivity",
}

BASE_COMPLETED_DIR = Path.home() / ".openclaw" / "podcasts" / "completed"


@dataclass(slots=True)
class RoutedPodcastArtifacts:
    """Final destination paths for routed podcast artifacts."""

    category: str
    audio_path: str | None
    transcript_path: str | None
    summary_path: str | None


def route_completed_artifacts(
    *,
    category: str,
    audio_path: str | None,
    transcript_path: str | None,
    summary_path: str | None,
) -> RoutedPodcastArtifacts:
    """Move existing artifact files into the selected category completed folder."""
    normalized_category = category if category in CATEGORY_FOLDERS else "general"
    destination_dir = BASE_COMPLETED_DIR / normalized_category
    destination_dir.mkdir(parents=True, exist_ok=True)

    return RoutedPodcastArtifacts(
        category=normalized_category,
        audio_path=_move_if_exists(audio_path, destination_dir),
        transcript_path=_move_if_exists(transcript_path, destination_dir),
        summary_path=_move_if_exists(summary_path, destination_dir),
    )


def _move_if_exists(path: str | None, destination_dir: Path) -> str | None:
    if not path:
        return None

    source = Path(path)
    if not source.exists() or not source.is_file():
        return None

    target_name = source.name
    target = destination_dir / target_name
    if target.exists():
        target = destination_dir / f"{source.stem}-{source.stat().st_mtime_ns}{source.suffix}"

    shutil.move(str(source), str(target))
    return str(target)
