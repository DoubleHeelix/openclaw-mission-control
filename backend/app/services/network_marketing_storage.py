"""Filesystem helpers for Network Marketing member artifacts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

BASE_MEMBERS_DIR = Path.home() / ".openclaw" / "network-marketing" / "members"


@dataclass(slots=True)
class MemberFolderPaths:
    """Resolved deterministic member folder paths."""

    member_dir: Path
    files_dir: Path
    notes_dir: Path
    history_dir: Path
    folder_key: str


def _slugify(value: str) -> str:
    candidate = value.strip().lower()
    candidate = re.sub(r"[^a-z0-9]+", "-", candidate)
    candidate = candidate.strip("-")
    return candidate or "member"


def _folder_key(display_name: str, member_id: UUID) -> str:
    slug = _slugify(display_name)
    return f"{slug}-{str(member_id)[:8]}"


def ensure_member_folder(*, organization_id: UUID, member_id: UUID, display_name: str) -> MemberFolderPaths:
    """Create and return deterministic per-member folder structure.

    Folder naming is stable and collision-safe: `<slug>-<member_id_prefix>`.
    """

    folder_key = _folder_key(display_name, member_id)
    member_dir = BASE_MEMBERS_DIR / str(organization_id) / folder_key
    files_dir = member_dir / "files"
    notes_dir = member_dir / "notes"
    history_dir = member_dir / "history"

    files_dir.mkdir(parents=True, exist_ok=True)
    notes_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)

    return MemberFolderPaths(
        member_dir=member_dir,
        files_dir=files_dir,
        notes_dir=notes_dir,
        history_dir=history_dir,
        folder_key=folder_key,
    )
