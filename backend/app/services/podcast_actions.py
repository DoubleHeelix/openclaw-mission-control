"""Podcast action extraction + same-record dedupe helpers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re

_ACTION_LINE_RE = re.compile(r"^\s*(?:[-*]|\d+[\.)])\s+(?P<text>.+?)\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
_ACTION_CUE_RE = re.compile(
    r"\b(next step|action|todo|follow up|follow-up|we will|should|need to|must|owner|by\s+\w+)\b",
    re.IGNORECASE,
)
_IMPERATIVE_VERB_RE = re.compile(
    r"\b(schedule|create|publish|send|review|update|follow|measure|implement|draft|share|build|test|fix|document|assign|plan)\b",
    re.IGNORECASE,
)


@dataclass(slots=True)
class ExtractedPodcastAction:
    text: str
    action_hash: str


def extract_actions(transcript_text: str | None, summary_text: str | None) -> list[ExtractedPodcastAction]:
    """Extract concrete, deduplicated action items from transcript/summary text."""
    candidates = _collect_candidates(transcript_text=transcript_text, summary_text=summary_text)

    normalized: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        text = _normalize_action(item)
        if len(text) < 12:
            continue
        key = _normalize_text(text)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)

    return [ExtractedPodcastAction(text=t, action_hash=hash_action(t)) for t in normalized]


def _collect_candidates(*, transcript_text: str | None, summary_text: str | None) -> list[str]:
    candidates: list[str] = []

    for blob in (summary_text or "", transcript_text or ""):
        if not blob.strip():
            continue

        # Prefer explicit action-plan style bullets when present.
        for line in blob.splitlines():
            line = line.strip()
            if not line:
                continue
            match = _ACTION_LINE_RE.match(line)
            if match:
                candidate = match.group("text").strip()
                if _looks_actionable(candidate):
                    candidates.append(candidate)
                    continue
            if _looks_actionable(line):
                candidates.append(line)

        # Fallback to actionable sentences.
        for sentence in _SENTENCE_SPLIT_RE.split(blob):
            sentence = sentence.strip()
            if not sentence:
                continue
            if _looks_actionable(sentence):
                candidates.append(sentence)

    return candidates


def _looks_actionable(text: str) -> bool:
    lowered = text.lower()
    if lowered.startswith(("podcast summary", "title:", "generated:", "executive summary", "key points", "decisions", "risks", "action plan")):
        return False
    if _ACTION_CUE_RE.search(text):
        return True
    return _IMPERATIVE_VERB_RE.search(text) is not None


def _normalize_action(text: str) -> str:
    cleaned = text.strip().strip("-•* ")
    cleaned = re.sub(r"^(next step|action|todo|follow up|follow-up)\s*[:\-]\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(".; ")
    return cleaned


def hash_action(action_text: str) -> str:
    return hashlib.sha256(_normalize_text(action_text).encode("utf-8")).hexdigest()


def filter_new_actions(actions: list[ExtractedPodcastAction], existing_hashes: set[str]) -> tuple[list[ExtractedPodcastAction], list[ExtractedPodcastAction]]:
    new_items: list[ExtractedPodcastAction] = []
    skipped: list[ExtractedPodcastAction] = []
    for action in actions:
        if action.action_hash in existing_hashes:
            skipped.append(action)
            continue
        existing_hashes.add(action.action_hash)
        new_items.append(action)
    return new_items, skipped


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).lower()
