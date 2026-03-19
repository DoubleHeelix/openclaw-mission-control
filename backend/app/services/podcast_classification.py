"""Podcast category classification helpers."""

from __future__ import annotations

from pathlib import Path

PODCAST_CATEGORIES = {
    "motivational",
    "teaching",
    "self-confidence-mindset",
    "general",
    "habits-productivity",
}

_CATEGORY_KEYWORDS: dict[str, set[str]] = {
    "motivational": {
        "motivation",
        "motivational",
        "inspire",
        "inspiration",
        "purpose",
        "drive",
    },
    "teaching": {
        "teaching",
        "teach",
        "lesson",
        "tutorial",
        "how to",
        "framework",
        "coaching",
    },
    "self-confidence-mindset": {
        "confidence",
        "mindset",
        "self belief",
        "self-belief",
        "inner critic",
        "self esteem",
        "resilience",
    },
    "habits-productivity": {
        "habit",
        "routine",
        "productivity",
        "focus",
        "time management",
        "deep work",
        "systems",
    },
}


def classify_podcast_category(*, title: str, summary: str | None, transcript_text: str | None) -> str:
    """Return one target category for a completed podcast artifact set."""
    haystack = "\n".join([title or "", summary or "", transcript_text or ""]).lower()
    if not haystack.strip():
        return "general"

    scores: dict[str, int] = {category: 0 for category in PODCAST_CATEGORIES}
    for category, keywords in _CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in haystack:
                scores[category] += 1

    best_category = max(scores, key=scores.get)
    if scores[best_category] == 0:
        return "general"
    return best_category


def load_text_if_exists(path: str | None) -> str | None:
    """Read UTF-8-ish text content from path when available."""
    if not path:
        return None
    candidate = Path(path)
    if not candidate.exists() or not candidate.is_file():
        return None
    try:
        return candidate.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return candidate.read_text(encoding="utf-8", errors="ignore")
