"""Podcast transcript summarization and summary artifact persistence."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
from uuid import UUID

from app.core.time import utcnow
from app.db import crud
from app.models.control_center import ControlCenterRecord
from app.services.podcast_classification import load_text_if_exists
from app.services.podcast_paths import podcast_summaries_dir

SUMMARIES_DIR = podcast_summaries_dir()

_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "i",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "their",
    "there",
    "they",
    "this",
    "to",
    "was",
    "we",
    "were",
    "what",
    "with",
    "you",
    "your",
}

_ACTION_CUE_RE = re.compile(
    r"\b(next step|action|todo|follow up|follow-up|we will|should|need to|must|owner|by\s+\w+)\b",
    re.IGNORECASE,
)
_DECISION_CUE_RE = re.compile(r"\b(decision|decide|agreed|chosen|we will)\b", re.IGNORECASE)
_RISK_CUE_RE = re.compile(r"\b(risk|blocker|issue|concern|dependency)\b", re.IGNORECASE)


def _safe_filename(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value.strip())
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "summary"


def _normalize_line(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    return normalized.strip("-•* ")


def _split_sentences(text: str) -> list[str]:
    if not text.strip():
        return []
    raw = re.split(r"(?<=[.!?])\s+", text.replace("\n", " "))
    out: list[str] = []
    for sentence in raw:
        cleaned = _normalize_line(sentence)
        if len(cleaned) < 30:
            continue
        out.append(cleaned)
    return out


def _top_keywords(text: str, limit: int = 6) -> list[str]:
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9'-]{2,}", text.lower())
    counts = Counter(word for word in words if word not in _STOPWORDS)
    return [word for word, _ in counts.most_common(limit)]


def _score_sentence(sentence: str, keywords: list[str]) -> int:
    lowered = sentence.lower()
    score = sum(1 for kw in keywords if kw in lowered)
    if _DECISION_CUE_RE.search(sentence):
        score += 2
    if _RISK_CUE_RE.search(sentence):
        score += 1
    if _ACTION_CUE_RE.search(sentence):
        score += 1
    return score


def _compact_action_text(text: str) -> str:
    compact = _normalize_line(text)
    compact = re.sub(r"^(next step|action|todo|follow up|follow-up)\s*[:\-]\s*", "", compact, flags=re.IGNORECASE)
    compact = compact.rstrip(".;")
    return compact


def _extract_top_sentences(sentences: list[str], keywords: list[str], limit: int = 4) -> list[str]:
    scored = sorted(
        ((idx, _score_sentence(sentence, keywords), sentence) for idx, sentence in enumerate(sentences)),
        key=lambda item: (item[1], -item[0], len(item[2])),
        reverse=True,
    )
    picked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for idx, _score, sentence in scored:
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        picked.append((idx, sentence))
        if len(picked) >= limit:
            break
    picked.sort(key=lambda item: item[0])
    return [sentence for _, sentence in picked]


def _extract_tagged_items(sentences: list[str], cue_re: re.Pattern[str], limit: int = 5) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for sentence in sentences:
        if not cue_re.search(sentence):
            continue
        compact = _compact_action_text(sentence)
        if len(compact) < 12:
            continue
        key = compact.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(compact)
        if len(items) >= limit:
            break
    return items


def _extract_action_plan(sentences: list[str], limit: int = 6) -> list[str]:
    verbs = re.compile(r"\b(schedule|create|publish|send|review|update|follow|measure|implement|draft|share|build|test|fix|document)\b", re.IGNORECASE)
    actions: list[str] = []
    seen: set[str] = set()
    for sentence in sentences:
        if not (_ACTION_CUE_RE.search(sentence) or verbs.search(sentence)):
            continue
        compact = _compact_action_text(sentence)
        if len(compact) < 15:
            continue
        key = compact.lower()
        if key in seen:
            continue
        seen.add(key)
        actions.append(compact)
        if len(actions) >= limit:
            break
    return actions


def _bullet_section(items: list[str], fallback: str) -> str:
    return "\n".join(f"- {item}" for item in items) if items else f"- {fallback}"


def _build_summary_text(*, title: str, transcript_text: str) -> str:
    sentences = _split_sentences(transcript_text)
    if not sentences:
        sentences = ["Transcript had no readable content to summarize."]

    corpus = " ".join(sentences)
    keywords = _top_keywords(corpus, limit=6)
    top_sentences = _extract_top_sentences(sentences, keywords, limit=4)

    if keywords:
        topic_line = f"This episode primarily covers: {', '.join(keywords[:4])}."
    else:
        topic_line = "This episode focuses on core updates, decisions, and follow-up actions."

    executive_lines = [topic_line, *top_sentences[:3]]
    executive_summary = " ".join(executive_lines)

    decisions = _extract_tagged_items(sentences, _DECISION_CUE_RE, limit=5)
    risks = _extract_tagged_items(sentences, _RISK_CUE_RE, limit=5)
    action_plan = _extract_action_plan(sentences, limit=6)
    key_points = top_sentences[:5] if top_sentences else sentences[:5]

    key_points_block = _bullet_section(key_points, "No key points extracted.")
    decisions_block = _bullet_section(decisions, "No explicit decisions detected.")
    risks_block = _bullet_section(risks, "No explicit risks detected.")
    action_plan_block = _bullet_section(action_plan, "No explicit action items detected.")

    return (
        f"Podcast Summary\n"
        f"Title: {title}\n"
        f"Generated: {utcnow().isoformat()}\n\n"
        "Executive Summary\n"
        f"{executive_summary}\n\n"
        "Key Points\n"
        f"{key_points_block}\n\n"
        "Decisions\n"
        f"{decisions_block}\n\n"
        "Risks\n"
        f"{risks_block}\n\n"
        "Action Plan\n"
        f"{action_plan_block}\n"
    )


async def process_summary_for_record(*, session, record_id: UUID) -> dict[str, object]:
    """Generate summary text artifact from transcript and persist record state."""
    record = await ControlCenterRecord.objects.by_id(record_id).first(session)
    if record is None:
        raise ValueError("Control-center record not found")

    existing_data = dict(record.data or {})
    transcript_path = (
        str(existing_data.get("transcript_path"))
        if isinstance(existing_data.get("transcript_path"), str)
        else None
    )
    transcript_text = load_text_if_exists(transcript_path)
    if not transcript_text:
        return {
            "summary_status": "failed",
            "error": "Transcript artifact not found. Run transcription before summary generation.",
        }

    processing_data = {
        **existing_data,
        "summary_status": "processing",
        "summary_error": None,
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
        SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
        stem = _safe_filename(Path(transcript_path).stem if transcript_path else record.title)
        timestamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
        summary_path = SUMMARIES_DIR / f"{record_id}-{stem}-{timestamp}.txt"

        summary_text = _build_summary_text(title=record.title, transcript_text=transcript_text)
        summary_path.write_text(summary_text, encoding="utf-8")

        completed_data = {
            **processing_data,
            "summary_status": "completed",
            "summary_path": str(summary_path),
            "summary_error": None,
            "summary_generated_at": utcnow().isoformat(),
            "summary_format": {
                "version": 2,
                "sections": ["executive_summary", "key_points", "decisions", "risks", "action_plan"],
            },
        }
        await crud.patch(
            session,
            record,
            {
                "summary": summary_text[:900],
                "data": completed_data,
                "updated_at": utcnow(),
            },
        )
        return {
            "summary_status": "completed",
            "summary_path": str(summary_path),
        }
    except Exception as exc:
        failed_data = {
            **processing_data,
            "summary_status": "failed",
            "summary_error": str(exc),
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
            "summary_status": "failed",
            "error": str(exc),
        }
