"""Lightweight pipeline state helpers for podcast workflow endpoints."""

from __future__ import annotations

from typing import Any


DEFAULT_STAGES = ["ingest", "transcribe", "summarize", "actions", "categorize"]


def merge_pipeline_state(data: dict[str, Any]) -> dict[str, Any]:
    pipeline = data.get("pipeline")
    if not isinstance(pipeline, dict):
        pipeline = {}
    pipeline.setdefault("status", "pending")
    pipeline.setdefault("completed_stages", [])
    pipeline.setdefault("retries", {stage: 0 for stage in DEFAULT_STAGES})
    pipeline.setdefault("max_retries", 1)
    pipeline.setdefault("last_error", None)
    data["pipeline"] = pipeline
    return data


def increment_retry(retries: dict[str, int], stage: str) -> int:
    current = int(retries.get(stage, 0))
    retries[stage] = current + 1
    return retries[stage]
