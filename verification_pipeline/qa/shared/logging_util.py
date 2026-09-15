"""Structured logging for per-topic QA runs.

Each topic has its own log file at qa/qa_<topic>/run.log. Lines are:
  ISO-timestamp | stage | message | k1=v1 k2=v2 ...

Required events (see PLAN.md §4.3):
  - Stage start and end (with timestamps + duration)
  - Row counts at each stage boundary (input -> output)
  - Subagent runs: chunk count, parallelism, model, duration, success/failure
  - Audit summary: total flags by category, A13 hits with disagreement rates
  - File-modified list for the run
"""

from __future__ import annotations

import datetime
from pathlib import Path


def _qa_root() -> Path:
    """Return the qa/ directory by walking up from this file's location."""
    return Path(__file__).resolve().parent.parent


def log_event(topic: str, stage: str, message: str, **fields) -> None:
    """Append one structured line to qa/qa_<topic>/run.log.

    Creates the parent directory if missing. Safe to call before any per-topic
    folder exists.

    Args:
        topic: short topic name (e.g. "overtime")
        stage: stage identifier (e.g. "stage_2", "phase_0_5")
        message: free-text human-readable description
        **fields: arbitrary key=value pairs appended after message
    """
    log_dir = _qa_root() / f"qa_{topic}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "run.log"

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    line = f"{ts} | {stage} | {message}"
    if kv:
        line += f" | {kv}"
    line += "\n"

    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)
