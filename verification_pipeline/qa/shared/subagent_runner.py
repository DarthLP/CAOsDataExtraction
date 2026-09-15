"""Concurrency wrapper around Claude Code's Task tool for subagent runs.

Each subagent processes one chunk JSONL and emits one chunk_NNN_corrections.csv.

PHASE 1: signature stub. Phase 2 implements; this is the only module that
actually invokes the Task tool. Wrapper concerns:
  - Max parallel verified at 12 (Phase 0.5 Step 0.5.2; recorded in CLAUDE.md).
  - Sonnet default; Opus override per-topic via qa_<topic>_aggregate.py.
  - Subagent prompts must be self-contained (cannot see parent context).
  - Subagents must NOT spawn further subagents.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional


# Verified empirically in Phase 0.5 Step 0.5.2.
MAX_PARALLEL_HARD_CAP = 12


def run_chunks(chunks: list[Path],
               system_prompt: str,
               max_parallel: int = 12,
               model: str = "sonnet",
               output_dir: Optional[Path] = None) -> list[Path]:
    """Spawn parallel subagent runs, one per chunk. Returns paths to the
    chunk_NNN_corrections.csv outputs.

    Default parallelism 12, hard cap MAX_PARALLEL_HARD_CAP. If chunks >
    max_parallel, batches sequentially across messages.

    Default model is Sonnet (cost/speed balance + handles structured CSV +
    verbatim-quote extraction reliably). Pass model='opus' only on explicit
    request for a topic that needs it (pension, term).

    Args:
        chunks: list of chunk_NNN.jsonl paths
        system_prompt: the subagent's static system prompt (built by
            worksheet_builder.build_subagent_prompt)
        max_parallel: target concurrency; min(max_parallel, MAX_PARALLEL_HARD_CAP)
            is the effective cap
        model: 'sonnet' | 'opus'
        output_dir: where chunk_NNN_corrections.csv files land

    Returns:
        list of output CSV paths (one per chunk, in input order)
    """
    raise NotImplementedError("Phase 2 port")
