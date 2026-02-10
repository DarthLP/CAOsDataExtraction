#!/usr/bin/env python3
"""
Extraction Validation Script

Validates salary and/or non-salary CAO extraction outputs against the source parsed markdown
documents. Samples one file per CAO number (at random) and scores hallucination, completeness,
accuracy, and temporal validity (salary only). Uses Gemini 2.5 Pro for LLM-based validation.

USAGE:
    python scripts/validation/validate_extraction.py --type salary --seed 42
    python scripts/validation/validate_extraction.py --type non_salary --seed 42
    python scripts/validation/validate_extraction.py --type both --seed 42 --max_files 10

ARGUMENTS:
    --type: salary | non_salary | both
    --seed: Random seed for reproducible sampling (default: 42)
    --max_files: Max number of CAOs to validate (optional)
    --cao_numbers: Comma-separated CAO numbers to limit scope (optional)
    --key_number: API key number (default: 1)
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import yaml
from dotenv import load_dotenv
from google import genai
from google.genai import types

from scripts.validation.validation_prompts import (
    build_non_salary_schema_text,
    build_salary_schema_text,
    build_validation_prompt,
    detect_salary_schema_variant,
)
try:
    from scripts_pipeline_helper.p4.retry_logic import handle_llm_errors
except ImportError:
    handle_llm_errors = None


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).resolve().parents[2] / "conf" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def setup_environment(key_number: int = 1) -> Tuple[str, int]:
    """Setup API key from environment."""
    load_dotenv()
    api_key = os.getenv(f"GOOGLE_API_KEY{key_number}")
    if not api_key:
        api_key = os.getenv("GOOGLE_API_KEY1")
        if not api_key:
            raise ValueError(
                f"Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 found. "
                "Set at least GOOGLE_API_KEY1 in .env"
            )
        key_number = 1
    return api_key, key_number


def get_safety_settings():
    """Return safety settings for Gemini API (all BLOCK_NONE)."""
    return [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
    ]


def discover_cao_numbers(
    llm_analysis_base: Path,
    extraction_type: str,
    config: dict,
) -> List[str]:
    """
    Discover CAO numbers that have complete extraction outputs.

    Args:
        llm_analysis_base: Base path to llm_analysis folder
        extraction_type: "salary", "non_salary", or "both"
        config: Config dict for paths

    Returns:
        Sorted list of CAO number strings
    """
    cao_set = None
    if extraction_type in ("salary", "both"):
        salary_dir = llm_analysis_base / "salary"
        if salary_dir.exists():
            cao_set = set(
                d.name for d in salary_dir.iterdir()
                if d.is_dir() and d.name.isdigit() and list(d.glob("*_analysis.json"))
            )
        else:
            cao_set = set()
        if extraction_type == "salary":
            return sorted(cao_set, key=int)

    if extraction_type in ("non_salary", "both"):
        part1_dir = llm_analysis_base / "non_salary" / "gen_bon_wag_pen_ter"
        if part1_dir.exists():
            non_salary_caos = set(
                d.name for d in part1_dir.iterdir()
                if d.is_dir() and d.name.isdigit() and list(d.glob("*_analysis.json"))
            )
        else:
            non_salary_caos = set()
        if extraction_type == "non_salary":
            return sorted(non_salary_caos, key=int)
        cao_set = cao_set & non_salary_caos if cao_set is not None else non_salary_caos

    return sorted(cao_set or [], key=int)


def get_files_for_cao(
    cao_number: str,
    llm_analysis_base: Path,
    extraction_type: str,
) -> List[str]:
    """
    Get list of base filenames (without _analysis.json) for a CAO that have complete outputs.

    Args:
        cao_number: CAO number
        llm_analysis_base: Base path to llm_analysis
        extraction_type: "salary", "non_salary", or "both"

    Returns:
        List of base filenames
    """
    def has_non_salary(base: str) -> bool:
        parts = ["gen_bon_wag_pen_ter", "lea_ove_tra", "hom_con_saf_chi_ai_fri"]
        return all(
            (llm_analysis_base / "non_salary" / p / cao_number / f"{base}_analysis.json").exists()
            for p in parts
        )

    def has_salary(base: str) -> bool:
        return (llm_analysis_base / "salary" / cao_number / f"{base}_analysis.json").exists()

    if extraction_type == "salary":
        salary_dir = llm_analysis_base / "salary" / cao_number
        if not salary_dir.exists():
            return []
        return [
            f.stem.replace("_analysis", "")
            for f in salary_dir.glob("*_analysis.json")
        ]

    if extraction_type == "non_salary":
        part1_dir = llm_analysis_base / "non_salary" / "gen_bon_wag_pen_ter" / cao_number
        if not part1_dir.exists():
            return []
        candidates = [f.stem.replace("_analysis", "") for f in part1_dir.glob("*_analysis.json")]
        return [b for b in candidates if has_non_salary(b)]

    # both: files that have BOTH salary and non_salary
    salary_dir = llm_analysis_base / "salary" / cao_number
    if not salary_dir.exists():
        return []
    salary_files = [f.stem.replace("_analysis", "") for f in salary_dir.glob("*_analysis.json")]
    return [b for b in salary_files if has_non_salary(b)]


def load_non_salary_data(
    cao_number: str,
    base_filename: str,
    llm_analysis_base: Path,
) -> Dict[str, Any]:
    """Load and merge non-salary data from three part folders."""
    parts = ["gen_bon_wag_pen_ter", "lea_ove_tra", "hom_con_saf_chi_ai_fri"]
    merged = {}
    for p in parts:
        path = llm_analysis_base / "non_salary" / p / cao_number / f"{base_filename}_analysis.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                merged.update(json.load(f))
    return merged


def get_markdown_path(
    cao_number: str,
    base_filename: str,
    parsed_markdown_base: Path,
) -> Optional[Path]:
    """Get path to markdown file. Tries base_filename.md."""
    md_path = parsed_markdown_base / cao_number / f"{base_filename}.md"
    if md_path.exists():
        return md_path
    return None


def upload_and_wait_for_file(client, markdown_path: Path, max_wait: int = 600) -> Any:
    """Upload markdown file and wait until ACTIVE."""
    uploaded = client.files.upload(file=markdown_path, config={"mime_type": "text/markdown"})
    poll_interval = 2
    waited = 0
    while waited < max_wait:
        try:
            resource = client.files.get(name=uploaded.name)
            if resource.state.name == "ACTIVE":
                return uploaded
            if resource.state.name == "FAILED":
                raise ValueError(f"File processing FAILED: {uploaded.name}")
        except Exception as e:
            if "FAILED" in str(e):
                raise
        time.sleep(poll_interval)
        waited += poll_interval
    raise TimeoutError(f"File not ACTIVE after {max_wait}s")


def run_validation(
    client,
    prompt: str,
    uploaded_file: Any,
    model: str = "gemini-2.5-pro",
    file_size_mb: float = 1.0,
) -> Dict[str, Any]:
    """Call Gemini API for validation and return parsed JSON. Includes retry logic."""
    contents = [prompt, uploaded_file]
    max_attempts = 5
    last_error = None
    for attempt in range(max_attempts):
        try:
            response = client.models.generate_content(
                model=model,
                contents=contents,
                config={
                    "temperature": 0.0,
                    "top_p": 0.1,
                    "top_k": 1,
                    "response_mime_type": "application/json",
                    "safety_settings": get_safety_settings(),
                },
            )
            text = response.text if hasattr(response, "text") and response.text else ""
            if not text.strip():
                raise ValueError("Empty response from model")
            # Remove markdown fences if present
            if text.strip().startswith("```"):
                lines = text.strip().split("\n")
                text = "\n".join(lines[1:-1]).strip()
            return json.loads(text)
        except Exception as e:
            last_error = e
            if handle_llm_errors and attempt < max_attempts - 1:
                should_retry, _, wait_time = handle_llm_errors(
                    e, attempt, max_attempts, file_size_mb, None, None
                )
                if should_retry and wait_time > 0:
                    print(f"    Retry in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            raise last_error
    raise last_error


def validate_single_file(
    client,
    cao_number: str,
    base_filename: str,
    extraction_type: str,
    config: dict,
    project_root: Path,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Validate a single file. Returns (result_dict, error_message).
    """
    paths = config["paths"]
    llm_analysis = project_root / "outputs" / "llm_analysis"
    parsed_md = project_root / paths.get("parsed_pdfs_markdown", "outputs/parsed_pdfs/parsed_pdfs_markdown")

    md_path = get_markdown_path(cao_number, base_filename, parsed_md)
    if not md_path:
        return None, f"Markdown not found: {base_filename}.md"

    if extraction_type == "salary":
        salary_path = llm_analysis / "salary" / cao_number / f"{base_filename}_analysis.json"
        if not salary_path.exists():
            return None, f"Salary file not found"
        with open(salary_path, "r", encoding="utf-8") as f:
            extracted = json.load(f)
        variant = detect_salary_schema_variant(extracted)
        schema_text = build_salary_schema_text(variant)
        prompt = build_validation_prompt(
            "salary",
            extracted,
            base_filename,
            schema_text,
            include_temporal_validity=True,
        )
    else:
        extracted = load_non_salary_data(cao_number, base_filename, llm_analysis)
        if not extracted:
            return None, "No non-salary data"
        schema_text = build_non_salary_schema_text()
        prompt = build_validation_prompt(
            "non_salary",
            extracted,
            base_filename,
            schema_text,
            include_temporal_validity=False,
        )

    file_size_mb = md_path.stat().st_size / (1024 * 1024)
    max_wait = 300 if file_size_mb <= 5 else 600 if file_size_mb <= 10 else 900

    uploaded = upload_and_wait_for_file(client, md_path, max_wait=max_wait)
    try:
        result = run_validation(client, prompt, uploaded, file_size_mb=file_size_mb)
        return result, None
    finally:
        try:
            client.files.delete(name=uploaded.name)
        except Exception:
            pass


def flatten_validation_for_csv(result: Dict, cao_number: str, filename: str) -> Dict[str, Any]:
    """Flatten validation result for CSV row."""
    row = {
        "cao_number": cao_number,
        "filename": filename,
        "hallucination_score": result.get("hallucination", {}).get("score"),
        "completeness_score": result.get("completeness", {}).get("score"),
        "accuracy_score": result.get("accuracy", {}).get("score"),
        "temporal_validity_score": result.get("temporal_validity", {}).get("score") if result.get("temporal_validity") else None,
        "overall_pass": result.get("overall", {}).get("pass"),
        "overall_rationale": result.get("overall", {}).get("rationale", ""),
    }
    h_issues = result.get("hallucination", {}).get("issues", [])
    c_issues = result.get("completeness", {}).get("issues", [])
    row["hallucination_count"] = len(h_issues)
    row["completeness_count"] = len(c_issues)
    row["hallucination_issues"] = json.dumps(h_issues, ensure_ascii=False) if h_issues else ""
    row["completeness_issues"] = json.dumps(c_issues, ensure_ascii=False) if c_issues else ""
    var_stats = result.get("variable_stats") or []
    row["variable_stats"] = json.dumps(var_stats, ensure_ascii=False) if var_stats else ""
    return row


def main():
    parser = argparse.ArgumentParser(description="Validate CAO extraction outputs")
    parser.add_argument("--type", choices=["salary", "non_salary", "both"], default="both")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max_files", type=int, default=None)
    parser.add_argument("--cao_numbers", type=str, default=None, help="Comma-separated CAO numbers")
    parser.add_argument("--key_number", type=int, default=1)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    os.chdir(project_root)

    config = load_config()
    paths = config["paths"]
    llm_analysis_base = project_root / "outputs" / "llm_analysis"
    output_validation = project_root / paths.get("outputs_validation", "outputs/validation")
    output_validation.mkdir(parents=True, exist_ok=True)

    api_key, _ = setup_environment(args.key_number)
    client = genai.Client(api_key=api_key)

    # Resolve extraction types to run
    types_to_run = ["salary", "non_salary"] if args.type == "both" else [args.type]

    cao_filter = None
    if args.cao_numbers:
        cao_filter = set(s.strip() for s in args.cao_numbers.split(",") if s.strip())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_results = {}
    csv_rows = []

    cao_numbers = discover_cao_numbers(llm_analysis_base, args.type, config)
    if cao_filter:
        cao_numbers = [c for c in cao_numbers if c in cao_filter]
    if args.max_files:
        cao_numbers = cao_numbers[: args.max_files]

    if not cao_numbers:
        print("No CAOs found")
        return

    random.seed(args.seed)
    for i, cao_number in enumerate(cao_numbers):
        files = get_files_for_cao(cao_number, llm_analysis_base, args.type)
        if not files:
            continue
        base_filename = random.choice(files)
        print(f"\n[{i+1}/{len(cao_numbers)}] CAO {cao_number}: {base_filename}")

        for extraction_type in types_to_run:
            print(f"  Validating {extraction_type}...")
            result, err = validate_single_file(
                client,
                cao_number,
                base_filename,
                extraction_type,
                config,
                project_root,
            )
            if err:
                print(f"    ERROR: {err}")
                continue
            if extraction_type not in all_results:
                all_results[extraction_type] = []
            all_results[extraction_type].append(
                {"cao_number": cao_number, "filename": base_filename, "result": result}
            )
            row = flatten_validation_for_csv(result, cao_number, base_filename)
            row["extraction_type"] = extraction_type
            csv_rows.append(row)

            # 3 min delay between salary and non_salary for same CAO
            if args.type == "both" and extraction_type == "salary":
                print("  Waiting 3 min before non_salary validation...")
                time.sleep(180)

        # Delay between CAOs
        if i < len(cao_numbers) - 1:
            time.sleep(45)

    for extraction_type in types_to_run:
        results = all_results.get(extraction_type, [])
        report_path = output_validation / f"{extraction_type}_validation_{timestamp}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Saved {extraction_type} report: {report_path}")

    # Save combined CSV
    if csv_rows:
        csv_path = output_validation / f"validation_summary_{timestamp}.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"Saved CSV: {csv_path}")

    # Console summary
    total = sum(len(r) for r in all_results.values())
    passed = sum(
        1 for r in csv_rows if r.get("overall_pass") is True
    )
    failed_caos = [
        r["cao_number"] for r in csv_rows
        if r.get("overall_pass") is False
    ]
    print()
    print("=" * 60)
    print(f"Validation complete: {passed}/{total} passed")
    if failed_caos:
        print(f"Failed CAOs: {', '.join(failed_caos)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
