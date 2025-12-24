# Quota Resume System - Usage Guide

## Overview

The quota resume system automatically saves your processing state when API quota is exhausted and resumes from where you left off after the quota resets. This works for both `p3_llmExtraction.py` and `p4_analysis.py` pipelines.

## How It Works

1. **When quota is exhausted**: The system saves:
   - Which file was being processed
   - Retry attempt number (for p3 only)
   - Statistics (successful/failed files)
   - Batch reset time

2. **Waiting**: The process waits until the batch-specific reset time (distributed throughout the day)

3. **Resuming**: After reset time, the process automatically:
   - Loads the saved state
   - Skips already-processed files
   - Resumes from the exact file and attempt where it stopped

## Running the Scripts

### P3 (LLM Extraction)

**Single process:**
```bash
python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 1
```

**Multiple processes (22 parallel processes):**
```bash
# Process 1
python -u pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 22 > logs/p3_log1.txt 2>&1 &

# Process 2
python -u pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 22 > logs/p3_log2.txt 2>&1 &

# ... continue for all 22 processes
python -u pipelines/p3_llmExtraction.py --key_number 22 --process_id 21 --total_processes 22 > logs/p3_log22.txt 2>&1 &
```

**With resume disabled:**
```bash
python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 22 --resume_on_quota false
```

### P4 (Analysis)

**Single process:**
```bash
python pipelines/p4_analysis.py --key_number 1 --process_id 0 --total_processes 1
```

**Multiple processes (22 parallel processes):**
```bash
# Process 1
python -u pipelines/p4_analysis.py --key_number 1 --process_id 0 --total_processes 22 > logs/p4_log1.txt 2>&1 &

# Process 2
python -u pipelines/p4_analysis.py --key_number 2 --process_id 1 --total_processes 22 > logs/p4_log2.txt 2>&1 &

# ... continue for all 22 processes
python -u pipelines/p4_analysis.py --key_number 22 --process_id 21 --total_processes 22 > logs/p4_log22.txt 2>&1 &
```

**With resume disabled:**
```bash
python pipelines/p4_analysis.py --key_number 1 --process_id 0 --total_processes 22 --resume_on_quota false
```

## Configuration

### Default Behavior

By default, resume is **enabled** (`resume_on_quota: true`).

### Config File

You can configure it in `conf/config.yaml`:
```yaml
resume_on_quota: true  # or false to disable
```

### Command-Line Override

The command-line argument overrides the config file:
```bash
--resume_on_quota true   # Enable (even if config says false)
--resume_on_quota false  # Disable (even if config says true)
```

## Batch Configuration

The 22 API keys are organized into 11 batches with staggered reset times:

- **Batch 1** (keys 1-2): Resets at 1:01 AM PT
- **Batch 2** (keys 3-4): Resets at 2:31 AM PT
- **Batch 3** (keys 5-6): Resets at 4:01 AM PT
- **Batch 4** (keys 7-8): Resets at 5:31 AM PT
- **Batch 5** (keys 9-10): Resets at 7:01 AM PT
- **Batch 6** (keys 11-12): Resets at 8:31 AM PT
- **Batch 7** (keys 13-14): Resets at 10:01 AM PT
- **Batch 8** (keys 15-16): Resets at 11:31 AM PT
- **Batch 9** (keys 17-18): Resets at 1:01 PM PT
- **Batch 10** (keys 19-20): Resets at 2:31 PM PT
- **Batch 11** (keys 21-22): Resets at 4:01 PM PT

## Resume State Files

Resume state is saved per API key in:
```
logs/resume_state_key{N}.json
```

Each file contains:
- Pipeline type (p3 or p4)
- Current file being processed
- Retry attempt (p3 only)
- Statistics
- Reset time

**Note**: These files are automatically created when quota is exhausted and cleared when processing completes.

## Batch Summary Log

Aggregated information is stored in:
```
logs/batch_summary.json
```

This file tracks all API keys' status and can be viewed to see the overall progress.

## What Happens When Quota is Hit

1. **Detection**: The system detects daily quota exhaustion (429 RESOURCE_EXHAUSTED)

2. **State Saving**: 
   - Saves current processing state to `logs/resume_state_key{N}.json`
   - Updates `logs/batch_summary.json`
   - Prints quota exhaustion message

3. **Waiting**:
   - Calculates batch-specific reset time
   - Waits until that time (shows progress every hour)
   - Process continues running (don't kill it!)

4. **Resuming**:
   - After reset time, automatically continues
   - Loads saved state
   - Skips to the saved file
   - Resumes from saved attempt (p3) or attempt 0 (p4)

## Important Notes

1. **Don't kill the process**: When quota is exhausted and it's waiting, let it run. It will automatically resume.

2. **Per-key isolation**: Each API key has its own resume state. If key 1 hits quota, keys 2-22 continue running.

3. **File distribution**: The file distribution algorithm (`i % total_processes == process_id`) is preserved. Each key still processes its assigned files.

4. **P3 vs P4**: 
   - **P3**: Resumes from exact retry attempt (e.g., attempt 5 of 8)
   - **P4**: Always resumes from attempt 0, but existing logic skips already-completed extraction parts

5. **Multiple quota hits**: If quota is hit again after resuming, it saves the new state and waits for the next reset cycle.

## Checking Status

To see which keys have resume states:
```bash
ls -la logs/resume_state_key*.json
```

To view batch summary:
```bash
cat logs/batch_summary.json | python -m json.tool
```

## Troubleshooting

**Q: Process seems stuck - what's happening?**
A: It's likely waiting for quota reset. Check the logs - it will show "QUOTA EXHAUSTED - Waiting for quota reset" and the reset time.

**Q: Can I manually trigger resume?**
A: No need - it's automatic. Just restart the script and it will detect the resume state and continue.

**Q: What if I want to start fresh?**
A: Delete the resume state file: `rm logs/resume_state_key{N}.json`

**Q: Does it work across script restarts?**
A: Yes! If you restart the script, it will automatically load the resume state and continue from where it left off.

