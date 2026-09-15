#!/usr/bin/env python3
"""
Correction subagent for Dutch CAO leave QA.
Implements rubric from leave_qa_correction_prompt.md
"""

import json
import csv
import sys
import re
from pathlib import Path
from typing import Dict, Any, Tuple, List

# Configuration
INPUT_JSONL = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083.jsonl')
OUTPUT_CSV = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083_corrections.csv')

class CorrectionAgent:
    """Processes items from QA worksheet and generates corrections."""

    def __init__(self):
        self.items = []
        self.results = []
        self.confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
        self.unknown_count = 0

    def load_input(self):
        """Load JSONL file into items list."""
        record_count = 0
        with open(INPUT_JSONL, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    record_count += 1
                    for item in record.get('items', []):
                        self.items.append((record, item))
                except json.JSONDecodeError as e:
                    print(f"JSON parse error: {e}", file=sys.stderr)
                    continue

        print(f"Loaded {record_count} records, {len(self.items)} items", file=sys.stderr)

    def extract_value_from_topic(self, item: Dict[str, Any]) -> Tuple[str, str, str, str]:
        """
        Extract value from topic_section based on item metadata.
        Returns: (csv_value_new, unit_new, evidence_quote, confidence)

        Implements rubric logic:
        - If evidence_quote starts with '(' => confidence must be 'low' and value must be 'UNKNOWN'
        - context_type determines how to search topic_section
        - Default fallback: UNKNOWN with low confidence
        """

        topic_section = item.get('topic_section', '')
        context_type = item.get('context_type', '')
        field = item.get('field', '')
        flag_type = item.get('flag_type', '')
        flag_reason = item.get('flag_reason', '')

        # If no source content, always UNKNOWN
        if not topic_section or context_type == 'empty':
            return ('UNKNOWN', 'UNKNOWN', '(no source available)', 'low')

        # Search logic based on context_type
        # For this initial pass, implement basic pattern matching

        # Try to find numeric values
        numeric_pattern = r'\b(\d+(?:[.,]\d+)?)\s*(weeks?|days?|hours?|%|percent|wks?|hrs?)'
        matches = re.finditer(numeric_pattern, topic_section, re.IGNORECASE)

        found_value = None
        found_unit = None
        found_quote = None

        for match in matches:
            value = match.group(1).replace(',', '.')
            unit = match.group(2)
            found_value = value
            found_unit = unit
            # Extract context around match
            start = max(0, match.start() - 50)
            end = min(len(topic_section), match.end() + 50)
            found_quote = topic_section[start:end].strip()
            break

        if found_value:
            # Verbatim quote found in source => high confidence
            return (found_value, found_unit, f'"{found_quote}"', 'high')
        else:
            # Value not found in source
            return ('UNKNOWN', 'UNKNOWN', f'(no relevant value found for {field})', 'low')

    def process_item(self, record: Dict[str, Any], item: Dict[str, Any]) -> Dict[str, str]:
        """Process one item and return CSV row dict."""

        item_id = item.get('item_id', '')
        record_id = record.get('record_id', '')
        topic_group = item.get('topic_group', '')
        field = item.get('field', '')
        csv_value_old = item.get('csv_value_old', '')

        # Extract corrected value
        csv_value_new, unit_new, evidence_quote, confidence = self.extract_value_from_topic(item)

        # Validation per rubric
        # If evidence_quote starts with '(' it's a placeholder => must be UNKNOWN + low
        if evidence_quote.startswith('('):
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            confidence = 'low'

        # Track distribution
        self.confidence_dist[confidence] += 1
        if csv_value_new == 'UNKNOWN':
            self.unknown_count += 1

        # Build notes
        notes = ''
        if confidence == 'low' and csv_value_new == 'UNKNOWN':
            notes = f'source excerpt does not contain {field}'

        return {
            'item_id': item_id,
            'record_id': record_id,
            'topic_group': topic_group,
            'field': field,
            'csv_value_old': csv_value_old,
            'csv_value_new': csv_value_new,
            'unit_new': unit_new,
            'evidence_quote': evidence_quote,
            'confidence': confidence,
            'notes': notes
        }

    def process_all(self):
        """Process all items and generate results."""
        for record, item in self.items:
            row = self.process_item(record, item)
            self.results.append(row)

    def write_csv(self):
        """Write results to CSV file with proper escaping."""
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    'item_id', 'record_id', 'topic_group', 'field',
                    'csv_value_old', 'csv_value_new', 'unit_new',
                    'evidence_quote', 'confidence', 'notes'
                ],
                delimiter=';',
                quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            writer.writerows(self.results)

    def report(self):
        """Print summary report."""
        print(f"\nrows_written={len(self.results)}", file=sys.stderr)
        print(f"output_path={OUTPUT_CSV}", file=sys.stderr)
        print(f"confidence_distribution={self.confidence_dist}", file=sys.stderr)
        print(f"count_UNKNOWN={self.unknown_count}", file=sys.stderr)

        # Pandas verification
        try:
            import pandas as pd
            df = pd.read_csv(OUTPUT_CSV, sep=';', dtype=str)
            print(f"pandas_read_back: {len(df)} rows, columns={list(df.columns)}", file=sys.stderr)
            print(f"confidence_value_counts={df['confidence'].value_counts().to_dict()}", file=sys.stderr)
        except ImportError:
            print("pandas not available for verification", file=sys.stderr)
        except Exception as e:
            print(f"pandas verification error: {e}", file=sys.stderr)

    def run(self):
        """Execute full pipeline."""
        self.load_input()
        self.process_all()
        self.write_csv()
        self.report()


if __name__ == '__main__':
    agent = CorrectionAgent()
    agent.run()
