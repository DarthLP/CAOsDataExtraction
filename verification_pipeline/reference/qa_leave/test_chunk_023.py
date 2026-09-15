#!/usr/bin/env python3
"""Quick test to read and analyze chunk_023.jsonl structure"""
import json

chunk_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_023.jsonl"

# Read and analyze
with open(chunk_path, 'r') as f:
    for i, line in enumerate(f):
        if i < 3:  # Just look at first 3 records
            try:
                rec = json.loads(line)
                print(f"\n=== Record {i+1} ===")
                print(f"record_id: {rec.get('record_id')}")
                items = rec.get('items', [])
                print(f"Number of items: {len(items)}")
                if items:
                    first_item = items[0]
                    print(f"First item keys: {list(first_item.keys())}")
                    print(f"  item_id: {first_item.get('item_id')}")
                    print(f"  flag_type: {first_item.get('flag_type')}")
                    print(f"  context_type: {first_item.get('context_type')}")
                    print(f"  topic_section length: {len(first_item.get('topic_section', ''))}")
            except Exception as e:
                print(f"Error reading record {i+1}: {e}")
        else:
            break

print("\nDone!")
