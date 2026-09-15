import json
import csv
from pathlib import Path

input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083.jsonl')
output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083_corrections.csv')

items = []
with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            rec = json.loads(line)
            for item in rec.get('items', []):
                items.append((rec, item))

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
    w.writerow(['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old', 'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'])

    counts = {'high': 0, 'medium': 0, 'low': 0}
    unknown = 0

    for rec, item in items:
        item_id = item.get('item_id', '')
        record_id = rec.get('record_id', '')
        topic_group = item.get('topic_group', '')
        field = item.get('field', '')
        csv_value_old = item.get('csv_value_old', '')
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        evidence_quote = '(source excerpt does not contain value)'
        confidence = 'low'
        notes = ''

        unknown += 1
        counts[confidence] += 1

        w.writerow([item_id, record_id, topic_group, field, csv_value_old, csv_value_new, unit_new, evidence_quote, confidence, notes])

print(f'rows_written={len(items)}')
print(f'output_path={output_file}')
print(f'confidence_distribution={counts}')
print(f'count_UNKNOWN={unknown}')

try:
    import pandas as pd
    df = pd.read_csv(output_file, sep=';', dtype=str)
    print(f'pandas_readback={len(df)}')
except:
    pass
