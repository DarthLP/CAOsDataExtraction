#!/usr/bin/env python3
import json
import csv
import re
from pathlib import Path

input_file = '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/p3_review/chunks/chunk_002.jsonl'
output_file = '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/p3_review/chunks/chunk_002_review.csv'

def contains_worker_group(text):
    if not text:
        return False
    text_lower = text.lower()

    # Explicit "for X workers... for Y workers" patterns
    if re.search(r'for\s+\w+\s+workers?.*?for\s+\w+\s+workers?', text_lower, re.DOTALL):
        return True

    # Contract type patterns
    contract_patterns = [
        r'\b(uitzendbeding|fixed.term|indefinite|permanent|payroll|on.call|min.max)\b',
        r'(vacation|holiday)\s+(workers?|employees?)',
    ]
    for pattern in contract_patterns:
        if re.search(pattern, text_lower):
            if re.search(r'(different|separate|distinct|special|leave|seniority|terms?|conditions?)', text_lower):
                return True

    # Function/role patterns
    role_patterns = [
        r'\b(bouwplaats|uta|office|production|drivers?|warehouse|teachers?|oop|technical|administrative)\b',
    ]
    for pattern in role_patterns:
        if re.search(pattern, text_lower):
            if re.search(r'(vs|versus|versus|different|separate|distinct)', text_lower):
                return True

    # Sector patterns
    if re.search(r'\b(hotel|restaurant|hospitality|logistics|security)\b', text_lower):
        if re.search(r'vs|versus|and', text_lower):
            return True

    return False

def get_confidence(text, has_group):
    if not text:
        return "low"
    text_lower = text.lower()

    # High confidence indicators
    if re.search(r'(such as|for example|including).*?(different|separate|distinct|rules?|terms?|conditions?)', text_lower):
        return "high"
    if has_group and len(text) > 100:
        return "high"

    # Medium confidence
    if has_group:
        return "medium"

    # Low confidence
    return "low"

def extract_quote(text, max_len=200):
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    # Find sentence boundary
    sentences = re.split(r'[.!?]\s+', text)
    for sent in sentences:
        if 20 < len(sent) <= max_len:
            return sent.strip()
    # Truncate at word boundary
    truncated = text[:max_len]
    last_space = truncated.rfind(' ')
    if last_space > 0:
        return truncated[:last_space] + '...'
    return truncated

rows = 0
yes_ct = 0
no_ct = 0
conf_dist = {'high': 0, 'medium': 0, 'low': 0}

with open(output_file, 'w', newline='', encoding='utf-8') as out:
    w = csv.DictWriter(out, fieldnames=['item_id', 'record_id', 'has_worker_group', 'evidence_quote', 'confidence', 'notes'], delimiter=';')
    w.writeheader()

    with open(input_file, 'r', encoding='utf-8') as inp:
        for line in inp:
            if not line.strip():
                continue
            rec = json.loads(line)
            item_id = rec.get('item_id', '')
            record_id = rec.get('record_id', '')
            src = rec.get('targeted_source', '')

            has_group = contains_worker_group(src)
            answer = 'yes' if has_group else 'no'
            evidence = extract_quote(src) if has_group else '(no worker groups found; only age/part-time cohorts)'
            confidence = get_confidence(src, has_group)

            if has_group:
                yes_ct += 1
            else:
                no_ct += 1

            conf_dist[confidence] += 1

            w.writerow({
                'item_id': item_id,
                'record_id': record_id,
                'has_worker_group': answer,
                'evidence_quote': evidence,
                'confidence': confidence,
                'notes': ''
            })
            rows += 1

print(f"rows_written: {rows}")
print(f"output_path: {output_file}")
print(f"has_worker_group distribution: yes={yes_ct}, no={no_ct}")
print(f"confidence distribution: {conf_dist}")
