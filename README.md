# CAOsDataExtraction

Run:  - 'conda activate caos-extract' to activate environment. A
      - 'caffeinate python' + filename to prevent computer from sleeping
        - e.g.: unbuffer caffeinate python pipelines/p0_webscraping.py 2>&1 | tee log.txt

This project uses AI to extract structured data from Dutch collective labour agreements (CAOs).

## 🔄 Workflow Overview

### 2. 🧪 Comparison & Evaluation
- Compare AI output to 20 ground truth results from previous RA.
- Build validation pipeline: flag sparse/malformed rows, optionally score confidence.

### 3. 🧼 Code Cleanup & Refactoring
- Modularize logic (prompt building, model querying, postprocessing).
- Improve error handling and logging.

### 4. 📋 Track Extraction Quality
- Track % of filled fields, errors, suspicious patterns.
- Save comparison results and quality metrics in a summary CSV.

---
CAO Source: [uitvoeringarbeidsvoorwaardenwetgeving.nl](https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66)


