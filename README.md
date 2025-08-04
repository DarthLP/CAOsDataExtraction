# CAOsDataExtraction

Run:  - 'conda activate caos-extract' to activate environment. A
      - 'caffeinate python' + filename to prevent computer from sleeping

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



Problems: 
📄 Processing 10/35: CAO 1045
  No PDFs found for CAO 1045

📄 Processing 32/35: CAO 623
  No PDFs found for CAO 623

📄 Processing 24/35: CAO 2693
  No PDFs found for CAO 2693

