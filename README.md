# CAOsDataExtraction

This project uses AI to extract structured data from Dutch collective labour agreements (CAOs).

## 🔄 Workflow Overview

### 1. 📊 Add Examples to Input Excel
Add example values directly into the description cells (using `Ex: ...` format) to guide the model.
CHECK

### 2. 🧠 Update Prompt
Prompt includes:
- Field descriptions from Excel
- Clarifying sentence: "Each field includes a short description. Example values are provided using the prefix ‘Ex: …’ to illustrate expected content."

### 3. 🔁 Two-Stage LLM Extraction
- **Stage 1:** Extract loosely structured relevant context (full tables, related text).
- **Stage 2:** Map this to structured JSON matching the Excel format.

### 4. 🧪 Comparison & Evaluation
- Compare 1-stage vs. 2-stage extraction.
- Compare AI output to 20 ground truth results from previous RA.
- Add retry logic for failures.
- Build validation pipeline: flag sparse/malformed rows, optionally score confidence.

### 5. 🧼 Code Cleanup & Refactoring
- Modularize logic (prompt building, model querying, postprocessing).
- Improve error handling and logging.

### 6. 📋 Track Extraction Quality
- Track % of filled fields, errors, suspicious patterns.
- Save comparison results and quality metrics in a summary CSV.

### 7. 🌐 Web Scraping Pipeline (Upcoming)
- Crawl CAO portals
- Download PDFs and metadata
- Feed files into existing extraction workflow

---
CAO Source: [uitvoeringarbeidsvoorwaardenwetgeving.nl](https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66)
